use async_trait::async_trait;
use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use bytes::{BufMut, Bytes, BytesMut};
use futures::future::Either;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::io;
use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{
    tcp::{ReadHalf, WriteHalf},
    TcpListener, TcpStream,
};
use tokio::sync::Mutex;

#[derive(Debug)]
enum PgMsg<'a> {
    Msg(char),
    Query(&'a str),
}

struct PgParser {}

impl PgParser {
    fn new() -> PgParser {
        PgParser {}
    }

    fn read<'a>(
        &self,
        buffer: &'a [u8],
    ) -> Result<(Option<PgMsg<'a>>, usize), Box<dyn std::error::Error + std::marker::Send>> {
        // If we don't have enough data to capture the type and size of
        // msg then bail.
        if buffer.len() < 5 {
            return Ok((None, 0));
        }

        // Capture command type and reset state.
        let tag = buffer[0] as char;
        let msg_size = BigEndian::read_i32(&buffer[1..5]) as usize;

        // No partial message support for now.
        if buffer.len() < msg_size {
            // HACK: I don't want to worry about copying the query buffer,
            // so I'm going to require the full query message.
            println!("PgParser: buffer len < msg_size");
            return Ok((None, 0));
        }

        // We know the command is always present at this point.
        match tag {
            'Q' => {
                let query = std::str::from_utf8(&buffer[5..msg_size]).unwrap();
                Ok((Some(PgMsg::Query(query)), msg_size + 1))
            }
            'X' => {
                // These tags we just want to skip over and ack that they were parsed.
                Ok((Some(PgMsg::Msg(tag)), msg_size + 1))
            }
            _ => {
                panic!("Uknown tag received: {}", tag);
            }
        }
    }
}

struct PgConn {
    conn: TcpStream,
    parser: PgParser,
    buffer: BytesMut,
    buffer_index: usize,
    buffer_size: usize,
}

impl PgConn {
    fn new(conn: TcpStream) -> PgConn {
        let mut buffer = BytesMut::with_capacity(8096);
        buffer.resize(8096, 0);
        let parser = PgParser::new();
        PgConn {
            conn,
            buffer,
            parser,
            buffer_index: 0,
            buffer_size: 0,
        }
    }

    async fn next<'a>(
        &'a mut self,
    ) -> Result<(&'a [u8], PgMsg<'a>), Box<dyn std::error::Error + std::marker::Send>> {
        // If there is no (or no more) buffer, read and set state.
        if self.buffer_index == 0 || self.buffer_index == self.buffer_size {
            println!("PgConn is going to read!");
            self.buffer_index = 0;
            self.buffer_size = self.conn.read(&mut self.buffer).await.unwrap();
            println!("PgConn read {} bytes!", self.buffer_size);
        }

        let bytes = &self.buffer[self.buffer_index..self.buffer_size];

        println!("PgConn bytes from read: {:?}", bytes);
        match self.parser.read(bytes)? {
            (Some(msg), bytes_read) => {
                self.buffer_index = bytes_read;
                Ok((bytes, msg))
            }
            x => panic!("Something new in the pg_conn next: {:?}", x),
        }
    }

    async fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.conn.write(&buffer).await
    }
}

struct PgConnManager {
    startup_message: StartupMessage,
}

impl PgConnManager {
    fn new(startup_message: StartupMessage) -> PgConnManager {
        PgConnManager { startup_message }
    }
}

#[async_trait]
impl bb8::ManageConnection for PgConnManager {
    type Connection = TcpStream;
    type Error = io::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        // We have a Query so we should connect to the server.
        println!("SRV Opening a new connection for query!");
        let mut pg_conn =
            TcpStream::connect("127.0.0.1:5432".parse::<SocketAddr>().unwrap()).await?;
        handle_server_startup(&mut pg_conn, &self.startup_message).await?;
        Ok(pg_conn)
    }

    async fn is_valid(&self, conn: Self::Connection) -> Result<Self::Connection, Self::Error> {
        Ok(conn)
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

#[derive(Clone)]
struct PgPooler {
    pools: Arc<Mutex<BTreeMap<String, bb8::Pool<PgConnManager>>>>,
}

impl PgPooler {
    fn new() -> PgPooler {
        PgPooler {
            pools: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    async fn get_pool(
        &mut self,
        startup_message: StartupMessage,
    ) -> io::Result<bb8::Pool<PgConnManager>> {
        // TODO: We assume the DB is always set.
        let database = startup_message.database.clone().unwrap();

        // Get lock around "pools", get or insert new pool, and clone.
        let mut pools = self.pools.lock().await;
        let pool = match pools.entry(database) {
            Entry::Occupied(pool) => pool.into_mut(),
            Entry::Vacant(pools) => {
                // TODO: Better to unlock here while connecting? Probably? Nested locking per
                // database?
                let manager = PgConnManager::new(startup_message);
                let pool = bb8::Pool::builder().max_size(50).build(manager).await?;
                pools.insert(pool)
            }
        }
        .clone();

        Ok(pool)
    }
}

async fn handle_server_startup(
    conn: &mut TcpStream,
    startup_message: &StartupMessage,
) -> io::Result<()> {
    startup_message.write(conn).await?;

    // Expect Authentication OK
    let mut buffer = [0; 1024];
    let n = conn.read(&mut buffer).await?;
    println!("Srv read: {}", n);
    let tag = buffer[0] as char;
    match buffer[0] as char {
        'R' => match BigEndian::read_i32(&buffer[5..9]) {
            0 => {}
            bad_auth => panic!("Expected Auth OK, found: {}", bad_auth),
        },
        _ => panic!(
            "Invalid tag after server startup (expected Auth OK): {}",
            tag
        ),
    };

    // Expect params, backend params, and finally a query OK
    let mut buffer = &buffer[9..];
    loop {
        let tag = buffer[0] as char;
        match tag {
            'K' | 'S' => {
                let size = BigEndian::read_i32(&buffer[1..5]) as usize + 1;
                buffer = &buffer[size..];
            }
            'Z' => break,
            _ => panic!("Invalid tag after server startup: {}", tag),
        };
    }
    println!("Srv conn Auth OK");
    Ok(())
}

#[derive(Debug, Default, Clone)]
struct StartupMessage {
    user: Option<String>,
    database: Option<String>,
    application_name: Option<String>,
    client_encoding: Option<String>,
}

impl StartupMessage {
    async fn write(&self, conn: &mut TcpStream) -> io::Result<usize> {
        let mut msg = [0; 1024];
        BigEndian::write_i32(&mut msg[4..8], 196608);
        write!(&mut msg[8..], "user")?;
        msg[12] = 0;

        let mut idx = 13;
        let user = self.user.clone().unwrap();
        write!(&mut msg[idx..], "{}", &user)?;
        idx += user.len();
        msg[idx] = 0;
        idx += 1;
        write!(&mut msg[idx..], "database")?;
        idx += 8;
        msg[idx] = 0;
        idx += 1;
        let database = self.database.clone().unwrap();
        write!(&mut msg[idx..], "{}", &database)?;
        idx += database.len();
        msg[idx] = 0;
        idx += 1;
        msg[idx] = 0;
        idx += 1;

        // Write size
        BigEndian::write_i32(&mut msg[0..4], idx as i32);
        println!("Srv startup for db: {:?}: {:?}", &database, &msg[..idx]);
        conn.write(&mut msg[..idx]).await
    }
}

fn read_cstr(start: usize, buffer: &mut BytesMut) -> Option<(usize, &[u8])> {
    match memchr::memchr(0, &buffer[start..]) {
        Some(pos) => {
            let end = start + pos;
            let cstr = &buffer[start..end];
            let size = end - start;
            Some((size, cstr))
        }
        None => None,
    }
}

fn print_startup_message(
    size: i32,
    mut buffer: &mut BytesMut,
) -> Result<StartupMessage, Box<dyn std::error::Error>> {
    // idx starts at 8 because first 8 bytes are already read.
    let mut offset = 8;
    let mut sm = StartupMessage::default();
    let mut value_for_header: Option<&str> = None;
    while offset < (size - 1) as usize {
        let (n, cstr) = read_cstr(offset, &mut buffer).unwrap();
        offset += n + 1;
        let cstr = std::str::from_utf8(&cstr)?;

        let skip_header_check = value_for_header.is_none();

        match value_for_header.take() {
            Some("user") => sm.user = Some(cstr.into()),
            Some("database") => sm.database = Some(cstr.into()),
            Some("application_name") => sm.application_name = Some(cstr.into()),
            Some("client_encoding") => sm.client_encoding = Some(cstr.into()),
            Some(_) | None => {}
        };

        if skip_header_check {
            match cstr {
                "user" => value_for_header = Some("user"),
                "database" => value_for_header = Some("database"),
                "application_name" => value_for_header = Some("application_name"),
                "client_encoding" => value_for_header = Some("client_encoding"),
                _ => panic!("New cstr startup header: {}", cstr),
            };
        }
    }
    Ok(sm)
}

// Objective 1: Done. Allow psql to connect to this server.
// Objective 2: Proxy frontend commands to postgres server.
// Objective 3: Connect to postgres server.
// Objective 4: Proxy backend commands to psql.
// Objective 5: Basic Pool!
// Objective 6: Complex query args.
#[tokio::main]
async fn main() -> io::Result<()> {
    let bind_addr = "127.0.0.1:7432".parse::<SocketAddr>().unwrap();
    println!("Listening on: {:?}", bind_addr);
    let mut listener = TcpListener::bind(bind_addr).await?;

    let pg_pooler = PgPooler::new();

    loop {
        let (mut client, _) = listener.accept().await?;
        let client_info = format!("{:?}", client);
        println!("Client connected: {:?}", client_info);
        tokio::spawn({
            let mut pg_pooler = pg_pooler.clone();

            async move {
                // Allocate buffer for the client and server
                let mut buffer = BytesMut::with_capacity(8096);
                buffer.resize(8096, 0);
                let mut pg_buffer = BytesMut::with_capacity(8096);
                pg_buffer.resize(8096, 0);

                // Startup / SSL Request
                let n = client.read(&mut buffer).await.unwrap();
                println!("Read: {:?}", n);
                let size = BigEndian::read_i32(&buffer[0..4]);

                let version = BigEndian::read_i32(&buffer[4..8]);
                let startup_message = if version == 80877103 {
                    // Reject SSL.
                    println!("SSL Request!");
                    let n = client.write(&[b'N']).await.unwrap();
                    println!("Wrote: {:?}", n);

                    let n = client.read(&mut buffer[..]).await.unwrap();
                    println!("Read: {:?}", n);
                    let size = BigEndian::read_i32(&buffer[0..4]);
                    let version = BigEndian::read_i32(&buffer[4..8]);
                    println!("{:?} / version: {}", &buffer[..n], version);
                    print_startup_message(size, &mut buffer).unwrap()
                } else {
                    println!("{:?} / startup version: {}", size, version);
                    print_startup_message(size, &mut buffer).unwrap()
                };
                println!("Client Startup Msg: {:?}", startup_message);

                // Authentication OK
                let mut msg = [0; 9];
                msg[0] = b'R';
                BigEndian::write_i32(&mut msg[1..5], 8);
                BigEndian::write_i32(&mut msg[5..9], 0);
                println!("{:?}", &msg[..]);
                let n = client.write(&msg[..]).await.unwrap();
                println!("Wrote: {:?}", n);

                // Ready For Query
                let mut msg = [0; 6];
                msg[0] = b'Z';
                BigEndian::write_i32(&mut msg[1..5], 5);
                msg[5] = b'I';
                let n = client.write(&msg[..]).await.unwrap();
                println!("Wrote: {:?}", n);

                let mut conn = PgConn::new(client);
                loop {
                    println!("Waiting for a query from the client");

                    // Wait for Query
                    let buffer = match conn.next().await.unwrap() {
                        (buffer, PgMsg::Query(query)) => {
                            println!("Query: {}", query);
                            buffer
                        }
                        (_, PgMsg::Msg('X')) => {
                            // Client closed the connection.
                            println!("Terminate received!");
                            break;
                        }
                        _ => unimplemented!(),
                    };

                    // Check out a pg_conn
                    let pool = pg_pooler.get_pool(startup_message.clone()).await.unwrap();
                    let mut pg_conn = pool.get().await.unwrap();

                    // Write Query to PG
                    pg_conn.write(&buffer).await.unwrap();

                    'transaction: loop {
                        println!("In the query loop!");

                        // Proxy query until transaction completes.
                        let n = match futures::future::select(
                            Box::pin(conn.next()),
                            //client.read(&mut buffer),
                            pg_conn.read(&mut pg_buffer),
                        )
                        .await
                        {
                            // Handle client
                            Either::Left((Ok((buffer, PgMsg::Query(query))), _)) => {
                                // Query
                                println!("Query: {}", query);
                                pg_conn.write(&buffer).await.unwrap();
                                continue;
                            }
                            Either::Left((Ok((buffer, PgMsg::Msg('X'))), _)) => {
                                // Client closed the connection.
                                println!("Terminate received!");
                                pg_conn.write(&buffer).await.unwrap();
                                break;
                            }
                            Either::Right((Ok(n), _)) => n,
                            x => panic!("Missed a select case!"),
                        };

                        // Server has data to send to client
                        println!("Writing to client: {}", pg_buffer[0] as char);
                        let n = n as usize;
                        println!("SRV read {} bytes", n);
                        conn.write(&pg_buffer[..n]).await.unwrap();

                        let mut idx = 0;
                        while idx < n {
                            let tag = pg_buffer[idx] as char;
                            println!("Server parsing: {}, idx: {}", tag, idx);
                            match tag {
                                'C' | 'D' | 'T' => {
                                    let size = BigEndian::read_i32(&pg_buffer[(idx + 1)..(idx + 5)])
                                        as usize;
                                    println!(
                                        "SRV: Tag: {}, Msg Size: {}, Msg: {:?}",
                                        tag,
                                        size,
                                        &pg_buffer[idx..(idx + size)]
                                    );
                                    idx += size + 1;
                                }
                                'Z' => {
                                    if pg_buffer[idx + 5] == b'I' {
                                        // Transaction completed. Return pg conn to pool.
                                        drop(pg_conn);
                                        println!("SRV: Connection was closed!");
                                        break 'transaction;
                                    }
                                    println!(
                                        "Server said READY FOR QUERY: {}",
                                        pg_buffer[idx + 5] as char
                                    );
                                    idx += 6;
                                }
                                _ => {
                                    println!(
                                        "New tag for server to parse: {} - b{}",
                                        tag, tag as u8
                                    );
                                    break;
                                }
                            }
                        }
                    }
                }

                println!("Client disconnected: {:?}", client_info);
            }
        });
    }
}
