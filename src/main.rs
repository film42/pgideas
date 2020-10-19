use async_trait::async_trait;
use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use bytes::{BufMut, Bytes, BytesMut};
use futures::future::Either;
use std::io;
use std::io::Write;
use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{
    tcp::{ReadHalf, WriteHalf},
    TcpListener, TcpStream,
};

struct PgConnManager {}

#[async_trait]
impl bb8::ManageConnection for PgConnManager {
    type Connection = TcpStream;
    type Error = io::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        // We have a Query so we should connect to the server.
        println!("SRV Opening a new connection for query!");
        let mut pg_conn =
            TcpStream::connect("127.0.0.1:5432".parse::<SocketAddr>().unwrap()).await?;
        handle_server_startup(&mut pg_conn).await?;
        Ok(pg_conn)
    }

    async fn is_valid(&self, conn: Self::Connection) -> Result<Self::Connection, Self::Error> {
        Ok(conn)
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        false
    }
}

async fn handle_server_startup(conn: &mut TcpStream) -> io::Result<()> {
    let mut msg = [0; 1024];
    BigEndian::write_i32(&mut msg[0..4], 23);
    BigEndian::write_i32(&mut msg[4..8], 196608);
    write!(&mut msg[8..], "user");
    msg[12] = 0;
    write!(&mut msg[13..], "postgres");
    msg[21] = 0;
    msg[22] = 0;
    println!("Srv startup: {:?}", &msg[..23]);
    conn.write(&mut msg[..23]).await?;

    // Expect Authentication OK
    let n = conn.read(&mut msg).await?;
    println!("Srv read: {}", n);
    let tag = msg[0] as char;
    match msg[0] as char {
        'R' => match BigEndian::read_i32(&msg[5..9]) {
            0 => {}
            bad_auth => panic!("Expected Auth OK, found: {}", bad_auth),
        },
        _ => panic!(
            "Invalid tag after server startup (expected Auth OK): {}",
            tag
        ),
    };

    // Expect params, backend params, and finally a query OK
    let mut msg = &msg[9..];
    loop {
        let tag = msg[0] as char;
        match tag {
            'K' | 'S' => {
                let msg_size = BigEndian::read_i32(&msg[1..5]) as usize + 1;
                msg = &msg[msg_size..];
            }
            'Z' => break,
            _ => panic!("Invalid tag after server startup: {}", tag),
        };
    }
    println!("Srv conn Auth OK");
    Ok(())
}

#[derive(Debug, Default)]
struct StartupMessage {
    user: Option<String>,
    database: Option<String>,
    application_name: Option<String>,
    client_encoding: Option<String>,
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

    let manager = PgConnManager {};
    let pg_conn_pool = bb8::Pool::builder()
        .max_size(50)
        .build(manager)
        .await
        .unwrap();

    loop {
        let (mut client, _) = listener.accept().await?;
        let client_info = format!("{:?}", client);
        println!("Client connected: {:?}", client_info);
        tokio::spawn({
            let pg_conn_pool = pg_conn_pool.clone();

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

                loop {
                    println!("Waiting for a query from the client");

                    // Wait for Query
                    let n = client.read(&mut buffer).await.unwrap();
                    let tag = buffer[0] as char;
                    match tag {
                        'Q' => {
                            //let tag = BigEndian::read_i32(&buffer[0..4]);
                            println!("Read: {:?}, Tag: {}", n, tag as char);
                            let size = BigEndian::read_i32(&buffer[1..5]) as usize;
                            let query = std::str::from_utf8(&buffer[5..size]).unwrap();
                            println!("Query: {}", query);
                        }
                        'X' => {
                            // Client closed the connection.
                            println!("Terminate received!");
                            break;
                        }
                        _ => panic!("Found new tag: {}", tag),
                    };

                    // Check out a pg_conn
                    let mut pg_conn = pg_conn_pool.get().await.unwrap();

                    // Write Query to PG
                    pg_conn.write(&buffer[..n]).await.unwrap();

                    'transaction: loop {
                        println!("In the query loop!");

                        // Proxy query until transaction completes.
                        match futures::future::select(
                            client.read(&mut buffer),
                            pg_conn.read(&mut pg_buffer),
                        )
                        .await
                        {
                            Either::Left((Ok(n), _)) => {
                                // Handle client
                                // Query
                                pg_conn.write(&buffer[..n]).await.unwrap();
                                println!("Delivered to server!");
                                let tag = buffer[0] as char;
                                match tag {
                                    'Q' => {
                                        //let tag = BigEndian::read_i32(&buffer[0..4]);
                                        println!("Read: {:?}, Tag: {}", n, tag as char);
                                        let size = BigEndian::read_i32(&buffer[1..5]) as usize;
                                        let query = std::str::from_utf8(&buffer[5..size]).unwrap();
                                        println!("Query: {}", query);

                                        //                                    // Empty Query Response
                                        //                                    let mut msg = [0; 5];
                                        //                                    msg[0] = b'I';
                                        //                                    BigEndian::write_i32(&mut msg[1..5], 4);
                                        //                                    let n = client.write(&msg[..]).await.unwrap();
                                        //                                    println!("Wrote: {:?}", n);
                                        //
                                        //                                    // Ready For Query
                                        //                                    let mut msg = [0; 6];
                                        //                                    msg[0] = b'Z';
                                        //                                    BigEndian::write_i32(&mut msg[1..5], 5);
                                        //                                    msg[5] = b'I';
                                        //                                    let n = client.write(&msg[..]).await.unwrap();
                                        //                                    println!("Wrote: {:?}", n);
                                    }
                                    'X' => {
                                        // Client closed the connection.
                                        println!("Terminate received!");
                                        break;
                                    }
                                    _ => {
                                        println!("Found new tag: {}", tag);
                                        //break;
                                    }
                                };
                            }
                            Either::Right((Ok(n), _)) => {
                                // Handle server
                                println!("Writing to client: {}", pg_buffer[0] as char);
                                let n = n as usize;
                                println!("SRV read {} bytes", n);
                                client.write(&pg_buffer[..n]).await.unwrap();

                                let mut idx = 0;
                                while idx < n {
                                    let tag = pg_buffer[idx] as char;
                                    println!("Server parsing: {}, idx: {}", tag, idx);
                                    match tag {
                                        'C' | 'D' | 'T' => {
                                            let size = BigEndian::read_i32(
                                                &pg_buffer[(idx + 1)..(idx + 5)],
                                            )
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
                            x => panic!("Missed a select case: {:?}", x),
                        }
                    }
                }

                println!("Client disconnected: {:?}", client_info);
            }
        });
    }
}
