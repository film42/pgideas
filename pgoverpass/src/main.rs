use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use bytes::{BufMut, Bytes, BytesMut};
use futures::future::Either;

use serde::Deserialize;
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
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::Mutex;

#[derive(Debug, Deserialize)]
struct Config {
    postgres_addr: SocketAddr,
}

#[derive(Debug)]
enum PgMsg<'a> {
    Msg(char),
    Query(&'a str),
}

struct PgParser {}

impl PgParser {
    fn parse<'a>(buffer: &'a [u8]) -> io::Result<(Option<PgMsg<'a>>, usize)> {
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
                let query = std::str::from_utf8(&buffer[5..msg_size])
                    .map_err(|_| io::ErrorKind::InvalidData)?;
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

async fn handle_server_startup<'a>(
    conn: &mut TcpStream,
    startup_message: &StartupMessage,
) -> io::Result<(usize, BytesMut)> {
    startup_message.write(conn).await?;

    let mut buffer = BytesMut::with_capacity(8096);
    buffer.resize(8096, 0);
    // Expect Authentication OK
    let n = conn.read(&mut buffer).await?;
    {
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
                    println!(
                        "buffer: {} / {:?}",
                        tag,
                        std::str::from_utf8(&buffer[..size])
                    );
                    buffer = &buffer[size..];
                }
                'Z' => break,
                _ => panic!("Invalid tag after server startup: {}", tag),
            };
        }
    }
    println!("Srv conn Auth OK");
    Ok((n, buffer))
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
        write!(&mut msg[idx..], "application_name")?;
        idx += 16;
        msg[idx] = 0;
        idx += 1;
        write!(&mut msg[idx..], "pgoverpass")?;
        idx += 8;
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

async fn write_parameter_status(
    client: &mut TcpStream,
    key: &str,
    value: &str,
) -> io::Result<usize> {
    // Parameter Status
    let mut msg = [0; 1024];
    msg[0] = b'S';

    let mut idx = 5;
    write!(&mut msg[idx..], "{}", key)?;
    idx += key.len();
    msg[idx] = 0;
    idx += 1;
    write!(&mut msg[idx..], "{}", value)?;
    idx += value.len();
    msg[idx] = 0;
    idx += 1;

    // Size is 1 less because the tag doesn't count.
    let size = idx - 1;
    BigEndian::write_i32(&mut msg[1..5], size as i32);

    println!("OUT!!! {:?}", &msg[..idx]);

    client.write(&msg[..idx]).await
}

fn parse_startup_message(size: i32, mut buffer: &mut BytesMut) -> io::Result<StartupMessage> {
    // idx starts at 8 because first 8 bytes are already read.
    let mut offset = 8;
    let mut sm = StartupMessage::default();
    let mut value_for_header: Option<&str> = None;
    while offset < (size - 1) as usize {
        let (n, cstr) = read_cstr(offset, &mut buffer).unwrap();
        offset += n + 1;

        // TODO: Wrap this in a better error.
        let cstr = std::str::from_utf8(&cstr).map_err(|_| io::ErrorKind::InvalidData)?;

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

async fn parse_client_query(
    client: &mut TcpStream,
    mut buffer: &mut BytesMut,
) -> io::Result<usize> {
    let n = client.read(&mut buffer).await?;
    let mut analyzed = 0;
    while analyzed < n {
        match PgParser::parse(&buffer)? {
            (Some(PgMsg::Query(_)), n) => analyzed += n,
            (Some(PgMsg::Msg('X')), _) => {
                // Client closed the connection.
                println!("Terminate received!");
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "Terminate received",
                ));
            }
            _ => unimplemented!(),
        };
    }

    Ok(n)
}

async fn new_server_connection(
    host: SocketAddr,
    startup_message: &StartupMessage,
) -> io::Result<TcpStream> {
    let mut pg_conn = TcpStream::connect(host).await?;
    handle_server_startup(&mut pg_conn, &startup_message).await?;
    Ok(pg_conn)
}

async fn handle_client_startup(client: &mut TcpStream) -> io::Result<StartupMessage> {
    let mut buffer = BytesMut::with_capacity(8096);
    buffer.resize(8096, 0);

    // Startup / SSL Request
    let n = client.read(&mut buffer).await?;
    println!("Read: {:?}", n);
    let size = BigEndian::read_i32(&buffer[0..4]);

    let version = BigEndian::read_i32(&buffer[4..8]);
    let startup_message = if version == 80877103 {
        // Reject SSL.
        println!("SSL Request!");
        let n = client.write(&[b'N']).await?;
        println!("Wrote: {:?}", n);

        let n = client.read(&mut buffer[..]).await?;
        println!("Read: {:?}", n);
        let size = BigEndian::read_i32(&buffer[0..4]);
        let version = BigEndian::read_i32(&buffer[4..8]);
        println!("{:?} / version: {}", &buffer[..n], version);
        parse_startup_message(size, &mut buffer)?
    } else {
        println!("{:?} / startup version: {}", size, version);
        parse_startup_message(size, &mut buffer)?
    };
    println!("Client Startup Msg: {:?}", startup_message);

    Ok(startup_message)
}

enum CopyDirection {
    ToServer,
    ToClient,
}

async fn handle_client(
    mut client: TcpStream,
    mut server_addr_channel: tokio::sync::watch::Receiver<SocketAddr>,
) -> io::Result<()> {
    // Get the startup message from the client.
    let startup_message = handle_client_startup(&mut client).await?;

    // Connect to real postgres server using the startup message.
    //let server_addr = "127.0.0.1:5432".parse::<SocketAddr>().unwrap();
    let server_addr = server_addr_channel.recv().await.unwrap();
    let mut pg_conn = TcpStream::connect(server_addr).await?;
    let (n, mut server_buffer) = handle_server_startup(&mut pg_conn, &startup_message).await?;

    // Relay AuthOK, Params, etc. from Server.
    let n = client.write(&server_buffer[..n]).await?;
    println!("Copied {} bytes from server to client.", n);

    // Now that both connections are in a ReadyForQuery state, we will
    // start the query loop.
    let mut client_buffer = BytesMut::with_capacity(8096);
    client_buffer.resize(8096, 0);
    loop {
        // Wait for the following:
        // 1. Query from the client.
        // 2. Error from the server.
        // TODO 3. A connection change event.
        let n = match futures::future::select(
            Box::pin(parse_client_query(&mut client, &mut client_buffer)),
            Box::pin(server_addr_channel.recv()),
        )
        .await
        {
            Either::Left((Ok(n), _)) => n,
            Either::Right((Some(server_addr), _)) => {
                println!(
                    "Reloading postgres connection using new server addr: {:?}",
                    server_addr
                );

                // Drop the current postgres connection.
                drop(pg_conn);

                // Reconnect to postgres using a new postgres connection.
                pg_conn = TcpStream::connect(server_addr).await?;
                handle_server_startup(&mut pg_conn, &startup_message).await?;
                continue;
            }
            _ => unimplemented!(),
        };

        // We have a query! Send all client buffer to server.
        let server_n = pg_conn.write(&client_buffer[..n]).await?;
        assert_eq!(n, server_n);

        // Wait for the following:
        // 1. Query from the client.
        // 2. Error from the server.
        // TODO 3. A connection change event.

        'transaction: loop {
            println!("In the query loop!");

            // Proxy query until transaction completes.
            let direction = {
                match futures::future::select(
                    Box::pin(parse_client_query(&mut client, &mut client_buffer)),
                    //client.read(&mut buffer),
                    pg_conn.read(&mut server_buffer),
                )
                .await
                {
                    // Handle client
                    Either::Left((Ok(n), _)) => {
                        // Query
                        (CopyDirection::ToServer, n)
                    }
                    Either::Left((Err(err), _)) => {
                        // Client closed the connection.
                        return Err(err);
                    }
                    Either::Right((Ok(n), _)) => {
                        // Response data
                        (CopyDirection::ToClient, n)
                    }
                    x => panic!("Missed a select case!"),
                }
            };

            match direction {
                (CopyDirection::ToServer, n) => pg_conn.write(&client_buffer[..n]).await?,
                (CopyDirection::ToClient, n) => {
                    // We got data back from the server. Send to client. TODO: Let's validate it first.
                    client.write(&server_buffer[..n]).await?;

                    // Server has data to send to client
                    println!("Writing to client: {}", server_buffer[0] as char);

                    let mut idx = 0;
                    while idx < n {
                        let tag = server_buffer[idx] as char;
                        println!("Server parsing: {}, idx: {}", tag, idx);
                        match tag {
                            'C' | 'D' | 'E' | 'T' => {
                                let size = BigEndian::read_i32(&server_buffer[(idx + 1)..(idx + 5)])
                                    as usize;
                                println!(
                                    "SRV: Tag: {}, Msg Size: {}, Msg: {:?}",
                                    tag,
                                    size,
                                    &server_buffer[idx..(idx + size)]
                                );
                                idx += size + 1;
                            }
                            'Z' => {
                                if server_buffer[idx + 5] == b'I' {
                                    println!("SRV: Transaction complete!");
                                    break 'transaction;
                                }
                                println!(
                                    "Server said READY FOR QUERY: {}",
                                    server_buffer[idx + 5] as char
                                );
                                idx += 6;
                            }
                            _ => {
                                println!("New tag for server to parse: {} - b{}", tag, tag as u8);
                                break;
                            }
                        }
                    }

                    n
                }
            };
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let bind_addr = "127.0.0.1:7432".parse::<SocketAddr>().unwrap();
    println!("Listening on: {:?}", bind_addr);
    let mut listener = TcpListener::bind(bind_addr).await?;

    let config_file =
        std::fs::File::open("examples/config.json").expect("file should open read only");
    let config: Config = serde_json::from_reader(config_file).expect("file should be proper JSON");

    println!("CONFIG: {:?}", config);

    let server_addr = "127.0.0.1:5432".parse::<SocketAddr>().unwrap();
    let (tx, rx) = tokio::sync::watch::channel(server_addr);

    tokio::spawn(async move {
        let mut stream = signal(SignalKind::hangup()).expect("SIGHUP stream should work");

        loop {
            stream.recv().await;
            let config_file =
                std::fs::File::open("examples/config.json").expect("file should open read only");
            let config: Config =
                serde_json::from_reader(config_file).expect("file should be proper JSON");
            println!("RELOADED CONFIG: {:?}", config);
            tx.broadcast(config.postgres_addr);
        }
    });

    loop {
        let (mut client, _) = listener.accept().await?;
        let client_info = format!("{:?}", client);
        println!("Client connected: {:?}", client_info);
        tokio::spawn({
            let rx = rx.clone();

            async move {
                match handle_client(client, rx).await {
                    Ok(()) => println!("The client connection closed for: {:?}", client_info),
                    Err(err) => println!(
                        "The client connection closed with an error for: {:?}, error: {:?}",
                        client_info, err
                    ),
                };
            }
        });
    }
}
