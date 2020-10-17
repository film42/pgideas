use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use std::io;
use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{
    tcp::{ReadHalf, WriteHalf},
    TcpListener, TcpStream,
};

// Objective 1: Allow psql to connect to this server.
// Objective 2: Connect to postgres server.
// Objective 3: Proxy frontend commands to postgres server.
// Objective 4: Proxy backend commands to psql.
// Objective 5: Basic Pool!
// Objective 6: Complex query args.
#[tokio::main]
async fn main() -> io::Result<()> {
    let bind_addr = "127.0.0.1:7432".parse::<SocketAddr>().unwrap();
    println!("Listening on: {:?}", bind_addr);
    let mut listener = TcpListener::bind(bind_addr).await?;
    loop {
        let (mut client, _) = listener.accept().await?;
        let client_info = format!("{:?}", client);
        println!("Client connected: {:?}", client_info);
        tokio::spawn({
            async move {
                println!("Client disconnected: {:?}", client_info);
                let mut buffer = [0; 8096];

                // Startup / SSL Request
                let n = client.read(&mut buffer[..]).await.unwrap();
                let size = BigEndian::read_i32(&buffer[0..4]);
                println!("Read: {:?}", n);

                let version = BigEndian::read_i32(&buffer[4..8]);
                if version == 80877103 {
                    // Reject SSL.
                    println!("SSL Request!");
                    let n = client.write(&[b'N']).await.unwrap();
                    println!("Wrote: {:?}", n);

                    let n = client.read(&mut buffer[..]).await.unwrap();
                    let size = BigEndian::read_i32(&buffer[0..4]);
                    let version = BigEndian::read_i32(&buffer[4..8]);
                    println!("{:?} / version: {}", size, version);
                    println!("Read: {:?}", n);
                } else {
                    println!("{:?} / startup version: {}", size, version);
                }

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
                    // Query
                    let n = client.read(&mut buffer[..]).await.unwrap();
                    let tag = buffer[0];
                    //let tag = BigEndian::read_i32(&buffer[0..4]);
                    println!("Read: {:?}, Tag: {}", n, tag as char);
                    if tag as char != 'Q' {
                        return;
                    }
                    let size = BigEndian::read_i32(&buffer[1..5]) as usize;
                    let query = std::str::from_utf8(&buffer[5..size]).unwrap();
                    println!("Query: {}", query);

                    // Empty Query Response
                    let mut msg = [0; 5];
                    msg[0] = b'I';
                    BigEndian::write_i32(&mut msg[1..5], 4);
                    let n = client.write(&msg[..]).await.unwrap();
                    println!("Wrote: {:?}", n);

                    // Ready For Query
                    let mut msg = [0; 6];
                    msg[0] = b'Z';
                    BigEndian::write_i32(&mut msg[1..5], 5);
                    msg[5] = b'I';
                    let n = client.write(&msg[..]).await.unwrap();
                    println!("Wrote: {:?}", n);
                }
            }
        });
    }
}
