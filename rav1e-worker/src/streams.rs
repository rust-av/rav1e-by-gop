use native_tls::TlsStream;
use std::io::{Read, Write};
use std::net::TcpStream;
use tungstenite::WebSocket;
use uuid::Uuid;

#[derive(Debug)]
pub struct ConnectedSocket {
    pub id: Uuid,
    pub socket: WebSocket<SwitchableStream>,
}

impl ConnectedSocket {
    pub fn new(socket: WebSocket<SwitchableStream>) -> Self {
        ConnectedSocket {
            id: Uuid::new_v4(),
            socket,
        }
    }
}

#[derive(Debug)]
pub enum SwitchableStream {
    TcpStream(TcpStream),
    TlsStream(TlsStream<TcpStream>),
}

impl Read for SwitchableStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            SwitchableStream::TcpStream(stream) => stream.read(buf),
            SwitchableStream::TlsStream(stream) => stream.read(buf),
        }
    }
}

impl Write for SwitchableStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            SwitchableStream::TcpStream(stream) => stream.write(buf),
            SwitchableStream::TlsStream(stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            SwitchableStream::TcpStream(stream) => stream.flush(),
            SwitchableStream::TlsStream(stream) => stream.flush(),
        }
    }
}
