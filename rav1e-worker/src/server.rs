use crate::channels::SlotRequestSender;
use crate::worker::SlotQueueItem;
use crate::{ConnectedSocket, SwitchableStream};
use anyhow::Result;
use bcrypt::{hash, verify, DEFAULT_COST};
use crossbeam_utils::thread::Scope;
use http::StatusCode;
use lazy_static::lazy_static;
use log::{debug, error, info, warn};
use native_tls::{Identity, TlsAcceptor};
use rav1e_by_gop::{SlotRequestMessage, WorkerQueryResponse, WORKER_QUERY_MESSAGE};
use std::env;
use std::fs::File;
use std::io::Read;
use std::net::{SocketAddrV4, TcpListener};
use tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tungstenite::protocol::WebSocketConfig;
use tungstenite::server::accept_hdr_with_config;
use tungstenite::Message;

lazy_static! {
    static ref HASHED_SERVER_PASSWORD: String =
        hash(env::var("SERVER_PASSWORD").unwrap(), DEFAULT_COST).unwrap();
}

pub fn start_listener(
    server_ip: SocketAddrV4,
    scope: &Scope,
    slot_request_sender: SlotRequestSender,
    worker_threads: usize,
) -> Result<()> {
    let server = TcpListener::bind(server_ip)?;
    let acceptor = if let Ok(identity_file) = env::var("IDENTITY_FILE") {
        // This is kind of a lot of boilerplate to set up a TLS server...
        let mut identity_file = File::open(identity_file)?;
        let mut identity = Vec::new();
        identity_file.read_to_end(&mut identity)?;
        let identity = Identity::from_pkcs12(
            &identity,
            &env::var("IDENTITY_PASSWORD").unwrap_or_else(|_| String::new()),
        )?;
        Some(TlsAcceptor::new(identity).unwrap())
    } else {
        None
    };

    // This thread watches for new incoming connections,
    // both for the initial negotiation and for new slot requests
    scope.spawn(move |scope| {
        info!("Websocket listener started on {}", server_ip);

        for stream in server.incoming() {
            let stream = match stream {
                Ok(stream) => stream,
                Err(e) => {
                    error!("Failed to receive connection: {}", e);
                    continue;
                }
            };
            let stream = match acceptor.as_ref() {
                Some(acceptor) => match acceptor.accept(stream) {
                    Ok(stream) => SwitchableStream::TlsStream(stream),
                    Err(e) => {
                        error!("Failed to accept TLS connection: {}", e);
                        continue;
                    }
                },
                None => SwitchableStream::TcpStream(stream),
            };
            match accept_hdr_with_config(
                stream,
                accept_callback,
                Some(WebSocketConfig {
                    max_send_queue: None,
                    max_message_size: None,
                    max_frame_size: None,
                }),
            ) {
                Ok(stream) => {
                    let connection = ConnectedSocket::new(stream);
                    info!("Received new connection {}", connection.id);

                    // Run each connection in its own thread to listen for initial messages,
                    // since each connection reader is blocking
                    let slot_request_sender = slot_request_sender.clone();
                    scope.spawn(move |_| {
                        initial_connection_listener(connection, worker_threads, slot_request_sender)
                    });
                }
                Err(e) => {
                    error!("Failed to complete connection handshake: {}", e);
                    continue;
                }
            };
        }
    });

    Ok(())
}

fn initial_connection_listener(
    mut connection: ConnectedSocket,
    worker_threads: usize,
    slot_request_sender: SlotRequestSender,
) {
    loop {
        match connection.socket.read_message() {
            Ok(Message::Text(data)) => {
                if data == WORKER_QUERY_MESSAGE {
                    debug!("Received worker query message from {}", connection.id);
                    let _ = connection.socket.write_message(Message::Binary(
                        rmp_serde::to_vec(&WorkerQueryResponse {
                            worker_count: worker_threads,
                        })
                        .unwrap(),
                    ));
                    let _ = connection.socket.close(None);
                    break;
                } else {
                    warn!(
                        "Received unknown text-formatted message for connection {}, ignoring: {}",
                        connection.id, data
                    );
                }
            }
            Ok(Message::Binary(data)) => {
                // Check to see if this is a request message
                if let Ok(message) = rmp_serde::from_read::<_, SlotRequestMessage>(data.as_slice())
                {
                    debug!("Received worker request from {}", connection.id);
                    slot_request_sender
                        .send(SlotQueueItem {
                            connection,
                            message,
                        })
                        .unwrap();
                    return;
                } else {
                    warn!(
                            "Received unexpected binary-formatted message for connection {} while expecting SlotRequestMessage",
                            connection.id
                        );
                }
            }
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {
                // No action needed
            }
            Ok(Message::Close(_)) => {
                info!("Connection {} closed by client", connection.id);
                return;
            }
            Err(e) => {
                warn!("Connection {} closed unexpectedly: {}", connection.id, e);
                return;
            }
        }
    }
}

fn accept_callback(req: &Request, resp: Response) -> std::result::Result<Response, ErrorResponse> {
    let auth = req.headers().get("X-RAV1E-AUTH");
    if let Some(password) = auth {
        if let Ok(password) = password.to_str() {
            if verify(password, &HASHED_SERVER_PASSWORD).unwrap() {
                return Ok(resp);
            }
        }
    };
    let mut resp = ErrorResponse::new(Some("Unauthorized".to_string()));
    *resp.status_mut() = StatusCode::UNAUTHORIZED;
    Err(resp)
}
