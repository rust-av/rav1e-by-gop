use crate::analyze::SlotReadySender;
use crate::WorkerConfig;
use anyhow::{bail, Result};
use crossbeam_channel::unbounded;
use http::request::Request;
use log::{debug, error, warn};
use rav1e::prelude::Rational;
use rav1e_by_gop::{
    create_muxer, ActiveConnection, EncoderMessage, ProgressInfo, ProgressStatus, Slot, SlotStatus,
    WorkerQueryResponse, WorkerStatusUpdate, WorkerUpdateChannel, WORKER_QUERY_MESSAGE,
};
use serde::de::DeserializeOwned;
use std::collections::BTreeSet;
use tungstenite::{connect, Error as WsError, Message};
use url::Url;
use v_frame::pixel::Pixel;

pub(crate) struct RemoteWorkerInfo {
    pub uri: Url,
    pub password: String,
    // Like local slots, this tracks which workers are active or inactive
    pub workers: Box<[bool]>,
    pub slot_status: SlotStatus,
    pub update_channel: WorkerUpdateChannel,
}

pub(crate) fn discover_remote_worker(worker: &WorkerConfig) -> Result<RemoteWorkerInfo> {
    // The http crate has a crap interface for setting the port (i.e. "no interface"),
    // so do it this kind of stupid way using two crates instead.
    let mut url = Url::parse(&if worker.secure {
        format!("wss://{}", worker.host)
    } else {
        format!("ws://{}", worker.host)
    })?;
    url.set_port(worker.port.or(Some(13415))).unwrap();

    debug!("Initial query to remote worker {}", url);
    let request = Request::builder()
        .uri(url.as_str())
        .header("X-RAV1E-AUTH", &worker.password)
        .body(())?;
    let mut connection = connect(request)?.0;
    connection.write_message(Message::Text(WORKER_QUERY_MESSAGE.to_string()))?;

    // Wait for response
    let resp;
    loop {
        match connection.read_message() {
            Ok(Message::Binary(data)) => {
                resp = rmp_serde::from_read::<_, WorkerQueryResponse>(data.as_slice())?;
                break;
            }
            Ok(Message::Close(_)) | Err(_) => {
                bail!("Connection closed unexpectedly with {}", url);
            }
            _ => {
                // Ignore pings
            }
        }
    }

    // Wait for connection close message
    loop {
        match connection.read_message() {
            Ok(Message::Close(_)) => {
                break;
            }
            Err(e) => {
                warn!(
                    "Handshake connection closed unexpectedly with {}: {}",
                    url, e
                );
                // But keep going anyway
                break;
            }
            _ => {
                // Ignore pings
            }
        }
    }

    Ok(RemoteWorkerInfo {
        uri: url,
        password: worker.password.clone(),
        workers: vec![false; resp.worker_count].into_boxed_slice(),
        slot_status: SlotStatus::None,
        update_channel: unbounded(),
    })
}

pub(crate) fn wait_for_slot_allocation<T: Pixel + DeserializeOwned + Default>(
    mut connection: ActiveConnection,
    slot_ready_sender: SlotReadySender,
) {
    loop {
        debug!("Waiting for slot allocation");
        match connection.socket.read_message() {
            Ok(Message::Binary(data)) => {
                let message: EncoderMessage<T> = match rmp_serde::from_read(data.as_slice()) {
                    Ok(msg) => msg,
                    Err(_) => {
                        continue;
                    }
                };
                if let EncoderMessage::SlotAllocated(connection_id) = message {
                    debug!("Slot reserved, sending to encoder");
                    let update_sender = connection.worker_update_sender.clone();
                    connection.connection_id = Some(connection_id);
                    let slot_in_worker = connection.slot_in_worker;
                    if slot_ready_sender
                        .send(Slot::Remote(Box::new(connection)))
                        .is_ok()
                    {
                        debug!("Slot allocated and sent to encoder");
                        update_sender
                            .send(WorkerStatusUpdate {
                                status: None,
                                slot_delta: Some((slot_in_worker, true)),
                            })
                            .unwrap();
                    } else {
                        debug!("Ignoring final slot");
                    }
                    return;
                }
            }
            Ok(Message::Close(_))
            | Err(WsError::ConnectionClosed)
            | Err(WsError::AlreadyClosed) => {
                warn!("Connection closed on requesting connection");
                return;
            }
            Err(e) => {
                error!(
                    "An unexpected error occurred on requesting connection: {}",
                    e
                );
                return;
            }
            _ => {
                // Ignore message
            }
        }
    }
}

pub(crate) fn remote_encode_segment<T: Pixel + DeserializeOwned + Default>(
    mut connection: ActiveConnection,
) {
    let encode_info = connection.encode_info.as_ref().unwrap();
    let video_info = &connection.video_info;
    let mut output = create_muxer(&encode_info.output_file).unwrap();
    let mut progress = ProgressInfo::new(
        Rational {
            num: video_info.time_base.den,
            den: video_info.time_base.num,
        },
        encode_info.frame_count,
        {
            let mut kf = BTreeSet::new();
            kf.insert(encode_info.start_frameno);
            kf
        },
        encode_info.segment_idx,
        encode_info.next_analysis_frame,
    );
    let _ = connection
        .progress_sender
        .send(ProgressStatus::Encoding(Box::new(progress.clone())));

    loop {
        match connection.socket.read_message() {
            Ok(Message::Binary(data)) => {
                let message: EncoderMessage<T> = match rmp_serde::from_read(data.as_slice()) {
                    Ok(msg) => msg,
                    Err(_) => {
                        continue;
                    }
                };
                match message {
                    EncoderMessage::SendEncodedPacket(packet) => {
                        output.write_frame(
                            packet.input_frameno as u64,
                            packet.data.as_ref(),
                            packet.frame_type,
                        );
                        progress.add_packet(*packet);
                        let _ = connection
                            .progress_sender
                            .send(ProgressStatus::Encoding(Box::new(progress.clone())));
                    }
                    EncoderMessage::SegmentFinished => {
                        debug!("Segment finished normally");
                        break;
                    }
                    EncoderMessage::EncodeFailed(e) => {
                        error!("VERY BAD YOU SHOULD JUST CTRL+C NOW! Remote encode failed for connection {}: {}", connection.connection_id.unwrap(), e);
                        break;
                    }
                    _ => {
                        continue;
                    }
                }
            }
            Ok(Message::Close(_))
            | Err(WsError::ConnectionClosed)
            | Err(WsError::AlreadyClosed) => {
                break;
            }
            Err(e) => {
                error!(
                    "An unexpected error occurred on connection {} listener: {}",
                    connection.connection_id.unwrap(),
                    e
                );
                break;
            }
            _ => {
                continue;
            }
        }
    }

    let _ = connection.socket.close(None);
    output.flush().unwrap();
    connection
        .worker_update_sender
        .send(WorkerStatusUpdate {
            status: None,
            slot_delta: Some((connection.slot_in_worker, false)),
        })
        .unwrap();
}
