use crate::analyze::SlotReadySender;
use crate::{WorkerConfig, CLIENT};
use anyhow::Result;
use crossbeam_channel::unbounded;
use log::{debug, error};
use rav1e_by_gop::{
    ActiveConnection, GetInfoResponse, GetProgressResponse, Output, ProgressStatus, Slot,
    SlotStatus, WorkerStatusUpdate, WorkerUpdateChannel,
};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use std::fs::File;
use std::io::{sink, Write};
use std::thread::sleep;
use std::time::Duration;
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
        format!("https://{}", worker.host)
    } else {
        format!("http://{}", worker.host)
    })?;
    url.set_port(worker.port.or(Some(13415))).unwrap();

    debug!("Initial query to remote worker {}", url);
    let resp: GetInfoResponse = CLIENT
        .get(&format!("{}{}", url, "info"))
        .header("X-RAV1E-AUTH", &worker.password)
        .send()?
        .json()?;

    Ok(RemoteWorkerInfo {
        uri: url,
        password: worker.password.clone(),
        workers: vec![false; resp.worker_count].into_boxed_slice(),
        slot_status: SlotStatus::Empty,
        update_channel: unbounded(),
    })
}

pub(crate) fn wait_for_slot_allocation<T: Pixel + DeserializeOwned + Default>(
    connection: ActiveConnection,
    slot_ready_sender: SlotReadySender,
) {
    loop {
        if let Ok(response) = CLIENT
            .get(&format!(
                "{}{}/{}",
                &connection.worker_uri, "enqueue", connection.request_id
            ))
            .header("X-RAV1E-AUTH", &connection.worker_password)
            .send()
            .and_then(|res| res.error_for_status())
        {
            if response.status() == StatusCode::ACCEPTED {
                debug!("Slot reserved, sending to encoder");
                let update_sender = connection.worker_update_sender.clone();
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
        sleep(Duration::from_secs(5));
    }
}

pub(crate) fn remote_encode_segment<T: Pixel + DeserializeOwned + Default>(
    connection: ActiveConnection,
) {
    loop {
        match CLIENT
            .get(&format!(
                "{}{}/{}",
                &connection.worker_uri, "segment", connection.request_id
            ))
            .header("X-RAV1E-AUTH", &connection.worker_password)
            .send()
            .and_then(|res| res.error_for_status())
            .and_then(|res| res.json::<GetProgressResponse>())
        {
            Ok(response) => {
                let _ = connection
                    .progress_sender
                    .send(ProgressStatus::Encoding(Box::new(
                        (&response.progress).into(),
                    )));
                if response.done {
                    loop {
                        match CLIENT
                            .get(&format!(
                                "{}{}/{}",
                                &connection.worker_uri, "segment_data", connection.request_id
                            ))
                            .header("X-RAV1E-AUTH", &connection.worker_password)
                            .send()
                            .and_then(|res| res.error_for_status())
                        {
                            Ok(response) => {
                                let mut output: Box<dyn Write> =
                                    match connection.encode_info.unwrap().output_file {
                                        Output::File(filename) => {
                                            Box::new(File::create(filename).unwrap())
                                        }
                                        Output::Null => Box::new(sink()),
                                        Output::Memory => unreachable!(),
                                    };
                                output.write_all(&response.bytes().unwrap()).unwrap();
                                output.flush().unwrap();
                                break;
                            }
                            Err(e) => {
                                error!("Failed to get encode from remote server: {}", e);
                                sleep(Duration::from_secs(5));
                            }
                        };
                    }
                    break;
                }
                sleep(Duration::from_secs(2));
            }
            Err(e) => {
                error!(
                    "Error getting progress update from remote worker {}: {}",
                    connection.worker_uri, e
                );
                sleep(Duration::from_secs(5));
            }
        }
    }

    connection
        .worker_update_sender
        .send(WorkerStatusUpdate {
            status: None,
            slot_delta: Some((connection.slot_in_worker, false)),
        })
        .unwrap();
}
