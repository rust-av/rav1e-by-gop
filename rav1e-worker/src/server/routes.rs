use crate::server::helpers::{
    handle_rejection_types, json_body, require_auth, with_state, ClientVersionMismatch,
};
use crate::server::CLIENT_VERSION_REQUIRED;
use crate::{try_or_500, EncodeItem, ENCODER_QUEUE, UUID_CONTEXT, UUID_NODE_ID};
use byteorder::{LittleEndian, WriteBytesExt};
use bytes::Bytes;
use http::header::{HeaderValue, CONTENT_TYPE};
use rav1e_by_gop::{
    decompress_frame, EncodeState, GetInfoResponse, GetProgressResponse, PostEnqueueResponse,
    SegmentFrameData, SerializableProgressInfo, SlotRequestMessage,
};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::v1::Timestamp;
use uuid::Uuid;
use warp::http::StatusCode;
use warp::reply::Response;
use warp::{Filter, Rejection, Reply};

pub fn get_routes(
    temp_dir: Option<PathBuf>,
    worker_threads: usize,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::path!("info")
        .and(warp::get())
        .and(require_auth())
        .and(with_state(worker_threads))
        .and_then(get_info)
        .or(warp::path!("enqueue" / Uuid)
            .and(warp::get())
            .and(require_auth())
            .and_then(get_enqueue))
        .or(warp::path!("enqueue")
            .and(warp::post())
            .and(require_auth())
            .and(json_body())
            .and_then(post_enqueue))
        .or(warp::path!("segment_data" / Uuid)
            .and(warp::post())
            .and(require_auth())
            .and(warp::body::bytes())
            .and(with_state(temp_dir))
            .and_then(post_segment_data))
        .or(warp::path!("segment" / Uuid)
            .and(warp::get())
            .and(require_auth())
            .and_then(get_segment))
        .or(warp::path!("segment_data" / Uuid)
            .and(warp::get())
            .and(require_auth())
            .and_then(get_segment_data))
        .recover(handle_rejection_types)
}

async fn get_info(_auth: (), worker_threads: usize) -> Result<impl Reply, Rejection> {
    Ok(warp::reply::with_status(
        warp::reply::json(&GetInfoResponse {
            worker_count: worker_threads,
        }),
        StatusCode::NOT_FOUND,
    ))
}

// this endpoint tells a client if their slot is ready
async fn get_enqueue(request_id: Uuid, _auth: ()) -> Result<impl Reply, Rejection> {
    let reader = ENCODER_QUEUE.read().await;
    let item = reader.get(&request_id);
    if let Some(item) = item {
        match item.read().await.state {
            EncodeState::Enqueued => Ok(warp::reply::with_status(
                warp::reply::json(&()),
                StatusCode::ACCEPTED,
            )),
            EncodeState::AwaitingData { .. } => Ok(warp::reply::with_status(
                warp::reply::json(&()),
                StatusCode::OK,
            )),
            _ => Ok(warp::reply::with_status(
                warp::reply::json(&()),
                StatusCode::GONE,
            )),
        }
    } else {
        Ok(warp::reply::with_status(
            warp::reply::json(&()),
            StatusCode::NOT_FOUND,
        ))
    }
}

// client hits this endpoint to say that they want to request a slot
// returns a JSON body with a request ID
async fn post_enqueue(_auth: (), body: SlotRequestMessage) -> Result<impl Reply, Rejection> {
    if !CLIENT_VERSION_REQUIRED.matches(&body.client_version) {
        return Err(warp::reject::custom(ClientVersionMismatch));
    }

    let ts = Timestamp::from_unix(&*UUID_CONTEXT, 1497624119, 1234);
    let request_id = try_or_500!(Uuid::new_v1(ts, &UUID_NODE_ID));
    ENCODER_QUEUE.write().await.insert(
        request_id,
        RwLock::new(EncodeItem::new(body.options, body.video_info)),
    );

    Ok(warp::reply::with_status(
        warp::reply::json(&PostEnqueueResponse { request_id }),
        StatusCode::ACCEPTED,
    ))
}

// client sends raw video data via this endpoint
async fn post_segment_data(
    request_id: Uuid,
    _auth: (),
    body: Bytes,
    temp_dir: Option<PathBuf>,
) -> Result<impl Reply, Rejection> {
    let queue_handle = ENCODER_QUEUE.read().await;
    if let Some(item) = queue_handle.get(&request_id) {
        let mut item_handle = item.write().await;
        let frame_data;
        if let EncodeState::AwaitingData { .. } = &mut item_handle.state {
            let compressed_frames: Vec<Vec<u8>> = try_or_500!(bincode::deserialize(&body));
            frame_data = match temp_dir {
                Some(temp_dir) => {
                    let frame_count = compressed_frames.len();
                    let mut temp_path = temp_dir;
                    temp_path.push(request_id.to_hyphenated().to_string());
                    temp_path.set_extension("py4m");
                    let file = File::create(&temp_path).unwrap();
                    let mut writer = BufWriter::new(file);
                    for frame in compressed_frames {
                        let frame_data = if item_handle.video_info.bit_depth == 8 {
                            bincode::serialize(&decompress_frame::<u8>(&frame)).unwrap()
                        } else {
                            bincode::serialize(&decompress_frame::<u16>(&frame)).unwrap()
                        };
                        writer
                            .write_u32::<LittleEndian>(frame_data.len() as u32)
                            .unwrap();
                        writer.write_all(&frame_data).unwrap();
                    }
                    writer.flush().unwrap();
                    SegmentFrameData::Y4MFile {
                        path: temp_path,
                        frame_count,
                    }
                }
                None => SegmentFrameData::CompressedFrames(compressed_frames),
            }
        } else {
            return Ok(warp::reply::with_status(
                warp::reply::json(&()),
                StatusCode::NOT_FOUND,
            ));
        }
        item_handle.state = EncodeState::Ready {
            raw_frames: Arc::new(frame_data),
        };
        return Ok(warp::reply::with_status(
            warp::reply::json(&()),
            StatusCode::OK,
        ));
    }

    Ok(warp::reply::with_status(
        warp::reply::json(&()),
        StatusCode::NOT_FOUND,
    ))
}

// returns progress on currently encoded segment
async fn get_segment(request_id: Uuid, _auth: ()) -> Result<impl Reply, Rejection> {
    let reader = ENCODER_QUEUE.read().await;
    if let Some(item) = reader.get(&request_id) {
        let item_reader = item.read().await;
        match item_reader.state {
            EncodeState::InProgress { ref progress, .. } => Ok(warp::reply::with_status(
                warp::reply::json(&GetProgressResponse {
                    progress: SerializableProgressInfo::from(&*progress),
                    done: false,
                }),
                StatusCode::OK,
            )),
            EncodeState::EncodingDone { ref progress, .. } => Ok(warp::reply::with_status(
                warp::reply::json(&GetProgressResponse {
                    progress: SerializableProgressInfo::from(&*progress),
                    done: true,
                }),
                StatusCode::OK,
            )),
            _ => Ok(warp::reply::with_status(
                warp::reply::json(&()),
                StatusCode::GONE,
            )),
        }
    } else {
        Ok(warp::reply::with_status(
            warp::reply::json(&()),
            StatusCode::NOT_FOUND,
        ))
    }
}

// if segment is ready, sends client the encoded video data
async fn get_segment_data(request_id: Uuid, _auth: ()) -> Result<impl Reply, Rejection> {
    // Check first without mutating the state
    let mut can_send_data = false;
    let queue_handle = ENCODER_QUEUE.read().await;
    if let Some(item) = queue_handle.get(&request_id) {
        if let EncodeState::EncodingDone { .. } = item.read().await.state {
            can_send_data = true;
        }
    }
    if !can_send_data {
        return Ok(warp::reply::with_status(
            Response::new(Vec::new().into()),
            StatusCode::NOT_FOUND,
        ));
    }

    // Now pop it from the queue and send it, freeing the resources simultaneously
    let mut queue_handle = ENCODER_QUEUE.write().await;
    if let Some(item) = queue_handle.remove(&request_id) {
        if let EncodeState::EncodingDone { encoded_data, .. } = item.into_inner().state {
            return Ok(warp::reply::with_status(
                {
                    let mut response = Response::new(encoded_data.into());
                    response.headers_mut().insert(
                        CONTENT_TYPE,
                        HeaderValue::from_static("application/octet-stream"),
                    );
                    response
                },
                StatusCode::OK,
            ));
        }
    }

    unreachable!()
}
