use clap::{App, Arg};
use lazy_static::lazy_static;
use log::{debug, log_enabled};
use rand::Rng;
use rav1e_by_gop::{EncodeOptions, EncodeState, VideoDetails};
use server::*;
use std::collections::BTreeMap;
use std::env;
use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::delay_for;
use uuid::v1::Context;
use uuid::Uuid;
use worker::*;

mod server;
mod worker;

#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

lazy_static! {
    pub static ref ENCODER_QUEUE: RwLock<BTreeMap<Uuid, RwLock<EncodeItem>>> =
        RwLock::new(BTreeMap::new());
    pub static ref UUID_CONTEXT: Context = Context::new(0);
    pub static ref UUID_NODE_ID: Box<[u8]> = {
        let mut id = Vec::with_capacity(6);
        let mut rng = rand::thread_rng();
        for _ in 0..6 {
            id.push(rng.gen());
        }
        id.into_boxed_slice()
    };
}

pub struct EncodeItem {
    pub state: EncodeState,
    pub options: EncodeOptions,
    pub video_info: VideoDetails,
}

impl EncodeItem {
    fn new(options: EncodeOptions, video_info: VideoDetails) -> Self {
        EncodeItem {
            state: EncodeState::Enqueued,
            options,
            video_info,
        }
    }
}

impl std::fmt::Debug for EncodeItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.state {
            EncodeState::Enqueued => f.write_str("Enqueued"),
            EncodeState::AwaitingInfo { .. } => f.write_str("Awaiting Segment Info"),
            EncodeState::AwaitingData { .. } => f.write_str("Awaiting Data"),
            EncodeState::Ready { ref raw_frames, .. } => f.write_fmt(format_args!(
                "Ready to encode {} frames",
                raw_frames.frame_count()
            )),
            EncodeState::InProgress { ref progress } => f.write_fmt(format_args!(
                "Encoding {} of {} frames",
                progress.frame_info.len(),
                progress.total_frames
            )),
            EncodeState::EncodingDone {
                ref encoded_data, ..
            } => f.write_fmt(format_args!("Done encoding {} bytes", encoded_data.len())),
        }
    }
}

#[tokio::main]
async fn main() {
    env::var("SERVER_PASSWORD").expect("SERVER_PASSWORD env var MUST be set!");

    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "rav1e_worker=info");
    }
    env_logger::init();

    let matches = App::new("rav1e-worker")
        .arg(
            Arg::with_name("LISTEN_IP")
                .help("Select which IP to listen on")
                .long("ip")
                .visible_alias("host")
                .default_value("0.0.0.0")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("LISTEN_PORT")
                .help("Select which port to listen on")
                .long("port")
                .short("p")
                .default_value("13415")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("MAX_THREADS")
                .help(
                    "Limit the number of threads that can be used for workers [default: num cpus]",
                )
                .long("threads")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("TEMP_DIR")
                .help("Store input segments in temp files in the specified directory; by default stores in memory")
                .long("temp-dir")
                .takes_value(true)
        )
        .get_matches();

    let server_ip = SocketAddrV4::new(
        matches.value_of("LISTEN_IP").unwrap().parse().unwrap(),
        matches.value_of("LISTEN_PORT").unwrap().parse().unwrap(),
    );
    let mut threads = num_cpus::get();
    if let Some(thread_setting) = matches
        .value_of("MAX_THREADS")
        .and_then(|val| val.parse().ok())
    {
        threads = threads.min(thread_setting);
    }
    let temp_dir = if let Some(temp_dir) = matches.value_of("TEMP_DIR") {
        let dir = PathBuf::from(temp_dir);
        if !dir.is_dir() {
            panic!("Specified temp dir does not exist or is not a directory");
        }
        if dir.metadata().unwrap().permissions().readonly() {
            panic!("Specified temp dir is not writeable");
        }
        Some(dir)
    } else {
        None
    };

    start_listener(server_ip, temp_dir, threads).await;
    start_workers(threads).await;

    loop {
        // Run the main thread forever until terminated
        if log_enabled!(log::Level::Debug) {
            let queue_handle = ENCODER_QUEUE.read().await;
            let mut items = Vec::with_capacity(queue_handle.len());
            for (key, item) in queue_handle.iter() {
                items.push((key, item.read().await));
            }
            debug!("Items in queue: {:?}", items);
        }
        delay_for(Duration::from_secs(5)).await;
    }
}
