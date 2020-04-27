use crate::channels::SlotRequestChannel;
use clap::{App, Arg};
use crossbeam_channel::unbounded;
use crossbeam_utils::thread::scope;
use server::*;
use std::env;
use std::net::SocketAddrV4;
use std::thread::sleep;
use std::time::Duration;
use streams::*;
use worker::*;

mod channels;
mod server;
mod streams;
mod worker;

fn main() {
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

    scope(|scope| {
        let slot_request_channel: SlotRequestChannel = unbounded();
        start_listener(server_ip, scope, slot_request_channel.0.clone(), threads)
            .expect("Server failed to start");
        start_workers(threads, scope, slot_request_channel.1);

        loop {
            // Run the thread forever until terminated
            sleep(Duration::from_secs(60));
        }
    })
    .unwrap();
}
