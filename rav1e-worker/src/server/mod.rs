use crate::server::routes::get_routes;
use bcrypt::{hash, DEFAULT_COST};
use lazy_static::lazy_static;
use log::info;
use std::env;
use std::net::SocketAddrV4;
use std::path::PathBuf;

mod helpers;
mod routes;

lazy_static! {
    static ref HASHED_SERVER_PASSWORD: String =
        hash(env::var("SERVER_PASSWORD").unwrap(), DEFAULT_COST).unwrap();
    static ref CLIENT_VERSION_REQUIRED: semver::VersionReq = {
        let server_version = semver::Version::parse(env!("CARGO_PKG_VERSION")).unwrap();
        if server_version.major > 0 {
            semver::VersionReq::parse(&format!("^{}.0.0", server_version.major)).unwrap()
        } else {
            semver::VersionReq::parse(&format!("~0.{}.0", server_version.minor)).unwrap()
        }
    };
}

pub async fn start_listener(
    server_ip: SocketAddrV4,
    temp_dir: Option<PathBuf>,
    worker_threads: usize,
) {
    // This thread watches for new incoming connections,
    // both for the initial negotiation and for new slot requests
    tokio::spawn(async move {
        info!("Remote listener started on {}", server_ip);

        match (env::var("TLS_CERT_PATH"), env::var("TLS_KEY_PATH")) {
            (Ok(cert_path), Ok(key_path)) => {
                warp::serve(get_routes(temp_dir, worker_threads))
                    .tls()
                    .cert_path(&cert_path)
                    .key_path(&key_path)
                    .run(server_ip)
                    .await;
            }
            _ => {
                warp::serve(get_routes(temp_dir, worker_threads))
                    .run(server_ip)
                    .await;
            }
        };
    });
}
