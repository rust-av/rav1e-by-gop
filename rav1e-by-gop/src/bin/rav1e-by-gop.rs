#![allow(clippy::cognitive_complexity)]
#![allow(clippy::too_many_arguments)]

mod analyze;
mod decode;
mod encode;
mod progress;
#[cfg(feature = "remote")]
mod remote;

#[cfg(feature = "remote")]
use std::time::Duration;
use std::{
    cmp,
    collections::BTreeSet,
    env,
    fmt,
    fs::File,
    io::{stdin, Read, Write},
    path::PathBuf,
    str::FromStr,
};

use anyhow::{ensure, Result};
use clap::{App, Arg, ArgMatches};
use console::{style, Term};
#[cfg(feature = "remote")]
use lazy_static::lazy_static;
use log::info;
use rav1e::prelude::*;
use rav1e_by_gop::*;
#[cfg(feature = "remote")]
use serde::Deserialize;
use systemstat::{ByteSize, Platform, System};

use self::{encode::*, progress::*};

#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[cfg(feature = "remote")]
lazy_static! {
    static ref CLIENT: reqwest::blocking::Client = reqwest::blocking::ClientBuilder::new()
        .timeout(None)
        .connect_timeout(Duration::from_secs(10))
        .build()
        .unwrap();
}

fn main() -> Result<()> {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "rav1e_by_gop=info");
    }
    env_logger::builder()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    // Thanks, borrow checker
    let mem_usage_default = MemoryUsage::default().to_string();

    #[allow(unused_mut)]
    let mut app = App::new("rav1e-by-gop")
        .arg(
            Arg::with_name("INPUT")
                .help("Sets the input file or command to use")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::with_name("OUTPUT")
                .help("IVF video output")
                .short("o")
                .long("output")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("SPEED")
                .help("rav1e speed level (0-10), smaller values are slower")
                .default_value("6")
                .short("s")
                .long("speed")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("QP")
                .help("Quantizer (0-255), smaller values are higher quality")
                .default_value("100")
                .short("q")
                .long("quantizer")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("MAX_BITRATE")
                .help("Max local bitrate (kbps)")
                .long("max-bitrate")
                .alias("vbv-maxrate")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("MIN_KEYINT")
                .help("Minimum distance between two keyframes")
                .default_value("12")
                .short("i")
                .long("min-keyint")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("MAX_KEYINT")
                .help("Maximum distance between two keyframes")
                .default_value("240")
                .short("I")
                .long("keyint")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("MAX_THREADS")
                .help("Limit the maximum number of threads that can be used")
                .long("threads")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("MEMORY_LIMIT")
                .help("Limit the number of threads based on the amount of memory on the system")
                .long("memory")
                .takes_value(true)
                .possible_values(&["light", "heavy", "unlimited"])
                .default_value(&mem_usage_default),
        )
        .arg(
            Arg::with_name("FRAMES")
                .help("Limit the number of frames to encode")
                .long("limit")
                .alias("frames")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("FORCE_RESUME")
                .help("Resume any in-progress encodes without being prompted")
                .long("resume")
                .conflicts_with("FORCE_OVERWRITE"),
        )
        .arg(
            Arg::with_name("FORCE_OVERWRITE")
                .help("Overwrite any in-progress encodes without being prompted")
                .long("overwrite")
                .conflicts_with("FORCE_RESUME"),
        )
        .arg(
            Arg::with_name("VERBOSE")
                .help("Print more stats at end of encode")
                .long("verbose")
                .short("v"),
        )
        .arg(
            Arg::with_name("NO_PROGRESS")
                .help(
                    "Hide the progress bars. Mostly useful for debugging. Automatically set if \
                     not running from a TTY.",
                )
                .long("no-progress")
                .hidden(true),
        )
        .arg(
            Arg::with_name("TILES")
                .help("Sets the number of tiles to use")
                .long("tiles")
                .short("t")
                .default_value("1"),
        )
        .arg(
            Arg::with_name("INPUT_TEMP_FILES")
                .help(
                    "Write y4m input segments to temporary files instead of keeping them in \
                     memory. Reduces memory usage but increases disk usage. Should enable more \
                     segments to run simultaneously.",
                )
                .long("tmp-input"),
        )
        .arg(
            Arg::with_name("LOCAL_WORKERS")
                .help("Limit the maximum number of local workers that can be used")
                .long("local-workers")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("COLOR_PRIMARIES")
                .help("Color primaries used to describe color parameters")
                .long("primaries")
                .possible_values(&ColorPrimaries::variants())
                .default_value("unspecified")
                .case_insensitive(true),
        )
        .arg(
            Arg::with_name("TRANSFER_CHARACTERISTICS")
                .help("Transfer characteristics used to describe color parameters")
                .long("transfer")
                .possible_values(&TransferCharacteristics::variants())
                .default_value("unspecified")
                .case_insensitive(true),
        )
        .arg(
            Arg::with_name("MATRIX_COEFFICIENTS")
                .help("Matrix coefficients used to describe color parameters")
                .long("matrix")
                .possible_values(&MatrixCoefficients::variants())
                .default_value("unspecified")
                .case_insensitive(true),
        );
    #[cfg(feature = "remote")]
    {
        app = app
            .arg(
                Arg::with_name("WORKERS")
                    .help("A path to the TOML file defining remote encoding workers")
                    .long("workers")
                    .default_value("workers.toml")
                    .takes_value(true),
            )
            .arg(
                Arg::with_name("NO_LOCAL")
                    .help("Disable local encoding threads--requires distributed workers")
                    .long("no-local"),
            );
    }
    let matches = app.get_matches();
    let opts = CliOptions::from(&matches);
    ensure!(
        opts.max_keyint >= opts.min_keyint,
        "Max keyint must be greater than or equal to min keyint"
    );
    ensure!(opts.qp <= 255, "QP must be between 0-255");

    let progress = load_progress_file(&opts.output, &matches);
    let (keyframes, next_analysis_frame) = if let Some(ref progress) = progress {
        info!("{} encode", style("Resuming").yellow());
        (progress.keyframes.clone(), progress.next_analysis_frame)
    } else {
        info!("{} encode", style("Starting").yellow());
        (BTreeSet::new(), 0)
    };

    info!(
        "Encoding using input from `{}`, speed {}, quantizer {}, keyint {}-{}",
        opts.input, opts.speed, opts.qp, opts.min_keyint, opts.max_keyint
    );
    perform_encode(keyframes, next_analysis_frame, &opts, progress).expect("Failed encoding");

    info!("{}", style("Finished!").yellow());

    Ok(())
}

#[derive(Debug, Clone)]
pub struct CliOptions {
    input: Input,
    output: Output,
    speed: usize,
    qp: usize,
    max_bitrate: Option<i32>,
    min_keyint: u64,
    max_keyint: u64,
    max_threads: Option<usize>,
    local_workers: Option<usize>,
    tiles: usize,
    max_frames: Option<u64>,
    verbose: bool,
    memory_usage: MemoryUsage,
    display_progress: bool,
    #[cfg(feature = "remote")]
    workers: Vec<WorkerConfig>,
    #[cfg(feature = "remote")]
    use_local: bool,
    temp_input: bool,
    color_primaries: ColorPrimaries,
    transfer_characteristics: TransferCharacteristics,
    matrix_coefficients: MatrixCoefficients,
}

impl From<&ArgMatches<'_>> for CliOptions {
    fn from(matches: &ArgMatches) -> Self {
        let input = matches.value_of("INPUT").unwrap();
        let output = PathBuf::from(matches.value_of("OUTPUT").unwrap());
        let output = if ["/dev/null", "nul", "null"]
            .contains(&output.as_os_str().to_string_lossy().to_lowercase().as_str())
        {
            Output::Null
        } else {
            Output::File(output)
        };
        let temp_input = match output {
            Output::File(_) => matches.is_present("INPUT_TEMP_FILES"),
            Output::Null => {
                info!("Null output unsupported with temp input files, disabling temp input");
                false
            }
            _ => unreachable!(),
        };
        CliOptions {
            input: if input == "-" {
                Input::Stdin
            } else {
                Input::File(PathBuf::from(input))
            },
            output,
            speed: matches.value_of("SPEED").unwrap().parse().unwrap(),
            qp: matches.value_of("QP").unwrap().parse().unwrap(),
            max_bitrate: matches
                .value_of("MAX_BITRATE")
                .map(|val| val.parse::<i32>().unwrap() * 1000),
            min_keyint: matches.value_of("MIN_KEYINT").unwrap().parse().unwrap(),
            max_keyint: matches.value_of("MAX_KEYINT").unwrap().parse().unwrap(),
            max_threads: matches
                .value_of("MAX_THREADS")
                .map(|threads| threads.parse().unwrap()),
            tiles: matches.value_of("TILES").unwrap().parse().unwrap(),
            local_workers: matches
                .value_of("LOCAL_WORKERS")
                .map(|workers| workers.parse().unwrap()),
            max_frames: matches
                .value_of("FRAMES")
                .map(|frames| frames.parse().unwrap()),
            verbose: matches.is_present("VERBOSE"),
            memory_usage: matches
                .value_of("MEMORY_LIMIT")
                .map(|val| MemoryUsage::from_str(val).expect("Invalid option for memory limit"))
                .unwrap_or_default(),
            display_progress: Term::stderr().features().is_attended()
                && !matches.is_present("NO_PROGRESS"),
            #[cfg(feature = "remote")]
            workers: {
                let workers_file_path = matches.value_of("WORKERS").unwrap();
                match File::open(workers_file_path) {
                    Ok(mut file) => {
                        let mut contents = String::new();
                        file.read_to_string(&mut contents).unwrap();
                        match toml::from_str::<WorkerFile>(&contents) {
                            Ok(res) => {
                                info!("Loaded remote workers file");
                                res.workers
                            }
                            Err(e) => {
                                panic!("Malformed remote workers file: {}", e);
                            }
                        }
                    }
                    Err(_) => {
                        info!("Could not open workers file; using local encoding only");
                        Vec::new()
                    }
                }
            },
            #[cfg(feature = "remote")]
            use_local: !matches.is_present("NO_LOCAL"),
            temp_input,
            color_primaries: matches
                .value_of("COLOR_PRIMARIES")
                .unwrap()
                .parse()
                .unwrap_or_default(),
            transfer_characteristics: matches
                .value_of("TRANSFER_CHARACTERISTICS")
                .unwrap()
                .parse()
                .unwrap_or_default(),
            matrix_coefficients: matches
                .value_of("MATRIX_COEFFICIENTS")
                .unwrap()
                .parse()
                .unwrap_or_default(),
        }
    }
}

impl From<&CliOptions> for EncodeOptions {
    fn from(other: &CliOptions) -> Self {
        EncodeOptions {
            speed: other.speed,
            qp: other.qp,
            max_bitrate: other.max_bitrate,
            tiles: other.tiles,
            color_primaries: other.color_primaries,
            transfer_characteristics: other.transfer_characteristics,
            matrix_coefficients: other.matrix_coefficients,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Input {
    File(PathBuf),
    Stdin,
}

impl Input {
    pub fn as_reader(&self) -> Result<Box<dyn Read + Send>> {
        Ok(match self {
            Input::File(filename) => Box::new(File::open(filename)?),
            Input::Stdin => Box::new(stdin()),
        })
    }
}

impl fmt::Display for Input {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Input::File(path) => write!(f, "{}", path.to_string_lossy().as_ref()),
            Input::Stdin => write!(f, "stdin"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum MemoryUsage {
    Light,
    Heavy,
    Unlimited,
}

impl Default for MemoryUsage {
    fn default() -> Self {
        MemoryUsage::Light
    }
}

impl FromStr for MemoryUsage {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "light" => Ok(MemoryUsage::Light),
            "heavy" => Ok(MemoryUsage::Heavy),
            "unlimited" => Ok(MemoryUsage::Unlimited),
            _ => Err(()),
        }
    }
}

impl fmt::Display for MemoryUsage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                MemoryUsage::Light => "light",
                MemoryUsage::Heavy => "heavy",
                MemoryUsage::Unlimited => "unlimited",
            }
        )
    }
}

fn decide_thread_count(
    opts: &CliOptions,
    video_info: &VideoDetails,
    has_remote_workers: bool,
    tiles: usize,
) -> usize {
    #[cfg(feature = "remote")]
    if !opts.use_local {
        return 0;
    }

    let mut num_threads = 0;

    // Limit based on available memory.
    let sys = System::new();
    let sys_memory = sys.memory();
    if let Ok(sys_memory) = sys_memory {
        let bytes_per_frame = bytes_per_frame(video_info);
        let rdo_lookahead_frames = 40;
        // The RDO Lookahead will have extra data loaded,
        // and will be uncompressed in memory.
        // The remainder of frames will have only basic YUV data loaded,
        // and will be compressed in memory.
        let bytes_per_segment = if opts.temp_input {
            bytes_per_frame * rdo_lookahead_frames * 9
        } else if opts.max_keyint <= rdo_lookahead_frames {
            bytes_per_frame * opts.max_keyint * 6
        } else {
            (bytes_per_frame * rdo_lookahead_frames * 9)
                + bytes_per_frame * (opts.max_keyint - rdo_lookahead_frames) * 6 / 10
        };
        let total = sys_memory.total.as_u64();
        match opts.memory_usage {
            MemoryUsage::Light => {
                // Uses 50% of memory, minimum 2GB,
                // minimum unreserved memory of 2GB,
                // maximum unreserved memory of 12GB
                let unreserved = cmp::min(
                    ByteSize::gb(12).as_u64(),
                    cmp::max(ByteSize::gb(2).as_u64(), sys_memory.total.as_u64() / 2),
                );
                let limit = cmp::max(ByteSize::gb(2).as_u64(), total.saturating_sub(unreserved));
                num_threads = cmp::max(
                    1,
                    // Having remote workers means input must continue
                    // to be read and analyzed while local encodes
                    // are happening. i.e. it's like having one extra thread.
                    (limit / bytes_per_segment) as usize - if has_remote_workers { 1 } else { 0 },
                ) * tiles;
            }
            MemoryUsage::Heavy => {
                // Uses 80% of memory, minimum 2GB,
                // minimum unreserved memory of 1GB,
                // maximum unreserved memory of 6GB
                let unreserved = cmp::min(
                    ByteSize::gb(6).as_u64(),
                    cmp::max(ByteSize::gb(1).as_u64(), sys_memory.total.as_u64() * 4 / 5),
                );
                let limit = cmp::max(ByteSize::gb(2).as_u64(), total.saturating_sub(unreserved));
                num_threads = cmp::max(
                    1,
                    (limit / bytes_per_segment) as usize - if has_remote_workers { 1 } else { 0 },
                ) * tiles;
            }
            MemoryUsage::Unlimited => {
                num_threads = num_cpus::get();
            }
        }
    }

    // Limit to the number of logical CPUs.
    num_threads = cmp::min(num_cpus::get(), num_threads);
    if let Some(max_threads) = opts.max_threads {
        num_threads = cmp::min(num_threads, max_threads);
    }

    num_threads
}

fn bytes_per_frame(video_info: &VideoDetails) -> u64 {
    let bytes_per_plane =
        video_info.width * video_info.height * if video_info.bit_depth > 8 { 2 } else { 1 };
    (match video_info.chroma_sampling {
        ChromaSampling::Cs420 => bytes_per_plane * 3 / 2,
        ChromaSampling::Cs422 => bytes_per_plane * 2,
        ChromaSampling::Cs444 => bytes_per_plane * 3,
        ChromaSampling::Cs400 => bytes_per_plane,
    }) as u64
}

#[cfg(feature = "remote")]
#[derive(Debug, Clone, Deserialize)]
pub struct WorkerFile {
    pub workers: Vec<WorkerConfig>,
}

#[cfg(feature = "remote")]
#[derive(Debug, Clone, Deserialize)]
pub struct WorkerConfig {
    pub host: String,
    #[serde(default)]
    pub port: Option<u16>,
    pub password: String,
    #[serde(default)]
    pub secure: bool,
}
