mod analyze;
mod decode;
mod encode;
mod muxer;

use self::encode::perform_encode;
use crate::encode::{load_progress_file, MemoryUsage};
use anyhow::{ensure, Result};
use clap::{App, Arg, ArgMatches};
use console::{style, Term};
use log::info;
use std::collections::BTreeSet;
use std::fs::File;
use std::io::Write;
use std::io::{stdin, Read};
use std::path::PathBuf;
use std::str::FromStr;
use std::{env, fmt};

#[derive(Debug, Clone)]
pub struct CliOptions {
    input: Input,
    output: PathBuf,
    speed: usize,
    qp: usize,
    min_keyint: u64,
    max_keyint: u64,
    max_threads: Option<usize>,
    verbose: bool,
    memory_usage: MemoryUsage,
    display_progress: bool,
}

impl From<&ArgMatches<'_>> for CliOptions {
    fn from(matches: &ArgMatches) -> Self {
        let input = matches.value_of("INPUT").unwrap();
        CliOptions {
            input: if input == "-" {
                Input::Stdin
            } else {
                Input::File(PathBuf::from(input))
            },
            output: PathBuf::from(matches.value_of("OUTPUT").unwrap()),
            speed: matches.value_of("SPEED").unwrap().parse().unwrap(),
            qp: matches.value_of("QP").unwrap().parse().unwrap(),
            min_keyint: matches.value_of("MIN_KEYINT").unwrap().parse().unwrap(),
            max_keyint: matches.value_of("MAX_KEYINT").unwrap().parse().unwrap(),
            max_threads: matches
                .value_of("MAX_THREADS")
                .map(|threads| threads.parse().unwrap()),
            verbose: matches.is_present("VERBOSE"),
            memory_usage: matches
                .value_of("MEMORY_LIMIT")
                .map(|val| MemoryUsage::from_str(val).expect("Invalid option for memory limit"))
                .unwrap_or_default(),
            display_progress: Term::stderr().is_term() && !matches.is_present("NO_PROGRESS"),
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

fn main() -> Result<()> {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "rav1e_by_gop=info");
    }
    env_logger::builder()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    // Thanks, borrow checker
    let mem_usage_default = MemoryUsage::default().to_string();

    let matches = App::new("rav1e-by-gop")
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
        .arg(Arg::with_name("NO_PROGRESS").help("Hide the progress bars. Mostly useful for debugging. Automatically set if not running from a TTY.").long("no-progress").hidden(true))
        .get_matches();
    let opts = CliOptions::from(&matches);
    ensure!(
        opts.output.extension().and_then(|ext| ext.to_str()) == Some("ivf"),
        "Output must be a .ivf file"
    );
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
