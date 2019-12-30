mod analyze;
mod decode;
mod encode;
mod muxer;

use self::analyze::detect_keyframes;
use self::encode::perform_encode;
use crate::analyze::get_total_frame_count;
use crate::encode::{get_progress_filename, ProgressInfo, SerializableProgressInfo};
use clap::{App, Arg, ArgMatches};
use console::Term;
use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::process::{Command, Stdio};

#[derive(Debug, Clone, Copy)]
pub struct CliOptions<'a> {
    input: Input<'a>,
    first_pass_input: Option<Input<'a>>,
    output: &'a Path,
    speed: usize,
    qp: usize,
    min_keyint: u64,
    max_keyint: u64,
    max_threads: Option<usize>,
}

impl<'a> From<&'a ArgMatches<'a>> for CliOptions<'a> {
    fn from(matches: &'a ArgMatches<'a>) -> Self {
        CliOptions {
            input: if matches.is_present("PIPED_INPUT") {
                Input::Pipe(matches.value_of("INPUT").unwrap())
            } else {
                Input::File(Path::new(matches.value_of("INPUT").unwrap()))
            },
            first_pass_input: matches.value_of("FAST_ANALYSIS").map(Input::Pipe),
            output: Path::new(matches.value_of("OUTPUT").unwrap()),
            speed: matches.value_of("SPEED").unwrap().parse().unwrap(),
            qp: matches.value_of("QP").unwrap().parse().unwrap(),
            min_keyint: matches.value_of("MIN_KEYINT").unwrap().parse().unwrap(),
            max_keyint: matches.value_of("MAX_KEYINT").unwrap().parse().unwrap(),
            max_threads: matches
                .value_of("MAX_THREADS")
                .map(|threads| threads.parse().unwrap()),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Input<'a> {
    File(&'a Path),
    Pipe(&'a str),
}

impl<'a> Input<'a> {
    pub fn as_reader(self) -> Result<Box<dyn Read>, Box<dyn Error>> {
        Ok(match self {
            Input::File(filename) => Box::new(File::open(filename)?) as Box<dyn Read>,
            Input::Pipe(command) => {
                let command = parse_argv(parse_args(command));
                let pipe = Command::new(&command[0])
                    .args(&command[1..])
                    .stdout(Stdio::piped())
                    .stderr(Stdio::null())
                    .spawn()?;
                Box::new(pipe.stdout.unwrap()) as Box<dyn Read>
            }
        })
    }
}

fn parse_args(s: &str) -> String {
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    s.chars()
        .map(|c| {
            if c == '"' && !in_single_quote {
                in_double_quote = !in_double_quote;
                '\n'
            } else if c == '\'' && !in_double_quote {
                in_single_quote = !in_single_quote;
                '\n'
            } else if !in_single_quote && !in_double_quote && char::is_whitespace(c) {
                '\n'
            } else {
                c
            }
        })
        .collect()
}

fn parse_argv(s: String) -> Vec<String> {
    s.split('\n')
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.to_string())
        .collect::<Vec<String>>()
}

fn main() {
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
            Arg::with_name("PIPED_INPUT")
                .help("Flags that the input is a command to pipe input from")
                .long("pipe")
                .alias("piped"),
        )
        .arg(
            Arg::with_name("FAST_ANALYSIS")
                .help("Specify an alternate piped command to use for the analysis pass")
                .long("fast-fp")
                .alias("fast-analysis")
                .takes_value(true)
                .requires("PIPED_INPUT"),
        )
        .arg(
            Arg::with_name("MAX_THREADS")
                .help("Limit the maximum number of threads that can be used")
                .long("threads")
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
        .get_matches();
    let opts = CliOptions::from(&matches);
    assert!(
        opts.output.extension().and_then(|ext| ext.to_str()) == Some("ivf"),
        "Output must be a .ivf file"
    );
    assert!(
        opts.max_keyint >= opts.min_keyint,
        "Max keyint must be greater than or equal to min keyint"
    );
    assert!(opts.qp <= 255, "QP must be between 0-255");

    let term = Term::stderr();

    let progress = if get_progress_filename(opts.output).is_file() {
        let resume = if matches.is_present("FORCE_RESUME") {
            true
        } else if matches.is_present("FORCE_OVERWRITE") {
            false
        } else if term.is_term() {
            let resolved;
            loop {
                let input = dialoguer::Input::<String>::new()
                    .with_prompt("Found progress file for this encode. [R]esume or [O]verwrite?")
                    .interact()
                    .unwrap();
                match input.to_lowercase().as_str() {
                    "r" | "resume" => {
                        resolved = true;
                        break;
                    }
                    "o" | "overwrite" => {
                        resolved = false;
                        break;
                    }
                    _ => {
                        eprintln!("Input not recognized");
                    }
                };
            }
            resolved
        } else {
            // Assume we want to resume if this is not a TTY
            // and no CLI option is given
            true
        };
        if resume {
            let progress_file = File::open(get_progress_filename(opts.output))
                .expect("Failed to open progress file");
            let progress_input: SerializableProgressInfo = serde_json::from_reader(progress_file)
                .expect("Progress file did not contain valid JSON");
            Some(ProgressInfo::from(&progress_input))
        } else {
            None
        }
    } else {
        None
    };

    let (keyframes, frame_count) = if let Some(ref progress) = progress {
        (progress.keyframes.clone(), progress.total_frames)
    } else {
        (
            detect_keyframes(&opts).expect("Failed to run keyframe detection"),
            get_total_frame_count(&opts).expect("Failed to get frame count"),
        )
    };

    eprintln!("\nEncoding {} segments...", keyframes.len());
    perform_encode(&keyframes, frame_count, &opts, progress).expect("Failed encoding");
    eprintln!("Finished!");
}
