use crate::CliOptions;
use av_scenechange::{detect_scene_changes, DetectionOptions, DetectionResults};
use console::{style, Term};
use std::error::Error;
use std::io::Write;

pub fn detect_keyframes(opts: &CliOptions) -> Result<DetectionResults, Box<dyn Error>> {
    let report_progress = |frames: usize, _kf: usize| {
        let mut term_err = Term::stderr();
        term_err.clear_line().unwrap();
        let _ = write!(
            term_err,
            "{} scene cuts: {} analyzed",
            style("Analyzing").yellow(),
            style(format!("{} frames", frames)).cyan()
        );
    };

    let mut term_err = Term::stderr();
    let _ = write!(term_err, "{} scene cuts...", style("Analyzing").yellow());

    let sc_opts = DetectionOptions {
        fast_analysis: opts.speed >= 10,
        ignore_flashes: false,
        lookahead_distance: 5,
        min_scenecut_distance: Some(opts.min_keyint as usize),
        max_scenecut_distance: Some(opts.max_keyint as usize),
        progress_callback: if term_err.is_term() {
            Some(Box::new(report_progress))
        } else {
            None
        },
    };
    let mut reader = if let Some(fast_fp) = opts.first_pass_input {
        fast_fp.as_reader()?
    } else {
        opts.input.as_reader()?
    };
    let mut dec = y4m::decode(&mut reader).expect("input is not a y4m file");
    let bit_depth = dec.get_bit_depth();
    Ok(if bit_depth == 8 {
        detect_scene_changes::<_, u8>(&mut dec, sc_opts)
    } else {
        detect_scene_changes::<_, u16>(&mut dec, sc_opts)
    })
}
