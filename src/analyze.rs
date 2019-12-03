use crate::CliOptions;
use av_scenechange::{detect_scene_changes, DetectionOptions};
use console::Term;
use std::error::Error;

pub fn detect_keyframes(opts: &CliOptions) -> Result<Vec<usize>, Box<dyn Error>> {
    eprint!("Analyzing scene cuts...");
    let report_progress = |frames: usize, _kf: usize| {
        let term_out = Term::stdout();
        term_out.move_cursor_to(0, 0).unwrap();
        term_out.clear_line().unwrap();
        eprint!("Analyzing scene cuts: {} frames analyzed", frames);
    };

    let term_err = Term::stderr();

    let sc_opts = DetectionOptions {
        use_chroma: opts.speed < 10,
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
