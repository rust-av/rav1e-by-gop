use crate::decode::{get_video_details, process_raw_frame, read_raw_frame, DecodeError};
use crate::CliOptions;
use anyhow::Result;
use av_scenechange::{DetectionOptions, SceneChangeDetector};
use crossbeam_channel::{Receiver, Sender};
use itertools::Itertools;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;
use v_frame::frame::Frame;
use v_frame::pixel::Pixel;
use y4m::Decoder;

pub(crate) struct SegmentData<T: Pixel> {
    pub segment_no: usize,
    pub slot_no: usize,
    pub next_analysis_frame: usize,
    pub start_frameno: usize,
    pub frames: Vec<Frame<T>>,
}
pub(crate) type AnalyzerSender<T> = Sender<Option<SegmentData<T>>>;
pub(crate) type AnalyzerReceiver<T> = Receiver<Option<SegmentData<T>>>;
pub(crate) type AnalyzerChannel<T> = (AnalyzerSender<T>, AnalyzerReceiver<T>);

pub(crate) type InputFinishedSender = Sender<()>;
pub(crate) type InputFinishedReceiver = Receiver<()>;
pub(crate) type InputFinishedChannel = (InputFinishedSender, InputFinishedReceiver);

pub(crate) fn run_first_pass<T: Pixel, R: Read + Send>(
    mut dec: Decoder<R>,
    opts: CliOptions,
    sender: AnalyzerSender<T>,
    pool: Arc<Mutex<Vec<bool>>>,
    next_frameno: usize,
    known_keyframes: BTreeSet<usize>,
    skipped_segments: BTreeSet<usize>,
) -> Result<()> {
    let sc_opts = DetectionOptions {
        fast_analysis: opts.speed >= 10,
        ignore_flashes: false,
        lookahead_distance: 5,
        min_scenecut_distance: Some(opts.min_keyint as usize),
        max_scenecut_distance: Some(opts.max_keyint as usize),
    };
    let cfg = get_video_details(&dec);
    let mut detector = SceneChangeDetector::new(dec.get_bit_depth(), cfg.chroma_sampling, &sc_opts);

    let mut analysis_frameno = next_frameno;
    let mut lookahead_frameno = 0;
    let mut segment_no = 0;
    let mut start_frameno;
    // The first keyframe will always be 0, so get the second keyframe.
    let mut next_known_keyframe = known_keyframes.iter().nth(1).copied();
    let mut keyframes: BTreeSet<usize> = known_keyframes.clone();
    keyframes.insert(0);
    let mut lookahead_queue = BTreeMap::new();
    loop {
        let target_slot;
        loop {
            // Wait for an open slot before loading more frames,
            // to reduce memory usage.
            let open_slot = pool.lock().unwrap().iter().position(|slot| !*slot);
            if let Some(slot) = open_slot {
                target_slot = slot;
                break;
            }

            sleep(Duration::from_millis(500));
        }

        let mut processed_frames: Vec<Frame<T>>;
        if let Some(next_keyframe) = next_known_keyframe {
            start_frameno = lookahead_frameno;
            next_known_keyframe = known_keyframes
                .iter()
                .copied()
                .find(|kf| *kf > next_keyframe);

            // Quickly seek ahead if this is a skipped segment
            if skipped_segments.contains(&(segment_no + 1)) {
                while lookahead_frameno < next_keyframe {
                    match read_raw_frame(&mut dec) {
                        Ok(_) => {
                            lookahead_frameno += 1;
                        }
                        Err(DecodeError::EOF) => {
                            break;
                        }
                        Err(e) => {
                            return Err(e.into());
                        }
                    }
                }
                segment_no += 1;
                continue;
            } else {
                processed_frames = Vec::with_capacity(next_keyframe - lookahead_frameno);
                while lookahead_frameno < next_keyframe {
                    match read_raw_frame(&mut dec) {
                        Ok(frame) => {
                            processed_frames.push(process_raw_frame(frame, &cfg));
                            lookahead_frameno += 1;
                        }
                        Err(DecodeError::EOF) => {
                            break;
                        }
                        Err(e) => {
                            return Err(e.into());
                        }
                    };
                }
            }
        } else {
            start_frameno = keyframes.iter().copied().last().unwrap();
            loop {
                // Load frames until the lookahead queue is filled
                while analysis_frameno + sc_opts.lookahead_distance > lookahead_frameno {
                    match read_raw_frame(&mut dec) {
                        Ok(frame) => {
                            lookahead_queue
                                .insert(lookahead_frameno, process_raw_frame(frame, &cfg));
                            lookahead_frameno += 1;
                        }
                        Err(DecodeError::EOF) => {
                            break;
                        }
                        Err(e) => {
                            return Err(e.into());
                        }
                    };
                }

                // Analyze the current frame for a scenechange
                if analysis_frameno != keyframes.iter().last().copied().unwrap() {
                    // The frame_queue should start at whatever the previous frame was
                    let frame_set = lookahead_queue
                        .iter()
                        .skip_while(|(frameno, _)| **frameno < analysis_frameno - 1)
                        .take(sc_opts.lookahead_distance + 1)
                        .map(|(_, frame)| frame)
                        .collect::<Vec<_>>();
                    if frame_set.len() >= 2 {
                        detector.analyze_next_frame(&frame_set, analysis_frameno, &mut keyframes);
                    } else {
                        // End of encode
                        keyframes.insert(*lookahead_queue.iter().last().unwrap().0 + 1);
                        break;
                    }

                    analysis_frameno += 1;
                    if keyframes.iter().last().copied().unwrap() == analysis_frameno - 1 {
                        // Keyframe found
                        break;
                    }
                } else if analysis_frameno < lookahead_frameno {
                    analysis_frameno += 1;
                } else {
                    // End of encode
                    sender.send(None)?;
                    return Ok(());
                }
            }

            // The frames comprising the segment are known, so set them in `processed_frames`
            let interval: (usize, usize) = keyframes
                .iter()
                .rev()
                .take(2)
                .rev()
                .copied()
                .collect_tuple()
                .unwrap();
            processed_frames = Vec::with_capacity(interval.1 - interval.0);
            for frameno in (interval.0)..(interval.1) {
                processed_frames.push(lookahead_queue.remove(&frameno).unwrap());
            }
        }

        pool.lock().unwrap()[target_slot] = true;
        sender.send(Some(SegmentData {
            segment_no,
            slot_no: target_slot,
            next_analysis_frame: analysis_frameno - 1,
            start_frameno,
            frames: processed_frames,
        }))?;
        segment_no += 1;
    }
}
