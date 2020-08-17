use std::collections::BTreeSet;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

use crate::encode::*;
use crate::muxer::create_file_muxer;
use crate::SegmentData;

use anyhow::Result;
use rav1e::{EncoderConfig, Pixel};
use rayon::{ThreadPool, ThreadPoolBuilder};
use serde::de::DeserializeOwned;

/// All configuration setup
#[derive(Default)]
pub struct Config {
    base: EncoderConfig,
    threads: usize,
    pool: Option<Arc<ThreadPool>>,
}

// Builder
impl Config {
    /// Create a default configuration
    ///
    /// same as Default::default()
    pub fn new() -> Self {
        Config::default()
    }

    /// Set the encoder configuration
    ///
    /// EncoderConfig contains the settings impacting the
    /// codec features used in the produced bitstream.
    pub fn with_encoder_config(mut self, enc: EncoderConfig) -> Self {
        self.base = enc;
        self
    }

    /// Set the number of workers in the threadpool
    ///
    /// The threadpool is shared across all the different parallel
    /// components in the encoder.
    pub fn with_threads(mut self, threads: usize) -> Self {
        self.threads = threads;
        self
    }

    /// Use the provided threadpool
    pub fn with_thread_pool(mut self, pool: Arc<ThreadPool>) -> Self {
        self.pool = Some(pool);
        self
    }
}

// Keep the settings for all the encoders
// TODO: hide it
pub struct Context<T: Pixel> {
    base: EncoderConfig,
    pool: Arc<ThreadPool>,
    workers_pool: threadpool::ThreadPool,
    _pd: std::marker::PhantomData<T>,
}

impl Config {
    pub fn new_context<T: Pixel>(&self) -> Context<T> {
        let pool = self.pool.as_ref().map(|p| p.clone()).unwrap_or_else(|| {
            let p = ThreadPoolBuilder::new()
                .num_threads(self.threads)
                .build()
                .unwrap();
            Arc::new(p)
        });

        let workers_pool = threadpool::ThreadPool::new(self.threads);

        Context {
            pool,
            base: self.base,
            workers_pool,
            _pd: std::marker::PhantomData,
        }
    }
}

impl<T: Pixel + DeserializeOwned> Context<T> {
    pub fn single_pass_config(&self) -> rav1e::Config {
        rav1e::config::Config::new()
            .with_thread_pool(self.pool.clone())
            .with_encoder_config(self.base)
    }

    pub fn encode_segment(
        &self,
        data: SegmentData,
        progress_sender: ProgressSender,
        segment_output_file: PathBuf,
    ) -> Result<()> {
        let mut progress = ProgressInfo::new(
            rav1e::prelude::Rational {
                num: self.base.time_base.den,
                den: self.base.time_base.num,
            },
            data.compressed_frames.len(),
            {
                let mut kf = BTreeSet::new();
                kf.insert(data.start_frameno);
                kf
            },
            data.segment_no + 1,
            data.next_analysis_frame,
            None,
        );
        let _ = progress_sender.send(ProgressStatus::Encoding(Box::new(progress.clone())));

        let mut ctx: rav1e::Context<T> = self
            .single_pass_config()
            .new_context()
            .expect("Cannot setup the encoder");

        self.workers_pool.execute(move || {
            let mut source = Source {
                compressed_frames: data.compressed_frames.into_iter().map(Rc::new).collect(),
                sent_count: 0,
            };
            let _ = progress_sender.send(ProgressStatus::Encoding(Box::new(progress.clone())));

            let mut output =
                create_file_muxer(&segment_output_file).expect("Failed to create segment output");

            loop {
                match process_frame(&mut ctx, &mut source).expect("Cannot encode the frame") {
                    ProcessFrameResult::Packet(packet) => {
                        output.write_frame(
                            packet.input_frameno as u64,
                            packet.data.as_ref(),
                            packet.frame_type,
                        );
                        progress.add_packet(*packet);
                        let _ = progress_sender
                            .send(ProgressStatus::Encoding(Box::new(progress.clone())));
                    }
                    ProcessFrameResult::NoPacket(_) => {
                        // Next iteration
                    }
                    ProcessFrameResult::EndOfSegment => {
                        output.flush().unwrap();
                        break;
                    }
                }
            }
        });

        Ok(())
    }
}
