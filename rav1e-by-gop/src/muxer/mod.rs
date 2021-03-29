// Copyright (c) 2017-2019, The rav1e contributors. All rights reserved
//
// This source code is subject to the terms of the BSD 2 Clause License and
// the Alliance for Open Media Patent License 1.0. If the BSD 2 Clause License
// was not distributed with this source code in the LICENSE file, you can
// obtain it at www.aomedia.org/license/software. If the Alliance for Open
// Media Patent License 1.0 was not distributed with this source code in the
// PATENTS file, you can obtain it at www.aomedia.org/license/patent.

mod ivf;

use std::{
    ffi::OsStr,
    fs::File,
    io,
    io::{BufWriter, Sink},
};

use anyhow::Result;
use rav1e::prelude::*;

pub use self::ivf::IvfMuxer;
use crate::Output;

pub trait Muxer {
    fn write_header(
        &mut self,
        width: usize,
        height: usize,
        framerate_num: usize,
        framerate_den: usize,
    );

    fn write_frame(&mut self, pts: u64, data: &[u8], frame_type: FrameType);

    fn flush(&mut self) -> io::Result<()>;
}

pub fn create_muxer(path: &Output) -> Result<Box<dyn Muxer>> {
    match path {
        Output::File(path) => {
            let ext = path
                .extension()
                .and_then(OsStr::to_str)
                .map(str::to_lowercase)
                .unwrap_or_else(|| "ivf".into());

            match &ext[..] {
                "ivf" => Ok(Box::new(IvfMuxer::<BufWriter<File>>::open(
                    path.to_str().unwrap(),
                )?)),
                _e => {
                    panic!(
                        "{} is not a supported extension, please change to .ivf",
                        ext
                    );
                }
            }
        }
        Output::Memory => Ok(Box::new(IvfMuxer::<Vec<u8>>::in_memory())),
        Output::Null => Ok(Box::new(IvfMuxer::<Sink>::null())),
    }
}
