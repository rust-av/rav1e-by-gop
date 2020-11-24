// Copyright (c) 2001-2016, Alliance for Open Media. All rights reserved
// Copyright (c) 2017-2019, The rav1e contributors. All rights reserved
//
// This source code is subject to the terms of the BSD 2 Clause License and
// the Alliance for Open Media Patent License 1.0. If the BSD 2 Clause License
// was not distributed with this source code in the LICENSE file, you can
// obtain it at www.aomedia.org/license/software. If the Alliance for Open
// Media Patent License 1.0 was not distributed with this source code in the
// PATENTS file, you can obtain it at www.aomedia.org/license/patent.

use super::Muxer;
use anyhow::Result;
use ivf::*;
use rav1e::prelude::*;
use std::fs::File;
use std::io;
use std::io::{sink, BufWriter, Sink, Write};

pub struct IvfMuxer<W: Write> {
    pub output: W,
}

impl<W: Write> Muxer for IvfMuxer<W> {
    fn write_header(
        &mut self,
        width: usize,
        height: usize,
        framerate_num: usize,
        framerate_den: usize,
    ) {
        write_ivf_header(
            &mut self.output,
            width,
            height,
            framerate_num,
            framerate_den,
        );
    }

    fn write_frame(&mut self, pts: u64, data: &[u8], _frame_type: FrameType) {
        write_ivf_frame(&mut self.output, pts, data);
    }

    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}

impl<W: Write> IvfMuxer<W> {
    pub fn open(path: &str) -> Result<IvfMuxer<BufWriter<File>>> {
        let ivf = IvfMuxer {
            output: BufWriter::new(File::create(path)?),
        };
        Ok(ivf)
    }

    pub fn null() -> IvfMuxer<Sink> {
        IvfMuxer { output: sink() }
    }

    pub fn in_memory() -> IvfMuxer<Vec<u8>> {
        IvfMuxer { output: Vec::new() }
    }
}
