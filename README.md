# rav1e-by-gop

## Introduction

`rav1e-by-gop` is a tool to multithread a rav1e encode
by splitting it into GOPs (at keyframe points).
This allows multithreading without the quality loss of tiles.

Disclaimer: This tool attempts to be memory efficient
by only opening one reader for the entire encoding step.
This avoids tools such as Vapoursynth,
which may be quite memory hungry,
from using all the memory on your system.
However, memory usage of this tool itself
is still `O(n)` where n is the number of threads.

By default, this tool will limit the number of threads
based on the memory available in your system.
You can adjust or disable this safeguard using the
`--memory` CLI option.

## Basic Usage

To encode a video from y4m to AV1 (in ivf container):

`rav1e-by-gop input.y4m -o output.ivf`

This will use the default encode settings specified by rav1e,
and use threads equal to the number of logical CPU cores.

## Advanced Usage

To configure rav1e settings (only some settings are configurable):

`rav1e-by-gop input.y4m -s 3 --qp 50 --min-keyint 12 --keyint 360 -o output.ivf`

To pipe in input:

`vspipe --y4m input.vpy - | rav1e-by-gop - -o output.ivf`

To limit the number of threads
(this tool will never use more than the number of CPUs):

`rav1e-by-gop input.y4m --threads 4 -o output.ivf`

## Distributed encoding

By default, the rav1e-by-gop client can run on a single machine.
However, it can be set up to take advantage of remote machines
running the rav1e worker for distributed encoding.

TODO: Usage details

## Limitations

- Only supports one-pass QP mode
  - One-pass bitrate mode will never be supported,
    because it's not suitable for the use cases where one would use this tool
  - Two-pass bitrate mode may be supported in the future
