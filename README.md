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
You probably want to have at least 2GB of RAM
per encoder thread, e.g. if you are running 16 threads,
32GB of RAM is the recommended minimum.

## Dependencies

You need `ffmpeg` installed on your machine.

## Basic Usage

To encode a video from y4m to AV1 (in ivf container):

`rav1e-by-gop input.y4m -o output.ivf`

This will use the default encode settings specified by rav1e,
and use threads equal to the number of logical CPU cores.

## Advanced Usage

To configure rav1e settings (only some settings are configurable):

`rav1e-by-gop input.y4m -s 3 --qp 50 --min-keyint 12 --keyint 360 -o output.ivf`

To pipe in input
(this is non-standard,
due to having to read through input twice):

`rav1e-by-gop "vspipe --y4m input.vpy -" --pipe -o output.ivf`

To use a faster input pipe for the analysis pass
(useful if your input pipe has expensive filtering):

`rav1e-by-gop "vspipe --y4m input.vpy -" --pipe --fast-fp "vspipe --y4m faster_input.vpy -" -o output.ivf`

To limit the number of threads
(this tool will never use more than the number of CPUs):

`rav1e-by-gop input.y4m --threads 4 -o output.ivf`
