## Version 0.2.0 (unreleased)
- [Feature] Encoding is now done in one pass instead of two.
  This should somewhat speed up the encoding process
  and make it more streamlined overall.
- [Breaking/Feature] The `--pass` option has been removed.
  It is no longer needed, and you can use your OS's standard
  method for piping in input instead, e.g.
  `ffmpeg -i myinput.mkv -f yuv4mpegpipe - | rav1e-by-gop - -o myencode.ivf`
- [Breaking] The `--fast-fp` option has been removed.
  It is no longer useful.
- [Feature] Add options for limiting number of threads
  based on available memory.
  No more needing to manually guess how many threads to use.
  Adjust via the `--memory` CLI option--
  enabled with "light" setting by default.
- [Breaking] Progress files from prior to this release will not work
  with this release. This means that in-progress encodes
  from a previous version cannot be resumed.
- Improved scenechange detection algorithm.

## Version 0.1.0
- Initial release
