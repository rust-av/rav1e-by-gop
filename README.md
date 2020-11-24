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

### Server setup

rav1e-worker runs a server that listens for connections from rav1e-by-gop,
then uses worker threads to encode segments for the remote machines.

By default, rav1e-worker will accept insecure connections,
because unfortunately self-signed certs will not work.

You can configure rav1e-worker to use secure TLS connections.
If you have a signed TLS certificate you would like to use,
or would like to get one for free from Let's Encrypt,
you can use environment variables to tell rav1e-by-gop to use it.

e.g.

```
TLS_CERT_PATH=/etc/letsencrypt/live/mydomain.example/cert.pem
TLS_KEY_PATH=/etc/letsencrypt/live/mydomain.example/privkey.pem
```

Setting both of these will automatically enable TLS in the rav1e-worker server.

You must also create a password for clients to use to connect to the server.
This is mandatory. You should set this password to the `SERVER_PASSWORD` environment variable.
It is highly recommended to use a secure password generator like [this one](https://correcthorsebatterystaple.net/).

You can then run `rav1e-worker`, which will listen by default on port 13415,
and spawn workers up to the number of logical CPU cores.
You can configure which IP and port to listen on as well as how many workers
to spawn via CLI args to rav1e-worker.

### Client setup

You can tell rav1e-by-gop to connect to remote workers by using a `workers.toml` file.
There is an example of this file in this repository.
By default, the client will look in the current working directory for the file.
You can override it by using the `--workers /path/to/workers.toml` CLI flag.

rav1e-by-gop will by default use the local worker threads as well.
You can disable local encoding entirely with the `--no-local` flag.

## Limitations

- Only supports one-pass QP mode
  - One-pass bitrate mode will never be supported,
    because it's not suitable for the use cases where one would use this tool
  - Two-pass bitrate mode may be supported in the future
