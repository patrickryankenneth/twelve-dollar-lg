# lg

Single-threaded C epoll load generator for the twelve-dollar server challenge. It replays the `bench/load.js` user model (feed, think, post, think, 15% like, 2% post) with very little memory per user.

## Build and run

    gcc -O2 -o lg lg.c
    ./lg HOST PORT USERS RAMP_S HOLD_S TOKENS_FILE [NSRC=8]

`TOKENS_FILE` has one user per line: `<jwt> <username>`. Build it from `seed/tokens.json`.

## Notes

- Raise the open-file limit before running it (for example `ulimit -n 1048576`).
- Past roughly 500k connections, use 16 source addresses (`NSRC=16`) so ephemeral ports don't run out.
- If you test io_uring servers in podman or docker, add `--security-opt seccomp=unconfined`. The default profile blocks io_uring, and servers with an epoll fallback will silently use it.
- The seed has 20,000 tokens, so users repeat beyond that.

## Limits

- It replays one user model only. It has no scripting, checks or k6 thresholds.
- It was compared against k6 at 150k users only, where server CPU and memory were similar. Parity above that is not shown.
- Treat its numbers as a local pre-check, not an official result.
