# Contributing to nexio

Thanks for helping. A few things keep the project reliable:

- **Run the checks** before opening a pull request:
  ```bash
  cargo fmt --check
  cargo clippy --all-targets --features http,tls,tokio-compat -- -D warnings
  cargo test --features http,tls,tokio-compat
  cargo test --features poll,http,tls,tokio-compat   # the portable backend (Linux)
  RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --features http,tls,tokio-compat
  ```
- **Tests come with fixes.** A bug fix needs a test that fails without it. Fix the bug a test finds; do not
  weaken the test.
- **No speed claims without numbers.** Performance changes need a reproducible benchmark and the raw results
  (see `bench/` and `reports/`). Negative results are welcome too.
- **`unsafe` needs a `// SAFETY:` comment** that states the invariant it relies on.
- **Both backends.** Public API must behave the same on the `io_uring` and portable backends; the shared tests
  in `tests/portable_api.rs` run on all of them.
- Linux-only `io_uring` internals can be tested on any Linux machine or VM; CI covers Windows, macOS and Linux.

By contributing you agree that your work is licensed under MIT OR Apache-2.0, like the rest of the project.
