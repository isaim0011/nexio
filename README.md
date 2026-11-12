# nexio

A from-scratch Rust async I/O runtime, aiming at io_uring on Linux with
a cross-platform fallback planned for later. This project exists to
honestly test whether a new, small runtime can compete with tokio,
compio, and monoio on speed, safety, and reliability — not to assume
it already does.

**Status: pre-v0, design phase.** Nothing here is benchmarked or
claimed to be fast yet. A real design-tradeoffs research pass (safety
pitfalls of completion I/O, what actually drives compio/monoio's
performance, whether io_uring's advantage shows up on small-core
hardware) is happening before the reactor implementation goes further,
so early decisions are informed rather than guessed. See
`DESIGN.md` (once written) for the architecture this research leads to.

This project is a sibling to [Turbine](https://github.com/isaim0011)
(a WebSocket rooms/presence library), which first ran into this
question while evaluating `compio`/`monoio` for its own Phase 5
performance work and found the gains more nuanced than expected on
small cloud VMs — see that investigation for the numbers that prompted
this project.
