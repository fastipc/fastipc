# Roadmap and known issues

What is open: checks that still need doing, known issues, and directions the project may take. The design's known
limits, which are deliberate rather than open, are in [`design/architecture.md`](design/architecture.md) §11 (for
example: a hung peer isn't detected, and peers must be the same user). A done item is deleted from this list; the
change that did it says so.

## Validation

- **Mixed elevation on Windows.** One elevated and one non-elevated process on one name, in both orders (for
  example with the Python binding, a listener in one terminal and a client in another): every pair must connect and
  exchange messages, and a pairing that fails must fail with a result code, never hang. The automated tests check
  the objects' owner, DACL and integrity label, but a pairing across elevations needs a person to accept the UAC
  prompt. Reports welcome.

## Known issues

None known.

## Directions

Ideas rather than commitments; an issue that asks for one of them helps decide.

- **arm64** (Linux and Windows): a new target, with its own floor in
  [`platform-support.md`](platform-support.md).
- **128-byte cache lines on Apple Silicon.** A ring's `head` and `tail` sit 64 bytes apart in the segment
  ([`protocol.md`](protocol.md) §5.1), which is one 128-byte line on the M1, so the writer and the reader of a ring
  share that line. Moving them apart changes the segment's layout (a protocol version); the process-local lines could
  be aligned to 128 bytes on macOS alone.
