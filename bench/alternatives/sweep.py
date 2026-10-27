"""Sweeps the comparison's buffer sizes (run.py --buffer), and chooses each library's size per pattern from the sweeps.

    python bench/alternatives/sweep.py run [--runs 3] [--buffers 65536,524288,4194304,33554432] [--cases ...]
                                           [--libs ...] [--aeron-conductor spin|backoff] [--aeron-fragment-limit N]
                                           [--out sweeps/<platform>.json]
    python bench/alternatives/sweep.py run --libs aeron --aeron-conductor backoff --out sweeps/<platform>-aeron-backoff.json
    python bench/alternatives/sweep.py run --libs aeron --aeron-conductor backoff --aeron-fragment-limit 1 \
                                           --out sweeps/<platform>-aeron-backoff-f1.json
    python bench/alternatives/sweep.py choose sweeps/<platform>.json sweeps/<platform>-aeron-backoff.json ...

run: every case at every size, one run.py round of one run per size, the sizes alternating round by round (each round
starts one size later), so that a drift of the machine's state spreads over every size alike; then each case's runs
per library and size, and their medians, into one file (sweeps/<platform>.json by default).

choose: one platform's sweeps (a platform is an OS and an architecture: each library's configuration is the fastest for
it there). For each library and pattern (stream, with the rtt cases; broadcast; queue; service), the configuration
whose geometric mean of the pattern's headline figures (msgs/s, calls/s, or for an rtt case 1 / p50) over every swept
case of the pattern the library runs (a broadcast library may run only the lossless cases or only the lossy one) is
the best, the fastest (a tie goes to the smaller buffer, then Aeron's default conductor, then its default fragment
limit, 16). A configuration is a buffer size and, for Aeron, its conductor's idle strategy and its fragment
limit: a sweep runs one of each (run --aeron-conductor, --aeron-fragment-limit; a sweep that doesn't record its limit
ran with 16), so each of Aeron's is a sweep of its own of the same platform. Aeron's buffer and conductor are the
fastest at the comparison's fragment limit (run.AERON_FRAGMENT_LIMIT, 1); the table shows the other limits beside it.
The conductor is one per platform (the media driver serves every pattern: the most patterns'
choice). A size the library refused or failed in any of those cases isn't a candidate. Prints the table (each size's
mean against the best one; a library whose buffer isn't settable shows how far its figures move by chance alone) and
the platform's entries of run.py's BUFFERS and AERON_CONDUCTOR.
"""

import argparse
import json
import math
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import run  # noqa: E402

BUFFERS = [64 * 1024, 512 * 1024, 4 * 1024 * 1024, 32 * 1024 * 1024]
CASES = ("stream-16,stream-1024,stream-65536,rtt-16,broadcast-4-lossless-16,broadcast-4-lossless-1024,"
         "broadcast-4-lossy-16,queue-4x4-16,queue-4x4-1024,service-4x4-16,service-4x4-1024")


def pattern_of(case_id):
    return next(pattern for c, pattern, *_ in run.CASES if c == case_id)


def figure(case_id, entry):
    """The case's headline figure, larger is better (page.py `primary`): an rtt case's 1 / p50."""
    if case_id.startswith("rtt-"):
        return 1e9 / entry["p50_ns"] if entry.get("p50_ns") else None
    return entry.get("calls_per_s" if case_id.startswith("service-") else "msgs_per_s") or None


def sweep(args):
    buffers = [int(b) for b in args.buffers.split(",")]
    out = args.out or HERE / "sweeps" / f"{run.PLATFORM}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    report, results = None, {}
    with tempfile.TemporaryDirectory() as tmp:
        for round_index in range(args.runs):
            for k in range(len(buffers)):
                buffer = buffers[(round_index + k) % len(buffers)]
                part = Path(tmp) / f"r{round_index}-{buffer}.json"
                print(f"== round {round_index + 1} of {args.runs}, buffer {buffer}", flush=True)
                cmd = [sys.executable, str(HERE / "run.py"), "--runs", "1", "--buffer", str(buffer), "--cases",
                       args.cases, "--out", str(part)]
                if args.libs:
                    cmd += ["--libs", args.libs]
                if args.aeron_conductor:
                    cmd += ["--aeron-conductor", args.aeron_conductor]
                if args.aeron_fragment_limit:
                    cmd += ["--aeron-fragment-limit", str(args.aeron_fragment_limit)]
                subprocess.run(cmd, check=True)
                data = json.loads(part.read_text())
                report = report or {k: v for k, v in data.items() if k not in ("results", "buffer", "runs")}
                for case_id, libs in data["results"].items():
                    for lib, entry in libs.items():
                        slot = results.setdefault(case_id, {}).setdefault(lib, {}).setdefault(
                            str(buffer), {"runs": [], "buffer_bytes": entry.get("buffer_bytes")})
                        slot["runs"] += entry["runs"]
                        for key in ("skipped", "error"):
                            if key in entry:
                                slot.setdefault(key + "s", []).append(entry[key])
    for libs in results.values():
        for sizes in libs.values():
            for slot in sizes.values():
                if slot["runs"]:
                    for metric in slot["runs"][0]:
                        slot[metric] = statistics.median(r[metric] for r in slot["runs"])
    report.update({"runs": args.runs, "buffers": buffers, "cases": args.cases.split(","), "results": results})
    out.write_text(json.dumps(report, indent=1), newline="\n")
    print(f"written: {out}")


def limit_of(sweep):
    """The fragment limit Aeron ran with in a sweep: one for every pattern (sweeps before it was recorded: 16)."""
    limit = sweep.get("aeron_fragment_limit") or 16
    return limit if isinstance(limit, int) else 16


def platform_of(report):
    """The platform (run.PLATFORM) a sweep ran on; sweeps from before the key was recorded ran on x86-64."""
    return report.get("platform_key") or report["os"]


def choose(args):
    sweeps = [json.loads(Path(f).read_text()) for f in args.sweeps]
    platforms = {platform_of(s) for s in sweeps}
    if len(platforms) != 1:
        sys.exit(f"the sweeps are of {len(platforms)} platforms ({', '.join(sorted(platforms))}): give one platform's")
    platform = platforms.pop()
    buffers = sweeps[0]["buffers"]
    libs = sorted({lib for s in sweeps for libs in s["results"].values() for lib in libs},
                  key=lambda lib: run.LIBS.index(lib))
    patterns = ["stream", "broadcast", "queue", "service"]
    # A configuration is a buffer size and, for Aeron, its conductor's idle strategy and its fragment limit (each sweep
    # runs one of each): (conductor, limit), the variant; None for every other library.
    aeron = lambda s, lib: ((s.get("aeron_conductor") or "spin", limit_of(s))
                            if lib == "aeron" else None)
    # Aeron's defaults first: its conductor (backoff), then its fragment limit (16).
    default_last = lambda v: (v[0] != "backoff", v[1] != 16) if v else ()
    chosen, conductors, rows = {}, {}, []
    for lib in libs:
        variants = sorted({aeron(s, lib) for s in sweeps if any(lib in r for r in s["results"].values())},
                          key=lambda v: (*default_last(v), v or ()))
        for pattern in patterns:
            means = {}
            for variant in variants:
                runs = [(s, c) for s in sweeps if aeron(s, lib) == variant for c in s["cases"]
                        if pattern_of(c) == pattern and lib in s["results"].get(c, {})]
                for buffer in buffers:
                    figures = [figure(c, s["results"][c][lib].get(str(buffer), {})) for s, c in runs]
                    if runs and all(figures):
                        means[(buffer, variant)] = math.exp(sum(map(math.log, figures)) / len(figures))
            if not any(1 for s in sweeps for c in s["cases"] if pattern_of(c) == pattern and lib in s["results"].get(c, {})):
                continue
            if not means:
                rows.append((lib, pattern, None, variants, means))
                continue
            # The fastest; a tie goes to the smaller buffer, then Aeron's default conductor, then its default fragment limit.
            fastest = lambda keys: min((k for k in keys if means[k] == max(means[j] for j in keys)),
                                       key=lambda k: (k[0], *default_last(k[1])))
            pick = fastest(list(means))
            if lib == "aeron":
                # Aeron's buffer and conductor at the comparison's fragment limit (run.AERON_FRAGMENT_LIMIT).
                headline = [k for k in means if k[1][1] == run.AERON_FRAGMENT_LIMIT]
                pick = fastest(headline) if headline else None
                if not pick:
                    rows.append((lib, pattern, None, variants, means))
                    continue
            rows.append((lib, pattern, pick, variants, means))
            if lib not in run.UNSIZED:
                chosen.setdefault(lib, {})[pattern] = pick[0]
            if pick[1]:
                conductors.setdefault(pick[1][0], []).append(pattern)
    label = lambda b: f"{b // 1024 // 1024} MiB" if b >= 1024 * 1024 else f"{b // 1024} KiB"
    print(f"## {platform}\n")
    print(f"| Library | Pattern | Chosen | {' | '.join(label(b) for b in buffers)} |")
    print(f"|---|---|---|{'---|' * len(buffers)}")
    for lib, pattern, pick, variants, means in rows:
        top = max(means.values()) if means else None  # against the fastest of every variant
        for variant in variants:
            fragments = lambda n: f"{n} fragment{'s' if n != 1 else ''}"
            name = f"{lib} ({variant[0]} conductor, {fragments(variant[1])})" if variant else lib
            cells = [f"{means[(b, variant)] / top:.2f}" if (b, variant) in means else "-" for b in buffers]
            shown = ("(not settable)" if lib in run.UNSIZED else label(pick[0]) if pick and pick[1] == variant else
                     "" if pick else "-")
            print(f"| {name} | {pattern} | {shown} | {' | '.join(cells)} |")
    print()
    size = lambda b: f"{b // 1024 // 1024} * MiB" if b >= 1024 * 1024 else f"{b // 1024} * KiB"
    print(f'"{platform}": {{')
    for lib, sizes in chosen.items():
        print(f'    "{lib}": {{' + ", ".join(f'"{p}": {size(b)}' for p, b in sizes.items()) + "},")
    print("},")
    if conductors:
        if len(conductors) > 1:
            print(f"Aeron's patterns chose different conductors: {conductors}; AERON_CONDUCTOR takes one per platform")
        best = max(conductors, key=lambda c: len(conductors[c]))
        print(f'AERON_CONDUCTOR["{platform}"] = "{best}"')


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("run")
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--buffers", default=",".join(map(str, BUFFERS)))
    p.add_argument("--cases", default=CASES)
    p.add_argument("--libs", default="")
    p.add_argument("--aeron-conductor", choices=("spin", "backoff"))
    p.add_argument("--aeron-fragment-limit", type=int, default=16, metavar="N")
    p.add_argument("--out", type=Path)
    p = sub.add_parser("choose")
    p.add_argument("sweeps", nargs="+")
    args = parser.parse_args()
    sweep(args) if args.command == "run" else choose(args)


if __name__ == "__main__":
    main()
