"""
Performance gates.

`bench-compare --ab` (the gate for library changes): the Zig benchmark suite (bench/zig) against this
tree's library and against another revision's library that speaks include/fipc.h, alternating in one session. Each run of
`fipc_bench` writes one JSON object per case (`--json`); a metric's value per side is the median over
the runs. A gated metric passes when this tree keeps at least its tolerance of the reference: the ratio
is new/reference for rates and reference/new for latencies, so above 1 means this tree is better. A quick run
by default: a scenario with a failing case is re-checked once, and judged on all its rounds.
docs/perf/baseline.md explains the noise.

`bench-compare` without `--ab`: the C# and Python benchmarks' medians against the baseline table in
docs/perf/bindings-baseline.md, from their "Throughput: <n> messages/sec" lines under "Test <i>/<n>: <name>".
"""

import json
import re
import statistics
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

THRESHOLD = 0.95

# The scenarios of fipc_bench (`fipc_bench list`); devtool names them zig-<scenario>
ZIG_SCENARIOS = (
    "fastipc",
    "fastipc-zerocopy",
    "rpc",
    "latency-spin",
    "latency-sleep",
    "wakeup",
    "wakeup-poll",
    "setup",
)

# The scenarios with cases in the performance gates (`fipc_bench gates`): all but wakeup-poll, which is shown only
GATE_SCENARIOS = tuple(name for name in ZIG_SCENARIOS if name != "wakeup-poll")

# The gated metrics and the least ratio each must keep. AB_INFO metrics, and every metric of the AB_INFO_SCENARIOS,
# are shown, not gated: a sleeping receiver's wake-up latency is bimodal on the baseline machine (one A/A run's p50
# differed by 24%, docs/perf/baseline.md)
AB_GATES = {"msgs_per_s": THRESHOLD, "cycles_per_s": 0.90, "p50_ns": 0.90}
AB_INFO = ("p99_ns", "cycle_p99_ns")
AB_INFO_SCENARIOS = ("latency-sleep", "wakeup")

_TEST = re.compile(r"Test (\d+)/\d+: (.+?)\s*[│|]?\s*$")
_THROUGHPUT = re.compile(r"Throughput: ([\d,.]+) messages/sec")
_BASELINE_ROW = re.compile(r"^\|\s*([\w-]+)\s*\|\s*(\d+\. [^|]+?)\s*\|([^|]+)\|([^|]+)\|\s*$")
_RATE = re.compile(r"([\d.]+)\s*([KM]?)")

Key = Tuple[str, str]  # (bench, "1. Tiny messages (16B)")


def parse_output(text: str) -> Dict[str, float]:
    """Messages/sec per test ("<i>. <name>") in one bench run's output."""
    rates: Dict[str, float] = {}
    test = None
    for line in text.splitlines():
        match = _TEST.search(line)
        if match:
            test = f"{match.group(1)}. {match.group(2).strip()}"
            continue
        match = _THROUGHPUT.search(line)
        if match and test:
            rates[test] = float(match.group(1).replace(",", ""))
            test = None
    return rates


def _parse_rate(text: str) -> float:
    match = _RATE.match(text.strip())
    if not match:
        raise ValueError(f"not a rate: {text!r}")
    return float(match.group(1)) * {"": 1.0, "K": 1e3, "M": 1e6}[match.group(2)]


# host_os -> its column of medians in the bindings' baseline table (after the Bench and Test columns)
BASELINE_COLUMNS = {"windows": 2, "linux": 3}


def load_baseline(path: Path, host_os: str) -> Dict[Key, float]:
    """Baseline medians for host_os ('windows' or 'linux') from the benchmark table."""
    column = BASELINE_COLUMNS[host_os]
    baseline: Dict[Key, float] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        match = _BASELINE_ROW.match(line)
        if match and match.group(1) != "Bench":
            median = match.group(column + 1).split("[")[0]
            baseline[(match.group(1), match.group(2))] = _parse_rate(median)
    return baseline


def _format_rate(rate: float) -> str:
    if rate >= 1e6:
        return f"{rate / 1e6:.2f}M"
    if rate >= 1e3:
        return f"{rate / 1e3:.1f}K"
    return f"{rate:.0f}"


def report(samples: Dict[Key, List[float]], baseline: Dict[Key, float], benches: List[str]) -> bool:
    """Print one line per baseline test of `benches`; returns True if every test passes."""
    passed = failed = 0
    print(f"\n{'bench':<32} {'test':<42} {'baseline':>9} {'median':>9} {'[min - max]':>19} {'ratio':>6}")
    for bench in benches:
        keys = sorted({k for k in baseline if k[0] == bench} | {k for k in samples if k[0] == bench},
                      key=lambda k: int(k[1].split(".")[0]))
        for key in keys:
            runs = samples.get(key, [])
            reference = baseline.get(key)
            if not runs:
                print(f"{bench:<32} {key[1]:<42} {_format_rate(reference):>9} {'-':>9} {'':>19} {'':>6}  FAIL (no result)")
                failed += 1
                continue
            median = statistics.median(runs)
            spread = f"[{_format_rate(min(runs))} - {_format_rate(max(runs))}]"
            if reference is None:
                print(f"{bench:<32} {key[1]:<42} {'-':>9} {_format_rate(median):>9} {spread:>19} {'':>6}  (no baseline)")
                continue
            ratio = median / reference
            ok = ratio >= THRESHOLD
            passed += ok
            failed += not ok
            print(f"{bench:<32} {key[1]:<42} {_format_rate(reference):>9} {_format_rate(median):>9} {spread:>19} "
                  f"{ratio:>6.2f}  {'PASS' if ok else 'FAIL'}")
    print(f"\n{passed} passed, {failed} failed (pass: median >= {THRESHOLD:.0%} of the baseline median)")
    return failed == 0


# === The Zig suite's A/B ===

AbKey = Tuple[str, str, str]  # (scenario, case, metric)


def load_records(path: Path) -> List[dict]:
    """The JSON objects fipc_bench wrote with --json, one per case."""
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def add_records(samples: Dict[AbKey, List[float]], order: List[AbKey], records: List[dict]) -> None:
    """Adds each gated or shown metric's median of each record to `samples` (and new keys to `order`)."""
    for record in records:
        for metric, values in record["metrics"].items():
            if metric in AB_GATES or metric in AB_INFO:
                key = (record["scenario"], record["case"], metric)
                if key not in samples and key not in order:
                    order.append(key)
                samples.setdefault(key, []).append(values["median"])


def _format_value(metric: str, value: float) -> str:
    if metric.endswith("_ns"):
        if value >= 1e6:
            return f"{value / 1e6:.2f} ms"
        if value >= 1e3:
            return f"{value / 1e3:.2f} us"
        return f"{value:.0f} ns"
    return _format_rate(value)


def _median_and_range(metric: str, runs: List[float]) -> str:
    return (f"{_format_value(metric, statistics.median(runs))} "
            f"({_format_value(metric, min(runs))}-{_format_value(metric, max(runs))})")


def report_zig_ab(new: Dict[AbKey, List[float]], ref: Dict[AbKey, List[float]], order: List[AbKey],
                  failed_scenarios: Optional[Set[str]] = None) -> bool:
    """Print one line per case and metric: the reference's and this tree's medians (with their ranges)
    and their ratio; returns True if every gated metric keeps its tolerance. Adds the scenarios with a
    failing case to `failed_scenarios`."""
    passed = failed = 0
    print(f"\n{'scenario':<28} {'case':<12} {'metric':<13} {'reference':>31} {'this tree':>31} {'ratio':>6}")
    for key in order:
        scenario, case, metric = key
        if key not in new or key not in ref:
            print(f"{scenario:<28} {case:<12} {metric:<13}  FAIL (missing on one side)")
            failed += 1
            if failed_scenarios is not None:
                failed_scenarios.add(scenario)
            continue
        new_median = statistics.median(new[key])
        ref_median = statistics.median(ref[key])
        higher_is_better = not metric.endswith("_ns")
        ratio = new_median / ref_median if higher_is_better else ref_median / new_median
        if metric in AB_GATES and scenario not in AB_INFO_SCENARIOS:
            ok = ratio >= AB_GATES[metric]
            passed += ok
            failed += not ok
            verdict = "PASS" if ok else "FAIL"
            if not ok and failed_scenarios is not None:
                failed_scenarios.add(scenario)
        else:
            verdict = "info"
        print(f"{scenario:<28} {case:<12} {metric:<13} {_median_and_range(metric, ref[key]):>31} "
              f"{_median_and_range(metric, new[key]):>31} {ratio:>6.2f}  {verdict}")
    tolerances = ", ".join(f"{metric} {tolerance:.0%}" for metric, tolerance in AB_GATES.items())
    print(f"\n{passed} passed, {failed} failed (ratio >1: this tree is better; least ratios: {tolerances}; "
          f"{' and '.join(AB_INFO_SCENARIOS)}: information only)")
    return failed == 0
