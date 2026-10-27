"""Writes the comparison's charts and tables (website/performance.html, and the overview's head-to-head in
website/index.html and README.md, with README.md's charts as SVG files in website/assets/charts/) from the results in
results/.

    python bench/alternatives/page.py

results/<series>.json and results/<series>-poll.json (run.py --out, without and with --poll) hold a machine's runs;
the series are linux (x86-64), windows, macos and linux-arm64, and a missing one is left out. The batched comparison
(run.py --batch, FastIPC and Aeron with both ends batching) is results/<series>-batched[-poll].json for batches of 16
and -batched-64[-poll].json for batches of 64; a series without them shows a dash there. Each library shows in
its best configuration per case and series: the faster of its variants (fastipc's copying or zero-copy calls,
ZeroMQ's and NNG's ipc:// or TCP) and of the two receive modes, which its tooltip names. The page's generated parts
sit between <!-- generated:NAME --> and <!-- /generated:NAME -->; everything else in them is written by hand.
"""

import html
import math
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import run  # noqa: E402
RESULTS = HERE / "results"
ROOT = HERE.parent.parent
PAGES = [ROOT / "website" / name for name in ("performance.html", "index.html")] + [ROOT / "README.md"]
CHARTS = ROOT / "website" / "assets" / "charts"

SERIES = [
    ("linux", "lx", "Linux x86-64"),
    ("windows", "wn", "Windows"),
    ("macos", "mc", "macOS"),
    ("linux-arm64", "la", "Linux ARM64"),
]
# the machine each series ran on: the overview's head over its columns
MACHINE = {"linux": "Intel Core i9 laptop", "windows": "Intel Core i9 laptop", "macos": "MacBook Air M1",
           "linux-arm64": "MacBook Air M1"}

# name, variants (run.py's libraries), how it is reached
LIBRARIES = [
    ("FastIPC", ["fastipc", "fastipc-zerocopy"]),
    ("iceoryx2", ["iceoryx2"]),
    ("Aeron", ["aeron"]),
    ("ZeroMQ", ["zeromq", "zeromq-tcp"]),
    ("NNG", ["nng", "nng-tcp"]),
    ("cpp-ipc", ["cpp-ipc"]),
    ("Boost.Interprocess", ["boost"]),
    ("Unix socket", ["unix-socket"]),
    ("TCP loopback", ["tcp-loopback"]),
]
VARIANT_LABEL = {
    "fastipc": "copying calls",
    "fastipc-zerocopy": "zero-copy calls",
    "zeromq": "ipc://",
    "zeromq-tcp": "TCP",
    "nng": "ipc://",
    "nng-tcp": "TCP",
}

KiB = 1024

# Libraries whose receive never blocks: the harness polls theirs in both receive modes.
POLL_ONLY = {"iceoryx2", "iceoryx2-cxx", "aeron"}

def size_label(size):
    return f"{size // KiB} KiB" if size >= KiB else f"{size} B"


def bytes_label(size):
    if size >= KiB * KiB:
        mib = size / KiB / KiB
        return f"{mib:.0f} MiB" if mib == int(mib) else f"{mib:.1f} MiB"
    return size_label(int(size))


# What a library's buffer is called in a tooltip; the others' are their buffers of messages.
BUFFER_NAME = {"fastipc": "ring", "fastipc-zerocopy": "ring", "aeron": "window"}

PATTERNS = [("stream", "Stream"), ("broadcast", "Broadcast"), ("queue", "Work queue"), ("service", "Service")]


def mapped(lib, pattern, buffer):
    """The shared memory a buffer of `buffer` bytes maps in the pattern's charted case (a stream, a broadcast to four
    subscribers, a work queue and a service of 4 × 4), for FastIPC (docs/protocol.md: a stream's two rings, a ring's
    cells half its size again, a queue's ring per producer and its return ring, a service's ring per client) and Aeron
    (a log of three terms, each twice the window); None for the others."""
    if lib.startswith("fastipc"):
        return buffer * {"stream": 2, "broadcast": 1.5, "queue": 1.5 * 5, "service": 1.5 * 4}[pattern]
    if lib == "aeron":
        return 6 * buffer
    return None


def load(suffix=""):
    """{series: [(poll, report)]} of the series whose results exist; `suffix` picks the batched ones (BATCHES)."""
    data = {}
    for key, _, _ in SERIES:
        runs = []
        for poll, name in ((False, f"{key}{suffix}.json"), (True, f"{key}{suffix}-poll.json")):
            path = RESULTS / name
            if path.exists():
                runs.append((poll, json.loads(path.read_text())))
        if runs:
            data[key] = runs
    return data


def primary(case_id):
    """The metric a case is ranked by, and whether higher is better."""
    if case_id.startswith("rtt-"):
        return "p50_ns", False
    if case_id.startswith("service-"):
        return "calls_per_s", True
    return "msgs_per_s", True


def best(data, series, case_id, variants):
    """The best entry of a library in a case: (entry, variant, polled), or None if it lacks the case."""
    metric, higher = primary(case_id)
    found = []
    for poll, report in data.get(series, []):
        for variant in variants:
            entry = report["results"].get(case_id, {}).get(variant)
            if entry and metric in entry and not entry.get("error"):
                found.append((entry, variant, poll))
    if not found:
        return None
    return (max if higher else min)(found, key=lambda f: f[0][metric])


def fmt_rate(value):
    for unit, scale in (("M", 1e6), ("K", 1e3)):
        if value >= scale:
            digits = 2 if value < 10 * scale else 1
            return f"{value / scale:.{digits}f}{unit}"
    return f"{value:.0f}"


def fmt_ns(value):
    if value >= 1e6:
        return f"{value / 1e6:.1f} ms"
    if value >= 1e4:
        return f"{value / 1e3:.0f} µs"
    if value >= 1e3:
        return f"{value / 1e3:.1f} µs"
    return f"{value:.0f} ns"


def aeron_leads(data, series, case_id):
    """Whether Aeron's best figure beats FastIPC's in a case and series: the cases the note under the scoreboard is
    about, each marked with its asterisk."""
    ours, theirs = value(data, series, case_id, "FastIPC"), value(data, series, case_id, "Aeron")
    return ours is not None and theirs is not None and ratio(case_id, ours, theirs) < 1


def mark(page=""):
    """The bold asterisk that links to the note on Aeron's leads, the page's #aeron-note (performance.html's note, the
    overview's line under its datasheet), or with `page` another page's."""
    return f'<a class="fn" href="{page}#aeron-note" aria-label="note on Aeron\'s leads">*</a>'


def how(variant, poll, entry, case_id):
    parts = []
    if variant in VARIANT_LABEL:
        parts.append(VARIANT_LABEL[variant])
    parts.append("polling receive" if poll or variant in POLL_ONLY else "waiting receive")
    if entry.get("buffer_bytes"):
        parts.append(f"{bytes_label(entry['buffer_bytes'])} {BUFFER_NAME.get(variant, 'buffer')}")
    if variant == "aeron" and entry.get("fragment_limit", run.AERON_FRAGMENT_LIMIT) != run.AERON_FRAGMENT_LIMIT:
        parts.append(f"up to {entry['fragment_limit']} messages per poll")
    if case_id.startswith("broadcast-") and entry.get("lost_pct", 0) >= 0.05:
        parts.append(f"{entry['lost_pct']:.1f}% lost")
    return ", ".join(parts)


def ranked(data, case_id, series):
    """(name, [best entry per series]) of each library that ran the case: FastIPC first, then the others best first by
    their geometric mean over the series they ran in, so that no machine's scale outweighs another's, and the closest
    rivals sit next to each other."""
    metric, higher = primary(case_id)
    rows = []
    for name, variants in LIBRARIES:
        found = [best(data, key, case_id, variants) for key, _, _ in series]
        if any(found):
            rows.append((name, found))

    def rank(row):
        name, found = row
        logs = [math.log(f[0][metric]) for f in found if f]
        mean = sum(logs) / len(logs)
        return (name != "FastIPC", -mean if higher else mean)

    return sorted(rows, key=rank)


def chart(data, case_id, title, note="", log_floor=None):
    """A bar chart of one case: a group per library (in `ranked` order), a bar per series. Round trips: shorter bars
    are better. With log_floor, bar lengths follow the logarithm of value / log_floor (latencies that span three
    decades)."""
    metric, higher = primary(case_id)
    series = [s for s in SERIES if s[0] in data]
    rows = [(name, [(key, cls, label, f) for (key, cls, label), f in zip(series, found)])
            for name, found in ranked(data, case_id, series)]
    values = [c[3][0][metric] for _, cells in rows for c in cells if c[3]]
    top = max(values) if values else 1
    out = [f'<div class="chart" role="figure" aria-label="{html.escape(title)}">']
    out.append(f'  <div class="chart-title"><h3>{html.escape(title)}</h3>{legend(series)}</div>')
    out.append('  <div class="bars">')
    for name, cells in rows:
        lib_class = ' class="lbl self"' if name == "FastIPC" else ' class="lbl"'
        out.append(f'    <div class="grp"><div{lib_class}>{html.escape(name)}</div><div class="track">')
        for key, cls, label, found in cells:
            if not found:
                out.append(f'      <div class="bar {cls} none"><b>—</b></div>')
                continue
            entry, variant, poll = found
            value = entry[metric]
            text = fmt_ns(value) if metric.endswith("_ns") else fmt_rate(value)
            if case_id.startswith("broadcast-") and entry.get("lost_pct", 0) >= 0.05:
                text += f" · {entry['lost_pct']:.0f}% lost"
            share = math.log(value / log_floor) / math.log(top / log_floor) if log_floor else value / top
            tip = f"{name} · {label} · {text} ({how(variant, poll, entry, case_id)})"
            star = mark() if name == "Aeron" and aeron_leads(data, key, case_id) else ""
            out.append(
                f'      <div class="bar {cls}" tabindex="0" data-tip="{html.escape(tip)}">'
                f'<span class="fill" style="width:calc((100% - 6.5em) * {share:.4f})"></span><b>{html.escape(text)}{star}</b></div>'
            )
        out.append("    </div></div>")
    out.append("  </div>")
    if note:
        out.append(f'  <p class="note chart-note">{note}</p>')
    out.append("</div>")
    return "\n".join(out)


def legend(series):
    spans = "".join(f'<span><i style="background:var(--s-{v})"></i>{html.escape(label)}</span>'
                    for _, v, label in [(k, {"lx": "linux", "wn": "win", "mc": "mac", "la": "arm"}[c], l)
                                        for k, c, l in series])
    return f'<div class="legend" aria-hidden="true"><span class="mach">{spans}</span></div>'


def table(data, case_ids, caption_of):
    """Every library's best figure in each case (in `ranked` order), a column per series."""
    series = [s for s in SERIES if s[0] in data]
    out = ['<div class="table-wrap"><table class="data compare">']
    head = "".join(f'<th scope="col" class="n">{html.escape(label)}</th>' for _, _, label in series)
    out.append(f'<thead><tr><th scope="col">Case</th><th scope="col">Library</th>{head}</tr></thead><tbody>')
    for case_id in case_ids:
        metric, _ = primary(case_id)
        first = True
        for name, cells in ranked(data, case_id, series):
            row_class = ' class="sep"' if first else ""
            case_cell = html.escape(caption_of(case_id)) if first else ""
            first = False
            tds = []
            for (key, _, _), found in zip(series, cells):
                if not found:
                    tds.append('<td class="n muted">—</td>')
                    continue
                entry, variant, poll = found
                if metric == "p50_ns":
                    text = f"{fmt_ns(entry['p50_ns'])} <small>p99 {fmt_ns(entry['p99_ns'])}</small>"
                elif case_id.startswith("broadcast-") and entry.get("lost_pct", 0) >= 0.05:
                    text = f"{fmt_rate(entry[metric])} <small>{entry['lost_pct']:.1f}% lost</small>"
                elif case_id.startswith("service-"):
                    text = f"{fmt_rate(entry[metric])} <small>p50 {fmt_ns(entry['p50_ns'])}</small>"
                else:
                    text = fmt_rate(entry[metric])
                if name == "Aeron" and aeron_leads(data, key, case_id):
                    text += mark()
                title = html.escape(how(variant, poll, entry, case_id))
                tds.append(f'<td class="n" title="{title}">{text}</td>')
            lib = f"<b>{html.escape(name)}</b>" if name == "FastIPC" else html.escape(name)
            out.append(f'<tr{row_class}><td class="case">{case_cell}</td><td>{lib}</td>{"".join(tds)}</tr>')
    out.append("</tbody></table></div>")
    return "\n".join(out)


def caption(case_id):
    parts = case_id.split("-")
    size = size_label(int(parts[-1]))
    if parts[0] == "stream":
        return f"Stream, {size}"
    if parts[0] == "rtt":
        return f"Round trip, {size}"
    if parts[0] == "broadcast":
        return f"{parts[1]} subscriber{'s' if parts[1] != '1' else ''}, {parts[2]}, {size}"
    if parts[0] == "queue":
        return f"{parts[1].replace('x', ' × ')}, {size}"
    if parts[0] == "service":
        clients, servers = parts[1].split("x")
        return f"{clients} client{'s' if clients != '1' else ''} × {servers} server{'s' if servers != '1' else ''}, {size}"
    return case_id


def configured(data, key, variant, pattern, field):
    """A series' recorded `field` (buffer_bytes, conductor, fragment_limit) for a library in a pattern: its wait-mode
    results' first, as every case of a pattern in a series runs one configuration. A result from before the field was
    recorded ran with the usual buffer (run.buffer_bytes), a spinning conductor and a fragment limit of 16."""
    for poll, report in data.get(key, []):
        if poll:
            continue
        for case_id, libs in report["results"].items():
            if case_id.split("-")[0].replace("rtt", "stream") == pattern and variant in libs:
                if field in libs[variant]:
                    return libs[variant][field]
                if field == "buffer_bytes":
                    return run.buffer_bytes(variant, int(case_id.rsplit("-", 1)[1]), None)
                if variant == "aeron":
                    return {"conductor": "spin", "fragment_limit": 16}.get(field)
                return None
    return None


def by_series(texts):
    """{series label: text} as one text where every series agrees, else "label, label: text" per distinct text."""
    groups = {}
    for label, text in texts.items():
        groups.setdefault(text, []).append(label)
    if len(groups) == 1:
        return next(iter(groups))
    return "<br>".join(f"{', '.join(labels)}: {text}" for text, labels in groups.items())


def buffers(data):
    """Each library's buffer per pattern on each platform, as its results record it (the fastest for it there:
    run.BUFFERS), and the memory FastIPC and Aeron map with it: a row per library, a column per pattern; a pattern the
    library lacks is a dash. Then Aeron's conductor and fragment limit on each platform."""
    has = lambda variant, pattern: any(
        variant in report["results"].get(case_id, {})
        for runs in data.values() for _, report in runs for case_id in report["results"]
        if (case_id.split("-")[0].replace("rtt", "stream")) == pattern)
    labels = {key: label for key, _, label in SERIES if key in data}
    out = ['<div class="table-wrap"><table class="data">']
    head = "".join(f'<th scope="col">{label}</th>' for _, label in PATTERNS)
    out.append(f'<thead><tr><th scope="col">Library</th>{head}</tr></thead><tbody>')
    for name, variants in LIBRARIES:
        tds = []
        for pattern, _ in PATTERNS:
            present = [v for v in variants if has(v, pattern)]
            if not present:
                tds.append('<td class="muted">—</td>')
                continue
            if present[0] in run.UNSIZED:
                tds.append(f"<td>{'its fixed queue' if present[0] == 'cpp-ipc' else 'the OS&#39;s'}</td>")
                continue
            texts = {}
            for key, label in labels.items():
                sizes = []
                for v in present:
                    buffer = configured(data, key, v, pattern, "buffer_bytes")
                    text = bytes_label(buffer) if buffer else "its default"
                    if buffer and mapped(v, pattern, buffer):
                        text += f" <small>maps {bytes_label(mapped(v, pattern, buffer))}</small>"
                    if v in VARIANT_LABEL and len({configured(data, key, w, pattern, "buffer_bytes")
                                                   for w in present}) > 1:
                        text = f"{VARIANT_LABEL[v]}: {text}"
                    if text not in sizes:
                        sizes.append(text)
                texts[label] = ", ".join(sizes)
            tds.append(f"<td>{by_series(texts)}</td>")
        lib = f"<b>{html.escape(name)}</b>" if name == "FastIPC" else html.escape(name)
        out.append(f"<tr><td>{lib}</td>{''.join(tds)}</tr>")
    out.append("</tbody></table></div>")
    strategy = {"spin": "spins", "backoff": "backs off into sleeps when idle (Aeron's default)"}
    conductors = {label: strategy[configured(data, key, "aeron", "stream", "conductor") or "spin"]
                  for key, label in labels.items()}
    out.append('<p class="note">Aeron\'s media driver conductor, which moves a publisher\'s flow-control limit: '
               f"{by_series(conductors).replace('<br>', '; ')}.</p>")
    limits = {label: ", ".join(f"{configured(data, key, 'aeron', pattern, 'fragment_limit') or 16} ({pattern})"
                               for pattern in ("stream", "broadcast"))
              for key, label in labels.items()}
    note = ('<p class="note">The most messages one of Aeron\'s subscription polls took (its fragment limit; a message '
            f"up to its MTU is one fragment): {by_series(limits).replace('<br>', '; ')}.")
    batched = [label for key, label in labels.items()
               if any((configured(data, key, "aeron", pattern, "fragment_limit") or 16) != run.AERON_FRAGMENT_LIMIT
                      for pattern in ("stream", "broadcast"))]
    if batched:
        note += (f" {' and '.join(batched)} ran before the comparison took one message per poll, so Aeron took more "
                 "there; their runs at one are to come.")
    out.append(note + "</p>")
    return "\n".join(out)


def machines(data):
    items = []
    for key, _, label in SERIES:
        if key not in data:
            continue
        report = data[key][0][1]
        items.append(
            f"<li><b>{html.escape(label)}:</b> {html.escape(report['cpu'])}, {html.escape(report['platform'])}; "
            f"{report['runs']} runs of {report['duration_ms'] / 1000:g} s after {report['warmup_ms'] / 1000:g} s of "
            f"warm-up, {html.escape(report['date'])}.</li>"
        )
    versions = data[next(iter(data))][0][1].get("versions", {})
    names = {"zmq": "ZeroMQ", "nng": "NNG", "cpp-ipc": "cpp-ipc", "iceoryx2": "iceoryx2", "aeron": "Aeron",
             "boost": "Boost.Interprocess"}
    listed = ", ".join(f"{names.get(k, k)} {v.lstrip('v')}" for k, v in versions.items())
    items.append(f"<li><b>Versions:</b> {html.escape(listed)}; FastIPC 1.0.0. The harness and the C and C++ libraries "
                 "are built by MSVC 17.13 on Windows, GCC 13.3 on Linux and Apple Clang 16 on macOS, iceoryx2 "
                 "by Rust 1.99 and FastIPC by Zig 0.17.0.</li>")
    return '<ul class="plain">\n' + "\n".join(items) + "\n</ul>"


def all_case_ids(data):
    ids = []
    for runs in data.values():
        for _, report in runs:
            for case_id in report["results"]:
                if case_id not in ids:
                    ids.append(case_id)
    return ids


HEADLINE = [
    ("rtt-16", "Round trip, 16 B, p50"),
    ("stream-16", "Stream, 16 B, one way"),
    ("stream-1024", "Stream, 1 KiB, one way"),
    ("stream-65536", "Stream, 64 KiB, one way"),
    ("broadcast-4-lossless-16", "Broadcast to 4, lossless, 16 B"),
    ("queue-4x4-16", "Work queue 4 × 4, 16 B"),
    ("service-1x1-16", "Service 1 × 1, 16 B, calls"),
    ("service-4x4-16", "Service 4 × 4, 16 B, calls"),
]


def value(data, series, case_id, name):
    """A library's best figure (its primary metric) in a case, or None."""
    variants = dict(LIBRARIES)[name]
    found = best(data, series, case_id, variants)
    return found[0][primary(case_id)[0]] if found else None


def runner_up(data, series, case_id):
    """The best library other than FastIPC in a case: (name, figure), or None."""
    _, higher = primary(case_id)
    others = [(name, value(data, series, case_id, name)) for name, _ in LIBRARIES if name != "FastIPC"]
    others = [o for o in others if o[1] is not None]
    if not others:
        return None
    return (max if higher else min)(others, key=lambda o: o[1])


def fmt(case_id, figure):
    return fmt_ns(figure) if primary(case_id)[0].endswith("_ns") else fmt_rate(figure)


def ratio(case_id, ours, theirs):
    """How many times better FastIPC's figure is (below 1: worse)."""
    _, higher = primary(case_id)
    return ours / theirs if higher else theirs / ours


def fmt_ratio(r):
    return f"{r:.2f}×" if r < 10 else f"{r:.0f}×"


# A lag smaller than this (FastIPC's figure over the other's above 1 / CLOSE) is about the runs' spread: shown orange
# ("close"), not red.
CLOSE = 1.05


def standing(r):
    """The CSS classes of a ratio's word: "faster", "tie" for equal figures, "slower", or "slower close" for a lag
    within the runs' spread."""
    if r == 1:
        return "tie"
    if r > 1:
        return "faster"
    return "slower close" if 1 / r < CLOSE else "slower"


def scoreboard(data):
    """FastIPC against the fastest other library in each headline case, a column per series."""
    series = [s for s in SERIES if s[0] in data]
    head = "".join(f'<th scope="col" class="n">{html.escape(label)}</th>' for _, _, label in series)
    out = ['<div class="table-wrap"><table class="data compare scoreboard">',
           f'<thead><tr><th scope="col">Case</th>{head}</tr></thead><tbody>']
    for case_id, label in HEADLINE:
        tds = []
        for key, _, _ in series:
            ours = value(data, key, case_id, "FastIPC")
            other = runner_up(data, key, case_id)
            if ours is None or other is None:
                tds.append('<td class="n muted">—</td>')
                continue
            r = ratio(case_id, ours, other[1])
            word = "as fast" if r == 1 else "faster" if r > 1 else "slower"
            shown = r if r >= 1 else 1 / r
            star = mark() if other[0] == "Aeron" and aeron_leads(data, key, case_id) else ""
            # The ratio first, then the library it is against: "Aeron 40.7M: 1.2× faster" read as Aeron's lead.
            # Non-breaking spaces: a narrow column breaks the line only after "faster" or "than".
            tds.append(f'<td class="n"><b>{fmt(case_id, ours)}</b> <small>{fmt_ratio(shown)}&nbsp;'
                       f'<span class="{standing(r)}">{word}</span> {"as" if r == 1 else "than"} '
                       f'{html.escape(other[0])}\'s&nbsp;{fmt(case_id, other[1])}{star}</small></td>')
        out.append(f'<tr><td>{html.escape(label)}</td>{"".join(tds)}</tr>')
    out.append("</tbody></table></div>")
    out.append(aeron_details(data))
    return "\n".join(out)


def percent(fraction):
    return f"{fraction * 100:.0f}%"


def size_words(size):
    """A message size as an adjective: '16-byte', '1 KiB'."""
    return size_label(size) if size >= KiB else f"{size}-byte"


def describe(case_ids):
    """The cases in words, a lossless or lossy broadcast's subscriber counts together: 'the 16-byte stream and the
    lossless 16-byte broadcasts to 1 and 4 subscribers'."""
    parts, broadcasts = [], {}
    for case_id in case_ids:
        kind, *rest = case_id.split("-")
        size = size_words(int(rest[-1]))
        if kind == "stream":
            parts.append(f"the {size} stream")
        elif kind == "rtt":
            parts.append(f"the {size} round trip")
        elif kind == "broadcast":
            mode = f"the {rest[1]} {size} broadcasts"
            if mode not in broadcasts:
                broadcasts[mode] = []
                parts.append(mode)
            broadcasts[mode].append(rest[0])
        else:
            parts.append(caption(case_id))
    for mode, counts in broadcasts.items():
        subscribers = "subscriber" if counts == ["1"] else "subscribers"
        parts[parts.index(mode)] = f"{mode if len(counts) > 1 else mode[:-1]} to {join(counts)} {subscribers}"
    return join(parts)


def aeron_lead_cases(data):
    """[(series label, [case ids where Aeron leads])] for the series where it leads in any."""
    out = []
    for key, _, label in SERIES:
        cases = [c for c in all_case_ids(data) if aeron_leads(data, key, c)]
        if cases:
            out.append((label, cases))
    return out


def against(aeron, fastipc, first):
    """Aeron's figure against FastIPC's in words, within the runs' spread (CLOSE) 'about as many': the first of a
    list says 'messages than FastIPC', the others leave it out."""
    r = aeron / fastipc
    if 1 / CLOSE < r < CLOSE:
        return "about as many messages as FastIPC" if first else "about as many"
    word = f"{percent(r - 1)} more" if r > 1 else f"{percent(1 - r)} fewer"
    return f"{word} messages than FastIPC" if first else word


def spinning_conductors(data):
    """The series whose Aeron media driver ran with a spinning conductor (its fastest setting there)."""
    return [label for key, _, label in SERIES
            if key in data and configured(data, key, "aeron", "stream", "conductor") == "spin"]


def aeron_details(data):
    """The performance page's subsection on Aeron (its #aeron-note, where every * links): where it leads, how each
    library's receiver waits, and Aeron's media driver and its conductor per series."""
    leads = aeron_lead_cases(data)
    out = ['<h3 style="margin-top:24px" id="aeron-note">Aeron in detail</h3>', '<div class="paragraphs">']
    if leads:
        where = "; ".join(f"on {series} in {describe(ids)}" for series, ids in leads)
        out.append(f'<p class="note"><b>*</b> Aeron leads FastIPC {where}.</p>')
    out.append('<p class="note">Both libraries can let a receiver sleep once it has spun briefly on an empty buffer; '
               "they differ in what ends the sleep. FastIPC's sender wakes a sleeping receiver as it sends (a futex "
               "on Linux, its counterparts on Windows and macOS), so the message is taken as soon as the wake-up "
               "lands. Aeron's receive never blocks: between empty polls, the application's idle strategy spins, "
               "yields or sleeps for a set time, and nothing cuts a sleep short, so a message that arrives during one "
               "waits for it to end. An Aeron receiver that sleeps frees its core at the cost of that delay; one that "
               "never sleeps keeps a core 100% busy even while no messages arrive. Every benchmark here polls Aeron "
               "without a pause, its lowest-latency setting. Each FastIPC figure is the better of its waiting "
               "receive and a polling one, which keeps a core busy like Aeron's.</p>")
    spin = spinning_conductors(data)
    others = [label for key, _, label in SERIES if key in data and label not in spin]
    if spin and others:
        conductor = (f"spins on {join(spin)} and backs off into sleeps when idle on {join(others)}, the fastest "
                     "setting measured on each. Where it spins, it keeps a second core busy")
    elif spin:
        conductor = "spins, its fastest setting here, which keeps a second core busy"
    else:
        conductor = "backs off into sleeps when idle, its fastest setting here"
    out.append('<p class="note">Aeron also needs its media driver running, as a process of its own (as here) or '
               "embedded in one of the applications. The driver's "
               f"conductor, which moves a publisher's flow-control limit, {conductor}. The receiver polls the same "
               "way under either setting.</p>")
    out.append("</div>")
    return "\n".join(out)


def hero_note(data):
    """The overview's line on the cases where Aeron leads, under its datasheet (its #aeron-note), or "" while it
    leads in none."""
    if not aeron_lead_cases(data):
        return ""
    spin = spinning_conductors(data)
    driver = f"; on {join(spin)} its media driver spins too" if spin else ""
    return ("<br><span id=\"aeron-note\"><b>*</b> In every benchmark, Aeron's receiver polls in a loop and never "
            f"sleeps, which keeps a core 100% busy{driver} "
            '(<a href="performance.html#aeron-note">Aeron in detail</a>).</span>')


HERO = [
    ("rtt-16", "Round trip, 16&nbsp;B", "Latency (ns, median)"),
    ("stream-16", "Stream, 16&nbsp;B", "Throughput (messages/s)"),
    ("stream-1024", "Stream, 1&nbsp;KiB", "Throughput (messages/s)"),
    ("broadcast-4-lossless-16", "Broadcast to 4", "Throughput (messages/s)"),
    ("queue-4x4-16", "Work queue 4&nbsp;×&nbsp;4", "Throughput (messages/s)"),
    ("service-1x1-16", "Service calls", "Throughput (calls/s)"),
]
SHORT = {"Boost.Interprocess": "Boost", "TCP loopback": "TCP"}


def hero(data):
    """The overview's datasheet: FastIPC's figure over the fastest other library's in a few cases (round trips: theirs
    over FastIPC's, so that above 1 is always faster), that library named under it, a column per series."""
    series = [s for s in SERIES if s[0] in data]
    machines = []
    for key, _, _ in series:
        if machines and machines[-1][0] == MACHINE[key]:
            machines[-1][1] += 1
        else:
            machines.append([MACHINE[key], 1])
    hw = "".join(f'<th scope="colgroup" colspan="{n}">{html.escape(m)}</th>' for m, n in machines)
    head = "".join(f'<th scope="col" class="n">{html.escape(label).replace("-", "&#8209;")}</th>'
                   for _, _, label in series)
    out = ['<div class="sheet-scroll"><table>',
           f'  <thead><tr class="hw"><td></td>{hw}</tr>',
           f'    <tr><th scope="col">Case</th>{head}</tr></thead>',
           '  <tbody>']
    for case_id, label, metric in HERO:
        tds = []
        for key, _, _ in series:
            ours = value(data, key, case_id, "FastIPC")
            other = runner_up(data, key, case_id)
            if ours is None or other is None:
                tds.append('<td class="n muted">—</td>')
                continue
            r = ratio(case_id, ours, other[1])
            tip = f"FastIPC {fmt(case_id, ours)}, {other[0]} {fmt(case_id, other[1])}"
            star = mark("performance.html") if other[0] == "Aeron" and aeron_leads(data, key, case_id) else ""
            tds.append(f'<td class="n" title="{html.escape(tip)}"><b class="{standing(r)}">{fmt_ratio(r)}{star}</b>'
                       f'<small>{html.escape(SHORT.get(other[0], other[0]))}</small></td>')
        out.append(f'    <tr><td class="case">{label}<small>{html.escape(metric)}</small></td>{"".join(tds)}</tr>')
    out.append("  </tbody>")
    out.append("</table></div>")
    return "\n".join(out)


def headline_record(data, where):
    """'FastIPC is the fastest, or level with the fastest, in 29 of the 32 measurements of <where>; Aeron leads the
    other 3' (the headline cases, HEADLINE)."""
    series = [s for s in SERIES if s[0] in data]
    first = total = 0
    others = set()
    for case_id, _ in HEADLINE:
        for key, _, _ in series:
            ours, other = value(data, key, case_id, "FastIPC"), runner_up(data, key, case_id)
            if ours is None or other is None:
                continue
            total += 1
            if ratio(case_id, ours, other[1]) >= 1:
                first += 1
            else:
                others.add(other[0])
    if first == total:
        return f"FastIPC is the fastest in every one of the {total} measurements of {where}"
    return (f"FastIPC is the fastest, or level with the fastest, in {first} of the {total} measurements of {where}; "
            f"{join(sorted(others))} {'leads' if len(others) == 1 else 'lead'} the other {total - first}")


CASES_LINK = "the performance page's [case-by-case table](https://fastipc.github.io/fastipc/performance.html#cases)"


def readme_hero(data):
    """README.md's head-to-head: the overview's datasheet as a Markdown table (FastIPC's lead in bold, an asterisk
    where Aeron leads), with the headline record over it."""
    series = [s for s in SERIES if s[0] in data]
    plain = lambda text: text.replace("&nbsp;", " ")
    lines = [f"{headline_record(data, CASES_LINK)}.", "",
             "| Case | " + " | ".join(label for _, _, label in series) + " |",
             "|---|" + "---:|" * len(series)]
    for case_id, label, metric in HERO:
        cells = []
        for key, _, _ in series:
            ours = value(data, key, case_id, "FastIPC")
            other = runner_up(data, key, case_id)
            if ours is None or other is None:
                cells.append("—")
                continue
            r = ratio(case_id, ours, other[1])
            figure = f"**{fmt_ratio(r)}**" if r > 1 else fmt_ratio(r)
            star = "\\*" if other[0] == "Aeron" and aeron_leads(data, key, case_id) else ""
            cells.append(f"{figure}{star} vs {SHORT.get(other[0], other[0])}")
        lines.append(f"| {plain(label)} <sub>{metric.lower()}</sub> | " + " | ".join(cells) + " |")
    return "\n" + "\n".join(lines) + "\n"


# README.md's charts: (file, case, title, log floor). GitHub shows a README's SVG as an image, so each carries its own
# colours, light and dark, and no script.
SVG_CHARTS = [
    ("stream-16", "stream-16", "Stream: 16-byte messages per second, one way", None),
    ("rtt-16", "rtt-16", "Round trip, 16 B, median (shorter is better; log scale)", 100),
    ("broadcast-4", "broadcast-4-lossless-16", "Broadcast to 4 subscribers: 16-byte messages per second", None),
    ("service-1x1", "service-1x1-16", "Service, 1 client × 1 server: 16-byte calls per second", None),
]
SVG_STYLE = """<style>
  .bg { fill: #fcfbf9; stroke: #d8d7d3; } .t { fill: #1f2023; } .m { fill: #5a5c61; } .self { fill: #a64b0c; }
  .lx { fill: #4f7ca6; } .wn { fill: #d0691c; } .mc { fill: #5f6166; } .la { fill: #2a8a74; } .rule { stroke: #e5e4e0; }
  @media (prefers-color-scheme: dark) {
    .bg { fill: #1c1b1a; stroke: #3a3937; } .t { fill: #ecebe9; } .m { fill: #a4a3a1; } .self { fill: #eba02f; }
    .lx { fill: #6b9ac6; } .wn { fill: #d97a2a; } .mc { fill: #b3b2af; } .la { fill: #3cb398; } .rule { stroke: #2b2a28; }
  }
  text { font-family: -apple-system, "Segoe UI", "Helvetica Neue", Arial, sans-serif; font-size: 12px; }
  .num { font-family: ui-monospace, "SFMono-Regular", Consolas, monospace; font-size: 11px; }
  .title { font-size: 15px; font-weight: 600; } .lib { font-size: 13px; } .self.lib { font-weight: 700; }
</style>"""


def svg_chart(data, case_id, title, log_floor=None):
    """One case as an SVG bar chart, like `chart`: a group per library (in `ranked` order), a bar per series."""
    metric, _ = primary(case_id)
    series = [s for s in SERIES if s[0] in data]
    rows = ranked(data, case_id, series)
    values = [f[0][metric] for _, found in rows for f in found if f]
    top = max(values) if values else 1
    width, left, pad, bar, gap, group_gap, head = 760, 150, 16, 11, 2, 10, 64
    span = width - left - pad - 84
    height = head + len(rows) * (len(series) * (bar + gap) + group_gap) + pad
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
           f'viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(title)}">',
           SVG_STYLE,
           f'<rect class="bg" x="0.5" y="0.5" width="{width - 1}" height="{height - 1}" rx="6"/>',
           f'<text class="t title" x="{pad}" y="28">{html.escape(title)}</text>']
    x = pad
    for _, cls, label in series:
        out.append(f'<rect class="{cls}" x="{x}" y="41" width="10" height="10" rx="2"/>'
                   f'<text class="m" x="{x + 14}" y="50">{html.escape(label)}</text>')
        x += 14 + 7.2 * len(label) + 18
    y = head
    for name, found in rows:
        group = len(series) * (bar + gap)
        cls = "self lib" if name == "FastIPC" else "t lib"
        out.append(f'<line class="rule" x1="{pad}" x2="{width - pad}" y1="{y - group_gap / 2:.1f}" '
                   f'y2="{y - group_gap / 2:.1f}"/>')
        out.append(f'<text class="{cls}" x="{pad}" y="{y + group / 2 + 4:.1f}">{html.escape(name)}</text>')
        for (key, s_cls, _), f in zip(series, found):
            if not f:
                out.append(f'<text class="m num" x="{left}" y="{y + bar - 1}">—</text>')
            else:
                v = f[0][metric]
                share = math.log(v / log_floor) / math.log(top / log_floor) if log_floor else v / top
                w = max(1.5, span * share)
                text = fmt_ns(v) if metric.endswith("_ns") else fmt_rate(v)
                out.append(f'<rect class="{s_cls}" x="{left}" y="{y}" width="{w:.1f}" height="{bar}" rx="1.5"/>'
                           f'<text class="m num" x="{left + w + 5:.1f}" y="{y + bar - 1}">{html.escape(text)}</text>')
            y += bar + gap
        y += group_gap
    out.append("</svg>")
    return "\n".join(out) + "\n"


def join(parts):
    """'a', 'a and b', 'a, b and c'."""
    return parts[0] if len(parts) == 1 else ", ".join(parts[:-1]) + " and " + parts[-1]


def faq_entries(data):
    """The questions, each with a short answer drawn from the results: (question, answer HTML). The answers give the
    takeaway and point to the charts and tables, which have the figures."""
    series = [s for s in SERIES if s[0] in data]

    def split(case_id, name):
        """FastIPC against a library in a case, per series: (labels where FastIPC leads, labels within the runs' spread
        (CLOSE), [(label, ratio)] where the library leads)."""
        wins, even, losses = [], [], []
        for key, _, label in series:
            ours, theirs = value(data, key, case_id, "FastIPC"), value(data, key, case_id, name)
            if ours is None or theirs is None:
                continue
            r = ratio(case_id, ours, theirs)
            if 1 / CLOSE < r < CLOSE:
                even.append(label)
            elif r > 1:
                wins.append(label)
            else:
                losses.append((label, r))
        return wins, even, losses

    def standing_of(case_id, name, what):
        """'FastIPC leads 16-byte one-way throughput on Windows and macOS, and Aeron leads on Linux x86-64 (by 12%)';
        for a round trip 'has the lower ...'."""
        latency = case_id.startswith("rtt")
        lead = "has the lower" if latency else "leads"
        wins, even, losses = split(case_id, name)
        if wins and not even and not losses:
            return f"FastIPC {lead} {what} on every platform here"
        parts = []
        if wins:
            parts.append(f"FastIPC {lead} {what} on {join(wins)}")
        if even:
            parts.append(f"the two are about even on {join(even)}" if parts
                         else f"FastIPC and {name} are about even in {what} on {join(even)}")
        if losses:
            by = join([f"{label} (by {percent(1 / r - 1)})" for label, r in losses])
            parts.append(f"{name} {'is quicker' if latency else 'leads'} on {by}" if parts
                         else f"{name} {lead} {what} on {by}")
        return parts[0] if len(parts) == 1 else ", ".join(parts[:-1]) + ", and " + parts[-1]

    def span(case_id, name):
        """How many times FastIPC's figure is the library's, lowest to highest over the series: '3.1 to 3.6 times'."""
        rs = [ratio(case_id, value(data, key, case_id, "FastIPC"), value(data, key, case_id, name))
              for key, _, _ in series
              if value(data, key, case_id, "FastIPC") is not None and value(data, key, case_id, name) is not None]
        word = lambda r: f"{r:.1f}" if r < 10 else f"{r:,.0f}"
        lo, hi = word(min(rs)), word(max(rs))
        return f"{lo} times" if lo == hi else f"{lo} to {hi} times"

    def record(name):
        """'every one of the N measurements' FastIPC and the library share where FastIPC is faster, or 'M of the N'."""
        won = total = 0
        for case_id in all_case_ids(data):
            for key, _, _ in series:
                ours, theirs = value(data, key, case_id, "FastIPC"), value(data, key, case_id, name)
                if ours is not None and theirs is not None:
                    total += 1
                    won += ratio(case_id, ours, theirs) > 1
        return f"every one of the {total} measurements" if won == total else f"{won} of the {total} measurements"

    cap = lambda text: text[0].upper() + text[1:]
    return [
        ("What is the fastest IPC library?", [
            f"It depends on the pattern and the message. {headline_record(data, 'the case-by-case table')}. Of the libraries here, only FastIPC "
            "has all four patterns (stream, broadcast, work queue and service) in shared memory. "
            '(<a href="#cases">Case by case</a>.)']),
        ("How does FastIPC compare with Aeron?", [
            "Aeron is the closest rival. "
            f"{cap(standing_of('rtt-16', 'Aeron', 'round-trip latency'))}. "
            f"{cap(standing_of('stream-16', 'Aeron', '16-byte one-way throughput'))}. "
            f"{cap(standing_of('stream-1024', 'Aeron', 'it at 1 KiB'))}. "
            "Aeron also works across machines, over UDP. On one machine FastIPC is simpler to set up: Aeron needs a "
            "media driver running, as a process of its own or embedded in one of the applications. Either library "
            "can let an idle receiver sleep, but a sleeping Aeron receiver sees a new message only when its sleep "
            "ends, whereas FastIPC's sender wakes its receiver as soon as it sends. Aeron has no work queue or "
            'request-reply service. (<a href="#stream">Charts above</a>; details in <a href="#aeron-note">Aeron in '
            "detail</a>.)"]),
        ("Is FastIPC faster than ZeroMQ?", [
            f"Yes, in {record('ZeroMQ')} where both ran: a 16-byte round trip, for example, is "
            f"{span('rtt-16', 'ZeroMQ')} as quick. ZeroMQ passes each message through a socket and its own I/O "
            "threads, while FastIPC writes it into memory both processes map. ZeroMQ also works across machines; "
            "FastIPC works within one."]),
        ("How does FastIPC compare with iceoryx2?", [
            f"Both are shared-memory libraries without a broker. FastIPC is faster in {record('iceoryx2')} where both "
            f"ran; its request-reply calls, for example, run {span('service-1x1-16', 'iceoryx2')} as fast. iceoryx2 "
            "has no work queue and delivers each request to every server, so its servers don't share a load. It has "
            "events and a blackboard, and runs on more platforms (QNX, FreeBSD and others)."]),
        ("How does FastIPC compare with NNG (nanomsg)?", [
            "NNG passes each message through its own threads, and every hand-over between threads costs a wake-up, "
            f"so it is far slower here: a 16-byte round trip is {span('rtt-16', 'NNG')} as quick with FastIPC. NNG "
            "works across machines; FastIPC works within one."]),
        ("Is shared memory faster than a Unix domain socket?", [
            f"Yes. Here a 16-byte round trip through FastIPC's shared memory is {span('rtt-16', 'Unix socket')} as "
            "quick as over a Unix domain socket. A socket costs a system call and a copy through the kernel for "
            "every send and receive; shared memory needs neither once the connection is set up."]),
        ("Can I run these benchmarks myself?", [
            "Yes. <code>bench/alternatives</code> in the repository fetches and builds every library at the version "
            "shown and builds the harness with CMake, and <code>run.py</code> runs every case and writes the "
            "medians. The README there has the commands, and the method above says what each case does."]),
    ]


def faq(data):
    items = "\n".join(f"<dt>{html.escape(q)}</dt>\n<dd>{''.join(f'<p>{p}</p>' for p in a)}</dd>"
                      for q, a in faq_entries(data))
    return f'<dl class="faq">\n{items}\n</dl>'


def faq_ld(data):
    def text(answer):
        return re.sub(r"<[^>]+>", "", " ".join(answer))
    doc = {
        "@context": "https://schema.org",
        "@type": "FAQPage",
        "mainEntity": [
            {"@type": "Question", "name": q, "acceptedAnswer": {"@type": "Answer", "text": text(a)}}
            for q, a in faq_entries(data)
        ],
    }
    return '<script type="application/ld+json">\n' + json.dumps(doc, indent=1, ensure_ascii=False) + "\n</script>"


# The batched comparison (run.py --batch): (batch, the suffix of its series' files). Batches of 16 are its figures;
# batches of 64 a row of their own.
BATCHES = [(16, "-batched"), (64, "-batched-64")]
BATCHED = [("FastIPC", ["fastipc"]), ("Aeron", ["aeron"])]


def load_batched():
    """{batch: {series: [(poll, report)]}} of the batched results that exist."""
    found = {batch: load(suffix) for batch, suffix in BATCHES}
    return {batch: data for batch, data in found.items() if data}


def batched_pair(batched, batch, key, case_id):
    """FastIPC's and Aeron's best entries in a batched case and series, each (entry, variant, polled), or None where
    either is missing."""
    data = batched.get(batch, {})
    pair = [best(data, key, case_id, variants) for _, variants in BATCHED]
    return pair if all(pair) else None


def batched_how(variant, poll, entry):
    """A batched figure's configuration: the library's batch calls, its receive and its buffer."""
    batch = entry.get("batch", 16)
    if variant == "aeron":
        parts = [f"aeron_exclusive_publication_offer_block of {batch}", f"polls of up to {batch} fragments",
                 "polling receive"]
    else:
        parts = [f"fipc_send_many of {batch}", f"fipc_recv_many of up to {batch}",
                 "polling receive" if poll else "waiting receive"]
    if entry.get("buffer_bytes"):
        parts.append(f"{bytes_label(entry['buffer_bytes'])} {BUFFER_NAME.get(variant, 'buffer')}")
    return ", ".join(parts)


def batched_caption(case_id, batch):
    """'Stream, 16 B', 'Broadcast to 4, lossless, 16 B', with ', batches of 64' for a batch other than the first."""
    kind, *rest = case_id.split("-")
    text = f"Broadcast to {rest[0]}, {rest[1]}, {size_label(int(rest[2]))}" if kind == "broadcast" else caption(case_id)
    return text + ("" if batch == BATCHES[0][0] else f", batches of {batch}")


def batched_table(batched):
    """FastIPC's figure with both ends batching against Aeron's, from the same run, in each batched case: a row per
    case and batch, a column per series (a dash where a series has no batched run yet)."""
    head = "".join(f'<th scope="col" class="n">{html.escape(label)}</th>' for _, _, label in SERIES)
    out = ['<div class="table-wrap"><table class="data compare scoreboard">',
           f'<thead><tr><th scope="col">Case</th>{head}</tr></thead><tbody>']
    for batch, _ in BATCHES:
        if batch not in batched:
            continue
        for index, case_id in enumerate(run.BATCHED_CASES):
            tds = []
            for key, _, _ in SERIES:
                pair = batched_pair(batched, batch, key, case_id)
                if not pair:
                    tds.append('<td class="n muted">—</td>')
                    continue
                (ours, _, _), (theirs, _, _) = pair
                r = ours["msgs_per_s"] / theirs["msgs_per_s"]
                word = "faster" if r >= 1 else "slower"
                title = "; ".join(f"{name}: {batched_how(variant, poll, entry)}"
                                  for (name, _), (entry, variant, poll) in zip(BATCHED, pair))
                tds.append(f'<td class="n" title="{html.escape(title)}"><b>{fmt_rate(ours["msgs_per_s"])}</b> '
                           f'<small>{fmt_ratio(r if r >= 1 else 1 / r)}&nbsp;<span class="{standing(r)}">{word}</span>'
                           f" than Aeron's&nbsp;{fmt_rate(theirs['msgs_per_s'])}</small></td>")
            row_class = ' class="sep"' if index == 0 and batch != BATCHES[0][0] else ""
            out.append(f'<tr{row_class}><td>{html.escape(batched_caption(case_id, batch))}</td>{"".join(tds)}</tr>')
    out.append("</tbody></table></div>")
    out.append(f'<p class="note">{batched_note(batched)}</p>')
    return "\n".join(out)


def batched_pairs(batched, case_id, batch=16):
    """[(series label, FastIPC's figure, Aeron's)] of the series with a batched run of the case."""
    out = []
    for key, _, label in SERIES:
        pair = batched_pair(batched, batch, key, case_id)
        if pair:
            out.append((label, pair[0][0]["msgs_per_s"], pair[1][0]["msgs_per_s"]))
    return out


def batched_leads(batched, case_id, batch=16):
    """Who leads a batched case where, and by how much: 'FastIPC on Linux x86-64 (by 19%), Aeron on Windows (by 7%)',
    and 'the two level on macOS' where the lead rounds to 0%."""
    by, level = {}, []
    for label, ours, theirs in batched_pairs(batched, case_id, batch):
        lead = percent(max(ours, theirs) / min(ours, theirs) - 1)
        if lead == "0%":
            level.append(label)
            continue
        leader = "FastIPC" if ours >= theirs else "Aeron"
        by.setdefault(leader, []).append(f"{label} (by {lead})")
    parts = [f"{name} on {join(where)}" for name, where in by.items()]
    if level:
        parts.append(f"the two level on {join(level)}")
    return ", ".join(parts)


def batched_note(batched):
    """Under the batched table: who leads each case with batches of 16, and which series have no batched run yet."""
    leads = [f"{describe([case_id])} {batched_leads(batched, case_id)}" for case_id in run.BATCHED_CASES
             if batched_pairs(batched, case_id)]
    text = f"Who is faster with batches of 16 at both ends: in {'; in '.join(leads)}." if leads else ""
    missing = [label for key, _, label in SERIES if key not in batched.get(BATCHES[0][0], {})]
    if missing:
        text += f" {join(missing)}: not run yet (—)."
    return text.strip()


def batched_figures(batched, case_id):
    """'FastIPC 102M and Aeron 86.2M messages per second on Linux x86-64, FastIPC 111M and Aeron 119M on Windows':
    both libraries' figures with batches of 16, from the same run, in every series that has them."""
    parts = [f"FastIPC {fmt_rate(ours)} and Aeron {fmt_rate(theirs)}" + (" messages per second" if not i else "")
             + f" on {label}" for i, (label, ours, theirs) in enumerate(batched_pairs(batched, case_id))]
    return ", ".join(parts)


def fragments(data):
    batched = load_batched()
    ids = all_case_ids(data)
    of = lambda prefix: [c for c in ids if c.startswith(prefix)]
    return {
        "hero": hero(data),
        "scoreboard": scoreboard(data),
        "hero-note": hero_note(data),
        "faq": faq(data),
        "faq-ld": faq_ld(data),
        "batched": batched_table(batched),
        "chart-stream": chart(data, "stream-16", "Stream: 16-byte messages per second, one way"),
        "chart-stream-1k": chart(data, "stream-1024", "Stream: 1 KiB messages per second, one way"),
        "chart-stream-64k": chart(data, "stream-65536", "Stream: 64 KiB messages per second, one way"),
        "chart-rtt": chart(data, "rtt-16", "Round trip, 16 B, p50 (shorter is better; log scale)", log_floor=100),
        "chart-broadcast": chart(data, "broadcast-4-lossless-16",
                                 "Broadcast to 4 subscribers, lossless: 16-byte messages per second"),
        "chart-broadcast-lossy": chart(data, "broadcast-4-lossy-16",
                                       "Broadcast to 4 subscribers, lossy: 16-byte messages received per second"),
        "chart-queue": chart(data, "queue-4x4-16", "Work queue, 4 producers × 4 consumers: 16-byte messages per second"),
        "chart-queue-1x1": chart(data, "queue-1x1-16", "Work queue, 1 producer × 1 consumer: 16-byte messages per second"),
        "chart-service": chart(data, "service-1x1-16", "Service, 1 client × 1 server: 16-byte calls per second"),
        "chart-service-4x4": chart(data, "service-4x4-16", "Service, 4 clients × 4 servers: 16-byte calls per second"),
        "table-stream": table(data, of("stream-") + of("rtt-"), caption),
        "table-broadcast": table(data, of("broadcast-"), caption),
        "table-queue": table(data, of("queue-"), caption),
        "table-service": table(data, of("service-"), caption),
        "buffers": buffers(data),
        "machines": machines(data),
        "readme-hero": readme_hero(data),
    }


def main():
    data = load()
    if not data:
        raise SystemExit(f"no results in {RESULTS}")
    parts = fragments(data)
    for page in PAGES:
        text = page.read_text(encoding="utf-8")
        for name, body in parts.items():
            pattern = re.compile(rf"(<!-- generated:{name} -->)(.*?)(<!-- /generated:{name} -->)", re.S)
            text = pattern.sub(lambda m: m.group(1) + "\n" + body + "\n" + m.group(3), text)
        page.write_text(text, encoding="utf-8", newline="\n")
        print(f"wrote {page}")
    CHARTS.mkdir(exist_ok=True)
    for name, case_id, title, log_floor in SVG_CHARTS:
        path = CHARTS / f"{name}.svg"
        path.write_text(svg_chart(data, case_id, title, log_floor), encoding="utf-8", newline="\n")
        print(f"wrote {path}")


if __name__ == "__main__":
    main()
