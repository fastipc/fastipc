"""Merges a run of some cases into a series' results: the run's cases replace the series' ones, in the series' order,
and their lines in the series' log the same way.

    python bench/alternatives/merge.py <series> <run.json> <run.log>      # e.g. windows, windows-poll, windows-batched

<series> names results/<series>.json and results/<series>.log; the run is run.py's --out and printed lines. A series
that doesn't exist yet becomes the run.
"""
import json
import sys
from pathlib import Path

RESULTS = Path(__file__).resolve().parent / "results"


def case_of(line):
    return line.split(" ", 1)[0]


def main():
    if len(sys.argv) != 4:
        sys.exit(__doc__)
    series, run_json, run_log = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
    series_json, series_log = RESULTS / f"{series}.json", RESULTS / f"{series}.log"
    new = json.loads(run_json.read_text())
    if not series_json.exists():
        series_json.write_text(json.dumps(new, indent=1), newline="\n")
        series_log.write_text("".join(line + "\n" for line in run_log.read_text().splitlines()), newline="\n")
        print(f"{series}: new, {len(new['results'])} cases")
        return
    old = json.loads(series_json.read_text())
    kind = lambda report: (report["os"], report["poll"], report.get("batch", 1))
    if kind(old) != kind(new):
        sys.exit(f"{run_json} is a run of another series (os {new['os']}, poll {new['poll']}, batch "
                 f"{new.get('batch', 1)})")

    replaced = set(new["results"])
    merged = {case_id: new["results"].get(case_id, entry) for case_id, entry in old["results"].items()}
    for case_id, entry in new["results"].items():
        merged.setdefault(case_id, entry)
    old["results"] = merged
    old["date"] = new["date"]
    series_json.write_text(json.dumps(old, indent=1), newline="\n")

    new_lines = [line for line in run_log.read_text().splitlines() if case_of(line) in replaced]
    lines, inserted = [], set()
    for line in series_log.read_text().splitlines():
        case_id = case_of(line)
        if case_id not in replaced:
            lines.append(line)
        elif case_id not in inserted:
            lines += [new_line for new_line in new_lines if case_of(new_line) == case_id]
            inserted.add(case_id)
    lines += [line for line in new_lines if case_of(line) not in inserted]
    series_log.write_text("\n".join(lines) + "\n", newline="\n")
    print(f"{series}: {len(replaced)} cases replaced")


if __name__ == "__main__":
    main()
