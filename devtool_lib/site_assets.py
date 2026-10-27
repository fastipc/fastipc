"""The website's stylesheets and scripts carry their content's hash in their URL (assets/site.css?v=<hash>), so a
browser that cached an older copy fetches the new one as soon as a page names it: GitHub Pages lets a browser keep a
file 10 minutes without asking, and some keep it longer.

    python devtool_lib/site_assets.py           # writes each page's hashes
    python devtool_lib/site_assets.py --check   # lists the stale ones (devtool test fast, through check_docs)
"""

import hashlib
import re
import sys
from pathlib import Path
from typing import List

ASSET = re.compile(r'((?:href|src)="assets/([\w.-]+\.(?:css|js)))(?:\?v=([0-9a-f]+))?"')


def digest(path: Path) -> str:
    """The first 8 hex digits of the file's SHA-256, its line ends as Git stores them (LF), so that every OS agrees."""
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()[:8]


def _pages(repo: Path):
    return sorted((repo / "website").glob("*.html"))


def problems(repo: Path) -> List[str]:
    found = []
    for page in _pages(repo):
        for match in ASSET.finditer(page.read_text(encoding="utf-8")):
            want = digest(repo / "website" / "assets" / match.group(2))
            if match.group(3) != want:
                found.append(f"website/{page.name}: assets/{match.group(2)} should be ?v={want} "
                             "(python devtool_lib/site_assets.py writes it)")
    return found


def stamp(repo: Path) -> List[Path]:
    changed = []
    for page in _pages(repo):
        text = page.read_text(encoding="utf-8")
        new = ASSET.sub(lambda m: f'{m.group(1)}?v={digest(repo / "website" / "assets" / m.group(2))}"', text)
        if new != text:
            page.write_text(new, encoding="utf-8", newline="\n")
            changed.append(page)
    return changed


if __name__ == "__main__":
    root = Path(__file__).resolve().parent.parent
    if "--check" in sys.argv:
        stale = problems(root)
        print("\n".join(stale) or "every asset's hash is current")
        sys.exit(1 if stale else 0)
    for page in stamp(root):
        print(f"wrote {page}")
