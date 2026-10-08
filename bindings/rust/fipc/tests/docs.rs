//! Every Rust block of the documentation is code that runs: each block of the READMEs and of the website's Rust page
//! must appear, line for line, in examples/ or in tests/snippets.rs (which run). Blank lines and indentation don't
//! count. A document that isn't there (a published crate) is skipped.

use std::fs;
use std::path::{Path, PathBuf};

/// The lines that count: trimmed, without blank ones.
fn lines(text: &str) -> Vec<String> {
    text.lines().map(str::trim).filter(|l| !l.is_empty()).map(str::to_owned).collect()
}

fn unescape_html(text: &str) -> String {
    text.replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", "\"").replace("&#39;", "'").replace("&amp;", "&")
}

/// The Rust blocks of a Markdown file (```rust) or an HTML page (<code class="language-rust">).
fn rust_blocks(path: &Path) -> Vec<String> {
    let Ok(text) = fs::read_to_string(path) else {
        return Vec::new();
    };
    let (open, close) = if path.extension().is_some_and(|e| e == "html") {
        ("<code class=\"language-rust\">", "</code>")
    } else {
        ("```rust\n", "\n```")
    };
    let mut blocks = Vec::new();
    let mut rest = text.as_str();
    while let Some(start) = rest.find(open) {
        let after = &rest[start + open.len()..];
        let end = after.find(close).expect("an unclosed block");
        blocks.push(unescape_html(&after[..end]));
        rest = &after[end..];
    }
    blocks
}

#[test]
fn every_rust_block_of_the_docs_runs() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repository = manifest.join("../../..");
    let mut sources = vec![fs::read_to_string(manifest.join("tests/snippets.rs")).unwrap()];
    for entry in fs::read_dir(manifest.join("examples")).unwrap() {
        sources.push(fs::read_to_string(entry.unwrap().path()).unwrap());
    }
    let sources: Vec<Vec<String>> = sources.iter().map(|s| lines(s)).collect();

    let docs = [
        manifest.join("README.md"),
        manifest.join("../README.md"),
        manifest.join("../fipc-sys/README.md"),
        repository.join("README.md"),
        repository.join("website/rust.html"),
        repository.join("website/index.html"),
    ];
    let mut checked = 0;
    for doc in &docs {
        for block in rust_blocks(doc) {
            let block = lines(&block);
            let found = sources.iter().any(|source| source.windows(block.len()).any(|w| w == block.as_slice()));
            assert!(
                found,
                "{}: this block runs nowhere (examples/, tests/snippets.rs):\n{}",
                doc.display(),
                block.join("\n")
            );
            checked += 1;
        }
    }
    if docs.iter().any(|d| d.ends_with("rust.html") && d.exists()) {
        assert!(checked >= 10, "only {checked} blocks found");
    }
}
