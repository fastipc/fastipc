// Every JavaScript block of the documentation is code that runs: each block of the binding's README and of the
// website's pages (js.html, and the overview's JavaScript tab) must appear, line for line, in examples/ or in
// tests/snippets.test.mjs (which run). Blank lines and indentation don't count; a document that isn't there (an
// installed package has no website) is skipped.
import assert from 'node:assert/strict';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { describe, test } from 'node:test';

import { binding, here, repository } from './support.mjs';

/** The lines that count: trimmed, without blank ones */
function lines(text) {
  return text.replace(/\r\n/g, '\n').split('\n').map((line) => line.trim()).filter((line) => line !== '');
}

function unescapeHtml(text) {
  return text.replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#39;/g, "'")
    .replace(/&amp;/g, '&');
}

/** The JavaScript blocks of a Markdown file (```js) or an HTML page (<code class="language-javascript">) */
function blocks(file) {
  const text = readFileSync(file, 'utf8').replace(/\r\n/g, '\n');
  if (file.endsWith('.html')) {
    return [...text.matchAll(/<code class="language-javascript">([\s\S]*?)<\/code>/g)].map((m) => unescapeHtml(m[1]));
  }
  return [...text.matchAll(/^```(?:js|javascript)\n([\s\S]*?)\n```$/gm)].map((m) => m[1]);
}

/** Whether `block`'s lines appear one after another in `source`'s */
function contains(source, block) {
  for (let start = 0; start + block.length <= source.length; start++) {
    if (block.every((line, i) => source[start + i] === line)) return true;
  }
  return false;
}

const sources = [
  ...readdirSync(path.join(binding, 'examples')).filter((n) => n.endsWith('.mjs'))
    .map((n) => path.join(binding, 'examples', n)),
  path.join(here, 'snippets.test.mjs'),
].map((file) => ({ file, lines: lines(readFileSync(file, 'utf8')) }));

const documents = [
  path.join(binding, 'README.md'),
  path.join(repository, 'README.md'),
  path.join(repository, 'website', 'js.html'),
  path.join(repository, 'website', 'index.html'),
];

describe('docs', () => {
  for (const document of documents) {
    test(`every JavaScript block of ${path.relative(repository, document)} runs`, (t) => {
      if (!existsSync(document)) return t.skip('not there');
      const found = blocks(document);
      for (const block of found) {
        const wanted = lines(block);
        const where = sources.find((source) => contains(source.lines, wanted));
        assert.ok(where, `this block of ${document} is in no example and no snippet:\n${block}`);
      }
    });
  }
});
