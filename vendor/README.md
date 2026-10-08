# Vendored packages

The build's one dependency, the official translate-c package, and the C compiler front end it uses, Aro, are kept
here as path dependencies, so that no build of this repository (and no project that uses it as a Zig dependency)
downloads anything. The build uses translate-c to translate `include/fipc.h` for the ABI checks of
`src/zig/abi.zig`; neither package is part of the built library.

The Node-API headers, which the JavaScript binding's addon (`bindings/js/src`) compiles against, are here for the same
reason: the build downloads nothing. They are declarations only; the addon links nothing of Node.js.

| Folder | Package | Source | Commit | License |
|---|---|---|---|---|
| `translate_c/` | translate-c | https://codeberg.org/ziglang/translate-c | `875969d3493e245e01bf5d7860f792d8f3eb9ef5` | MIT ([`translate_c/LICENSE`](translate_c/LICENSE)) |
| `node-api-headers/` | node-api-headers (npm) | https://github.com/nodejs/node-api-headers | `8d0ef96a771d67f79b5436604d8c697ea70b618e` (1.9.0) | MIT ([`node-api-headers/LICENSE`](node-api-headers/LICENSE)) |
| `aro/` | Aro (arocc) | https://codeberg.org/ziglang/arocc | `d0c8c4d9c55daa7ef6e40cf0f630a5b5e900989b` | MIT ([`aro/LICENSE`](aro/LICENSE)); Unicode data: Unicode License v3 ([`aro/LICENSE-UNICODE`](aro/LICENSE-UNICODE)) |

`node-api-headers/` holds the npm package's `include/` folder and its `LICENSE`, unchanged (`npm pack
node-api-headers@1.9.0`). The other folders each hold exactly the files their package's `build.zig.zon` lists in
`.paths`, as `zig fetch` stores them:
`aro/` hashes to `aro-0.0.0-JSD1QtuBNwCASyBtNF3pqTl_W3oAJQGEVyFAtrBSE_Pa`, and `translate_c/`, before the one change
below, to `translate_c-0.0.0-Q_BUWoFOBwAhz77Zd15HCVuhTKzdUKc94kezmCeJ7IC_`.

## Changes

- `translate_c/build.zig.zon`: the dependency `aro` is `.path = "../aro"` in place of its URL and hash, so that it
  resolves to `aro/` here.

Nothing else is changed. `devtool format` and `devtool loc` leave this folder out.

## Updating

The Node-API headers: `npm pack node-api-headers@<version>`, replace `node-api-headers/include/` and `LICENSE` with the
package's, update the table, then `zig build` and the JavaScript binding's tests. The addon asks for Node-API 8
(`NAPI_VERSION` in `bindings/js/src/fipc_node.c`), which newer headers still declare.

translate-c and Aro:

1. In a scratch project (`zig init`), `zig fetch --save git+https://codeberg.org/ziglang/translate-c#<commit>` and
   then `zig build --fetch`: both packages land in its `zig-pkg/`, each in a folder named by its hash (translate-c's
   `build.zig.zon` names the Aro commit).
2. Replace the files of `translate_c/` and `aro/` with those packages' files, check that `zig fetch vendor/aro` (and
   `zig fetch vendor/translate_c`, before the next step) print their hashes, and apply the change above again.
3. Update the table and the hashes here, then build and run the tests: the ABI checks run in every build.
