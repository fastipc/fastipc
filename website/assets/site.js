(function () {
  "use strict";

  // Syntax colouring (highlight.js, loaded before this file; the page reads fine without it)
  if (window.hljs) {
    // Zig, which highlight.js doesn't bundle: keywords, types, builtins, strings, numbers and enum literals
    window.hljs.registerLanguage("zig", function (hljs) {
      return {
        name: "Zig",
        keywords: {
          keyword: "const var fn pub return if else while for switch try catch orelse defer errdefer comptime " +
            "struct enum union error test break continue unreachable and or inline export extern callconv " +
            "threadlocal noreturn anytype anyerror",
          literal: "true false null undefined",
          type: "bool void type usize isize f16 f32 f64 f80 f128 c_int c_uint c_long c_ulong anyopaque " +
            "comptime_int comptime_float"
        },
        contains: [
          hljs.COMMENT("//", "$"), // also /// and //! doc comments
          { scope: "string", begin: /\\\\/, end: /$/ }, // a multiline string's line
          { scope: "string", begin: /"/, end: /"/, illegal: /\n/, contains: [hljs.BACKSLASH_ESCAPE] },
          { scope: "string", match: /'(?:\\[^'\n]+|[^'\\\n])'/ },
          { scope: "built_in", match: /@[A-Za-z_]\w*/ },
          { scope: "type", match: /\b[iu]\d+\b/ },
          { scope: "number", match: /\b(?:0x[\da-fA-F_]+|0o[0-7_]+|0b[01_]+|\d[\d_]*(?:\.[\d_]+)?(?:[eE][+-]?\d+)?)\b/ },
          { scope: "literal", match: /(?<![\w)\]])\.[A-Za-z_]\w*(?!\w|\s*=[^=])/ } // .none, .awake; not .field =
        ]
      };
    });
    document.querySelectorAll("pre code[class*='language-']").forEach(function (el) {
      try { window.hljs.highlightElement(el); } catch (e) { /* leave it plain */ }
    });
  }

  // Tabs: <div data-tabs="key"> with .tabs buttons[aria-controls] and panels
  function store(key, value) {
    try { if (value === undefined) return localStorage.getItem(key); localStorage.setItem(key, value); } catch (e) { return null; }
    return null;
  }
  document.querySelectorAll("[data-tabs]").forEach(function (box) {
    var key = "fastipc-tab-" + box.getAttribute("data-tabs");
    var buttons = Array.prototype.slice.call(box.querySelectorAll(".tabs [role='tab']"));
    function select(btn, remember) {
      buttons.forEach(function (b) {
        var on = b === btn;
        b.setAttribute("aria-selected", on ? "true" : "false");
        b.tabIndex = on ? 0 : -1;
        var panel = document.getElementById(b.getAttribute("aria-controls"));
        if (panel) panel.hidden = !on;
      });
      if (remember) store(key, btn.id);
    }
    buttons.forEach(function (b, i) {
      b.addEventListener("click", function () { select(b, true); });
      b.addEventListener("keydown", function (e) {
        var j = e.key === "ArrowRight" ? i + 1 : e.key === "ArrowLeft" ? i - 1 : null;
        if (j === null) return;
        var next = buttons[(j + buttons.length) % buttons.length];
        select(next, true); next.focus(); e.preventDefault();
      });
    });
    var saved = store(key);
    var first = buttons.filter(function (b) { return b.id === saved; })[0] || buttons[0];
    if (first) select(first, false);
  });

  // Copy buttons: data-copy="text", or the code block they belong to
  document.querySelectorAll(".copy").forEach(function (btn) {
    btn.addEventListener("click", function () {
      var text = btn.getAttribute("data-copy");
      var source = null;
      if (!text) {
        source = btn.closest(".code").querySelector("pre");
        text = source ? source.innerText : "";
      }
      function done(label) { btn.textContent = label; setTimeout(function () { btn.textContent = "copy"; }, 1400); }
      function fallback() {
        var target = source || (btn.parentElement && btn.parentElement.querySelector("code"));
        if (target) { var r = document.createRange(); r.selectNodeContents(target); var s = getSelection(); s.removeAllRanges(); s.addRange(r); }
        done("selected");
      }
      try {
        navigator.clipboard.writeText(text).then(function () { done("copied"); }, fallback);
      } catch (e) { fallback(); }
    });
  });

  // Bar chart tooltips: any element with data-tip
  var tip = document.createElement("div");
  tip.className = "tip";
  tip.setAttribute("role", "tooltip");
  tip.hidden = true;
  document.body.appendChild(tip);
  function show(el, x, y) {
    tip.textContent = el.getAttribute("data-tip");
    tip.hidden = false;
    var w = tip.offsetWidth, h = tip.offsetHeight;
    var left = Math.min(Math.max(8, x - w / 2), document.documentElement.clientWidth - w - 8);
    tip.style.left = left + window.scrollX + "px";
    tip.style.top = y + window.scrollY - h - 10 + "px";
  }
  document.querySelectorAll("[data-tip]").forEach(function (el) {
    el.addEventListener("pointermove", function (e) { show(el, e.clientX, e.clientY); });
    el.addEventListener("pointerleave", function () { tip.hidden = true; });
    el.addEventListener("focus", function () { var r = el.getBoundingClientRect(); show(el, r.left + r.width / 2, r.top); });
    el.addEventListener("blur", function () { tip.hidden = true; });
  });
})();
