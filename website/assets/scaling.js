// The scaling chart on python.html: measured speedups (scaling-data.js, from bench/python/dispatch_bench.py grid),
// for the grid's task times.
(function () {
  "use strict";
  var DATA = window.FIPC_SCALING;
  var root = document.getElementById("scaling-chart");
  if (!DATA || !root) return;

  var SVG = "http://www.w3.org/2000/svg";
  var SERIES = [
    { key: "fipc", name: "FastIPC", call: "fipc Conn", shape: "circle" },
    { key: "pipe", name: "Pipe", call: "multiprocessing.Pipe", shape: "square" },
    { key: "tcp", name: "TCP", call: "loopback socket", shape: "triangle" },
    { key: "queue", name: "Queue", call: "multiprocessing.Queue", shape: "diamond" }
  ];
  var state = { os: "windows", task: 3, shown: { fipc: true, pipe: true, tcp: true, queue: true } };
  try {
    var saved = JSON.parse(localStorage.getItem("fastipc-scaling") || "null");
    if (saved && DATA[saved.os]) state.os = saved.os;
  } catch (e) { /* defaults */ }

  // Drawn at the plot's own width (720 at most), so the text keeps its size on a phone
  var W = 720, H = 330, M = { l: 40, r: 104, t: 14, b: 40 };
  var svg = root.querySelector(".plot svg");
  var tip = document.createElement("div");
  tip.className = "tip tip-lines";
  tip.hidden = true;
  document.body.appendChild(tip);

  function el(name, attrs, parent) {
    var e = document.createElementNS(SVG, name);
    for (var k in attrs) e.setAttribute(k, attrs[k]);
    if (parent) parent.appendChild(e);
    return e;
  }
  function fmt(x) { return x >= 10 ? x.toFixed(1) : x.toFixed(2); }
  function us(x) { return x >= 1000 ? x / 1000 + " ms" : x + " µs"; }
  function d() { return DATA[state.os]; }
  function task() { return d().task_us[state.task]; }
  function measured(key) { return d().speedup[key]["0"][String(task())]; }
  // c with 2 workers or more (with one, the dispatcher waits out each round trip)
  function cs(key) { var w = d().workers; return d().overhead[key].c.filter(function (c, j) { return w[j] > 1; }); }
  function cMin(key) { return Math.min.apply(null, cs(key)); }
  function cMax(key) { return Math.max.apply(null, cs(key)); }
  function ideal(p) { return p; }

  function marker(shape, x, y, cls, parent) {
    var r = 4.5;
    if (shape === "circle") return el("circle", { cx: x, cy: y, r: r, "class": cls }, parent);
    if (shape === "square") return el("rect", { x: x - r, y: y - r, width: 2 * r, height: 2 * r, "class": cls }, parent);
    if (shape === "triangle") return el("path", { d: "M" + x + " " + (y - r - 1) + "L" + (x + r + 1) + " " + (y + r) + "L" + (x - r - 1) + " " + (y + r) + "Z", "class": cls }, parent);
    return el("path", { d: "M" + x + " " + (y - r - 1) + "L" + (x + r + 1) + " " + y + "L" + x + " " + (y + r + 1) + "L" + (x - r - 1) + " " + y + "Z", "class": cls }, parent);
  }

  // The y axis holds still while the task time changes: its top fits every task time of this OS
  function yTop() {
    var top = ideal(d().workers[d().workers.length - 1]);
    SERIES.forEach(function (s) {
      var byTask = d().speedup[s.key]["0"];
      for (var t in byTask) byTask[t].forEach(function (v) { top = Math.max(top, v); });
    });
    var steps = [1, 2, 3, 4, 5, 6, 8, 10, 12, 14, 16];
    for (var i = 0; i < steps.length; i++) if (steps[i] >= top * 1.04) return steps[i];
    return Math.ceil(top);
  }

  function draw() {
    while (svg.firstChild) svg.removeChild(svg.firstChild);
    W = Math.round(Math.max(300, Math.min(720, svg.parentNode.clientWidth - 12)));
    H = W < 520 ? 280 : 330;
    svg.setAttribute("viewBox", "0 0 " + W + " " + H);
    var workers = d().workers, pMax = workers[workers.length - 1], top = yTop();
    var X = function (p) { return M.l + (p / pMax) * (W - M.l - M.r); };
    var Y = function (v) { return H - M.b - (Math.min(v, top) / top) * (H - M.t - M.b); };
    var tick = top <= 4 ? 0.5 : top <= 8 ? 1 : 2;
    var g = el("g", {}, svg);
    for (var v = 0; v <= top + 1e-9; v += tick) {
      el("line", { x1: M.l, x2: W - M.r, y1: Y(v), y2: Y(v), "class": "grid" }, g);
      el("text", { x: M.l - 8, y: Y(v) + 4, "text-anchor": "end", "class": "s" }, g).textContent = (v % 1 ? v.toFixed(1) : v) + "×";
    }
    workers.forEach(function (p) {
      el("text", { x: X(p), y: H - M.b + 16, "text-anchor": "middle", "class": "s" }, g).textContent = p;
    });
    el("text", { x: (M.l + W - M.r) / 2, y: H - 6, "text-anchor": "middle", "class": "s" }, g).textContent = "worker processes";
    el("line", { x1: M.l, x2: W - M.r, y1: Y(0), y2: Y(0), "class": "axis-line" }, g);
    // One process doing it all: below this line, the workers slow the job down
    el("line", { x1: M.l, x2: W - M.r, y1: Y(1), y2: Y(1), "class": "one" }, g);
    if (tick > 1 && Y(0) - Y(1) >= 12 && Y(1) - Y(tick) >= 12) el("text", { x: M.l - 8, y: Y(1) + 4, "text-anchor": "end", "class": "s" }, g).textContent = "1×";

    function curve(f, cls) {
      var pts = [];
      for (var p = 1; p <= pMax + 1e-9; p += 0.25) pts.push(X(p).toFixed(1) + "," + Y(f(p)).toFixed(1));
      return el("polyline", { points: pts.join(" "), "class": cls }, g);
    }
    curve(ideal, "ideal");
    var ends = [{ y: Y(ideal(pMax)), text: "free IPC " + fmt(ideal(pMax)) + "×", cls: "s" }];

    SERIES.forEach(function (s, i) {
      if (!state.shown[s.key]) return;
      var vals = measured(s.key), cls = "c" + (i + 1);
      el("polyline", { points: workers.map(function (p, j) { return X(p) + "," + Y(vals[j]); }).join(" "), "class": "meas " + cls }, g);
      workers.forEach(function (p, j) { marker(s.shape, X(p), Y(vals[j]), "dot " + cls, g); });
      ends.push({ y: Y(vals[vals.length - 1]), text: s.name + " " + fmt(vals[vals.length - 1]) + "×", cls: "t" });
    });
    // End labels: in value order, at least 14 px apart
    ends.sort(function (a, b) { return a.y - b.y; });
    for (var i = 1; i < ends.length; i++) if (ends[i].y - ends[i - 1].y < 14) ends[i].y = ends[i - 1].y + 14;
    // ... and above the x axis's numbers: pushed down too far, they move back up together
    var floor = H - M.b + 2;
    for (var j = ends.length - 1; j >= 0; j--) {
      var limit = j === ends.length - 1 ? floor : ends[j + 1].y - 14;
      if (ends[j].y > limit) ends[j].y = limit;
    }
    ends.forEach(function (e) { el("text", { x: W - M.r + 6, y: e.y + 4, "class": e.cls }, g).textContent = e.text; });

    // Hover: the nearest worker count, every series' value at it
    var guide = el("line", { y1: M.t, y2: H - M.b, "class": "guide", visibility: "hidden" }, g);
    var hit = el("rect", { x: M.l, y: M.t, width: W - M.l - M.r, height: H - M.t - M.b, fill: "transparent" }, g);
    function hover(e) {
      var box = svg.getBoundingClientRect(), x = (e.clientX - box.left) * (W / box.width), best = 0;
      workers.forEach(function (p, j) { if (Math.abs(X(p) - x) < Math.abs(X(workers[best]) - x)) best = j; });
      var p = workers[best];
      guide.setAttribute("x1", X(p)); guide.setAttribute("x2", X(p)); guide.setAttribute("visibility", "visible");
      var lines = [p + (p === 1 ? " worker" : " workers")];
      SERIES.forEach(function (s) {
        if (state.shown[s.key]) lines.push(s.name + "  " + fmt(measured(s.key)[best]) + "×");
      });
      lines.push("free IPC  " + fmt(ideal(p)) + "×");
      tip.textContent = lines.join("\n");
      tip.hidden = false;
      var w = tip.offsetWidth, h = tip.offsetHeight;
      tip.style.left = Math.min(Math.max(8, e.clientX - w / 2), document.documentElement.clientWidth - w - 8) + window.scrollX + "px";
      tip.style.top = Math.max(8, e.clientY - h - 14) + window.scrollY + "px";
    }
    hit.addEventListener("pointermove", hover);
    hit.addEventListener("pointerleave", function () { tip.hidden = true; guide.setAttribute("visibility", "hidden"); });
  }

  function table() {
    var body = root.querySelector("tbody");
    body.innerHTML = "";
    SERIES.forEach(function (s, i) {
      var o = d().overhead[s.key], vals = measured(s.key), best = 0;
      vals.forEach(function (v, j) { if (v > vals[best]) best = j; });
      var tr = document.createElement("tr");
      tr.innerHTML = "<td><svg class=\"key-mark\" viewBox=\"0 0 14 14\" aria-hidden=\"true\"></svg>" + s.name +
        " <small>" + s.call + "</small></td><td class=\"n\">" + o.R.toFixed(1) + " µs</td><td class=\"n\">" + cMin(s.key).toFixed(1) + "–" + cMax(s.key).toFixed(1) +
        " µs</td><td class=\"n\">" + fmt(task() / cMin(s.key)) + "×</td><td class=\"n\">" + fmt(vals[best]) + "× <small>at " +
        d().workers[best] + "</small></td>";
      marker(s.shape, 7, 7, "dot c" + (i + 1), tr.querySelector("svg"));
      body.appendChild(tr);
    });
  }

  function summary() {
    var f = measured("fipc"), q = measured("queue"), w = d().workers;
    var fBest = Math.max.apply(null, f), qBest = Math.max.apply(null, q);
    var text = "Tasks of " + us(task()) + ": with " + w[f.indexOf(fBest)] +
      " workers, FastIPC finishes the job " + fmt(fBest) + "× as fast as one process; through multiprocessing.Queue, the best is " +
      fmt(qBest) + "×" + (qBest < 1 ? ", slower than not using workers at all." : ".");
    root.querySelector(".scaling-summary").textContent = text;
  }

  function sync() {
    root.querySelectorAll("[data-os]").forEach(function (b) { b.setAttribute("aria-pressed", b.getAttribute("data-os") === state.os); });
    root.querySelectorAll("[data-series]").forEach(function (b) { b.setAttribute("aria-pressed", !!state.shown[b.getAttribute("data-series")]); });
    var range = root.querySelector("input[type=range]");
    range.value = state.task;
    root.querySelector("output").textContent = us(task());
    range.setAttribute("aria-valuetext", us(task()));
    root.querySelector(".scaling-meta").textContent = d().label;
    draw(); table(); summary();
  }

  root.querySelectorAll("[data-os]").forEach(function (b) {
    b.addEventListener("click", function () {
      state.os = b.getAttribute("data-os");
      try { localStorage.setItem("fastipc-scaling", JSON.stringify({ os: state.os })); } catch (e) { /* not kept */ }
      sync();
    });
  });
  root.querySelectorAll("[data-series]").forEach(function (b) {
    b.addEventListener("click", function () { var k = b.getAttribute("data-series"); state.shown[k] = !state.shown[k]; sync(); });
  });
  root.querySelector("input[type=range]").addEventListener("input", function (e) { state.task = +e.target.value; sync(); });
  root.querySelectorAll("[data-series] svg").forEach(function (s, i) { marker(SERIES[i].shape, 7, 7, "dot c" + (i + 1), s); });
  var lastWidth = 0;
  window.addEventListener("resize", function () {
    var w = svg.parentNode.clientWidth;
    if (w !== lastWidth) { lastWidth = w; draw(); }
  });
  sync();
})();
