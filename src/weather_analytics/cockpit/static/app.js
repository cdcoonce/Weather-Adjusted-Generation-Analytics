/* src/weather_analytics/cockpit/static/app.js
   Light client-side interactivity. Reads the JSON island and, on filter change,
   recomputes KPIs and redraws the SVG line charts + hbars. The aggregation math
   mirrors weather_analytics.cockpit.charts (keep them in sync). */
(function () {
  "use strict";
  var el = document.getElementById("cockpit-data");
  if (!el) return;
  var D = JSON.parse(el.textContent);
  var assets = D.assets || [];
  var daily = D.daily || [];
  var weather = D.weather || [];
  var typeById = {};
  assets.forEach(function (a) { typeById[a.asset_id] = a.asset_type; });

  var fType = document.getElementById("f-type");
  var fAsset = document.getElementById("f-asset");
  var fStart = document.getElementById("f-start");
  var fEnd = document.getElementById("f-end");

  function num(v) { return v === null || v === undefined || isNaN(+v) ? 0 : +v; }

  function allowedIds() {
    var t = fType.value, a = fAsset.value;
    var ids = assets
      .filter(function (x) { return (!t || x.asset_type === t) && (!a || x.asset_id === a); })
      .map(function (x) { return x.asset_id; });
    return new Set(ids);
  }

  function inRange(d) {
    var s = fStart.value, e = fEnd.value;
    if (s && d < s) return false;
    if (e && d > e) return false;
    return true;
  }

  function filt(rows, ids) {
    return rows.filter(function (r) { return ids.has(r.asset_id) && inRange(r.date); });
  }

  function fmt(n, dp) { return n.toLocaleString("en-US", { minimumFractionDigits: dp, maximumFractionDigits: dp }); }

  function setKpis(fd, fw) {
    var netGen = 0, curt = 0, cfSum = 0, perfSum = 0;
    fd.forEach(function (r) { netGen += num(r.total_net_generation_mwh); curt += num(r.total_curtailment_mwh); cfSum += num(r.daily_capacity_factor); });
    fw.forEach(function (r) { perfSum += num(r.performance_score); });
    var cf = fd.length ? cfSum / fd.length : 0;
    var perf = fw.length ? perfSum / fw.length : 0;
    put("capacity_factor", fmt(cf * 100, 1) + "%");
    put("net_generation", fmt(netGen, 0));
    put("performance_score", fmt(perf, 2));
    put("curtailment", fmt(curt, 0));
  }

  function put(key, val) {
    var n = document.querySelector('[data-kpi="' + key + '"]');
    if (n) n.textContent = val;
  }

  function byDateSum(rows, attr) {
    var acc = {};
    rows.forEach(function (r) { acc[r.date] = (acc[r.date] || 0) + num(r[attr]); });
    return Object.keys(acc).sort().map(function (d) { return [d, acc[d]]; });
  }

  function byDateMean(rows, attr) {
    var g = {};
    rows.forEach(function (r) { (g[r.date] = g[r.date] || []).push(num(r[attr])); });
    return Object.keys(g).sort().map(function (d) {
      var a = g[d]; return [d, a.reduce(function (s, v) { return s + v; }, 0) / a.length];
    });
  }

  function lineSvg(pairs, width, height, pad) {
    if (!pairs.length) return '<div class="nodata">no data in range</div>';
    var yMax = Math.max.apply(null, pairs.map(function (p) { return p[1]; })) || 1;
    var n = pairs.length;
    function x(i) { return pad + (n > 1 ? (i / (n - 1)) * (width - 2 * pad) : 0); }
    function y(v) { return height - pad - (v / yMax) * (height - 2 * pad); }
    var pts = pairs.map(function (p, i) { return [x(i), y(p[1])]; });
    var poly = pts.map(function (p) { return p[0].toFixed(1) + "," + p[1].toFixed(1); }).join(" ");
    var area = "M " + pts[0][0].toFixed(1) + " " + (height - pad).toFixed(1) + " " +
      pts.map(function (p) { return "L " + p[0].toFixed(1) + " " + p[1].toFixed(1); }).join(" ") +
      " L " + pts[n - 1][0].toFixed(1) + " " + (height - pad).toFixed(1) + " Z";
    return '<svg viewBox="0 0 ' + width + ' ' + height + '" preserveAspectRatio="none" role="img">' +
      '<path d="' + area + '" fill="rgba(90,169,230,0.20)" />' +
      '<polyline points="' + poly + '" fill="none" stroke="var(--accent)" stroke-width="2" /></svg>' +
      '<div class="axis"><span>' + pairs[0][0] + '</span><span>max ' + yMax.toFixed(2) +
      '</span><span>' + pairs[n - 1][0] + '</span></div>';
  }

  function hbars(rows, showType) {
    if (!rows.length) return '<div class="nodata">no data in range</div>';
    var maxV = Math.max.apply(null, rows.map(function (r) { return r.value; })) || 0;
    return rows.map(function (r) {
      var pct = maxV ? Math.max(1.5, (r.value / maxV) * 100) : 0;
      var cls = "hbar-fill" + (showType && r.type === "solar" ? " solar" : "");
      return '<div class="hbar-row"><div class="hbar-label">' + r.label + '</div>' +
        '<div class="hbar-track"><div class="' + cls + '" style="width: ' + pct + '%"></div></div>' +
        '<div class="hbar-value">' + fmt(r.value, 2) + '</div></div>';
    }).join("");
  }

  function setHtml(id, html) { var n = document.getElementById(id); if (n) n.innerHTML = html; }

  function assetBars(fd) {
    var g = {};
    fd.forEach(function (r) { (g[r.asset_id] = g[r.asset_id] || []).push(num(r.daily_capacity_factor)); });
    return Object.keys(g).sort().map(function (id) {
      var a = g[id]; return { label: id, value: a.reduce(function (s, v) { return s + v; }, 0) / a.length, type: typeById[id] || "" };
    });
  }

  function typeSplit(fd) {
    var t = {};
    fd.forEach(function (r) { var k = typeById[r.asset_id] || "unknown"; t[k] = (t[k] || 0) + num(r.total_net_generation_mwh); });
    return Object.keys(t).sort().map(function (k) { return { label: k, value: t[k] }; });
  }

  function apply() {
    var ids = allowedIds();
    var fd = filt(daily, ids), fw = filt(weather, ids);
    setKpis(fd, fw);
    setHtml("chart-generation", lineSvg(byDateSum(fd, "total_net_generation_mwh"), 720, 200, 8));
    setHtml("chart-capacity_factor", lineSvg(byDateMean(fd, "daily_capacity_factor"), 720, 160, 8));
    setHtml("chart-performance", lineSvg(byDateMean(fw, "performance_score"), 720, 160, 8));
    setHtml("chart-asset_bars", hbars(assetBars(fd), true));
    setHtml("chart-type_split", hbars(typeSplit(fd), false));
  }

  [fType, fAsset, fStart, fEnd].forEach(function (c) {
    if (c) c.addEventListener("change", apply);
  });
})();
