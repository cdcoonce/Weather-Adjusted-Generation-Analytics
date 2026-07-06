/* src/weather_analytics/cockpit/static/app.js
   Client-side interactivity for the WAGA cockpit. Reads the JSON island and, on
   load and on every filter change, recomputes KPIs and redraws the SVG charts,
   technology bars, battery/gas series, and the fleet roster. The aggregation
   math mirrors weather_analytics.cockpit.charts (keep them in sync). */
(function () {
  "use strict";
  var el = document.getElementById("cockpit-data");
  if (!el) return;
  var D = JSON.parse(el.textContent);
  var assets = D.assets || [];
  var daily = D.daily || [];
  var weather = D.weather || [];

  var TYPE_COLORS = {
    wind: "#4f7d63", solar: "#b3873f", battery: "#8fa8c4",
    gas: "#9e6c52", unknown: "#a0a0a0",
  };
  var PRIMARY = "#4f7d63", GOLD = "#b3873f";
  var RENEWABLE = { wind: 1, solar: 1 };
  var CF_TYPES = { wind: 1, solar: 1, gas: 1 };

  var typeById = {}, nameById = {}, capById = {}, regionById = {};
  assets.forEach(function (a) {
    typeById[a.asset_id] = a.asset_type;
    nameById[a.asset_id] = a.display_name || a.asset_id;
    capById[a.asset_id] = +a.capacity_mw || 0;
    regionById[a.asset_id] = a.region || "";
  });
  function typeOf(id) { return typeById[id] || "unknown"; }
  function colorFor(t) { return TYPE_COLORS[t] || TYPE_COLORS.unknown; }

  var fType = document.getElementById("f-type");
  var fAsset = document.getElementById("f-asset");
  var fStart = document.getElementById("f-start");
  var fEnd = document.getElementById("f-end");

  function num(v) { return v === null || v === undefined || isNaN(+v) ? 0 : +v; }
  function fmt(n, dp) {
    return n.toLocaleString("en-US", { minimumFractionDigits: dp, maximumFractionDigits: dp });
  }

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

  /* ---- KPIs ---- */
  function setKpis(fd) {
    var netGen = 0, curt = 0, co2 = 0, batt = 0, renGen = 0, served = 0;
    var cfSum = 0, cfN = 0;
    fd.forEach(function (r) {
      var t = r.asset_type || typeOf(r.asset_id);
      netGen += num(r.total_net_generation_mwh);
      curt += num(r.total_curtailment_mwh);
      co2 += num(r.total_co2_tonnes);
      if (t === "battery") batt += num(r.total_discharge_mwh);
      if (RENEWABLE[t]) renGen += num(r.total_net_generation_mwh);
      if (CF_TYPES[t]) { served += num(r.total_net_generation_mwh); cfSum += num(r.daily_capacity_factor); cfN++; }
    });
    put("net_generation", fmt(netGen, 0));
    put("capacity_factor", fmt(cfN ? (cfSum / cfN) * 100 : 0, 1) + "%");
    put("renewable_share", fmt(served > 0 ? (renGen / served) * 100 : 0, 0) + "%");
    put("co2_emissions", fmt(co2, 0));
    put("battery_throughput", fmt(batt, 0));
    put("curtailment", fmt(curt, 0));
  }
  function put(key, val) {
    var n = document.querySelector('[data-kpi="' + key + '"]');
    if (n) n.textContent = val;
  }

  /* ---- aggregation helpers ---- */
  function byDateSum(rows, attr, typeFilter) {
    var acc = {};
    rows.forEach(function (r) {
      if (typeFilter && !typeFilter[r.asset_type || typeOf(r.asset_id)]) return;
      acc[r.date] = (acc[r.date] || 0) + num(r[attr]);
    });
    return Object.keys(acc).sort().map(function (d) { return [d, acc[d]]; });
  }
  function byDateMean(rows, attr, typeFilter, valid) {
    var g = {};
    rows.forEach(function (r) {
      if (typeFilter && !typeFilter[r.asset_type || typeOf(r.asset_id)]) return;
      if (valid && (r[attr] === null || r[attr] === undefined)) return;
      (g[r.date] = g[r.date] || []).push(num(r[attr]));
    });
    return Object.keys(g).sort().map(function (d) {
      var a = g[d]; return [d, a.reduce(function (s, v) { return s + v; }, 0) / a.length];
    });
  }

  /* ---- SVG line ---- */
  function lineSvg(pairs, width, height, color) {
    if (!pairs.length) return '<div class="nodata">no data in range</div>';
    var vals = pairs.map(function (p) { return p[1]; });
    var yMin = Math.min.apply(null, vals.concat(0));
    var yMax = Math.max.apply(null, vals);
    var span = (yMax - yMin) || 1, pad = 8, n = pairs.length;
    function x(i) { return pad + (n > 1 ? (i / (n - 1)) * (width - 2 * pad) : 0); }
    function y(v) { return height - pad - ((v - yMin) / span) * (height - 2 * pad); }
    var base = y(0);
    var pts = pairs.map(function (p, i) { return [x(i), y(p[1])]; });
    var poly = pts.map(function (p) { return p[0].toFixed(1) + "," + p[1].toFixed(1); }).join(" ");
    var area = "M " + pts[0][0].toFixed(1) + " " + base.toFixed(1) + " " +
      pts.map(function (p) { return "L " + p[0].toFixed(1) + " " + p[1].toFixed(1); }).join(" ") +
      " L " + pts[n - 1][0].toFixed(1) + " " + base.toFixed(1) + " Z";
    return '<svg viewBox="0 0 ' + width + ' ' + height + '" preserveAspectRatio="none" role="img">' +
      '<path d="' + area + '" fill="' + color + '" fill-opacity="0.12" />' +
      '<polyline points="' + poly + '" fill="none" stroke="' + color + '" stroke-width="2" /></svg>' +
      '<div class="axis"><span>' + pairs[0][0] + '</span><span>peak ' + yMax.toFixed(2) +
      '</span><span>' + pairs[n - 1][0] + '</span></div>';
  }

  /* ---- stacked-area generation mix by technology ---- */
  function stackedMix(fd, width, height) {
    var order = ["wind", "solar", "gas", "battery"];
    var dates = {};
    fd.forEach(function (r) { dates[r.date] = 1; });
    var dateList = Object.keys(dates).sort();
    if (!dateList.length) return '<div class="nodata">no data in range</div>';
    var dIdx = {}; dateList.forEach(function (d, i) { dIdx[d] = i; });
    var series = {};
    order.forEach(function (t) { series[t] = new Array(dateList.length).fill(0); });
    fd.forEach(function (r) {
      var t = r.asset_type || typeOf(r.asset_id);
      if (!series[t]) return;
      var contrib = t === "battery" ? num(r.total_discharge_mwh)
        : Math.max(0, num(r.total_net_generation_mwh));
      series[t][dIdx[r.date]] += contrib;
    });
    var present = order.filter(function (t) {
      return series[t].some(function (v) { return v > 0; });
    });
    if (!present.length) return '<div class="nodata">no data in range</div>';
    var totals = dateList.map(function (_, i) {
      return present.reduce(function (s, t) { return s + series[t][i]; }, 0);
    });
    var yMax = Math.max.apply(null, totals) || 1;
    var pad = 8, n = dateList.length;
    function x(i) { return pad + (n > 1 ? (i / (n - 1)) * (width - 2 * pad) : 0); }
    function y(v) { return height - pad - (v / yMax) * (height - 2 * pad); }
    var cum = new Array(n).fill(0);
    var paths = "";
    present.forEach(function (t) {
      var lower = cum.slice();
      var upper = cum.map(function (c, i) { return c + series[t][i]; });
      cum = upper;
      var top = upper.map(function (v, i) { return "L " + x(i).toFixed(1) + " " + y(v).toFixed(1); }).join(" ");
      var bot = lower.map(function (v, i) { return "L " + x(i).toFixed(1) + " " + y(v).toFixed(1); }).reverse().join(" ");
      var d = "M " + x(0).toFixed(1) + " " + y(lower[0]).toFixed(1) + " " + top + " " + bot + " Z";
      paths += '<path d="' + d + '" fill="' + colorFor(t) + '" fill-opacity="0.85" />';
    });
    return '<svg viewBox="0 0 ' + width + ' ' + height + '" preserveAspectRatio="none" role="img">' +
      paths + '</svg>' +
      '<div class="axis"><span>' + dateList[0] + '</span><span>peak ' + fmt(yMax, 0) +
      ' MWh</span><span>' + dateList[n - 1] + '</span></div>';
  }

  /* ---- horizontal bars ---- */
  function hbars(rows) {
    if (!rows.length) return '<div class="nodata">no data in range</div>';
    var maxV = Math.max.apply(null, rows.map(function (r) { return r.value; })) || 0;
    return rows.map(function (r) {
      var pct = maxV ? Math.max(1.5, (r.value / maxV) * 100) : 0;
      return '<div class="hbar-row"><div class="hbar-label" title="' + r.label + '">' + r.label + '</div>' +
        '<div class="hbar-track"><div class="hbar-fill" style="width: ' + pct + '%; background: ' + r.color + '"></div></div>' +
        '<div class="hbar-value">' + fmt(r.value, 2) + '</div></div>';
    }).join("");
  }

  function setHtml(id, html) { var n = document.getElementById(id); if (n) n.innerHTML = html; }

  function assetBars(fd) {
    var g = {};
    fd.forEach(function (r) { (g[r.asset_id] = g[r.asset_id] || []).push(num(r.daily_capacity_factor)); });
    return Object.keys(g).sort().map(function (id) {
      var a = g[id];
      return { label: nameById[id] || id, value: a.reduce(function (s, v) { return s + v; }, 0) / a.length,
        color: colorFor(typeOf(id)) };
    });
  }

  function typeSplit(fd) {
    var t = {};
    fd.forEach(function (r) {
      var k = r.asset_type || typeOf(r.asset_id);
      t[k] = (t[k] || 0) + num(r.total_net_generation_mwh);
    });
    return Object.keys(t).sort().map(function (k) { return { label: k, value: t[k], color: colorFor(k) }; });
  }

  /* ---- fleet roster ---- */
  function roster(fd, ids) {
    var g = {};
    fd.forEach(function (r) {
      (g[r.asset_id] = g[r.asset_id] || { cf: [], gen: 0 });
      g[r.asset_id].cf.push(num(r.daily_capacity_factor));
      g[r.asset_id].gen += num(r.total_net_generation_mwh);
    });
    var ordered = assets.filter(function (a) { return ids.has(a.asset_id); });
    if (!ordered.length) return '<div class="nodata">no assets selected</div>';
    var rows = ordered.map(function (a) {
      var s = g[a.asset_id] || { cf: [0], gen: 0 };
      var cf = s.cf.reduce(function (x, y) { return x + y; }, 0) / s.cf.length;
      return '<tr><td>' + (nameById[a.asset_id] || a.asset_id) + '</td>' +
        '<td><span class="type-chip" style="--dot: ' + colorFor(a.asset_type) + '">' + a.asset_type + '</span></td>' +
        '<td>' + fmt(+a.capacity_mw, 0) + ' MW</td>' +
        '<td>' + (a.region || "—") + '</td>' +
        '<td>' + (cf * 100).toFixed(1) + '%</td>' +
        '<td>' + fmt(s.gen, 0) + '</td></tr>';
    }).join("");
    return '<div style="overflow-x:auto"><table class="roster"><thead><tr>' +
      '<th>asset</th><th>technology</th><th>capacity</th><th>region</th>' +
      '<th>capacity factor</th><th>net gen (MWh)</th></tr></thead><tbody>' +
      rows + '</tbody></table></div>';
  }

  function apply() {
    var ids = allowedIds();
    var fd = filt(daily, ids), fw = filt(weather, ids);
    setKpis(fd);
    setHtml("chart-generation", lineSvg(byDateSum(fd, "total_net_generation_mwh"), 720, 200, PRIMARY));
    setHtml("chart-mix", stackedMix(fd, 720, 220));
    setHtml("chart-asset_bars", hbars(assetBars(fd)));
    setHtml("chart-type_split", hbars(typeSplit(fd)));
    setHtml("chart-battery_soc", lineSvg(byDateMean(fd, "avg_soc_pct", { battery: 1 }, true), 720, 160, TYPE_COLORS.battery));
    setHtml("chart-emissions", lineSvg(byDateSum(fd, "total_co2_tonnes", { gas: 1 }), 720, 160, TYPE_COLORS.gas));
    setHtml("chart-capacity_factor", lineSvg(byDateMean(fd, "daily_capacity_factor", CF_TYPES), 720, 160, PRIMARY));
    setHtml("chart-performance", lineSvg(byDateMean(fw, "performance_score"), 720, 160, GOLD));
    setHtml("fleet-roster", roster(fd, ids));
  }

  [fType, fAsset, fStart, fEnd].forEach(function (c) {
    if (c) c.addEventListener("change", apply);
  });
  apply();
})();
