// Render del portafolio: textos bilingües, proyectos y demos sobre data/demos.json.
(() => {
  const TRAMOS = ["Al día", "1-30", "31-60", "61-90", "90+"];
  const TRAMO_EN = { "Al día": "Current", "Pagado": "Paid off" };
  const CANALES = ["Web", "App", "WhatsApp"];
  const $ = (sel) => document.querySelector(sel);
  const el = (tag, cls, html) => {
    const n = document.createElement(tag);
    if (cls) n.className = cls;
    if (html !== undefined) n.innerHTML = html;
    return n;
  };
  const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

  let lang = "es";
  try { lang = localStorage.getItem("lang") || (navigator.language || "es").slice(0, 2); } catch (e) { /* sin almacenamiento */ }
  if (!UI[lang]) lang = "en";

  let data = null;
  let chart = null;
  let rollMode = "prestamos";
  const t = (k) => UI[lang][k];
  const pct = (v) => (v * 100).toFixed(1) + (lang === "es" ? " %" : "%");
  const mes = (ym) => t("months")[Number(ym.slice(5, 7)) - 1];
  const tramo = (x) => (lang === "en" && TRAMO_EN[x]) || (x === "Pagado" ? t("paid") : x);
  const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

  // ------------------------------------------------------------ textos
  function renderText() {
    document.documentElement.lang = lang;
    document.querySelectorAll("[data-i18n]").forEach((n) => { n.textContent = t(n.dataset.i18n); });
    $("#lang").textContent = lang === "es" ? "EN" : "ES";
    $("#github-link").href = CONFIG.githubUrl;
    $("#linkedin-link").href = CONFIG.linkedinUrl;
    $("#email-link").href = `mailto:${CONFIG.email}`;

    const stats = $("#stats");
    stats.replaceChildren(...t("stats").map(([b, s]) => el("div", "stat", `<b>${esc(b)}</b><span>${esc(s)}</span>`)));

    $("#skills").replaceChildren(...SKILLS.map((g) => {
      const card = el("div", "skill", `<h3>${esc(g[lang])}</h3>`);
      const chips = el("div", "chips");
      g.items.forEach((i) => chips.append(el("span", "chip", esc(i))));
      card.append(chips);
      return card;
    }));

    $("#projects").replaceChildren(...PROJECTS.map((p) => {
      const card = el("article", "project");
      card.id = `p-${p.id}`;
      card.innerHTML = `
        <span class="tag">${esc(p.tag[lang])}</span>
        <h3>${esc(p.title[lang])}</h3>
        <p>${esc(p.problem[lang])}</p>
        <h4>${esc(t("results"))}</h4>
        <ul class="results">${p.results[lang].map((r) => `<li>${esc(r)}</li>`).join("")}</ul>
        <details><summary>${esc(t("actions"))}</summary><ul>${p.actions[lang].map((a) => `<li>${esc(a)}</li>`).join("")}</ul></details>
        <div class="chips">${p.stack.map((s) => `<span class="chip">${esc(s)}</span>`).join("")}</div>`;
      return card;
    }));

    $("#code-list").replaceChildren(...CODE.map((c) => el("div", "code-item",
      `<code>${esc(c.path)}</code><p>${esc(c[lang])}</p><a href="${esc(CONFIG.repoUrl)}/blob/main/${esc(c.path)}" target="_blank" rel="noopener">${esc(t("openGithub"))} →</a>`)));

    const cv = $("#cv-link");
    if (CONFIG.cvUrl) {
      cv.href = CONFIG.cvUrl;
      cv.textContent = t("cvDownload");
      cv.removeAttribute("aria-disabled");
    } else {
      cv.textContent = t("cvSoon");
      cv.setAttribute("aria-disabled", "true");
    }

    Object.entries({ approval: "demo-approval", vintage: "demo-vintage", roll: "demo-roll", quality: "demo-quality" }).forEach(([k, id]) => {
      const box = document.getElementById(id);
      box.querySelector("h3").textContent = DEMOS[k].title[lang];
      box.querySelector(".read").textContent = DEMOS[k].read[lang];
    });
  }

  // ------------------------------------------------------------ demos
  function renderApproval() {
    const meses = [...new Set(data.aprobacion.map((r) => r.mes))].sort();
    const serie = (canal) => meses.map((m) => {
      const r = data.aprobacion.find((x) => x.mes === m && x.canal === canal);
      return r ? +(r.tasa_aprobacion * 100).toFixed(1) : null;
    });
    const total = meses.map((m) => {
      const rs = data.aprobacion.filter((x) => x.mes === m);
      const s = rs.reduce((a, r) => a + r.solicitudes, 0);
      const a = rs.reduce((acc, r) => acc + r.aprobadas, 0);
      return +((a / s) * 100).toFixed(1);
    });
    const colores = { Web: css("--c-web"), App: css("--c-app"), WhatsApp: css("--c-wa") };
    const ds = CANALES.map((c) => ({ label: c, data: serie(c), borderColor: colores[c], backgroundColor: colores[c], tension: .25, pointRadius: 4, borderWidth: 2.5 }));
    ds.push({ label: t("total"), data: total, borderColor: css("--c-total"), backgroundColor: css("--c-total"), borderDash: [6, 5], tension: .25, pointRadius: 3, borderWidth: 2 });
    if (chart) chart.destroy();
    if (!window.Chart) return;
    chart = new Chart(document.getElementById("chart-approval"), {
      type: "line",
      data: { labels: meses.map(mes), datasets: ds },
      options: {
        responsive: true, maintainAspectRatio: false, spanGaps: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
          legend: { labels: { color: css("--muted"), usePointStyle: true } },
          tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${c.parsed.y}${lang === "es" ? " %" : "%"}` } },
        },
        scales: {
          y: { suggestedMin: 20, suggestedMax: 80, ticks: { color: css("--muted"), callback: (v) => v + "%" }, grid: { color: css("--line") } },
          x: { ticks: { color: css("--muted") }, grid: { display: false } },
        },
      },
    });
  }

  const shade = (v, max, rgb) => `rgba(${rgb}, ${(0.1 + 0.6 * Math.min(1, v / (max || 1))).toFixed(2)})`;

  function renderVintage() {
    const cosechas = [...new Set(data.mora.map((r) => r.cosecha))].sort();
    const max = Math.max(...data.mora.filter((r) => data.madurez[r.cosecha]).map((r) => r.mora_30));
    const head = `<tr><th class="row"></th>${CANALES.map((c) => `<th>${c}</th>`).join("")}<th>${esc(t("total"))}</th></tr>`;
    const rows = cosechas.map((c) => {
      const rs = data.mora.filter((r) => r.cosecha === c);
      const tot = rs.reduce((a, r) => a + r.saldo_mora_30, 0) / rs.reduce((a, r) => a + r.desembolsado, 0);
      const cell = (v) => (v === undefined ? "<td>—</td>" : `<td style="background:${v ? shade(v, max, "214, 69, 69") : "transparent"}">${pct(v)}</td>`);
      const madura = data.madurez[c];
      return `<tr class="${madura ? "" : "immature"}"><th class="row">${mes(c)} 2026${madura ? "" : `<span class="badge">${esc(t("notMature"))}</span>`}</th>${CANALES.map((k) => cell(rs.find((r) => r.canal === k)?.mora_30)).join("")}${cell(tot)}</tr>`;
    });
    $("#table-vintage").innerHTML = `<thead>${head}</thead><tbody>${rows.join("")}</tbody>`;
  }

  function renderRoll() {
    const sel = $("#roll-month");
    const cortes = [...new Set(data.roll.map((r) => r.corte_desde))].sort();
    const actual = sel.value || cortes[cortes.length - 1];
    sel.innerHTML = cortes.map((c) => {
      const hasta = data.roll.find((r) => r.corte_desde === c).corte_hasta;
      return `<option value="${c}">${mes(c)} → ${mes(hasta)}</option>`;
    }).join("");
    sel.value = actual;

    const filas = data.roll.filter((r) => r.corte_desde === actual);
    const cols = [...TRAMOS, "Pagado"];
    const head = `<tr><th class="row">${esc(t("from"))} \\ ${esc(t("to"))}</th>${cols.map((c) => `<th>${esc(tramo(c))}</th>`).join("")}</tr>`;
    const body = TRAMOS.map((desde, i) => {
      const rs = filas.filter((r) => r.tramo_desde === desde);
      const tot = rs.reduce((a, r) => a + r[rollMode], 0);
      if (!tot) return "";
      const tds = cols.map((hacia, j) => {
        const v = (rs.find((r) => r.tramo_hasta === hacia) || {})[rollMode] || 0;
        const p = v / tot;
        const peor = hacia !== "Pagado" && j > i;
        const color = !v ? "transparent" : (j === i ? "rgba(100, 116, 139, .18)" : shade(p, 1, peor ? "214, 69, 69" : "11, 138, 126"));
        return `<td style="background:${color}" title="${v.toLocaleString()}">${v ? pct(p) : "·"}</td>`;
      }).join("");
      return `<tr><th class="row">${esc(tramo(desde))}</th>${tds}</tr>`;
    }).join("");
    $("#table-roll").innerHTML = `<thead>${head}</thead><tbody>${body}</tbody>`;

    const base = filas.filter((r) => r.tramo_desde === "1-30");
    const tot = base.reduce((a, r) => a + r[rollMode], 0);
    const a31 = (base.find((r) => r.tramo_hasta === "31-60") || {})[rollMode] || 0;
    const pag = (base.find((r) => r.tramo_hasta === "Pagado") || {})[rollMode] || 0;
    $("#roll-sentence").textContent = tot ? (lang === "es"
      ? `De lo que estaba en 1-30 al cierre de ${mes(actual)}, ${pct(a31 / tot)} pasó a 31-60 y ${pct(pag / tot)} salió pagado.`
      : `Of what was in 1-30 at the ${mes(actual)} close, ${pct(a31 / tot)} rolled to 31-60 and ${pct(pag / tot)} paid off.`) : "";
  }

  function renderQuality() {
    const L = DEMOS.quality.labels[lang];
    const q = data.calidad;
    const c = data.conciliacion;
    const tile = (num, label, ok) => `<div class="tile"><b class="${ok ? "ok" : ""}">${num}</b><span>${esc(label)}</span></div>`;
    $("#tiles-quality").innerHTML = [
      tile(q.pagos_duplicados, L.pagos_duplicados),
      tile(q.pagos_antes_del_desembolso, L.pagos_antes_del_desembolso),
      tile(q.prestamos_con_solicitud_rechazada, L.prestamos_con_solicitud_rechazada),
      tile(`${q.valores_distintos_de_ciudad} → 6`, L.ciudades),
      tile(`${c.no_cuadran_con_pagos_crudos} → ${c.no_cuadran_sin_duplicados}`, L.conc, true),
    ].join("");
  }

  function renderDemos() {
    if (!data) return;
    renderApproval();
    renderVintage();
    renderRoll();
    renderQuality();
  }

  // ------------------------------------------------------------ SQL y DAX bajo demanda
  const fuentes = { "demo-approval": [DEMOS.approval.sql], "demo-vintage": [DEMOS.vintage.sql], "demo-roll": [DEMOS.roll.sql], "demo-quality": [DEMOS.quality.sql, DEMOS.quality.sql2] };
  Object.entries(fuentes).forEach(([id, paths]) => {
    const det = document.querySelector(`#${id} details.sql`);
    det.addEventListener("toggle", async () => {
      const code = det.querySelector("code");
      if (!det.open || code.dataset.loaded) return;
      code.textContent = t("loading");
      const textos = await Promise.all(paths.map((p) => fetch(p).then((r) => r.text()).catch(() => `-- ${p}`)));
      code.textContent = textos.join("\n\n");
      code.dataset.loaded = "1";
    });
  });
  fetch("powerbi/medidas.dax").then((r) => r.text()).then((s) => { $("#dax").textContent = s; }).catch(() => {});

  // ------------------------------------------------------------ idioma y arranque
  $("#lang").addEventListener("click", () => {
    lang = lang === "es" ? "en" : "es";
    try { localStorage.setItem("lang", lang); } catch (e) { /* sin almacenamiento */ }
    renderText();
    renderDemos();
  });
  $("#roll-month").addEventListener("change", renderRoll);
  document.querySelectorAll("#demo-roll .toggle button").forEach((b) => b.addEventListener("click", () => {
    rollMode = b.dataset.mode;
    document.querySelectorAll("#demo-roll .toggle button").forEach((x) => x.classList.toggle("on", x === b));
    renderRoll();
  }));

  renderText();
  fetch("data/demos.json").then((r) => r.json()).then((d) => { data = d; renderDemos(); });
})();
