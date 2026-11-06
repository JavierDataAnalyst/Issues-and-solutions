// Render del portafolio: hoja de vida bilingüe, proyectos y demos sobre data/demos.json.
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
  const bold = (s) => esc(s).replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>");

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

  // ------------------------------------------------------------ portada: modelo estrella + texto cifrado
  // Un modelo estrella (tabla de hechos al centro y dimensiones alrededor) con datos que fluyen hacia los hechos,
  // sobre un campo de caracteres que cambian (como datos cifrados). Las palabras de KEYWORDS se "descifran",
  // se quedan unos segundos y se desvanecen, intercalándose.
  const SCHEMA = {
    fact: { name: "fact_prestamos", cols: ["prestamo_id   PK", "cliente_id    FK", "fecha_id      FK", "producto_id   FK", "monto · saldo · dpd"] },
    dims: [
      { name: "dim_cliente", cols: ["score · ciudad"], x: 0.31, y: 0.17 },
      { name: "dim_fecha", cols: ["mes · cosecha"], x: 0.58, y: 0.10 },
      { name: "dim_producto", cols: ["tipo · plazo"], x: 0.85, y: 0.17 },
      { name: "dim_canal", cols: ["web · app"], x: 0.85, y: 0.83 },
      { name: "dim_tramo_mora", cols: ["1-30 · 31-60"], x: 0.58, y: 0.90 },
      { name: "dim_portafolio", cols: ["nombre · tier"], x: 0.31, y: 0.83 },
    ],
  };

  function initMatrix() {
    const box = $("#hero-art");
    box.innerHTML = '<canvas aria-hidden="true"></canvas>';
    const cv = box.querySelector("canvas");
    const ctx = cv.getContext("2d");
    const CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";
    const FS = 13, LH = 18, CW = 8;
    const MONO = "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace";
    const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    const rnd = (n) => Math.floor(Math.random() * n);
    const ch = () => CHARS[rnd(CHARS.length)];
    const cen = (b) => [b.x + b.w / 2, b.y + b.h / 2];
    let W = 0, H = 0, cols = 0, rows = 0, grid = [], words = [], visible = true, last = 0;
    let fact = null, dims = [], boxes = [], masked = new Uint8Array(0);

    function addWord(now, age = 0) {
      const list = KEYWORDS[lang];
      const text = list[rnd(list.length)];
      if (text.length + 2 > cols) return;
      const row = rnd(rows), col = rnd(cols - text.length);
      for (let i = 0; i < text.length; i++) if (masked[row * cols + col + i]) return; // no encima de las tablas
      const choca = words.some((w) => Math.abs(w.row - row) < 1 && col < w.col + w.text.length + 2 && w.col < col + text.length + 2);
      if (choca || words.some((w) => w.text === text)) return;
      words.push({ text, row, col, born: now - age, life: 5200 + rnd(3800), tone: Math.random() < 0.55 ? "143, 180, 255" : "222, 228, 238" });
    }

    function size() {
      const dpr = Math.min(2, window.devicePixelRatio || 1);
      W = box.clientWidth; H = box.clientHeight;
      cv.width = W * dpr; cv.height = H * dpr;
      cv.style.width = `${W}px`; cv.style.height = `${H}px`;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      cols = Math.ceil(W / CW); rows = Math.ceil(H / LH);
      grid = Array.from({ length: rows }, () => Array.from({ length: cols }, () => ({ c: ch(), a: 0.10 + Math.random() * 0.26 })));

      const fw = Math.min(176, W * 0.28), fh = 112;
      const dw = Math.min(134, W * 0.22), dh = 50;
      fact = { ...SCHEMA.fact, x: W * 0.58 - fw / 2, y: H * 0.5 - fh / 2, w: fw, h: fh };
      dims = SCHEMA.dims.map((d) => ({ ...d, x: W * d.x - dw / 2, y: Math.min(H - dh - 4, Math.max(4, H * d.y - dh / 2)), w: dw, h: dh }));
      boxes = [fact, ...dims];
      // celdas tapadas por las tablas: ahí no se dibujan caracteres
      masked = new Uint8Array(rows * cols);
      for (let r = 0; r < rows; r++) {
        for (let c = 0; c < cols; c++) {
          const x = c * CW + CW / 2, y = r * LH + LH / 2 + 2;
          if (boxes.some((b) => x > b.x - 6 && x < b.x + b.w + 6 && y > b.y - 6 && y < b.y + b.h + 6)) masked[r * cols + c] = 1;
        }
      }

      words = [];
      const now = performance.now();
      for (let i = 0; i < 60 && words.length < Math.max(4, Math.round(rows / 2)); i++) addWord(now, 1200 + rnd(2500));
    }

    function drawSchema(now) {
      // relaciones uno a varios, con datos que fluyen de cada dimensión hacia los hechos
      const [fx, fy] = cen(fact);
      dims.forEach((d, i) => {
        const [dx, dy] = cen(d);
        ctx.strokeStyle = "rgba(110, 150, 230, .38)";
        ctx.lineWidth = 1.2;
        ctx.beginPath(); ctx.moveTo(dx, dy); ctx.lineTo(fx, fy); ctx.stroke();
        const t = reduce ? 0.5 : (now / 2800 + i / dims.length) % 1;
        ctx.fillStyle = "rgba(125, 200, 255, .85)";
        ctx.beginPath(); ctx.arc(dx + (fx - dx) * t, dy + (fy - dy) * t, 2.2, 0, Math.PI * 2); ctx.fill();
      });
      boxes.forEach((b, k) => {
        const esHechos = k === 0;
        ctx.save();
        ctx.beginPath();
        if (ctx.roundRect) ctx.roundRect(b.x, b.y, b.w, b.h, 6); else ctx.rect(b.x, b.y, b.w, b.h);
        ctx.fillStyle = "rgba(12, 22, 48, .94)";
        ctx.fill();
        ctx.clip();
        ctx.fillStyle = esHechos ? "rgba(61, 110, 230, .38)" : "rgba(61, 100, 200, .24)";
        ctx.fillRect(b.x, b.y, b.w, 19);
        ctx.font = `600 11px ${MONO}`;
        ctx.fillStyle = "rgba(225, 233, 248, .95)";
        ctx.fillText(b.name, b.x + 8, b.y + 4);
        ctx.font = `10.5px ${MONO}`;
        ctx.fillStyle = "rgba(160, 175, 205, .8)";
        b.cols.forEach((c, j) => ctx.fillText(c, b.x + 8, b.y + 25 + j * 14));
        ctx.restore();
        ctx.strokeStyle = esHechos ? "rgba(140, 180, 255, .75)" : "rgba(120, 155, 230, .5)";
        ctx.lineWidth = esHechos ? 1.5 : 1;
        ctx.beginPath();
        if (ctx.roundRect) ctx.roundRect(b.x, b.y, b.w, b.h, 6); else ctx.rect(b.x, b.y, b.w, b.h);
        ctx.stroke();
      });
    }

    function draw(now) {
      if (!reduce) requestAnimationFrame(draw);
      if (!visible || (now - last < 55 && !reduce)) return; // ~18 cuadros por segundo
      last = now;
      ctx.clearRect(0, 0, W, H);
      ctx.textBaseline = "top";
      drawSchema(now);

      // caracteres que mutan alrededor de las tablas
      if (!reduce) for (let k = 0; k < cols * rows * 0.012; k++) grid[rnd(rows)][rnd(cols)].c = ch();
      words = words.filter((w) => now - w.born < w.life);
      const ocupado = new Set();
      words.forEach((w) => { for (let i = 0; i < w.text.length; i++) ocupado.add(w.row * cols + w.col + i); });
      ctx.font = `${FS}px ${MONO}`;
      for (let r = 0; r < rows; r++) {
        for (let c = 0; c < cols; c++) {
          const idx = r * cols + c;
          if (masked[idx] || ocupado.has(idx)) continue;
          const cell = grid[r][c];
          ctx.fillStyle = `rgba(150, 164, 190, ${cell.a})`;
          ctx.fillText(cell.c, c * CW, r * LH + 2);
        }
      }

      // palabras clave: se descifran letra por letra, se mantienen y se desvanecen
      ctx.font = `600 ${FS}px ${MONO}`;
      words.forEach((w) => {
        const t = now - w.born;
        const listas = Math.floor(w.text.length * Math.min(1, t / 900));
        const fade = Math.min(1, t / 300, (w.life - t) / 900);
        for (let i = 0; i < w.text.length; i++) {
          if (w.text[i] === " ") continue;
          const lista = i < listas;
          ctx.fillStyle = `rgba(${w.tone}, ${(fade * (lista ? 0.95 : 0.35)).toFixed(2)})`;
          ctx.fillText(lista ? w.text[i] : ch(), (w.col + i) * CW, w.row * LH + 2);
        }
      });
      if (!reduce && words.length < Math.max(4, Math.round(rows / 2)) && Math.random() < 0.08) addWord(now);
    }

    size();
    if ("ResizeObserver" in window) new ResizeObserver(() => { size(); if (reduce) draw(performance.now()); }).observe(box);
    if ("IntersectionObserver" in window) new IntersectionObserver(([e]) => { visible = e.isIntersecting; }).observe(box);
    draw(performance.now());
  }

  // ------------------------------------------------------------ textos
  function renderText() {
    document.documentElement.lang = lang;
    document.querySelectorAll("[data-i18n]").forEach((n) => { n.textContent = t(n.dataset.i18n); });
    $("#lang").textContent = lang === "es" ? "EN" : "ES";
    $("#github-link").href = CONFIG.githubUrl;
    $("#linkedin-link").href = CONFIG.linkedinUrl;
    $("#email-link").href = `mailto:${CONFIG.email}`;

    const cv = $("#cv-link");
    if (CONFIG.cvUrl) {
      cv.href = CONFIG.cvUrl;
      cv.setAttribute("download", "Javier_Daza_CV.pdf");
      cv.textContent = t("cvDownload");
      cv.removeAttribute("aria-disabled");
    } else {
      cv.textContent = t("cvSoon");
      cv.setAttribute("aria-disabled", "true");
    }

    $("#stats").replaceChildren(...t("stats").map(([b, label, cap]) =>
      el("div", "stat", `<small>${esc(label)}</small><b>${esc(b)}</b><span>${esc(cap)}</span>`)));

    $("#skills").replaceChildren(...SKILLS.map((s) =>
      el("div", "skill", `<p class="kicker">${esc(s.label[lang])}</p><h3>${esc(s.title[lang])}</h3><p>${esc(s.items[lang])}</p>`)));

    $("#timeline").replaceChildren(...EXPERIENCE.map((j) => el("article", "card job", `
      <span class="period">${esc(j.period[lang])}</span>
      <h3>${esc(j.role[lang])}</h3>
      <p class="org">${esc(j.org[lang])}</p>
      <ul>${j.bullets[lang].map((b) => `<li>${bold(b)}</li>`).join("")}</ul>`)));

    $("#education").replaceChildren(
      ...EDUCATION.map((e) => el("div", "card", `<p class="kicker">${esc(e.period)}</p><b>${esc(e.title[lang])}</b><span>${esc(e.org)}</span>`)),
      el("div", "card", `<p class="kicker">${esc(t("inProgress"))}</p><b>${esc(IN_PROGRESS[lang])}</b>`),
    );

    $("#projects").replaceChildren(...PROJECTS.map((p) => {
      const card = el("article", "card project");
      card.id = `p-${p.id}`;
      card.innerHTML = `
        <p class="kicker">${esc(p.tag[lang])}</p>
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

    Object.entries({ approval: "demo-approval", vintage: "demo-vintage", roll: "demo-roll", quality: "demo-quality" }).forEach(([k, id]) => {
      const box = document.getElementById(id);
      box.querySelector("h3").textContent = DEMOS[k].title[lang];
      box.querySelector(".read").textContent = DEMOS[k].read[lang];
      box.querySelector(".note").textContent = DEMOS[k].note[lang];
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
    const ds = CANALES.map((c) => ({ label: c, data: serie(c), borderColor: colores[c], backgroundColor: colores[c], tension: .25, pointRadius: 3, borderWidth: 2.2 }));
    ds.push({ label: t("total"), data: total, borderColor: css("--c-total"), backgroundColor: css("--c-total"), borderDash: [6, 5], tension: .3, pointRadius: 3, borderWidth: 2 });
    if (chart) chart.destroy();
    if (!window.Chart) return;
    Chart.defaults.font.family = "Inter, system-ui, sans-serif";
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
          y: { suggestedMin: 20, suggestedMax: 80, ticks: { color: css("--dim"), callback: (v) => v + "%" }, grid: { color: css("--line") } },
          x: { ticks: { color: css("--dim") }, grid: { display: false } },
        },
      },
    });
  }

  // tonos sobrios: rojo vino para riesgo, verde oscuro para mejora
  const RISK = "153, 27, 27";
  const GOOD = "15, 118, 88";
  const shade = (v, max, rgb) => `rgba(${rgb}, ${(0.18 + 0.5 * Math.min(1, v / (max || 1))).toFixed(2)})`;

  function renderVintage() {
    const cosechas = [...new Set(data.mora.map((r) => r.cosecha))].sort();
    // ratio exacto desde los montos: mora_30 viene redondeado a 4 decimales en el SQL
    const ratio = (r) => r && r.saldo_mora_30 / r.desembolsado;
    const max = Math.max(...data.mora.filter((r) => data.madurez[r.cosecha]).map(ratio));
    const suma = (rs) => rs.reduce((a, r) => a + r.saldo_mora_30, 0) / rs.reduce((a, r) => a + r.desembolsado, 0);
    const head = `<tr><th class="row">${lang === "es" ? "Cosecha" : "Vintage"}</th>${CANALES.map((c) => `<th>${c}</th>`).join("")}<th>${esc(t("total"))}</th></tr>`;
    const cell = (v, heat = true) => (v === undefined ? "<td>—</td>"
      : `<td style="background:${heat && v ? shade(v, max, RISK) : "transparent"}">${v ? pct(v) : "–"}</td>`);
    const rows = cosechas.map((c) => {
      const rs = data.mora.filter((r) => r.cosecha === c);
      const madura = data.madurez[c];
      return `<tr class="${madura ? "" : "immature"}"><th class="row">${mes(c)} 2026${madura ? "" : `<span class="badge">${esc(t("notMature"))}</span>`}</th>${CANALES.map((k) => cell(ratio(rs.find((r) => r.canal === k)))).join("")}<td class="risk-text">${pct(suma(rs))}</td></tr>`;
    });
    // fila total solo con cosechas maduras: sumar las que no han madurado bajaría la mora artificialmente
    const maduras = data.mora.filter((r) => data.madurez[r.cosecha]);
    const total = `<tr class="total"><th class="row">${lang === "es" ? "Total maduras" : "Mature total"}</th>${CANALES.map((k) => `<td>${pct(suma(maduras.filter((r) => r.canal === k)))}</td>`).join("")}<td class="risk-text">${pct(suma(maduras))}</td></tr>`;
    $("#table-vintage").innerHTML = `<thead>${head}</thead><tbody>${rows.join("")}${total}</tbody>`;
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
        const color = !v ? "transparent" : (j === i ? "rgba(120, 128, 145, .16)" : shade(p, 1, peor ? RISK : GOOD));
        return `<td style="background:${color}" title="${v.toLocaleString()}">${v ? pct(p) : "–"}</td>`;
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
    const tile = (num, label, ok) => `<div class="tile-q"><b class="${ok ? "ok" : ""}">${num}</b><span>${esc(label)}</span></div>`;
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

  // ------------------------------------------------------------ pestaña activa según la sección visible
  const tabs = [...document.querySelectorAll(".tabs a")];
  if ("IntersectionObserver" in window) {
    const obs = new IntersectionObserver((entries) => {
      entries.forEach((e) => {
        if (!e.isIntersecting) return;
        tabs.forEach((a) => a.classList.toggle("active", a.getAttribute("href") === `#${e.target.id}`));
      });
    }, { rootMargin: "-45% 0px -50% 0px" });
    tabs.forEach((a) => { const s = document.querySelector(a.getAttribute("href")); if (s) obs.observe(s); });
    // en la portada no hay pestaña activa
    window.addEventListener("scroll", () => { if (window.scrollY < 250) tabs.forEach((a) => a.classList.remove("active")); }, { passive: true });
  }

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
  initMatrix();
  fetch("data/demos.json").then((r) => r.json()).then((d) => { data = d; renderDemos(); });
})();
