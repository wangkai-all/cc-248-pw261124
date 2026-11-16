/* Catalog collection console — vanilla JS SPA */
"use strict";

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

async function api(path, opts = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(opts.headers || {}) },
    ...opts,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  const text = await res.text();
  let data;
  try { data = text ? JSON.parse(text) : {}; }
  catch { data = { raw: text }; }
  return { ok: res.ok, status: res.status, data };
}

const show = (el, msg, ok = true) => {
  el.className = "result " + (ok ? "ok" : "err");
  el.textContent = typeof msg === "string" ? msg : JSON.stringify(msg, null, 2);
};
const esc = (s) => String(s ?? "").replace(/[&<>"]/g,
  (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const fmtTime = (t) => t ? new Date(t * 1000).toLocaleTimeString() : "-";

// ---------------- tabs ----------------
$$(".tab").forEach((btn) => btn.addEventListener("click", () => {
  $$(".tab").forEach((b) => b.classList.remove("active"));
  $$(".panel").forEach((p) => p.classList.remove("active"));
  btn.classList.add("active");
  $("#tab-" + btn.dataset.tab).classList.add("active");
  if (btn.dataset.tab === "source") refreshState();
  if (btn.dataset.tab === "tasks") refreshTasks();
  if (btn.dataset.tab === "versions") refreshVersions();
  if (btn.dataset.tab === "requests") refreshRequests();
  if (btn.dataset.tab === "collect") refreshResumeArea();
}));

// ---------------- source state ----------------
async function refreshState() {
  const { data } = await api("/api/state");
  const grid = $("#source-grid");
  grid.innerHTML = "";
  const updTypes = new Set(), idsByType = {};
  for (const [key, obj] of Object.entries(data)) {
    const [type, id] = key.split("/");
    (idsByType[type] ||= []).push(id);
    updTypes.add(type);
    const div = document.createElement("div");
    div.className = "obj" + (obj.deleted ? " deleted" : "");
    div.innerHTML = `
      <div class="head">
        <span class="key">${esc(key)}</span>
        <span>
          ${obj.deleted ? '<span class="badge dead">已删除</span>' : ""}
          <span class="badge rev">rev ${obj.rev}</span>
          <span class="badge">${esc(obj.etag.slice(0, 8))}</span>
        </span>
      </div>
      <pre class="body">${esc(JSON.stringify(obj.body, null, 2))}</pre>`;
    grid.appendChild(div);
  }
  const fill = (selType, selId) => {
    selType.innerHTML = [...updTypes].sort()
      .map((t) => `<option>${t}</option>`).join("");
    const fillIds = () => {
      selId.innerHTML = (idsByType[selType.value] || [])
        .map((i) => `<option>${esc(i)}</option>`).join("");
    };
    selType.addEventListener("change", fillIds);
    fillIds();
  };
  fill($("#upd-type"), $("#upd-id"));
  fill($("#del-type"), $("#del-id"));
  fill($("#sch-type"), $("#sch-id"));
  // prefill update body when an object is picked
  const prefill = () => {
    const key = $("#upd-type").value + "/" + $("#upd-id").value;
    const obj = data[key];
    if (obj) $("#upd-body").value = JSON.stringify(obj.body, null, 2);
  };
  $("#upd-id").addEventListener("change", prefill);
  prefill();
}

$("#btn-refresh-state").addEventListener("click", refreshState);

// ---------------- update / delete ----------------
$("#btn-update").addEventListener("click", async () => {
  let body;
  try { body = JSON.parse($("#upd-body").value); }
  catch { return show($("#upd-result"), "JSON 解析失败", false); }
  const headers = {};
  const im = $("#upd-ifmatch").value.trim();
  if (im) headers["If-Match"] = im;
  const { status, data } = await api(
    `/api/source/${$("#upd-type").value}/${$("#upd-id").value}`,
    { method: "PUT", body, headers });
  show($("#upd-result"),
    `HTTP ${status}\n` + JSON.stringify(data, null, 2),
    status < 400);
  refreshState();
});

$("#btn-delete").addEventListener("click", async () => {
  const headers = {};
  const im = $("#del-ifmatch").value.trim();
  if (im) headers["If-Match"] = im;
  const { status, data } = await api(
    `/api/source/${$("#del-type").value}/${$("#del-id").value}`,
    { method: "DELETE", headers });
  show($("#del-result"),
    `HTTP ${status} ` + JSON.stringify(data), status < 400);
  refreshState();
});

// ---------------- faults & schedules ----------------
$("#btn-fault").addEventListener("click", async () => {
  const oids = $("#fault-oids").value.split(",").map((s) => s.trim()).filter(Boolean);
  const { data } = await api("/api/faults", { method: "POST", body: {
    fail_oids: oids, status: Number($("#fault-status").value),
    times: Number($("#fault-times").value),
    persistent: $("#fault-persistent").checked } });
  show($("#fault-result"), JSON.stringify(data, null, 2));
});
$("#btn-fault-clear").addEventListener("click", async () => {
  const { data } = await api("/api/faults",
    { method: "POST", body: { clear: true } });
  show($("#fault-result"), "已清除 " + JSON.stringify(data));
});
$("#btn-schedule").addEventListener("click", async () => {
  let patch = {};
  if ($("#sch-patch").value.trim()) {
    try { patch = JSON.parse($("#sch-patch").value); }
    catch { return show($("#sch-result"), "补丁 JSON 解析失败", false); }
  }
  const { data } = await api("/api/schedule", { method: "POST", body: {
    type: $("#sch-type").value, id: $("#sch-id").value,
    on_get: Number($("#sch-onget").value), patch } });
  show($("#sch-result"), JSON.stringify(data, null, 2));
});
$("#btn-schedule-clear").addEventListener("click", async () => {
  const { data } = await api("/api/schedule",
    { method: "POST", body: { clear: true } });
  show($("#sch-result"), "已清除 " + JSON.stringify(data));
});

// ---------------- one-click scenes ----------------
$$("[data-scene]").forEach((btn) => btn.addEventListener(
  "click", () => runScene(btn.dataset.scene)));

async function runScene(scene) {
  const out = $("#scene-result");
  if (scene === "reset") {
    await api("/api/faults", { method: "POST", body: { clear: true } });
    await api("/api/schedule", { method: "POST", body: { clear: true } });
    return show(out, "已清除故障与调度注入");
  }
  if (scene === "cycle") {
    // wire product p100's dataset to the self-referential cyclic dataset
    const { data: st } = await api("/api/state");
    const body = st["product/p100"].body;
    body.dataset = "dataset/cyclic";
    const r = await api("/api/source/product/p100",
      { method: "PUT", body });
    return show(out, "p100 已改指 dataset/cyclic（自引用环）\n" +
      JSON.stringify(r.data, null, 2), r.ok);
  }
  if (scene === "missing") {
    const r = await api("/api/source/field/region", { method: "DELETE" });
    return show(out, "已删除 field/region（dataset/sales 仍引用它）\n" +
      JSON.stringify(r.data), r.ok);
  }
  if (scene === "conflict") {
    const r = await api("/api/schedule", { method: "POST", body: {
      type: "dataset", id: "sales", on_get: 3,
      patch: { name: "销售数据集-并发修订" } } });
    return show(out,
      "已调度：第 3 次 GET dataset/sales 时来源提交新修订，"
      + "闭合核对将收到条件冲突\n" + JSON.stringify(r.data, null, 2));
  }
  if (scene === "flaky") {
    const r = await api("/api/faults", { method: "POST", body: {
      fail_oids: ["dataset/sales"], status: 500, times: 2 } });
    return show(out,
      "dataset/sales 前两次请求返回 500（采集器重试后仍会耗尽，"
      + "任务失败但保留已钉引用，可续采）\n"
      + JSON.stringify(r.data, null, 2));
  }
}

// ---------------- collect ----------------
$("#btn-collect").addEventListener("click", async () => {
  const sync = $("#col-sync").checked;
  const { status, data } = await api("/api/tasks", { method: "POST", body: {
    catalog_id: $("#col-catalog").value,
    strategy: $("#col-strategy").value, async: !sync,
    note: "console" } });
  if (sync) {
    renderTaskOutcome($("#collect-result"), data);
  } else {
    show($("#collect-result"),
      `任务已启动 ${data.task_id}（seq=${data.seq}），到“任务”页查看进度`);
    setTimeout(pollRunning, 600);
  }
});

function renderTaskOutcome(el, rec) {
  if (rec.status === "failed") {
    el.className = "result err";
    el.innerHTML = "";
    const box = document.createElement("div");
    box.className = "errbox";
    box.textContent = `任务 ${rec.task_id} 未能闭合（${rec.error.kind}）：${rec.error.reason}`;
    el.appendChild(box);
    el.appendChild(document.createTextNode(
      "请求代价：" + JSON.stringify(rec.cost)));
  } else if (rec.status === "closed") {
    el.className = "result ok";
    el.textContent = `任务 ${rec.task_id} 已闭合，对象 ${Object.keys(
      rec.snapshot.objects).length} 个；可到“版本存储”发布。\n`
      + "请求代价：" + JSON.stringify(rec.cost, null, 2);
  } else {
    show(el, `任务状态：${rec.status}`);
  }
}

async function pollRunning() {
  const { data } = await api("/api/tasks");
  if (data.some((t) => t.status === "running")) setTimeout(pollRunning, 700);
  refreshResumeArea();
}

async function refreshResumeArea() {
  const { data } = await api("/api/tasks");
  const area = $("#resume-area");
  const failed = data.filter((t) => t.status === "failed");
  if (!failed.length) {
    area.innerHTML = '<span class="hint">当前没有失败任务。</span>';
    return;
  }
  area.innerHTML = "";
  for (const t of failed) {
    const full = (await api("/api/tasks/" + t.task_id)).data;
    const div = document.createElement("div");
    div.className = "errbox";
    const pins = full.progress?.visited_pins
      ? Object.keys(full.progress.visited_pins) : [];
    div.innerHTML = `
      <b>${esc(t.task_id)}</b> seq=${t.seq} · ${esc(t.strategy)}
      · 已续采 ${t.resume_count} 次<br>
      原因（${esc(full.error?.kind)}）：${esc(full.error?.reason)}<br>
      <span class="small">已保留钉住引用 ${pins.length} 个：${
        esc(pins.join(", "))}</span><br>
      <button data-resume="${esc(t.task_id)}">断点续采</button>`;
    area.appendChild(div);
  }
  $$("[data-resume]").forEach((b) => b.addEventListener("click", async () => {
    const r = await api(`/api/tasks/${b.dataset.resume}/resume`,
      { method: "POST" });
    if (!r.ok) alert(r.data.error);
    setTimeout(() => refreshResumeArea(), 800);
  }));
}

// ---------------- tasks ----------------
async function refreshTasks() {
  const { data } = await api("/api/tasks");
  const sel = $("#task-select");
  sel.innerHTML = data.map((t) =>
    `<option value="${t.task_id}">${t.task_id} · ${t.status} · seq ${t.seq}
     · ${t.strategy}</option>`).join("");
}
$("#btn-refresh-tasks").addEventListener("click", refreshTasks);
$("#btn-load-task").addEventListener("click", async () => {
  const id = $("#task-select").value;
  if (!id) return;
  const { data: rec } = await api("/api/tasks/" + id);
  const el = $("#task-detail");
  el.innerHTML = "";
  const head = document.createElement("div");
  head.innerHTML = `
    <div class="kv">
      <span class="k">任务 / seq</span><span>${esc(rec.task_id)} / ${rec.seq}</span>
      <span class="k">目录 / 策略</span><span>${esc(rec.catalog_id)} / ${esc(rec.strategy)}</span>
      <span class="k">状态</span><span class="status-${esc(rec.status)}">${esc(rec.status)}</span>
      <span class="k">续采次数</span><span>${rec.resume_count}</span>
      <span class="k">已发布版本</span><span>${esc(rec.published_version || "—")}</span>
    </div>`;
  el.appendChild(head);

  if (rec.error) {
    const box = document.createElement("div");
    box.className = "errbox";
    box.innerHTML = `<b>不能闭合的原因（${esc(rec.error.kind)}）</b><br>${
      esc(rec.error.reason)}<br><span class="small">${
      esc(JSON.stringify(rec.error.details))}</span>`;
    el.appendChild(box);
    if (rec.status === "failed") {
      const btn = document.createElement("button");
      btn.className = "primary";
      btn.textContent = "断点续采";
      btn.addEventListener("click", async () => {
        const r = await api(`/api/tasks/${rec.task_id}/resume`,
          { method: "POST" });
        if (!r.ok) alert(r.data.error);
        setTimeout(() => $("#btn-load-task").click(), 900);
      });
      el.appendChild(btn);
      const pubHint = document.createElement("span");
      pubHint.className = "hint";
      pubHint.textContent = " 闭合成功后可发布（受围栏保护）";
      el.appendChild(pubHint);
    }
  }

  // cost
  if (rec.cost && rec.cost.requests !== undefined) {
    const c = rec.cost;
    const line = document.createElement("div");
    line.className = "costline";
    line.innerHTML = [
      ["总请求", c.requests], ["条件请求", c.conditional],
      ["304 省字节", c.not_modified], ["412 冲突", c.precondition_failed],
      ["重试", c.retries], ["接收字节", c.bytes_received],
    ].map(([k, v]) => `<span class="pill">${k}: ${v}</span>`).join("")
      + `<span class="pill">状态分布: ${esc(JSON.stringify(c.by_status))}</span>`;
    el.appendChild(line);
  }

  // attempts & rounds
  for (const a of rec.attempts || []) {
    const d = document.createElement("div");
    d.className = "card";
    d.style.margin = "8px 0";
    d.innerHTML = `<h3>尝试 @ ${fmtTime(a.started_at)} — ${esc(a.status)}
      ${a.reason ? "（" + esc(a.kind) + "）" : ""}</h3>
      ${a.reason ? `<div class="errbox">${esc(a.reason)}</div>` : ""}
      ${(a.rounds || []).map((r) => `
        <div class="small">第 ${r.round} 轮：变化 ${r.changed.length}
        个 [${esc(r.changed.join(", "))}]，缺失 ${r.missing.length}
        个，本轮请求 ${r.requests_this_round}</div>`).join("")}
      <div class="small edge">遍历路径: ${esc((a.visited_path || []).join(" → "))}</div>`;
    el.appendChild(d);
  }

  // progress / pins / edges
  if (rec.progress) {
    const pins = rec.progress.visited_pins || {};
    const d = document.createElement("div");
    d.className = "card";
    d.innerHTML = `<h3>恢复进度（已钉引用 ${Object.keys(pins).length}）</h3>
      <div class="small edge">${esc(Object.keys(pins).join("\n")) || "—"}</div>
      <h3 style="margin-top:10px">引用边（${(rec.progress.edges || []).length}）</h3>
      <div class="small edge">${esc((rec.progress.edges || []).map((e) => e.join(" → ")).join("\n"))}</div>`;
    el.appendChild(d);
  }

  // snapshot
  if (rec.snapshot) {
    const d = document.createElement("div");
    d.className = "card";
    const objs = rec.snapshot.objects;
    d.innerHTML = `<h3>闭合快照（${Object.keys(objs).length} 个对象，版本不可变）</h3>`;
    const table = document.createElement("table");
    table.innerHTML = `<thead><tr><th>对象</th><th>rev</th><th>ETag</th></tr></thead>`
      + "<tbody>" + Object.entries(objs).map(([k, o]) =>
        `<tr><td>${esc(k)}</td><td>${o.rev}</td><td>${esc(o.etag.slice(0, 10))}</td></tr>`
      ).join("") + "</tbody>";
    d.appendChild(table);
    if (rec.status === "closed" && !rec.published_version) {
      const btn = document.createElement("button");
      btn.className = "primary";
      btn.textContent = "发布该闭合快照";
      btn.addEventListener("click", async () => {
        const r = await api(`/api/tasks/${rec.task_id}/publish`,
          { method: "POST" });
        alert(r.ok ? `已发布 ${r.data.version}` : "围栏拒绝: " + r.data.error);
        refreshVersions();
      });
      d.appendChild(btn);
    }
    el.appendChild(d);
  }
});

// ---------------- versions ----------------
async function refreshVersions() {
  const { data } = await api("/api/versions");
  $("#version-fence").innerHTML = `
    <div class="okbox">发布围栏 epoch=<b>${data.index.epoch}</b>
    （只接受 seq 更大的任务，迟到旧任务无法回退）；
    最新版本：<b>${esc(data.index.latest_version || "尚未发布")}</b>；
    下一个任务 seq=${data.index.next_seq}</div>`;
  $("#version-select").innerHTML = data.versions.map((v) =>
    `<option value="${v.version}">${v.version} · ${v.catalog_id}
     · 对象 ${v.object_count} · ${v.strategy}</option>`).join("");
  const detail = $("#version-detail");
  detail.innerHTML = data.versions.length ? `<table>
    <thead><tr><th>版本</th><th>发布时间</th><th>任务</th><th>seq</th>
      <th>目录</th><th>策略</th><th>对象数</th><th>请求代价</th></tr></thead>
    <tbody>${data.versions.map((v) => `<tr>
      <td>${v.version}</td><td>${fmtTime(v.published_at)}</td>
      <td>${esc(v.task_id)}</td><td>${v.seq}</td>
      <td>${esc(v.catalog_id)}</td><td>${esc(v.strategy)}</td>
      <td>${v.object_count}</td>
      <td class="small">${esc(JSON.stringify(v.cost))}</td></tr>`).join("")}
    </tbody></table>` : '<span class="hint">还没有发布版本。</span>';
}
$("#btn-refresh-versions").addEventListener("click", refreshVersions);
$("#btn-load-version").addEventListener("click", async () => {
  const v = $("#version-select").value;
  if (!v) return;
  const { data: doc } = await api("/api/versions/" + v);
  const el = $("#version-detail");
  const wrap = document.createElement("div");
  wrap.className = "card";
  wrap.innerHTML = `<h3>${esc(doc.version)} 不可变快照</h3>
    <pre class="body">${esc(JSON.stringify({
      version: doc.version, task_id: doc.task_id, seq: doc.seq,
      catalog_id: doc.catalog_id, strategy: doc.strategy,
      object_revs: Object.fromEntries(Object.entries(doc.objects)
        .map(([k, o]) => [k, { rev: o.rev, etag: o.etag.slice(0, 8) }])),
      edges: doc.edges, cost: doc.cost }, null, 2))}</pre>`;
  el.appendChild(wrap);
});

// ---------------- raw requests ----------------
async function refreshRequests() {
  const q = $("#req-filter").value.trim();
  const { data } = await api("/api/requests" + (q ? `?task_id=${q}` : ""));
  $("#req-table tbody").innerHTML = data.slice().reverse().map((r) => `
    <tr>
      <td>${r.seq}</td><td>${esc(r.method)}</td>
      <td class="edge">${esc(r.path)}</td>
      <td class="status-${r.status < 300 ? "closed"
        : r.status < 500 ? "running" : "failed"}">${r.status}</td>
      <td class="small">${esc(JSON.stringify(
        Object.fromEntries(Object.entries(r.request_headers)
          .filter(([k]) => /if-|etag/i.test(k))))}</td>
      <td class="small">${esc(r.request_headers["x-task-id"] || "")}</td>
      <td class="raw"><pre class="body">${esc(
        Object.entries(r.response_headers).map(([k, v]) => `${k}: ${v}`)
          .join("\n"))}\n${esc(r.body)}</pre></td>
    </tr>`).join("");
}
$("#btn-refresh-req").addEventListener("click", refreshRequests);
$("#btn-clear-req").addEventListener("click", async () => {
  await api("/api/requests", { method: "DELETE" });
  refreshRequests();
});

// boot
refreshState();
