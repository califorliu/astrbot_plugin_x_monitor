/**
 * X 监控转发 · 管理页
 *
 * 沙箱约束（决定了下面的写法）：
 *  - 无 allow-modals：alert / confirm / prompt 会静默失效 → 自绘 toast 与 Promise 化 modal
 *  - 无 allow-same-origin：拿不到 Dashboard cookie，也不能直接 <img src="/api/...">
 *    → 一切数据走 bridge，页面不直接 fetch
 *  - 路由带插件名前缀，但**页面侧 endpoint 不能带**（bridge.apiGet("overview")）
 *  - 用户数据一律用 DOM 构造写入，不拼 innerHTML
 */
(function () {
  "use strict";

  // 页面 SDK 由宿主注入。**不要在脚本顶层同步取它**：注入可能晚于本脚本执行，
  // 那样拿到的是 undefined，报错会表现为
  // "Cannot read properties of undefined (reading 'apiGet')"。
  // 这里改成惰性获取 + 有限等待，不再依赖注入时序。
  let bridge = null;
  let modalOpen = false;
  let renderSeq = 0;
  let configPromise = null;
  const state = { overview: null, config: null, tab: "matrix", fatal: null };

  /** 等待宿主注入 window.AstrBotPluginPage；超时返回 null。 */
  async function waitForBridge(timeoutMs) {
    const deadline = Date.now() + (timeoutMs || 5000);
    while (Date.now() < deadline) {
      if (window.AstrBotPluginPage) return window.AstrBotPluginPage;
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    return null;
  }

  // ---------------------------------------------------------------- DOM 工具

  function el(tag, attrs, ...children) {
    const node = document.createElement(tag);
    if (attrs) {
      for (const [key, value] of Object.entries(attrs)) {
        if (value === null || value === undefined || value === false) continue;
        if (key === "class") node.className = value;
        else if (key === "text") node.textContent = String(value);
        else if (key.startsWith("on") && typeof value === "function") {
          node.addEventListener(key.slice(2), value);
        } else if (value === true) node.setAttribute(key, "");
        else node.setAttribute(key, String(value));
      }
    }
    for (const child of children) {
      if (child === null || child === undefined || child === false) continue;
      node.appendChild(typeof child === "string" ? document.createTextNode(child) : child);
    }
    return node;
  }

  const $ = (id) => document.getElementById(id);

  let toastTimer = null;
  function toast(message, kind) {
    const box = $("toast");
    box.textContent = String(message);
    box.className = "toast" + (kind ? " " + kind : "");
    if (toastTimer) clearTimeout(toastTimer);
    toastTimer = setTimeout(() => box.classList.add("hidden"), kind === "err" ? 8000 : 3500);
  }

  /** 替代 confirm()：返回 Promise<boolean>。可重入保护：已开着就直接返回 false。 */
  function ask(title, message) {
    if (modalOpen) return Promise.resolve(false);
    modalOpen = true;
    return new Promise((resolve) => {
      const modal = $("modal");
      $("modal-title").textContent = title;
      $("modal-text").textContent = message;
      modal.classList.remove("hidden");

      const done = (value) => {
        modalOpen = false;
        modal.classList.add("hidden");
        $("modal-ok").removeEventListener("click", onOk);
        $("modal-cancel").removeEventListener("click", onCancel);
        resolve(value);
      };
      const onOk = () => done(true);
      const onCancel = () => done(false);
      $("modal-ok").addEventListener("click", onOk);
      $("modal-cancel").addEventListener("click", onCancel);
    });
  }

  /** 请求期间禁用按钮，避免重复提交（第二次会 404，产生"成功+失败"双提示）。 */
  async function withBusy(button, fn) {
    if (button.disabled) return;
    button.disabled = true;
    try {
      await fn();
    } finally {
      button.disabled = false;
    }
  }

  // ---------------------------------------------------------------- 数据访问

  async function apiGet(endpoint, params) {
    if (!bridge) {
      throw new Error("页面 SDK 未就绪（window.AstrBotPluginPage 不存在）");
    }
    try {
      return await bridge.apiGet(endpoint, params);
    } catch (err) {
      toast("读取失败：" + (err && err.message ? err.message : err), "err");
      throw err;
    }
  }

  async function apiPost(endpoint, body) {
    if (!bridge) {
      throw new Error("页面 SDK 未就绪（window.AstrBotPluginPage 不存在）");
    }
    return bridge.apiPost(endpoint, body || {});
  }

  async function reload() {
    setStatus("加载中…");
    try {
      state.overview = await apiGet("overview");
      setStatus("");
      render();
    } catch (err) {
      setStatus("加载失败");
    }
  }

  function setStatus(text) {
    $("status").textContent = text || "";
  }

  // ---------------------------------------------------------------- 渲染分发

  function render() {
    const seq = ++renderSeq;
    if (state.overview) {
      $("version").textContent = "v" + (state.overview.version || "?");
      $("page-title").textContent =
        bridge && typeof bridge.t === "function"
          ? bridge.t("pages.manage.title", "X 监控转发 · 管理")
          : "X 监控转发 · 管理";
    }
    for (const tab of ["matrix", "status", "config"]) {
      const panel = $("tab-" + tab);
      panel.classList.toggle("hidden", tab !== state.tab);
      panel.replaceChildren();
    }
    document.querySelectorAll(".tab-btn").forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.tab === state.tab);
    });
    // SDK 缺失是"粘性"的：切 tab 不能把排障说明清掉变成白页
    if (state.fatal) {
      renderFatal();
      return;
    }
    if (state.tab === "matrix") renderMatrix($("tab-matrix"));
    else if (state.tab === "status") renderStatus($("tab-status"));
    else renderConfig($("tab-config"), seq);
  }

  function renderFatal() {
    $("tab-" + state.tab).replaceChildren(
      el(
        "div",
        { class: "card" },
        el("h3", null, "无法加载页面"),
        el("div", { class: "empty" }, state.fatal)
      )
    );
  }

  // ---------------------------------------------------------------- 订阅矩阵

  function platformIdOf(umo) {
    return String(umo || "").split(":")[0];
  }

  function shortUmo(umo) {
    const parts = String(umo || "").split(":");
    return parts.length === 3 ? parts[2] : String(umo || "");
  }

  /**
   * 群选择器：已知群下拉 +「手动输入…」。
   *
   * 刻意**不猜**平台前缀 —— umo 第一段是平台**实例 id**（用户给适配器实例起的名字，
   * 例如「mybot」），不是适配器名（例如 aiocqhttp）。拼错了会得到一个永远发不出去的
   * umo，框架只回一句 "cannot find platform for session ..."。
   *
   * 手动模式下：有平台实例表就「实例下拉 + 群号」拼；取不到就只让填完整 umo
   * （与后端 resolve_umo 的降级行为保持一致）。
   */
  function groupPicker(platforms, groups) {
    const known = el("select", null);
    for (const group of groups) {
      known.appendChild(
        el("option", { value: group.umo }, group.label + " · " + platformIdOf(group.umo))
      );
    }
    known.appendChild(el("option", { value: "__manual__" }, "手动输入…"));

    let manual;
    let readManual;
    if (platforms.length) {
      const platformSel = el("select", null);
      for (const platform of platforms) {
        platformSel.appendChild(
          el(
            "option",
            { value: platform.id },
            platform.name ? platform.id + "（" + platform.name + "）" : platform.id
          )
        );
      }
      // 会话类型也要能选：私聊是 FriendMessage，写死 GroupMessage 会导致
      // 未登记的私聊目标完全无法订阅，而且预览还把错误的 umo 显示成"对的"。
      const typeSel = el("select", null);
      typeSel.appendChild(el("option", { value: "GroupMessage" }, "群聊"));
      typeSel.appendChild(el("option", { value: "FriendMessage" }, "私聊"));
      const groupInput = el("input", { type: "text", placeholder: "群号 / 用户号", size: "12" });
      const preview = el("span", { class: "muted" });
      const build = () => {
        const pid = platformSel.value;
        const sid = groupInput.value.trim();
        return pid && sid ? pid + ":" + typeSel.value + ":" + sid : "";
      };
      const sync = () => {
        const umo = build();
        preview.textContent = umo ? "→ " + umo : "";
      };
      platformSel.addEventListener("change", sync);
      typeSel.addEventListener("change", sync);
      groupInput.addEventListener("input", sync);
      sync();
      manual = el("span", { class: "row" }, platformSel, typeSel, groupInput, preview);
      readManual = build;
    } else {
      const fullInput = el("input", {
        type: "text",
        placeholder: "<平台实例ID>:GroupMessage:<群号>",
        size: "30",
      });
      manual = el(
        "span",
        { class: "row" },
        fullInput,
        el("span", { class: "muted" }, "本内核取不到平台实例列表，请填完整会话标识")
      );
      readManual = () => fullInput.value.trim();
    }

    const syncMode = () => {
      manual.classList.toggle("hidden", known.value !== "__manual__");
    };
    known.addEventListener("change", syncMode);
    if (!groups.length) known.value = "__manual__";
    syncMode();

    return {
      node: el("span", { class: "row" }, known, manual),
      read: () => (known.value === "__manual__" ? readManual() : known.value || ""),
    };
  }

  function renderMatrix(panel) {
    const data = state.overview;
    if (!data) return;

    const picker = groupPicker(data.platforms || [], data.groups || []);
    const handleInput = el("input", { type: "text", placeholder: "X handle（不含 @）", size: "16" });
    const nickInput = el("input", { type: "text", placeholder: "昵称（可选，同时用作图库 tag）", size: "22" });
    const mediaOnly = el("input", { type: "checkbox" });
    const addBtn = el("button", {
      type: "button",
      class: "btn primary",
      text: "添加订阅",
      onclick: async () => {
        const umo = picker.read();
        const handle = handleInput.value.trim().replace(/^@/, "");
        if (!umo || !handle) {
          toast("请选择群（或填完整会话标识）并填写 handle", "err");
          return;
        }
        addBtn.disabled = true;
        try {
          const result = await apiPost("edge/add", {
            umo,
            handle,
            nickname: nickInput.value.trim(),
            media_only: mediaOnly.checked,
          });
          toast(
            result.created
              ? "已订阅 @" + result.handle
              : "该订阅已存在，昵称 / 仅媒体已更新",
            "ok"
          );
          handleInput.value = "";
          nickInput.value = "";
          mediaOnly.checked = false;
          await reload();
        } catch (err) {
          toast("添加失败：" + (err && err.message ? err.message : err), "err");
        } finally {
          addBtn.disabled = false;
        }
      },
    });

    panel.appendChild(
      el(
        "div",
        { class: "card" },
        el("h3", null, "添加订阅（账号 × 群）"),
        el(
          "div",
          { class: "row" },
          picker.node,
          handleInput,
          nickInput,
          el("label", { class: "inline" }, mediaOnly, "仅媒体"),
          addBtn
        ),
        el(
          "div",
          { class: "muted", style: "margin-top:8px" },
          "只能从「已知群」里选，或手动选平台实例 + 填群号。"
            + "新群请先在目标群里执行 /x在哪（会登记该会话）。首次订阅不补发历史。"
        )
      )
    );

    // ---- 矩阵：行 = 账号，列 = 群 ----
    const authors = data.authors || [];
    const groups = data.groups || [];
    const edgeMap = new Map();
    for (const edge of data.edges || []) {
      edgeMap.set(edge.umo + "|" + edge.author_id, edge);
    }

    const showMatrix = authors.length > 0 && groups.length > 0;
    if (!showMatrix) {
      panel.appendChild(
        el(
          "div",
          { class: "card" },
          el("h3", null, "订阅矩阵"),
          el(
            "div",
            { class: "empty" },
            "还没有可显示的矩阵。先在上方添加一个订阅；群会随订阅一起出现。"
          )
        )
      );
    }

    const header = el("tr", null, el("th", null, "账号"));
    for (const group of groups) {
      header.appendChild(el("th", { class: "center", title: group.umo }, group.label));
    }
    header.appendChild(el("th", { class: "center" }, "操作"));

    const rows = authors.map((author) => {
      const tr = el("tr", null);
      const name = author.display_name
        ? author.display_name + " (@" + author.handle + ")"
        : "@" + author.handle;
      tr.appendChild(
        el(
          "td",
          null,
          el("div", null, name),
          el(
            "div",
            { class: "muted" },
            author.baseline_ready ? "已建立基线" : "待首次抓取（尚未建立基线）",
            author.source ? " ｜ 数据源 " + author.source : ""
          )
        )
      );

      for (const group of groups) {
        const edge = edgeMap.get(group.umo + "|" + author.author_id);
        const box = el("input", {
          type: "checkbox",
          checked: !!edge,
          title: edge ? "点击取消订阅" : "点击订阅",
          onchange: async (event) => {
            const target = event.target;
            target.disabled = true;
            try {
              if (target.checked) {
                await apiPost("edge/add", { umo: group.umo, handle: author.handle });
                toast("已订阅 @" + author.handle + " → " + group.label, "ok");
              } else if (edge) {
                await apiPost("edge/remove", { edge_id: edge.edge_id });
                toast("已取消订阅 @" + author.handle + " → " + group.label, "ok");
              }
              await reload();
            } catch (err) {
              target.checked = !target.checked;
              target.disabled = false;
              toast("操作失败：" + (err && err.message ? err.message : err), "err");
            }
          },
        });
        tr.appendChild(el("td", { class: "center" }, box));
      }

      let delAuthorBtn = null;
      tr.appendChild(
        el(
          "td",
          { class: "center" },
          (delAuthorBtn = el("button", {
            type: "button",
            class: "btn tiny danger",
            text: "删账号",
            onclick: async () => {
              const ok = await ask(
                "删除账号",
                "将删除 @" + author.handle + " 及其在**所有群**的订阅、已收录推文与投递记录。\n该操作不可撤销。"
              );
              if (!ok) return;
              await withBusy(delAuthorBtn, async () => {
                try {
                  const result = await apiPost("author/remove", {
                    author_id: author.author_id,
                  });
                  toast(
                    "已删除，连带 " + result.edges + " 条订阅、" + result.posts + " 条推文",
                    "ok"
                  );
                  await reload();
                } catch (err) {
                  toast("删除失败：" + (err && err.message ? err.message : err), "err");
                }
              });
            },
          }))
        )
      );
      return tr;
    });

    if (showMatrix) {
      panel.appendChild(
        el(
          "div",
          { class: "card" },
          el("h3", null, "订阅矩阵（勾选即订阅）"),
          el(
            "div",
            { class: "scroll" },
            el("table", null, el("thead", null, header), el("tbody", null, ...rows))
          )
        )
      );
    }

    // ---- 订阅明细：逐条改昵称 / 开关 / 仅媒体 ----
    const detailRows = (data.edges || []).map((edge) => {
      let delEdgeBtn = null;
      const nickInput2 = el("input", { type: "text", value: edge.nickname, size: "16" });
      const enabledBox = el("input", { type: "checkbox", checked: edge.enabled });
      const mediaBox = el("input", { type: "checkbox", checked: edge.media_only });
      const save = el("button", {
        type: "button",
        class: "btn tiny",
        text: "保存",
        onclick: async () => {
          await withBusy(save, async () => {
            try {
              const result = await apiPost("edge/update", {
                edge_id: edge.edge_id,
                nickname: nickInput2.value.trim(),
                enabled: enabledBox.checked,
                media_only: mediaBox.checked,
              });
              // 同步本地快照，后续渲染与刚保存的值保持一致
              edge.nickname = result.nickname;
              edge.enabled = result.enabled;
              edge.media_only = result.media_only;
              // 刻意**不**整页 reload：reload 会重建整个面板，把其它行里
              // 还没保存的编辑静默还原掉，而且毫无提示
              toast("已保存（其它未保存的改动保持不变）", "ok");
            } catch (err) {
              toast("保存失败：" + (err && err.message ? err.message : err), "err");
            }
          });
        },
      });
      return el(
        "tr",
        null,
        el("td", null, shortUmo(edge.umo)),
        el("td", null, "@" + edge.handle),
        el("td", null, nickInput2),
        el("td", { class: "center" }, enabledBox),
        el("td", { class: "center" }, mediaBox),
        el("td", { class: "center" }, edge.pending ? String(edge.pending) : "—"),
        el(
          "td",
          { class: "center" },
          save,
          " ",
          (delEdgeBtn = el("button", {
            type: "button",
            class: "btn tiny danger",
            text: "删除",
            onclick: async () => {
              const ok = await ask(
                "删除订阅",
                "删除 " + shortUmo(edge.umo) + " 对 @" + edge.handle + " 的订阅？"
              );
              if (!ok) return;
              await withBusy(delEdgeBtn, async () => {
                try {
                  await apiPost("edge/remove", { edge_id: edge.edge_id });
                  toast("已删除", "ok");
                  await reload();
                } catch (err) {
                  toast("删除失败：" + (err && err.message ? err.message : err), "err");
                }
              });
            },
          }))
        )
      );
    });

    panel.appendChild(
      el(
        "div",
        { class: "card" },
        el("h3", null, "订阅明细"),
        detailRows.length
          ? el(
              "div",
              { class: "scroll" },
              el(
                "table",
                null,
                el(
                  "thead",
                  null,
                  el(
                    "tr",
                    null,
                    el("th", null, "群"),
                    el("th", null, "账号"),
                    el("th", null, "昵称"),
                    el("th", { class: "center" }, "推送"),
                    el("th", { class: "center" }, "仅媒体"),
                    el("th", { class: "center" }, "待发"),
                    el("th", { class: "center" }, "操作")
                  )
                ),
                el("tbody", null, ...detailRows)
              )
            )
          : el("div", { class: "empty" }, "还没有订阅。")
      )
    );

    // ---- 每群免打扰 ----
    const dndRows = (data.groups || []).map((group) => {
      const enabledBox = el("input", { type: "checkbox", checked: group.dnd_enabled });
      const rangesInput = el("input", {
        type: "text",
        value: group.dnd_ranges,
        placeholder: "23:00-08:00",
        size: "24",
      });
      const save = el("button", {
        type: "button",
        class: "btn tiny",
        text: "保存",
        onclick: async () => {
          await withBusy(save, async () => {
            try {
              const ranges = rangesInput.value
                .split(/[,，\n]/)
                .map((s) => s.trim())
                .filter(Boolean);
              const result = await apiPost("group/dnd", {
                umo: group.umo,
                enabled: enabledBox.checked,
                ranges,
              });
              group.dnd_enabled = result.enabled;
              group.dnd_ranges = (result.ranges || []).join(",");
              toast("已保存（其它未保存的改动保持不变）", "ok");
            } catch (err) {
              toast("保存失败：" + (err && err.message ? err.message : err), "err");
            }
          });
        },
      });
      return el(
        "tr",
        null,
        el("td", null, group.label, el("div", { class: "muted" }, group.umo)),
        el("td", { class: "center" }, enabledBox),
        el("td", null, rangesInput),
        el("td", { class: "center" }, group.in_dnd ? el("span", { class: "pill warn" }, "生效中") : el("span", { class: "pill" }, "未生效")),
        el("td", { class: "center" }, String(group.queue)),
        el("td", { class: "center" }, save)
      );
    });

    panel.appendChild(
      el(
        "div",
        { class: "card" },
        el("h3", null, "每群免打扰"),
        dndRows.length
          ? el(
              "div",
              { class: "scroll" },
              el(
                "table",
                null,
                el(
                  "thead",
                  null,
                  el(
                    "tr",
                    null,
                    el("th", null, "群"),
                    el("th", { class: "center" }, "启用"),
                    el("th", null, "时段"),
                    el("th", { class: "center" }, "当前"),
                    el("th", { class: "center" }, "待发"),
                    el("th", { class: "center" }, "操作")
                  )
                ),
                el("tbody", null, ...dndRows)
              )
            )
          : el("div", { class: "empty" }, "还没有已知的群。在群里执行 /x在哪 即可登记。")
      )
    );

    // ---- 测试与回放 ----
    const testPicker = groupPicker(data.platforms || [], data.groups || []);
    const testHandle = el("input", { type: "text", placeholder: "handle（回放用）", size: "14" });
    const replayMax = Number((data.stats && data.stats.replay_max) || 10) || 10;
    const testCount = el("input", {
      type: "number",
      value: "3",
      min: "1",
      max: String(replayMax),
      style: "width:70px",
    });
    panel.appendChild(
      el(
        "div",
        { class: "card" },
        el("h3", null, "测试与回放"),
        el(
          "div",
          { class: "row" },
          testPicker.node,
          el("button", {
            type: "button",
            class: "btn",
            text: "发送测试消息",
            onclick: async (event) => {
              const umo = testPicker.read();
              if (!umo) return toast("请先选择群（或填完整会话标识）", "err");
              event.target.disabled = true;
              try {
                const result = await apiPost("test/send", { umo });
                toast(result.sent ? "已发送" : "发送失败，请看 AstrBot 日志", result.sent ? "ok" : "err");
              } catch (err) {
                toast("发送失败：" + (err && err.message ? err.message : err), "err");
              } finally {
                event.target.disabled = false;
              }
            },
          }),
          testHandle,
          testCount,
          el("button", {
            type: "button",
            class: "btn",
            text: "回放最近 n 条",
            onclick: async (event) => {
              const umo = testPicker.read();
              const handle = testHandle.value.trim().replace(/^@/, "");
              if (!umo || !handle) return toast("请选择群并填写 handle", "err");
              event.target.disabled = true;
              try {
                const result = await apiPost("test/replay", {
                  umo,
                  handle,
                  count: Number(testCount.value) || 3,
                });
                toast("已回放 " + result.sent + "/" + result.total + " 条", "ok");
              } catch (err) {
                toast("回放失败：" + (err && err.message ? err.message : err), "err");
              } finally {
                event.target.disabled = false;
              }
            },
          })
        ),
        el(
          "div",
          { class: "muted", style: "margin-top:8px" },
          "回放不推进水位、不写投递记录，可反复执行；与 /x补发 等价。"
        )
      )
    );
  }

  // ---------------------------------------------------------------- 运行状态

  function statCard(key, value) {
    return el("div", { class: "stat" }, el("div", { class: "k" }, key), el("div", { class: "v" }, String(value)));
  }

  function humanBytes(bytes) {
    const n = Number(bytes) || 0;
    if (n < 1024) return n + " B";
    if (n < 1024 * 1024) return (n / 1024).toFixed(1) + " KB";
    if (n < 1024 * 1024 * 1024) return (n / 1024 / 1024).toFixed(1) + " MB";
    return (n / 1024 / 1024 / 1024).toFixed(2) + " GB";
  }

  function renderStatus(panel) {
    const data = state.overview;
    if (!data) return;
    const stats = data.stats || {};
    const pool = stats.image_pool || {};
    // 拿不到字段时显示 "—"：以前会显示"关闭"，把"数据缺失"说成"功能关闭"
    const poolText = !stats.image_pool
      ? "—"
      : !pool.enabled
        ? "关闭"
        : pool.available
          ? "已入库 " + (pool.added || 0) + " 张"
          : "不可用";
    // 文案由服务端给（payload.tag_state_text）：以前这里有一份硬编码拷贝，
    // 已经和 main.py 分叉过 —— rejected_only 那版少了可执行的下一步。
    const poolTagWarnText = pool.tag_state_text || "";
    const poolRejected = pool.rejected_nicknames || [];

    panel.appendChild(
      el(
        "div",
        { class: "card" },
        el("h3", null, "总览"),
        el(
          "div",
          { class: "grid-stats" },
          statCard("订阅", stats.edges),
          statCard("账号", stats.authors),
          statCard("已收录推文", stats.posts),
          statCard("待投递队列", stats.queue),
          statCard("媒体缓存", humanBytes(stats.cache_bytes)),
          statCard("缓存配额", humanBytes(stats.cache_quota_bytes)),
          statCard("最近 20 次失败", stats.recent_failed + " / " + stats.recent_total),
          statCard("数据源", stats.provider || "未初始化"),
          statCard("发送形态", stats.send_mode === "forward" ? "合并转发" : "普通链"),
          statCard("翻译", stats.translate_enabled ? "开启" : "关闭"),
          statCard("图库联动", poolText)
        ),
        pool.enabled && !pool.available
          ? el(
              "div",
              { class: "muted", style: "margin-top:10px" },
              "⚠️ 图库联动已开启但不可用：" + (pool.reason_text || pool.reason || "原因未知") +
                // unknown 态（读订阅表失败）时 available 也是 false，这里要把状态文案带上，
                // 否则它在页面上完全没有出口
                (pool.tag_state_text ? "；" + pool.tag_state_text : "") +
                "。推文推送不受影响。"
            )
          : null,
        pool.enabled && pool.available && poolTagWarnText
          ? el(
              "div",
              { class: "muted", style: "margin-top:10px" },
              "⚠️ 图库联动当前不会入库：" + poolTagWarnText +
                (poolRejected.length ? "涉及：" + poolRejected.join("、") : "")
            )
          : null,
        // 全局状态是 ready 时，仍可能有个别订阅的昵称不能用（A 群可用、B 群是 NUL）——
        // 这条以前完全没有出口，B 的图会永远不入库而页面看起来正常。
        pool.enabled && pool.available && poolTagWarnText === "" && poolRejected.length
          ? el(
              "div",
              { class: "muted", style: "margin-top:10px" },
              "⚠️ 另有 " + poolRejected.length + " 个昵称不能用作图库分类名（" +
                poolRejected.join("、") + "）→ 这些订阅不会入库。"
            )
          : null,
        pool.enabled && pool.available && (pool.download_failed || pool.timeouts || pool.budget_exhausted || pool.tag_failures)
          ? el(
              "div",
              { class: "muted", style: "margin-top:10px" },
              "⚠️ 图库入库有失败：" +
                [
                  pool.download_failed ? pool.download_failed + " 张照片下载失败" : "",
                  pool.timeouts ? "超时 " + pool.timeouts + " 次" : "",
                  pool.tag_failures ? "tag 写入失败 " + pool.tag_failures + " 次" : "",
                  pool.budget_exhausted ? "预算用尽 " + pool.budget_exhausted + " 次" : ""
                ]
                  .filter(Boolean)
                  .join("；") +
                "。这些图不会补归档，详情见机器人日志与 /x状态。"
            )
          : null,
        (data.provider_chain && data.provider_chain.degraded) || stats.provider_degraded
          ? el(
              "div",
              { class: "muted", style: "margin-top:10px" },
              "⚠️ 数据源降级中：主源连续失败已被旁路，正在使用后面的源；详情见机器人指令 /x源（仅全局管理员）。"
            )
          : null,
        data.provider_chain && (data.provider_chain.chain || []).length
          ? el(
              "div",
              { class: "muted", style: "margin-top:10px" },
              "数据源明细：" +
                data.provider_chain.chain
                  .map(
                    (item) =>
                      item.name +
                      "（成功 " +
                      item.ok +
                      "／失败 " +
                      item.failures +
                      (item.not_found ? "／账号不存在 " + item.not_found : "") +
                      (item.broken ? "／源探测失败" : "") +
                      (item.tripped ? "／已熔断" : "") +
                      "）"
                  )
                  .join("　")
            )
          : null,
        stats.last_error
          ? el(
              "div",
              { class: "muted", style: "margin-top:10px" },
              "最近一次失败：@" + stats.last_error_handle + " — " + stats.last_error
            )
          : null
      )
    );

    const authorRows = (data.authors || []).map((author) =>
      el(
        "tr",
        null,
        el("td", null, "@" + author.handle),
        el("td", null, author.display_name || "—"),
        el(
          "td",
          { class: "center" },
          author.baseline_ready ? el("span", { class: "pill on" }, "已建基线") : el("span", { class: "pill warn" }, "待首次抓取")
        ),
        el("td", null, author.last_seen_ts ? new Date(author.last_seen_ts * 1000).toLocaleString() : "—"),
        el(
          "td",
          null,
          author.source
            ? el(
                "span",
                { class: "pill " + (author.source === (stats.provider || "") ? "on" : "warn") },
                author.source
              )
            : "—"
        ),
        el(
          "td",
          { class: "center" },
          String((data.edges || []).filter((e) => e.author_id === author.author_id).length)
        )
      )
    );

    panel.appendChild(
      el(
        "div",
        { class: "card" },
        el("h3", null, "监控账号"),
        authorRows.length
          ? el(
              "div",
              { class: "scroll" },
              el(
                "table",
                null,
                el(
                  "thead",
                  null,
                  el(
                    "tr",
                    null,
                    el("th", null, "账号"),
                    el("th", null, "昵称"),
                    el("th", { class: "center" }, "基线"),
                    el("th", null, "水位时间"),
                    el("th", null, "数据源"),
                    el("th", { class: "center" }, "订阅数")
                  )
                ),
                el("tbody", null, ...authorRows)
              )
            )
          : el("div", { class: "empty" }, "还没有监控账号。")
      )
    );
  }

  // ---------------------------------------------------------------- 全局配置

  function fieldControl(key, entry, value) {
    const type = String(entry.type || "string");
    if (type === "bool") {
      const box = el("input", { type: "checkbox", checked: !!value });
      return { node: el("label", { class: "inline" }, box, "启用"), read: () => box.checked, initial: !!value };
    }
    if (entry.options && Array.isArray(entry.options)) {
      const select = el("select", null);
      entry.options.forEach((option, index) => {
        const label = entry.labels && entry.labels[index] ? entry.labels[index] : option;
        select.appendChild(
          el("option", { value: option, selected: String(value) === String(option) }, String(label))
        );
      });
      return { node: select, read: () => select.value, initial: value };
    }
    if (type === "int" || type === "float") {
      const slider = entry.slider || {};
      const input = el("input", {
        type: "number",
        value: value === null || value === undefined ? "" : value,
        min: slider.min !== undefined ? slider.min : null,
        max: slider.max !== undefined ? slider.max : null,
        step: type === "int" ? (slider.step || 1) : slider.step || "any",
        style: "width:140px",
      });
      return {
        node: input,
        read: () => (input.value.trim() === "" ? null : Number(input.value)),
        initial: value,
      };
    }
    if (type === "list") {
      const lines = Array.isArray(value) ? value.join("\n") : value ? String(value) : "";
      const area = el("textarea", { rows: "3" });
      area.value = lines;
      return {
        node: area,
        read: () =>
          area.value
            .split(/[\n,，]/)
            .map((s) => s.trim())
            .filter(Boolean),
        initial: value,
      };
    }
    if (type === "text") {
      const area = el("textarea", { rows: "3" });
      area.value = value === null || value === undefined ? "" : String(value);
      return { node: area, read: () => area.value, initial: value };
    }
    const input = el("input", {
      type: "text",
      value: value === null || value === undefined ? "" : String(value),
      size: "40",
    });
    return { node: input, read: () => input.value, initial: value };
  }

  function sameValue(a, b) {
    if (Array.isArray(a) || Array.isArray(b)) return JSON.stringify(a || []) === JSON.stringify(b || []);
    return String(a) === String(b);
  }

  /** 读取配置，带 in-flight 去重：快速切 tab 不会并发拉多次、也不会渲染出重复卡片。 */
  async function ensureConfig() {
    if (state.config) return state.config;
    if (!configPromise) {
      configPromise = apiGet("config")
        .then((data) => {
          state.config = data;
          return data;
        })
        .finally(() => {
          configPromise = null;
        });
    }
    return configPromise;
  }

  function renderConfig(panel, seq) {
    ensureConfig()
      .then((data) => {
        // 渲染代次守卫：切走或重渲染后丢弃这次结果，否则会往已清空的面板里再 append 一份
        if (seq !== renderSeq) return;
        const controls = new Map();
        const fields = Object.entries(data.schema || {}).map(([key, entry]) => {
          const control = fieldControl(key, entry, data.values ? data.values[key] : entry.default);
          controls.set(key, { entry, control });
          return el(
            "div",
            { class: "field" },
            el("div", { class: "name" }, entry.description || key, el("span", { class: "muted" }, "  (" + key + ")")),
            entry.hint ? el("div", { class: "hint" }, entry.hint) : null,
            control.node
          );
        });

        const saveBtn = el("button", {
          type: "button",
          class: "btn primary",
          text: "保存修改",
          onclick: async () => {
            const values = {};
            const invalid = [];
            for (const [key, item] of controls) {
              const current = item.control.read();
              // 数字框清空时 Number("") === 0，会被当成"改成了 0"提交；
              // 而 NaN 序列化成 null 又会让整批保存失败、且不指出是哪个字段。
              if (current === null || (typeof current === "number" && !Number.isFinite(current))) {
                invalid.push(key);
                continue;
              }
              if (!sameValue(current, item.control.initial)) values[key] = current;
            }
            if (invalid.length) {
              toast("这些字段不能为空或格式不对：" + invalid.join("、"), "err");
              return;
            }
            if (!Object.keys(values).length) {
              toast("没有改动", "ok");
              return;
            }
            await withBusy(saveBtn, async () => {
              try {
                const result = await apiPost("config/save", { values });
                toast("已保存 " + result.count + " 项：" + (result.saved || []).join("、"), "ok");
                state.config = null;
                await reload();
              } catch (err) {
                toast("保存失败：" + (err && err.message ? err.message : err), "err");
              }
            });
          },
        });

        panel.appendChild(
          el(
            "div",
            { class: "card" },
            el("h3", null, "全局配置"),
            el(
              "div",
              { class: "muted", style: "margin-bottom:10px" },
              "由插件自带的 _conf_schema.json 驱动；只提交改动过的项，后端逐项校验，任一项不合法则整体不落盘。"
            ),
            ...fields,
            el("div", { class: "row" }, saveBtn)
          )
        );
      })
      .catch(() => {
        panel.appendChild(el("div", { class: "empty" }, "配置读取失败。"));
      });
  }

  // ---------------------------------------------------------------- 启动

  function applyTheme(ctx) {
    if (ctx && ctx.isDark !== undefined) {
      document.documentElement.setAttribute("data-theme", ctx.isDark ? "dark" : "light");
    }
  }

  function showFatal(message) {
    state.fatal = message;
    setStatus("SDK 未就绪");
    renderFatal();
  }

  async function boot() {
    document.querySelectorAll(".tab-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        state.tab = btn.dataset.tab;
        render();
      });
    });
    $("refresh").addEventListener("click", () => {
      state.config = null;
      reload();
    });

    // 先等 SDK，再碰任何 bridge.* —— 否则就是那个 "reading 'apiGet'" 报错
    bridge = await waitForBridge(5000);
    if (!bridge) {
      showFatal(
        "未检测到页面 SDK（window.AstrBotPluginPage）。\n" +
          "· 确认 AstrBot 版本 >= v4.24.1\n" +
          "· 对 Dashboard 整页硬刷新（Ctrl+F5）\n" +
          "· 若仍不行，看 AstrBot 日志里 [x_monitor] 开头的注册信息"
      );
      return;
    }

    try {
      if (typeof bridge.ready === "function") {
        // ready() 可能永远不 resolve（宿主没 post context）——加超时，
        // 否则 boot() 永久挂起、整页空白且没有任何提示
        applyTheme(
          await Promise.race([
            bridge.ready(),
            new Promise((resolve) => setTimeout(() => resolve(null), 5000)),
          ])
        );
      }
      if (typeof bridge.onContext === "function") {
        bridge.onContext(applyTheme);
      }
    } catch (err) {
      // ready() 失败不阻塞页面：后面的 apiGet 会给出更具体的错误
    }
    await reload();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
