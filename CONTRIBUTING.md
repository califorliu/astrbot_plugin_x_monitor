# 贡献指南

> **本文件是开发侧的唯一权威来源**：开发环境、架构说明、代码规范、模板、离线自检用法与提交规范。
> 别处只写摘要和链接，不复制正文。
>
> 仓库级硬性规则见 [AGENTS.md](AGENTS.md) ｜ 用户文档见 [README.md](README.md) ｜
> 版本变更见 [CHANGELOG.md](CHANGELOG.md)。

## 目录

- [1. 开发环境](#1-开发环境)
- [2. 仓库结构](#2-仓库结构)
- [3. 架构总览](#3-架构总览)
- [4. 代码规范](#4-代码规范)
- [5. 模板](#5-模板)
- [6. 离线自检](#6-离线自检)
- [7. 提交规范](#7-提交规范)
- [8. 发版检查清单](#8-发版检查清单)

---

## 1. 开发环境

| 项 | 要求 |
|---|---|
| Python | **3.10+**（用到 PEP 604 的 `X \| None` 与 PEP 585 的 `list[str]`） |
| 运行时依赖 | `httpx[http2]>=0.25.0`（见 `requirements.txt`，唯一一项） |
| 跑离线自检 | **不需要**安装 AstrBot，**不需要** httpx，**不需要**外网 |
| 跑真机验证 | 需要一台 AstrBot ≥ 4.13.0（WebUI 管理页需要 ≥ 4.24.1）与一个 OneBot 协议端 |

**Windows 控制台必须先设 `PYTHONUTF8=1`**：

```powershell
$env:PYTHONUTF8 = "1"
python tests/offline_check.py
```

不设的话，自检会在含 `⇒`（U+21D2）的断言标签处抛 `UnicodeEncodeError`，
被入口的 `except BaseException` 捕获后**以退出码 2 中断** —— 此时后半段断言一条都没跑，
但输出看起来像"跑完了"。**看到退出码 2 就是中断，不是通过。**

另外：**不要并行跑两个自检**。自检的临时目录按 PID 隔离，但两个进程仍会互相干扰。

## 2. 仓库结构

```
main.py                                  插件主体（单文件，约 6.8k 行）
metadata.yaml                            包身份：name/display_name/desc/version/author/repo/
                                         support_platforms/astrbot_version
_conf_schema.json                        配置项声明与默认值（**唯一声明处**，59 项）
requirements.txt                         运行时依赖
README.md                                用户文档
CHANGELOG.md                             版本变更记录（版本记录的权威）
CONTRIBUTING.md                          本文件
AGENTS.md                                仓库级规则
LICENSE                                  MIT
pages/manage/index.html                   WebUI 管理页
pages/manage/app.js                       页面逻辑（约 1.1k 行）
pages/manage/style.css                    页面样式
tests/offline_check.py                    离线自检（自带 astrbot.* / httpx 替身）
tests/fixtures/fxtwitter_feed_sample.xml  **合成**字段形状样本（5 条，内容全虚），随包发布
.astrbot-plugin/i18n/{zh-CN,en-US}.json   插件页本地化文案
.gitignore                               挡住运行时数据与构建产物
```

> 发版用 `git tag` + `git archive`，所以**没有提交的内容就是没公开** ——
> `.gitattributes export-ignore` 对 GitHub 自动生成的归档不起作用。

## 3. 架构总览

### 3.1 `main.py` 的分层

单文件，按"纯函数在前、有状态在后"排列：

| 区段 | 内容 |
|---|---|
| 模块 docstring | 功能、群级三档权限表、触发方式、存储路径 —— 改行为时同步改这里 |
| 导入 | `from __future__ import annotations`；`astrbot.api.web` 用 `try/except` 优雅降级（旧内核没有 Plugin Pages） |
| 常量 | 插件名与版本、数据源名（`PROVIDER_NAMES`，**这些字符串同时是用户配置值，改名等于破坏配置**）、默认降级链、媒体与昵称上限 |
| 纯工具函数 | `to_bool` / `normalize_nick` / `parse_nickname_input` / `sniff_ext` / 免打扰时段解析 / 帖子分类 / 媒体提取 / `validate_umo` / `validate_config_value` / `build_share_url` … |
| RSS/HTML/XML 解析 | `_HtmlTextExtractor`、`repair_xml_text`、`_entry_to_status`、`parse_feed_xml` |
| `FetchResult` | `@dataclass`：`ok` / `no_new` / `not_found` / `author` / `posts` / `error` / `source` / `provider` |
| `Store` | SQLite：SCHEMA、MIGRATIONS、`_conn()` 事务上下文、全部 SQL |
| `FetchProvider` 家族 | 基类 + `FxTwitterProvider` / `RssProvider` / `RssHubProvider` |
| `ProviderChain` | 降级、熔断、pin、探测。**层间语义的权威就是它的类文档串** |
| `MediaCache` | 有界媒体缓存（配额 + TTL + LRU + 并发去重） |
| `ImagePoolBridge` | 与 `astrbot_plugin_image_pool` 的兼容层 |
| `Translator` | 复用 AstrBot Provider 的翻译（介入判定 + 超时预算 + 熔断） |
| `build_provider_chain` | 按配置装配降级链 |
| `XMonitorPlugin(Star)` | 插件本体：路由表、生命周期、权限、轮询、投递、发送、WebUI、命令 |

`XMonitorPlugin` 内部分段（自上而下）：`ROUTES` + `__init__`/`_init_db`/`initialize` → 降级链管理 →
HTTP 客户端与 `terminate` → **配置读取 helper** → 权限模型 → 推文解析 → 轮询 →
图库联动入库 → 媒体预处理 → 免打扰与投递 → 节点/正文/媒体构造 → 发送 → **WebUI** → **命令**。

### 3.2 数据层

数据目录由 `StarTools.get_data_dir(PLUGIN_NAME)` 决定（= `data/plugin_data/astrbot_plugin_x_monitor/`），
**不在插件目录里** —— 插件升级会整体替换插件目录。

7 张表：`authors`、`edges`、`posts`、`deliveries`、`meta`、`groups`、`known_groups`
（`known_groups` 刻意与 `groups` 分开：前者是"见过这个群"，后者是"这个群有免打扰覆盖行"）。

**四条不变量**（详见 [AGENTS.md](AGENTS.md) §2）：`UNIQUE(edge_id, post_id)`；
`edges.enqueued_ts` 只增不减（用条件 `UPDATE ... WHERE enqueued_ts < ?` 实现单调）；
抓取失败不写水位、不置基线；首次订阅不补发历史。

一轮抓取的提交集中在 `Store.commit_poll`：显式 `BEGIN IMMEDIATE`，依次写 posts → 为每条边入队
→ 推进两条水位，末尾 `commit()`，异常 `rollback(); raise`。

**连接生命周期**：`Store._conn()` 是"开连接 → commit → 异常 rollback → **无论如何 close**"。
`with sqlite3.connect(...)` 只是事务上下文、**不会关闭连接**，在 Windows 上会实打实占住文件句柄。

### 3.3 数据源

`FetchProvider` 是隐式协议，契约只有两条：类属性 `name`，以及
`async fetch_user(handle, since_ts, with_replies) -> FetchResult`。
**`fetch_user` 禁止抛异常** —— 任何失败都必须收敛成 `FetchResult(ok=False)`。

语义契约（最容易写错的一条）：`ok=True, no_new=True`（真的没新帖）与 `ok=False`（源不可用）
是两回事。把"解析不出来"当成"没有新帖"会造成**永久漏推**。

### 3.4 媒体、翻译、图库

- **MediaCache**：缓存键 = `sha256(最终要发的 URL)`，文件名 `<sha256>.<ext>`。
  画质档位由调用方先用 `apply_twimg_quality` 算好再传进来，缓存层**不改写 URL**。
  查找用 `glob(f"{digest}.*")` 并取 mtime 最新；写入是 `mkstemp` + `os.replace` 原子写。
- **Translator**：`should_translate()` 是纯函数式的介入判定（开关、熔断、长度、目标语言、
  `skip_langs`、中文占比）；单条调用包在 `asyncio.wait_for(..., translate_timeout_sec)` 里；
  连续失败达 `translate_max_consecutive_failures` 后**本轮**熔断，下一轮重置。
- **ImagePoolBridge**：对端是 `astrbot_plugin_image_pool`，唯一入口是公开方法 `ingest_files`。
  解析顺序固定为 `get_registered_star()` → `activated` → `star_cls` → `inspect.isclass` 判定 →
  `hasattr` 能力探测。**绝不直写对端数据库**，**绝不抛进轮询循环**。

### 3.5 WebUI

页面在 `pages/manage/`，后端路由由 `XMonitorPlugin.ROUTES` 声明，在 `__init__` 里经
`context.register_web_api()` 注册。10 条路由：`overview`、`config`、`config/save`、
`edge/add`、`edge/update`、`edge/remove`、`author/remove`、`group/dnd`、`test/send`、`test/replay`。

三条硬约束：

1. **路由必须带插件名前缀**（`/astrbot_plugin_x_monitor/overview`），而页面侧 endpoint **不能带**
   （`bridge.apiGet("overview")`）。
2. **处理函数不能有位置参数** —— 一切都从 `web_api.request` 读。
3. **请求体一律走 `_json_body()`**：`request.json(default={})` 只在**解析异常**时返回默认值，
   合法 JSON 但非对象（`[1,2]` / `"x"` / `5`）会原样返回，紧接着的 `body.get` 就是
   `AttributeError` → HTTP 500。

**新增路由后必须重载插件**（注册只发生在 `__init__`，`registered_web_apis` 从不被清理）；
只改静态资源才只需刷新页面。

前端约定（`pages/manage/app.js`）：`<script type="module">`（模块默认 defer，才会晚于宿主注入 SDK）；
bridge 惰性获取 + 有限等待；自绘 toast / Promise 化 modal 替代 `alert`/`confirm`/`prompt`
（iframe 无 `allow-modals`，它们会**静默失效**）；用户数据一律 DOM 构造写入，**不拼 `innerHTML`**；
异步渲染带回代次守卫，切走后丢弃迟到结果。

## 4. 代码规范

### 4.1 语言与风格

- **注释与 docstring 一律中文。** 模块 docstring 用"功能 / 权限 / 触发方式 / 存储"式分段。
- **类型注解用 PEP 585 + PEP 604**：`list[str]`、`dict[str, dict]`、`str | None`。
  **不要引入 `typing`**（本项目 0 处 `List[` / `Dict[` / `Optional[`）。
- 文件顶部保留 `from __future__ import annotations`。
- 命名：模块常量全大写；类 PascalCase；私有 helper 前缀 `_`；命令 handler 统一 `cmd_*`；
  WebUI handler 统一 `api_*`。

### 4.2 日志

统一用 `from astrbot.api import logger`，**不自建 logger**。
每条日志带 `[x_monitor]` 前缀，便于在宿主日志里过滤。

### 4.3 异常处理

- 宽 `except Exception` 只用于"解析失败退化为默认值"或"富化步骤失败不阻断主流程"，
  并且**必须**要么记一条日志、要么写清为什么静默。
- **凡包住可取消 await 的兜底 `except`，都要单独放行 `asyncio.CancelledError: raise`** ——
  否则插件停不下来。
- 事务里的异常一律 `rollback(); raise`，**不吞**。

### 4.4 配置读取（最容易踩的坑）

```python
self.cfg(key, default)        # 原始值，None 归一成 default
self.bool_cfg(key, default)   # 真布尔，认得 "1"/"true"/"yes"/"on"/"是"/"开"
self.int_cfg(key, default)
self.float_cfg(key, default)
```

> ⚠️ **字符串枚举键必须用 `cfg()` 读并比较字符串，绝不能用 `bool_cfg()`。**
> `bool_cfg` 返回布尔，而 `bool_cfg(key, "download") == "download"` **恒为假** ——
> 这个坑曾让 `media_mode=download` 静默失效三个版本（选"本地下载"的用户一直在走直链）。

配置项只在 `_conf_schema.json` 声明一次；自检有**双向断言**：
代码读到的键必须都在 schema 里，且 schema 声明的键都必须真的被读到（无死配置）。

### 4.5 异步纪律

- 不在事件循环里做阻塞 I/O；同步实现的对端一律 `asyncio.to_thread`。
- 一个并发槽里不要串行 await 多个下载 —— 用带独立小信号量的 `asyncio.gather`，
  否则一个账号会长时间占住 `fetch_concurrency` 的槽位、饿死其它账号。
- 需要排序的两个事件一律用 `time.monotonic()`；墙钟时间只用于展示。

### 4.6 注释纪律

不要在注释里写开发过程用语：阶段名（`P5` / `P7`）、"本轮"、"独立验证"、"对抗审核"、
审查轮次编号（`L-C2` / `N-3` / `O-N4` …）。这些会随包发布给公开读者看。
描述**为什么这样写**，而不是"这是第几轮审查查出来的"。

## 5. 模板

### 5.1 新增一条聊天指令

```python
    @filter.command("x示例", alias={"xdemo"})
    async def cmd_demo(self, event: AstrMessageEvent, handle: str = ""):
        """一句话说明：/x示例 <handle>

        需要时在这里补充更长的说明。
        """
        if not str(handle).strip():
            yield event.plain_result("用法：/x示例 <handle>")
            event.stop_event()
            return
(Showing lines 196-225 of 468. Use offset=226 to continue.)
        edge = self.store.get_edge_by_handle(event.unified_msg_origin, handle)
        if edge is None:
            yield event.plain_result(f"ℹ️ 本群没有订阅 @{handle.lstrip('@')}。")
            event.stop_event()
            return
        ok, why = self.can_manage_edge(event, edge)
        if not ok:
            yield event.plain_result(why)
            event.stop_event()
            return
        # ……业务逻辑……
        yield event.plain_result("✅ 完成。")
        event.stop_event()
```

硬性要求：

- handler 是 **async generator**（用 `yield`），第一个参数恒为 `event: AstrMessageEvent`。
- **每个出口都要 `event.stop_event()`**，否则同一条被唤醒的消息会继续落到 LLM，
  用户会同时收到插件回复和 AI 回复。
- **参数类型只有两种**：`xxx: str = ""`，或 `raw: GreedyStr`。
  **`GreedyStr` 一律不能有默认值** —— `CommandFilter` 只在参数没有默认值时才认得它；
  而且"有默认值的参数后面跟无默认值的参数"在语法上也不成立。
  拿到 `raw` 后自己 `split(None, 1)` 切分。
- **指令名不带前缀**（唤醒由 AstrBot 的 `wake_prefix` 负责，前缀在匹配前已被剥掉）。
- **不要用 `@filter.regex`** —— 它不受唤醒门控，会在所有消息上触发，与共用同一接入层的其它 bot 抢答。
- **权限在 handler 内部自校验**（`can_manage_edge` / `_is_global_admin`），
  **不要用 `@filter.permission_type`** —— 那授予的是 AstrBot 的**全局**管理员，属于提权。
- 新增指令后必须能在 `/x帮助` 里看到：自检有断言「帮助清单覆盖全部已注册指令」。

### 5.2 新增一个配置项

先在 `_conf_schema.json` 里声明（**唯一声明处**），再在代码里读。条目形状：

```json
  "示例开关": {
    "description": "一句话说明（会显示在配置页标题）",
    "hint": "更长的解释：默认值是什么、改了会怎样。",
    "type": "bool",
    "obvious_hint": true,
    "default": false
  },
```

可用类型与附加字段：

| `type` | 附加字段 | 说明 |
|---|---|---|
| `bool` | — | 真开关。**读取用 `bool_cfg`** |
| `int` | `slider: {min,max,step}` | 读取用 `int_cfg` |
| `float` | `slider: {min,max,step}` | 读取用 `float_cfg` |
| `string` | `options` + `labels` | 枚举。**读取用 `cfg` 比较字符串** |
| `list` | 可带 `options` + `labels` | 列表；`labels` 与 `options` 长度必须一致 |
| `text` | — | 多行长文本 |

`options` 枚举的 `default` 必须在 `options` 内 —— 这两条都有断言守着。

### 5.3 新增一个数据源

要改的落点（缺一个就会有断言变红或功能静默不生效）：

1. 加常量 `PROVIDER_XXX` 并加进 `PROVIDER_NAMES`（需要时也加进 `DEFAULT_FETCH_CHAIN`）。
2. 写提供者类：继承 `FetchProvider`，实现 `name` 与 `async fetch_user(...)`；
   参考 `FxTwitterProvider` / `RssProvider` / `RssHubProvider`。
3. 在 `build_provider_chain` 的装配分支里加一条。
4. `_conf_schema.json` 的 `fetch_chain.options` **和** `labels` 都要加（两者长度必须一致）。
5. `normalize_chain_names` 只放行 `PROVIDER_NAMES` 里的名字，新名字自动生效。
6. 自检的三条断言会自动覆盖新源，**无需改断言**，但新源必须让它们通过：
   `PROVIDER_NAMES` ↔ `fetch_chain.options` 一致、默认链只由已知源组成、
   **每个声明过的源都能被装配出来**。
7. 层间语义的权威是 `ProviderChain` 的类文档串 —— 改了语义就同步改它。

### 5.4 新增一条 WebUI 路由

1. 在 `ROUTES` 里加一行 `(子路径, 处理方法名, ["GET"|"POST"], "说明")`。
2. 写 `async def api_xxx(self) -> dict:` —— **不能有位置参数**。
   POST 的第一行固定是 `body = await self._json_body()`。
3. 页面侧调用时 endpoint **不带**插件名前缀：`bridge.apiPost("xxx", payload)`。
4. **重载插件**后才会生效。

### 5.5 新增一条断言

```python
    rep.check("<一句中文描述>", <实得值>, <期望值>)   # 严格 ==
    rep.true("<一句中文描述>", <布尔表达式>)          # bool() 后必须为 True
    rep.false("<一句中文描述>", <布尔表达式>)         # bool() 后必须为 False
    rep.contains("<一句中文描述>", <容器或字符串>, <子串>)
```

- 断言必须写在 `def main() -> int:` 内部、`return rep.finish()` 之前。
- **不要自己 `sys.exit`**：失败信息只由 `finish()` 统一汇总。
- 标签写清"期望什么"，因为失败时只会打印 `got=... want=...`。
- 多行形态用 `rep.check(` + 三个参数各占一行。

**断言质量比数量重要**：一条永远为真的断言等于没测。新增断言后做一次**证伪** ——
把被测行为改回旧写法，确认这条断言真的变红（见 §6）。

### 5.6 新增一次数据库迁移

`Store.MIGRATIONS` 是 `(表, 列, 声明)` 三元组列表。加一条即可：

```python
        ("edges", "new_column", "INTEGER NOT NULL DEFAULT 0"),
```

启动时按 `PRAGMA table_info` 探测缺列并 `ALTER TABLE ADD COLUMN`，**每列独立 `try/except`**，
失败项汇总告警一次。**一个失败的 `ALTER` 绝不能让插件构造失败** —— 那会把升级变成"插件直接加载不了"。

## 6. 离线自检

### 6.1 跑法

```powershell
$env:PYTHONUTF8 = "1"
python tests/offline_check.py
```

不装 AstrBot、不装 httpx、不连外网。harness 会把 `astrbot.*` 与 `httpx` 全部 stub 掉，
再用 `importlib` 按文件路径加载 `main.py`。

> `load_plugin()` 里的 `sys.modules[name] = module` 是**承重**的，不要删：`main.py` 用了
> `from __future__ import annotations` + `@dataclass`，dataclass 处理字符串注解时会回查
> `sys.modules[cls.__module__]`，缺了会 `AttributeError`。

### 6.2 退出码

| 码 | 含义 |
|---|---|
| `0` | 全部通过（末行 `ALL PASSED (N checks)`） |
| `1` | 有断言失败（末行 `FAILED: M of N` 并逐条列出） |
| `2` | **跑到汇总之前就中断**（例如 `UnicodeEncodeError`）—— 后半段断言一条都没跑，**不是通过** |

### 6.3 断言数从哪来

末行的 `N` 是**运行期**计数，不是静态调用点数：循环会把同一个调用点放大
（数据源逐个装配、随包文件逐个检查等）。所以"断言数变了"既可能是新增/删除，也可能是
某个循环的数据变了 —— **判断回归要比"相同断言数下的失败集合"**。

开发仓里若存在真实语料 `resources/feed.xml`，会额外跑一段整体回归（12 条断言）；
**它不随包发布**，缺失时打印 `SKIP` 并少 12 条。看到断言数变化先确认是不是这条。

### 6.4 自检是变更探针，不是验收证据

绿灯只说明"没弄坏已知断言"，**不能**证明协议端行为、真实时间线、真实 AstrBot 加载与升级、
跨插件真机联动。需要真机才能确定的结论必须标 `待核实`。

### 6.5 证伪是硬要求

声称"修好了"之前：

1. 把改动**改回旧行为**（或禁用新分支）。
2. 重跑自检：**对应断言必须变红**。
3. 恢复改动，断言重新变绿。

第 2 步不变红 ⇒ 这条断言**没有守着你声称的行为**，那不是"修好了"，是"没测到"。

**作者不得自验。** 改动者不能用自己的自检结论结案；需要独立方按"能不能证伪"去验，
结论要落到具体断言或命令输出，不能只说"已修复"。

## 7. 提交规范

采用 **Conventional Commits**，**主题与正文用中文**。

```
<type>(<scope>): <中文主题，一句话，不超过 50 字>

- 变更要点（为什么改，不是重复主题）
- 影响面：命令 / 配置项 / 数据表 / 页面
```

### 7.1 `type`

| type | 用于 |
|---|---|
| `feat` | 新增用户可见能力 |
| `fix` | 修复缺陷 |
| `perf` | 性能与资源占用 |
| `refactor` | 不改变行为的重构 |
| `docs` | 只改文档 |
| `test` | 只改测试与 fixture |
| `chore` | 构建、依赖、杂项 |
| `chore(release)` | 发版准备 |

### 7.2 `scope`

用改动所在的层，常用：`fetch`（抓取/降级链）、`store`（数据层）、`send`（发送/合并转发）、
`media`（媒体缓存）、`dnd`（免打扰/队列）、`i18n`、`webui`、`image-pool`、`release`、`docs`。

### 7.3 示例

```
feat(fetch): 新增 RSSHub 实例池冷却

- 单实例连续失败后冷却 10 分钟，其余实例继续服务
- 新增配置 rsshub_instances 与实例级健康展示
- /x源 探测不污染主链熔断计数
```

```
fix(media): media_mode=download 此前恒假、从未生效

- bool_cfg 返回布尔，与字符串比较恒为假，改为 cfg() 比较字符串
- 影响：选"本地下载"的用户此前一直在走直链
```

### 7.4 破坏性变更

在 type 后加 `!`，并在正文写 `BREAKING CHANGE:` 段落说明影响与迁移方式：

```
feat(config)!: 数据源名改为稳定字符串

BREAKING CHANGE: fetch_chain 里的旧写法不再被识别，需改为 fxtwitter / fxtwitter_rss / rsshub。
```
```

### 7.5 版本与标签

- 版本号出现在 **3 处**：`metadata.yaml: version`、`main.py: PLUGIN_VERSION`、
  `CHANGELOG.md` 的 `## vX.Y.Z` 小节。**自检断言强制它们一致。**
- 发布时还要与 git tag `v<version>` 一致。
- 打标签：`git tag -a v1.4.0 -m "v1.4.0：<一句话要点>"`。

### 7.6 不要提交

运行时数据与产物已在 `.gitignore` 里：`data/`、`*.sqlite3*`、`*.log`、`*.zip`、`dist/`、
`__pycache__/`。**公开边界就是"提交了什么"** —— 打包时筛不掉东西，
所以不要提交本机绝对路径、私人邮箱、真实群号、真实第三方 feed 语料或任何凭证。

## 8. 发版检查清单

1. `CHANGELOG.md` 增加 `## vX.Y.Z` 小节，只列**用户可见**的变化。
2. 同步版本号：`metadata.yaml`、`main.py: PLUGIN_VERSION`（`@register` 用的是这个常量）、
   `CHANGELOG.md`。
3. 跑离线自检，与"相同断言数下的基线"比失败集合。
4. 需要真机确认的项（合并转发节点昵称、payload 上限、WebUI 在真实 Dashboard 的表现、
   与图库的联动）逐项目视确认，未确认的写 `待核实`。
5. 确认没有把开发期内容（审查记录、实测语料、本机信息）带进提交。
6. 打 tag、推送、按 tag 建 Release。

> 维护者另有一套打包与发布前守卫脚本，位于**开发工作区**、**不随包发布**；
> 它核对 metadata 合法性、版本一致性、归档白名单、体积上限与脱敏命中。
> 贡献者只需保证第 1–5 步成立。
(End of file - total 468 lines)