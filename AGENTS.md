# AGENTS.md — astrbot_plugin_x_monitor

> **本文件属于发布层。** 本目录既是插件仓根，也是打包根；它写的是**仓库级、长期稳定**的规则，
> 面向任何在本仓工作的 AI 协作者。改动本目录前先读它。
>
> 同一类事实只设一个权威来源，本文件不复制别处正文：
> 用户文档 [README.md](README.md) ｜ 版本变更 [CHANGELOG.md](CHANGELOG.md) ｜
> 开发环境 / 架构 / 代码规范 / 模板 / 自检用法 / 提交规范 [CONTRIBUTING.md](CONTRIBUTING.md)。
>
> 若本目录位于**维护者的开发工作区**中（即上一层还有一份 `AGENTS.md`），那份描述本机环境、
> 工作区布局与文档路由，**不随包发布**。两层分工是：本层管「这个插件仓库的规则」，
> 上层管「这台机器 / 这个工作区的规则」。**同一条规则不要写在两层** —— 重复会浪费上下文预算，
> 并且迟早分叉。公开克隆本仓的读者没有上一层，忽略即可。

## 1. 这是什么

AstrBot 的 **Star 插件**：监控 X（原 Twitter）账号并把新帖转发到会话 —— 多群独立订阅与群级三档权限、
多源降级抓取（L1 FxTwitter API → L2 fxtwitter RSS → L3 自建 RSSHub 实例池）、图片 / 视频获取、
基于 AstrBot Provider 的 LLM 翻译、合并转发（自定义昵称）、免打扰时段补发，并提供 WebUI 管理页。

- **版本权威链（四者一致，有自检断言）**：`metadata.yaml: version` = `main.py: PLUGIN_VERSION`
  = `@register(...)` 传入的版本 = `CHANGELOG.md` 里的 `## vX.Y.Z`。
- **对端**：`astrbot_plugin_image_pool` v1.7.0+，**只**通过它的公开方法 `ingest_files` 调用。
- **支持平台**：`aiocqhttp`（合并转发依赖协议端；非 OneBot 平台会退回普通消息链）。

## 2. 四条正确性不变量（都有断言守着）

1. `UNIQUE(edge_id, post_id)` —— 结构上杜绝同一会话重复推送
2. `edges.enqueued_ts` 只增不减 —— 单调水位
3. 抓取失败**不写** `last_seen_ts`、**不置** `baseline_ready` —— 区分「网络失败」与「确实没有新帖」
4. 首次订阅建立基线、**不补发历史**

一轮抓取的提交是**单个事务**：写 posts → 为每条边入队 → 推进两条水位。任一环节失败整体回滚，
下一轮用同一个 `since` 重新拉到，既不会漏也不会重。

## 3. 硬性规则

- **运行时数据只走 `StarTools.get_data_dir(PLUGIN_NAME)`**
  （= `data/plugin_data/astrbot_plugin_x_monitor/`），**绝不写进插件目录** ——
  插件目录会被升级整体替换。
- **触发域用 `@filter.command`，不用 regex 兜底**；群内**不要**注册裸关键词自动回复。
  回复后按语义决定 `event.stop_event()` / `event.should_call_llm(True)`，
  注意后者的布尔含义与名字**相反**（`True` 才是「禁止默认 LLM 请求」）。
- **配置项只在 `_conf_schema.json` 声明一次**，代码读到的键必须被它声明（自检有双向断言）。
  字符串枚举键（如 `media_mode`）必须用 `cfg()` 读并比较字符串，**不要**用 `bool_cfg()`
  —— 后者返回布尔，与字符串比较恒假。原因与实例见 [CONTRIBUTING.md](CONTRIBUTING.md) §4.4。
- **跨插件调用**：每次调用前**重新解析对端实例** + 检查 `activated` + `hasattr` 探测能力；
  **绝不直写对端数据库**；富化步骤（下载、翻译）失败**不得**阻断抓取 / 水位 / 入队。
- **需要排序的两个事件一律用 `time.monotonic()`**；墙钟时间只用于展示。
- **免打扰、限速、积压上限都按群隔离**；批量操作必须有上限，并在日志里说明截断。
- **加固类改动优先「移动 / 复制」而非「删除」**；破坏性功能默认关闭。
- **注释与文档用中文**；面向公开的内容不写本机私有事实（实例名、群号、绝对路径、邮箱）。
- **未获用户明确授权，不执行 commit / push / 发布。**

## 4. 验证要求

- 改完必跑 `python tests/offline_check.py`（Windows 控制台需先 `PYTHONUTF8=1`）。
- **离线自检是变更探针，不是验收证据**：绿灯只说明「没弄坏已知断言」。
- 声称修好之前必须做一次**证伪**（把改动改回旧行为，对应断言必须变红），
  且**作者不得自验** —— 改动者不能用自己的自检结论结案。
- 需要真机 / 协议端才能确定的结论（合并转发节点昵称、payload 上限、
  WebUI 在真实 Dashboard 的表现、与 image_pool 的联动）**必须标 `待核实`**。
- 命令、判据、基线与断言模板见 [CONTRIBUTING.md](CONTRIBUTING.md) §6。

## 5. 文档权威链

| 事实 | 权威来源 |
|---|---|
| 项目用途与用户用法 | [README.md](README.md) |
| 版本变更记录 | [CHANGELOG.md](CHANGELOG.md) |
| 开发环境、架构、代码规范、模板、自检用法、提交规范 | [CONTRIBUTING.md](CONTRIBUTING.md) |
| 仓库级规则 | 本文件 |
| 配置项定义与默认值 | `_conf_schema.json` |
| 版本号 | `metadata.yaml` |
| 数据源层间语义 | `main.py` 里 `ProviderChain` 的类文档串 |
| 跨插件接口与降级行为 | `main.py` 的 `ImagePoolBridge` |

改动时保持「一类事实一个权威来源」：别处只写简短摘要和链接，不复制正文。
(End of file - total 78 lines)