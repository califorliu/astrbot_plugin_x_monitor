"""AstrBot 插件：X（原 Twitter）帖子监控与转发。

功能
    1. 监控目标 X 账号：增 / 删 / 列表 / 按群独立订阅（每群一份独立列表与游标）
    2. 帖子原文与翻译：复用 AstrBot 已配置的 LLM Provider（超时预算 + 熔断），不额外请求第三方翻译
    3. 帖子图片 / 视频获取：原图直链下载到本地有界缓存，防盗链与配额淘汰
    4. SnowLuma(OneBot) 多类型发送：
       - forward：合并转发（Nodes），节点昵称 = 订阅自定义昵称或推主名，uin = 机器人自身 ID
       - chain  ：同一条消息链（正文 + 图片），视频独立另发
       - 超 payload 预算自动拆批，失败逐级降级
    5. 免打扰时段：时段内只入队不发送，时段外按速率逐条补发，超积压上限丢弃最旧并汇总告知
    6. 图库联动（可选，默认关）：把推文照片归档进 astrbot_plugin_image_pool，
       tag = 该订阅的自定义昵称；单向依赖 + 能力探测，对端缺失/停用/旧版本都不影响推送

群级权限（沿用 astrbot_plugin_image_pool 的既有模型）
    | 模式      | 订阅（增删本群监控目标） | 查看（列表 / 状态） |
    |-----------|--------------------------|---------------------|
    | whitelist | 全员                     | 全员                |
    | readonly  | 仅管理员                 | 全员                |
    | none      | 仅管理员                 | 仅管理员            |

    管理员分两级，均**不借道 AstrBot 全局管理员**：
      - plugin_admins：全局管理员，可管整库、可改群模式与授权
      - group_admins：`群号:UID`（`*:UID` 表示所有群），只能管本群订阅，可改本群昵称与免打扰

触发方式
    与 image_pool 一致：插件指令**不需要带前缀**。AstrBot 的 wake_prefix（如 `/`）负责唤醒，
    唤醒后框架再匹配插件指令，WakingCheckStage 已把前缀从 message_str 剥掉。
    因此这里一律用 @filter.command —— **不要**用 @filter.regex（它不受唤醒门控，会与共存 bot 抢答）。

存储
    data/plugin_data/astrbot_plugin_x_monitor/
      xmonitor.sqlite3   authors / edges / posts / deliveries / meta
      media/<sha>.<ext>  有界媒体缓存
"""

from __future__ import annotations

import asyncio
import hashlib
import html as _html
import inspect
import json
import os
import random
import re
import sqlite3
import tempfile
import time
import unicodedata
import xml.etree.ElementTree as ET
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Image, Plain, Video
from astrbot.api.platform import MessageType
from astrbot.api.star import Context, Star, StarTools, register
from astrbot.core.message.components import Node, Nodes
from astrbot.core.star.filter.command import GreedyStr

try:  # Plugin Pages 需要 AstrBot >= v4.24.1
    from astrbot.api import web as web_api

    WEB_API_AVAILABLE = True
except Exception:  # pragma: no cover - 旧版本优雅降级
    web_api = None
    WEB_API_AVAILABLE = False

PLUGIN_NAME = "astrbot_plugin_x_monitor"
PLUGIN_VERSION = "1.4.0"
FXTWITTER_BASE = "https://api.fxtwitter.com"
FXTWITTER_RSS_BASE = "https://fxtwitter.com"

#: 抓取提供者的稳定名字。**这些字符串同时是配置值**，改动等于破坏用户配置。
PROVIDER_FXTWITTER = "fxtwitter"
PROVIDER_FXTWITTER_RSS = "fxtwitter_rss"
PROVIDER_RSSHUB = "rsshub"
PROVIDER_NAMES = (PROVIDER_FXTWITTER, PROVIDER_FXTWITTER_RSS, PROVIDER_RSSHUB)
DEFAULT_FETCH_CHAIN = [PROVIDER_FXTWITTER, PROVIDER_FXTWITTER_RSS, PROVIDER_RSSHUB]

#: 探测某个源是否可用时用的 handle。挑的是必然存在、且内容无关紧要的账号。
DEFAULT_PROBE_HANDLE = "X"

#: 单个提供者单次抓取的整体超时（秒）。网络层已有 httpx 超时，这里再包一层，
#: 防止某个提供者内部逻辑卡死把整轮轮询拖住。
FETCH_PROVIDER_TIMEOUT = 30.0

# ---------------------------------------------------------------- 常量 / 纯函数

ILLEGAL_CHARS = re.compile(r'[\\/:*?"<>|\x00-\x1f]')
MAX_NICK_LEN = 32
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

VALID_MODES = {"whitelist", "readonly", "none"}
#: 群级模式 -> 非管理员拥有的能力
MODE_CAPS: dict[str, dict[str, bool]] = {
    "whitelist": {"subscribe": True, "view": True},
    "readonly": {"subscribe": False, "view": True},
    "none": {"subscribe": False, "view": False},
}

KIND_ORIGINAL = "original"
KIND_REPLY = "reply"
KIND_REPOST = "repost"
KIND_QUOTE = "quote"

STATE_QUEUED = "QUEUED"
STATE_DELIVERED = "DELIVERED"
STATE_FAILED = "FAILED"
STATE_DROPPED = "DROPPED"

EXT_BY_MAGIC = (
    (b"\x89PNG\r\n\x1a\n", ".png"),
    (b"\xff\xd8\xff", ".jpg"),
    (b"GIF8", ".gif"),
    (b"RIFF", ".webp"),
    (b"BM", ".bmp"),
)
#: fxtwitter 给附件补的占位锚文本（真实 feed 里 20/90 条带 ``<a>Media</a>``）。
#: 这些**不是推文正文**，必须从 text 里剔除。
MEDIA_PLACEHOLDER_TEXTS = {"media", "video", "photo", "image", "gif", "picture"}
#: 上面的占位锚点只在这些媒体域上成立，避免把用户自己写的 "Media" 链接也吃掉
MEDIA_PLACEHOLDER_HOSTS = ("video.twimg.com", "pbs.twimg.com", "twimg.com")
MAX_MEDIA_BYTES = 32 * 1024 * 1024

# ---------------------------------------------------------------- image_pool 联动常量

#: 对端插件的 AstrBot 插件名（= 它的 metadata.yaml ``name``，也是 star_map 的键）
IMAGE_POOL_PLUGIN_NAME = "astrbot_plugin_image_pool"
#: 对端公开方法名。老版本没有它 —— 只能**能力探测**，绝不能硬依赖。
IMAGE_POOL_INGEST_METHOD = "ingest_files"
#: 入库时写入的 source 标记，与 image_pool 的 /加图（manual）和被动采集（auto）区分开。
#: 注意：image_pool 默认的保留期清理只处理 ``source='auto'``，所以 X 归档图不会被自动淘汰，
#: 也不计入 ``max_pool_mb`` 的容量统计（目录只涨不缩，请自行留意磁盘）。
IMAGE_POOL_SOURCE = "x"

#: 兼容层不可用原因的文案（/x状态 与 WebUI 直接显示，排障不必翻日志）
IMAGE_POOL_REASON_TEXT = {
    "sync_off": "联动开关未打开（image_pool_sync_enabled）",
    "not_installed": "未安装 astrbot_plugin_image_pool",
    "plugin_off": "image_pool 插件当前未启用（activated=False）",
    "old_version": "image_pool 版本过旧（没有 ingest_files 公开方法）",
    "no_context": "拿不到插件上下文，无法解析对端实例",
    "resolve_error": "解析对端实例时异常",
    "bad_contract": "对端 star_cls 不是实例（装配异常）",
    "no_tag": "该账号的订阅都没有设置自定义昵称",
    "no_photo": "这条帖子没有照片",
    "no_new_posts": "本轮没有新帖落库",
    "download_failed": "照片没能下载到本地",
    "timeout": "入库超时（对端可能稍后才写入）",
    "budget_exhausted": "本轮入库预算用尽",
    "error": "入库异常",
}

#: 归档画质的合法值（与 ``_conf_schema.json`` 的 options 一致）。
#: 非法值一律回落 ``large`` —— ``apply_twimg_quality`` 的默认是 ``orig``（最贵档），
#: 让"配置写错"悄悄变成"用最贵档下载"正好与默认省磁盘的承诺相反。
IMAGE_POOL_QUALITIES = ("orig", "large", "small")

#: "联动能不能出图"的五种状态文案。**这里是唯一来源**：`/x状态` 直接用它，
#: WebUI 也从 payload 的 ``tag_state_text`` 读它（早先 app.js 里有一份硬编码拷贝，
#: 已经和这边分叉过 —— 独立验证 L-C2）。文案里**不要用 Markdown**：`plain_result`
#: 不渲染，用户会看到字面的 `**`（L-N5）。
IMAGE_POOL_TAG_STATE_TEXT = {
    "ready": "",
    "no_nickname": "没有任何订阅设置自定义昵称 → 不会入库（用 /x昵称 <handle> <昵称> 设置）",
    "rejected_only": (
        "所有昵称都不能用作图库分类名 → 不会入库"
        "（换一个不含 NUL/CON 这类 Windows 保留名、且能被 UTF-8 编码的昵称）"
    ),
    "paused_only": "只有暂停推送的订阅设了昵称 → 当前不会入库（用 /x推送 开 恢复）",
    "unknown": "读取订阅列表失败，无法判断联动是否会入库（详见日志）",
}

#: Windows 保留设备名：作为**目录名**时 ``open()`` 会直接抛 ``FileNotFoundError``
#: （实测 ``NUL``；``CON``/``AUX``/``COM1`` 反而可用，但一并拒掉更稳）。
#: 这种 tag 绝不能交给对端 —— 会把同一条帖子的其余 tag 一起带崩。
IMAGE_POOL_DEVICE_NAMES = frozenset(
    ["con", "prn", "aux", "nul"]
    + [f"com{i}" for i in range(1, 10)]
    + [f"lpt{i}" for i in range(1, 10)]
)

#: 区分"键不存在"与"键的值就是 None"，用于配置落盘失败时的精确回滚
_MISSING = object()


def to_str_list(value) -> list[str]:
    """把配置项（list / 逗号分隔字符串 / 单值）统一成去空的字符串列表。"""
    if value is None:
        return []
    if isinstance(value, str):
        items = re.split(r"[,\n]", value)
    elif isinstance(value, (list, tuple, set)):
        items = list(value)
    else:
        items = [value]
    out: list[str] = []
    for item in items:
        text = str(item).strip()
        if text:
            out.append(text)
    return out


def to_bool(value, default: bool = False) -> bool:
    """宽松但**不误判**地解析布尔。

    存在的意义：``bool("false")`` 是 ``True``。WebUI / JSON 里传字符串布尔是常态，
    直接 ``bool()`` 会把"关"写成"开"，而且这种 bug 静默无痕。
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        low = value.strip().lower()
        if low in ("1", "true", "yes", "on", "是", "开", "启用"):
            return True
        if low in ("0", "false", "no", "off", "否", "关", "停用", ""):
            return False
    return default


def _nick_clean(text) -> str:
    """昵称的规范化主体（**不截断**）：过滤非法字符、压空白、剔除无法编码的字符。

    单独抽出来是为了让调用方能在截断**之前**判断"是不是太长了" ——
    ``normalize_nick`` 会静默截断到 32 字，而 WebUI 的 400 文案却声称"长度不超过 32 字"
    （独立验证 N-2：40 字的输入被静默截断，用户以为自己填的就是那 40 个字）。
    """
    name = ILLEGAL_CHARS.sub("", str(text))
    name = re.sub(r"\s+", " ", name).strip()
    return name.encode("utf-8", "ignore").decode("utf-8")


def normalize_nick(text) -> str:
    """规范化昵称：它同时用作合并转发节点名与图库 tag，两处的约束取交集。

    图库 tag 的规则：不可含 ``\\/:*?"<>|`` 与控制字符，长度 <= 32。
    这里统一过滤 + 压空白 + 截断，返回空串表示"未设置"。

    **孤立代理对必须在这里剔除**（``"\\ud800"``）：WebUI 的 JSON 能把它塞进来，
    而 SQLite 写 UTF-8 时会抛 ``UnicodeEncodeError`` —— 指令路径表现为"发了没反应"、
    WebUI 路由表现为 500，且异常发生在**写库**而不是校验处，排查方向完全被带偏。
    ``encode(errors="ignore")`` 顺带把所有无法编码成 UTF-8 的字符一起挡掉。
    """
    if text is None:
        return ""
    return _nick_clean(text)[:MAX_NICK_LEN]


def pool_tag(text) -> str:
    """把昵称规范化成 **image_pool 会接受的分类名（tag）**，空串表示"这个名字不可用"。

    ``normalize_nick`` 与 image_pool 的 ``normalize_name`` 规则几乎一致（同一份
    ``ILLEGAL_CHARS``、同一个 32 字上限），但后者多做 **NFKC 归一化**与**首尾 ``" ."`` 剥离**：
    昵称 ``"初音."`` 在 x_monitor 里存成 ``初音.``，图库那边却会变成 ``初音``。
    这里按对端的规则再走一遍，保证"图库里的 tag"与"用户设的昵称"不会出现肉眼难查的差异。

    返回空串表示这个名字**不能**交给对端，调用方必须跳过而不是硬塞：

    - 归一化后为空 / 只剩 ``.`` —— 对端会回落到「未分类」，把外部图与自采图混在一起；
    - **Windows 保留设备名**（``NUL`` / ``CON`` / ``COM1``…）—— 对端拿它当目录名时
      ``open()`` 会抛 ``FileNotFoundError``，而它的每个 tag 是一条独立写入路径，
      一个坏 tag 会把同帖其余 tag 一起带走；
    - **无法编码成 UTF-8 的字符**（WebUI 的 JSON 可以塞进孤立代理对 ``\\ud800``）——
      对端写文件名时会抛 ``UnicodeEncodeError``，同上。
    """
    if not text:
        return ""
    name = unicodedata.normalize("NFKC", str(text)).strip()
    name = ILLEGAL_CHARS.sub("", name).strip(" .")
    if not name or name in {".", ".."}:
        return ""
    # 判"主干名"：`NUL.txt` 这类带后缀的目录名在 Windows 上同样不可用
    if name.split(".")[0].strip().casefold() in IMAGE_POOL_DEVICE_NAMES:
        return ""
    # 含零宽字符的 tag 对端已经拒收（image_pool `_ensure_category`），这里必须一致：
    # 否则状态页判 `ready`、`rejected_tags` 不涨，用户的图**静默不入库**（独立验证 O-N4）。
    if any(ch in name for ch in "\u200b\u200c\u200d\ufeff"):
        return ""
    try:
        name.encode("utf-8")
    except UnicodeEncodeError:
        return ""
    return name[:MAX_NICK_LEN]


def parse_nickname_input(raw) -> tuple[bool, str, str]:
    """把用户输入的昵称解析成可入库的值。返回 ``(是否合法, 结果, 错误文案)``。

    与"直接 ``normalize_nick``"的差别（每一条都对应一个真实反例）：

    * **``None`` = 用户明确要清空** → 合法，返回空串；
    * **非字符串一律拒绝**：JSON 里的 ``0`` / ``false`` / ``[]`` / ``{}`` 会被
      ``str()`` 强转成 ``"0"`` / ``"False"`` / ``"[]"`` / ``"{}"`` 存进去，
      那不是用户想要的昵称（独立验证 N-2）；
    * **原文非空但规范化后为空 → 拒绝**：``normalize_nick`` 对孤立代理对与空串都返回 ``""``，
      直接拿结果写库会把"非法输入"当成"清空昵称"并回 200 成功 —— 用户以为改了昵称，
      其实旧昵称被静默抹掉（L-N3）；
    * **规范化后超过 ``MAX_NICK_LEN`` → 拒绝**，而不是静默截断（N-2）。
    """
    if raw is None:
        return True, "", ""
    if not isinstance(raw, str):
        return False, "", f"昵称必须是字符串（收到 {type(raw).__name__}）"
    full = _nick_clean(raw)
    if raw.strip() and not full:
        # 两种成因都写出来：全被非法字符剔掉（如 `\\\\`）与含无法编码的字符，
        # 只说后者会把用户引向错误方向（独立验证 O-N7）。
        return False, "", "昵称不可用（含非法字符，或全由无法编码的字符组成）"
    if len(full) > MAX_NICK_LEN:
        return False, "", f"昵称过长（最多 {MAX_NICK_LEN} 字，收到 {len(full)} 字）"
    return True, full[:MAX_NICK_LEN], ""


def sniff_ext(head: bytes, fallback: str = ".jpg") -> str:
    """按魔数推断扩展名，失败时回退。

    ``ftyp`` 是 ISO-BMFF 的**偏移 4** 魔数，不在 ``EXT_BY_MAGIC`` 的"从头开始"表里，
    必须单独判——否则从 ``video.twimg.com`` 下载的视频会落成 ``.jpg``。

    但 ``ftyp`` 只是容器族，**brand**（偏移 8..12）才决定真实类型：
    早期实现一律返回 ``.mp4``，于是 mov / m4a / 3gp / heic / avif 全被当成 mp4。
    """
    if len(head) >= 12 and head[4:8] == b"ftyp":
        brand = bytes(head[8:12])
        if brand == b"qt  ":
            return ".mov"
        if brand in (b"M4A ", b"M4B ", b"M4P "):
            return ".m4a"
        if brand[:2] == b"3g":
            return ".3gp"
        if brand in (b"heic", b"heix", b"mif1", b"msf1"):
            return ".heic"
        if brand in (b"avif", b"avis"):
            return ".avif"
        return ".mp4"
    for magic, ext in EXT_BY_MAGIC:
        if head.startswith(magic):
            if ext == ".webp" and not head.startswith(b"RIFF"):
                continue
            return ext
    return fallback


def parse_hhmm(text: str) -> int | None:
    """``"23:00"`` -> 1380（当日分钟数）。解析失败返回 None。"""
    m = re.fullmatch(r"\s*(\d{1,2})\s*[:：]\s*(\d{1,2})\s*", str(text or ""))
    if not m:
        return None
    hour, minute = int(m.group(1)), int(m.group(2))
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return hour * 60 + minute


def parse_dnd_ranges(items) -> list[tuple[int, int]]:
    """把 ``["23:00-08:00", "12:30-13:30"]`` 解析成分钟区间列表。

    支持跨天（start > end）。每个条目内部还会按逗号/分号/顿号再拆一次——
    WebUI 的 list 是「一个 chip 一行」，用户很可能把多段写进同一个 chip。
    非法条目直接跳过，不抛异常：配置写错不应该让整个插件失效。
    """
    ranges: list[tuple[int, int]] = []
    for raw in to_str_list(items):
        for piece in re.split(r"[,\n;；、]", str(raw)):
            text = piece.replace("~", "-").replace("—", "-").replace("－", "-").strip()
            if not text:
                continue
            parts = [p for p in re.split(r"\s*-\s*", text) if p.strip()]
            if len(parts) != 2:
                continue
            start, end = parse_hhmm(parts[0]), parse_hhmm(parts[1])
            if start is None or end is None:
                continue
            ranges.append((start, end))
    return ranges


def in_quiet_hours(now_minute: int, ranges: list[tuple[int, int]]) -> bool:
    """判断当前分钟数是否落在免打扰区间内（支持跨天）。"""
    for start, end in ranges:
        if start == end:
            continue
        if start < end:
            if start <= now_minute < end:
                return True
        else:  # 跨天，例如 23:00-08:00
            if now_minute >= start or now_minute < end:
                return True
    return False


def now_minute_in(tz_name: str) -> int:
    """取指定时区的"当日分钟数"。取不到时区信息时回退本地时间。"""
    try:
        from zoneinfo import ZoneInfo

        tz = ZoneInfo(tz_name or "Asia/Shanghai")
        local = datetime.now(tz)
    except Exception:
        local = datetime.now()
    return local.hour * 60 + local.minute


def format_ts(ts: int, tz_name: str = "Asia/Shanghai") -> str:
    """把秒级时间戳格式化成本地可读时间。"""
    try:
        from zoneinfo import ZoneInfo

        tz = ZoneInfo(tz_name or "Asia/Shanghai")
        dt = datetime.fromtimestamp(int(ts), tz)
    except Exception:
        dt = datetime.fromtimestamp(int(ts))
    return dt.strftime("%Y-%m-%d %H:%M")


def chinese_ratio(text: str) -> float:
    """中文字符占比（用于跳过明显不需要翻译的文本）。"""
    if not text:
        return 0.0
    han = sum(1 for ch in text if "\u4e00" <= ch <= "\u9fff")
    return han / max(1, len(text))


def classify_post(obj: dict) -> str:
    """判定帖子类型，顺序即优先级。

    ``replying_to`` 是**对象**（父帖 id 在 ``status`` 字段，没有 ``id``）；
    ``quote`` 可能是 ``tombstone``（被删的引用帖，无 text/author）。
    """
    if obj.get("reposted_by"):
        return KIND_REPOST
    if obj.get("replying_to"):
        return KIND_REPLY
    quote = obj.get("quote")
    if isinstance(quote, dict) and quote.get("type") != "tombstone":
        return KIND_QUOTE
    return KIND_ORIGINAL


def pick_video_url(media_obj: dict) -> str:
    """从 formats 里挑 mp4 且码率最高的一档；绝不取 m3u8。"""
    best_url, best_bitrate = "", -1
    for fmt in media_obj.get("formats") or []:
        if not isinstance(fmt, dict):
            continue
        if fmt.get("container") != "mp4":
            continue
        bitrate = int(fmt.get("bitrate") or 0)
        if bitrate >= best_bitrate and fmt.get("url"):
            best_bitrate, best_url = bitrate, str(fmt["url"])
    return best_url or str(media_obj.get("url") or "")


def extract_media(obj: dict) -> list[dict]:
    """抽出可下载的媒体条目。

    注意：无媒体时 ``media`` 是 ``{}``（**没有 ``all`` 键**），不能假定存在。
    """
    media = obj.get("media")
    if not isinstance(media, dict):
        return []
    out: list[dict] = []
    for item in media.get("all") or []:
        if not isinstance(item, dict):
            continue
        kind = str(item.get("type") or "")
        if kind == "photo":
            url = str(item.get("url") or "")
            if url:
                out.append({"type": "photo", "url": url})
        elif kind in ("video", "gif"):
            url = pick_video_url(item)
            if url:
                out.append(
                    {
                        "type": "video",
                        "url": url,
                        "thumbnail": str(item.get("thumbnail_url") or ""),
                    }
                )
    return out


def post_has_media(obj: dict) -> bool:
    """帖子是否含**自带媒体**（图片 / 视频 / GIF）。

    口径（已确认）：``media.all`` 存在且非空即算，且只认 ``photo`` / ``video`` / ``gif``。

    刻意**不**把 ``media.external``（YouTube 等外链卡片）算进来 —— 那是链接预览，
    不是帖子自带的媒体；否则"带链接的纯文字帖"也会被当成含媒体。
    ``media.mosaic`` 的 ``mosaic_photo`` 也不在 ``media.all`` 里，天然不计入。
    """
    return bool(extract_media(obj))


def edge_media_only(edge) -> bool:
    """读订阅边的"仅媒体"标记。

    老库在迁移前、或传入的对象缺该键时按 ``False``（拉全部）处理，
    绝不因为一个可选的收窄开关把正常推送打断。
    """
    try:
        return bool(int(edge["media_only"]))
    except Exception:
        return False


def validate_umo(umo: str) -> tuple[bool, str]:
    """只做**格式**校验：``platform:MessageType:session_id``，三段且都非空。

    注意第一段是**平台实例 id**（用户在 AstrBot 里给该适配器实例起的名字，
    例如「mybot」），**不是**适配器名（``aiocqhttp``）。这两者混用会得到一个
    永远发不出去的 umo —— 框架只会回一句
    ``cannot find platform for session ...``。
    """
    parts = str(umo or "").split(":")
    if len(parts) != 3 or not all(p.strip() for p in parts):
        return False, "完整会话标识需形如 <平台实例ID>:GroupMessage:<群号>（第一段是平台实例 id，不是适配器名）"
    return True, ""


def platform_id_of(umo: str) -> str:
    """umo 第一段 = 平台实例 id。"""
    return str(umo or "").split(":")[0]


def group_id_of(umo: str) -> str:
    """从 umo 取最后一段作为群号（aiocqhttp 系适配器的 session_id 就是群号）。"""
    return str(umo or "").split(":")[-1]


def validate_config_value(entry: dict, value) -> tuple[bool, object, str]:
    """按 ``_conf_schema.json`` 的条目校验并规范化一个配置值。

    返回 ``(ok, normalized, error)``。纯函数，便于离线测试 ——
    WebUI 的配置编辑直接复用它，避免"前端限制了但后端没校验"。

    规则与 AstrBot 渲染器保持一致：
      - ``bool``：接受 true/false 与常见中英文写法
      - ``int`` / ``float``：``slider`` 存在时**钳制**到 [min, max]（渲染器也是钳制）
      - ``list``：归一成字符串列表；有 ``options`` 时逐项校验
      - ``string`` / ``text``：有 ``options`` 时必须在其中
    """
    kind = str(entry.get("type") or "string")
    options = entry.get("options")

    if kind == "bool":
        if isinstance(value, bool):
            return True, value, ""
        if isinstance(value, str):
            low = value.strip().lower()
            if low in ("1", "true", "yes", "on", "是", "开", "启用"):
                return True, True, ""
            if low in ("0", "false", "no", "off", "否", "关", "停用"):
                return True, False, ""
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if value in (0, 1):
                return True, bool(value), ""
        return False, None, "应为布尔值"

    if kind in ("int", "float"):
        if isinstance(value, bool):
            return False, None, "应为数字"
        try:
            number = float(value)
        except (TypeError, ValueError):
            return False, None, "应为数字"
        slider = entry.get("slider") or {}
        low, high = slider.get("min"), slider.get("max")
        if isinstance(low, (int, float)) and number < low:
            number = float(low)
        if isinstance(high, (int, float)) and number > high:
            number = float(high)
        if kind == "int":
            return True, int(round(number)), ""
        return True, float(number), ""

    if kind == "list":
        items = to_str_list(value)
        if options:
            bad = [item for item in items if item not in options]
            if bad:
                return False, None, f"含非法选项：{', '.join(bad)}"
        return True, items, ""

    text = "" if value is None else str(value)
    if options and text not in options:
        return False, None, f"必须是 {'/'.join(str(o) for o in options)} 之一"
    return True, text, ""


def build_share_url(obj: dict) -> str:
    """帖子的可点链接。

    **优先用 feed/API 给的 ``url``**：它才是权威地址。只有当来源没给 url 时，
    才退化成"用 author.screen_name + id 拼"——而那个 handle 是从 URL 或
    ``dc:creator`` 猜出来的，猜错就会生成一条"看起来能点、内容却不对"的链接
    （实测反例：条目 link 是别人的 id，作者被猜成频道主人）。
    """
    url = str(obj.get("url") or "").strip()
    if url:
        return url
    handle = str((obj.get("author") or {}).get("screen_name") or "")
    post_id = str(obj.get("id") or "")
    if handle and post_id:
        return f"https://x.com/{handle}/status/{post_id}"
    return ""


def post_sort_key(obj: dict) -> int:
    try:
        return int(obj.get("created_timestamp") or 0)
    except Exception:
        return 0


def dedupe_by_id(items: list[dict]) -> list[dict]:
    """按推文 id 去重，保留首个。

    ``with_replies=true`` 时服务端会插入被回复的原帖，实测同一 id 会出现两次。
    """
    seen: set[str] = set()
    out: list[dict] = []
    for item in items:
        pid = str(item.get("id") or "")
        if not pid or pid in seen:
            continue
        seen.add(pid)
        out.append(item)
    return out


# ---------------------------------------------------------------- RSS 解析（纯函数）
#
# 这一节全部是纯函数：RSS 是**没有契约的第二数据源**——FxEmbed 自己都发生过 V1→V2
# 迁移，RSSHub / Nitter fork 的字段更是各写各的。因此解析必须分层防御：
#   · 命名空间一律按本地名匹配（rss/channel 与 atom/feed 同一条代码路径）
#   · 拿不到字段就跳过该字段，**绝不抛异常**（抛异常会被上层记成"网络失败"）
#   · 解析不出任何条目 → 返回 None，交给调用方判定"这个源不可用"，
#     绝不能退化成"没有新帖"（那就是永久漏推）


class _HtmlTextExtractor(HTMLParser):
    """把 RSS ``description`` 里的 HTML 片段拆成**本帖正文**与**引用帖**两部分。

    RSS 的 description 是**转义后的 HTML**，直接显示会带一堆 ``<br>`` ``<a>``。
    刻意不用 BeautifulSoup：requirements 里没有它，为一个纯文本还原多一个依赖不值得。

    ``<blockquote>`` 是 fxtwitter 用来承载"被引用 / 被回复的那条帖"的：实测真实 feed 里
    90 条中有 50 条带 blockquote。它**不属于本帖正文**，必须单独取出，否则
    （a）正文会掺进别人的话，（b）翻译会把别人的话一起译，
    （c）``post_text`` 里"📎 引用"的专位永远空着。
    """

    #: 这些标签的内容是"结构"而不是"正文"，必须丢掉
    SKIP_TAGS = {"script", "style", "head", "noscript"}
    #: 这些标签意味着一次换行
    BREAK_TAGS = {"br", "p", "div", "li", "tr", "h1", "h2", "h3", "h4", "h5", "blockquote"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._parts: list[str] = []
        self._quote_parts: list[str] = []
        self._skip = 0
        self._quote_depth = 0
        self._images: list[str] = []
        self._quote_links: list[str] = []
        #: 与 ``<a>`` 一一对应的栈：该锚点是否是"附件占位"
        self._anchor_placeholder: list[bool] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        name = str(tag or "").lower()
        if name in self.SKIP_TAGS:
            self._skip += 1
            return
        if name == "img":
            for key, value in attrs or []:
                if str(key).lower() != "src" or not value:
                    continue
                # 引用帖里的图属于被引用那条帖，不能算本帖媒体
                if not self._quote_depth:
                    self._images.append(str(value).strip())
        if name == "a":
            href = ""
            for key, value in attrs or []:
                if str(key).lower() == "href" and value:
                    href = str(value).strip()
            if self._quote_depth and href:
                self._quote_links.append(href)
            # fxtwitter 会给附件补一个纯占位锚文本（``<a href="…mp4">Media</a>``），
            # 真实 feed 里 20/90 条带这一行。它是附件标记不是正文，必须丢掉，
            # 否则会渲染成正文末尾一行孤零零的 "Media"、还会污染翻译输入。
            self._anchor_placeholder.append(_is_media_placeholder_anchor(href))
        if name == "blockquote":
            self._quote_depth += 1
        if name in self.BREAK_TAGS:
            self._parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        name = str(tag or "").lower()
        if name in self.SKIP_TAGS:
            self._skip = max(0, self._skip - 1)
            return
        if name == "a" and self._anchor_placeholder:
            self._anchor_placeholder.pop()
        if name == "blockquote":
            self._quote_depth = max(0, self._quote_depth - 1)
        if name in self.BREAK_TAGS:
            self._parts.append("\n")

    def handle_data(self, data) -> None:
        if self._skip or not data:
            return
        if self._anchor_placeholder and self._anchor_placeholder[-1]:
            if _normalize_text(data).lower() in MEDIA_PLACEHOLDER_TEXTS:
                return
        if self._quote_depth:
            self._quote_parts.append(data)
        else:
            self._parts.append(data)

    @property
    def text(self) -> str:
        return "".join(self._parts)

    @property
    def quote_text(self) -> str:
        return "".join(self._quote_parts)

    @property
    def images(self) -> list[str]:
        return list(self._images)

    @property
    def quote_links(self) -> list[str]:
        return list(self._quote_links)


def _normalize_text(text: str) -> str:
    text = re.sub(r"[ \t\r\f\v]+", " ", str(text or ""))
    text = re.sub(r"\n\s*\n\s*", "\n", text)
    return text.strip()


def _feed_html(raw) -> str:
    """RSS 的 description 是**转义后的 HTML**（``&lt;p&gt;…``），先还原成真标签。

    不还原的话标签会被当成正文原样显示；而还原后 ``&amp;lt;`` 这类嵌套实体
    只会退化成普通文本，不会凭空造出标签。
    """
    text = str(raw or "")
    if not text:
        return ""
    try:
        return _html.unescape(text)
    except Exception:
        return text


def _strip_html_text(raw) -> str:
    """HTML 片段 → 压好空白的**本帖正文**（不含引用帖）。任何异常都退化为"原样返回"。"""
    text = _feed_html(raw)
    if not text:
        return ""
    try:
        parser = _HtmlTextExtractor()
        parser.feed(text)
        parser.close()
        text = parser.text
    except Exception:
        pass
    return _normalize_text(text)


def _extract_img_urls(raw) -> list[str]:
    """从 HTML 片段里抠出**本帖**的 ``<img src>``（引用帖里的不算），保序去重。"""
    text = _feed_html(raw)
    if not text:
        return []
    try:
        parser = _HtmlTextExtractor()
        parser.feed(text)
        parser.close()
        found = parser.images
    except Exception:
        found = []
    out: list[str] = []
    for url in found:
        if url and url not in out:
            out.append(url)
    return out


def _split_post_body(raw) -> tuple[str, dict | None]:
    """HTML 片段 → ``(本帖正文, 引用帖对象或 None)``。

    引用帖对象尽量做成 FxTwitter 的 ``quote`` 形状（``type`` / ``text`` / ``url`` /
    ``author.screen_name``），这样 ``post_text`` 的"📎 引用 @某人"与
    ``classify_post`` 的 ``quote`` 判定都能直接复用，不必为 RSS 另写一套渲染。

    **已知不精确**：RSS 无法区分"引用"与"回复"（两者都渲染成 blockquote，实测样本里
    没有任何 "Replying to" 之类标记）。这里统一按"引用"处理——正文干净、引用内容有归属，
    代价是回复帖在 L2 上不会被 ``filter_replies`` 拦下。
    """
    text = _feed_html(raw)
    if not text:
        return "", None
    try:
        parser = _HtmlTextExtractor()
        parser.feed(text)
        parser.close()
        main = _normalize_text(parser.text)
        quote_text = _normalize_text(parser.quote_text)
        quote_links = [url for url in parser.quote_links if url]
    except Exception:
        return _strip_html_text(raw), None
    if not quote_text and not quote_links:
        return main, None
    url = quote_links[0] if quote_links else ""
    handle = _handle_from_url(url)
    return main, {
        "type": "quote",
        "text": quote_text,
        "url": url,
        "author": {"id": handle, "screen_name": handle, "name": "", "avatar_url": ""},
    }


#: X 的**真实路由段**：出现在 ``/{h}/status/`` 的作者位上时一定不是账号名。
#: 这份表要**够宽**（否则会发明出不存在的作者、把帖子误判成转推而静默丢掉），
#: 但也要**只放 X 自己用的保留路径**——``rss``/``user``/``lists``/``feed`` 这些词
#: 是可能被真实账号注册的，它们在作者位上就该被当成账号名。
ROUTE_ONLY_SEGMENTS = {
    "i", "web", "home", "search", "explore", "intent", "settings", "messages",
    "notifications", "compose", "logout", "login", "signup", "about", "tos", "privacy",
}
#: 只在"从链接反推 channel handle"时才排除的保留词（含 feed/rss 这类**路径**词）：
#: ``nitter.example/rss`` 这种 URL 的末段是路径不是账号，必须排除。
RESERVED_PATH_SEGMENTS = {
    "i", "web", "home", "search", "explore", "intent", "status", "statuses",
    "rss", "feed", "feeds", "atom", "xml", "user", "users", "hashtag", "lists",
}
#: XML 自己就认的 5 个预定义实体。修复器**必须原样保留**它们：
#: 把 ``&amp;`` 再转义成 ``&amp;amp;`` 会让属性值里的 URL 变成字面 ``&amp;``。
XML_PREDEFINED_ENTITIES = {"amp", "lt", "gt", "quot", "apos"}
#: 常见 HTML 实体（**不是** XML 实体）。XML 解析器遇到 ``&nbsp;`` 会直接报错，
#: 而 RSSHub / Nitter fork 这类第三方源大量输出 HTML 实体，必须容错。
HTML_NAMED_ENTITIES = {
    "nbsp": " ", "copy": "©", "reg": "®", "trade": "™", "hellip": "…",
    "mdash": "—", "ndash": "–", "lsquo": "‘", "rsquo": "’",
    "ldquo": "“", "rdquo": "”", "middot": "·", "bull": "•",
    "laquo": "«", "raquo": "»", "deg": "°", "times": "×", "divide": "÷",
    "euro": "€", "pound": "£", "yen": "¥", "sect": "§", "para": "¶",
    "dagger": "†", "prime": "′", "Prime": "″", "minus": "−", "plusmn": "±",
}
#: 常见但 RSS 里可能没声明的前缀 → 命名空间。补齐它比整份 feed 判死划算得多。
COMMON_XML_NAMESPACES = {
    "media": "http://search.yahoo.com/mrss/",
    "dc": "http://purl.org/dc/elements/1.1/",
    "content": "http://purl.org/rss/1.0/modules/content/",
    "atom": "http://www.w3.org/2005/Atom",
    "sy": "http://purl.org/rss/1.0/modules/syndication/",
    "slash": "http://purl.org/rss/1.0/modules/slash/",
    "wfw": "http://wellformedweb.org/CommentAPI/",
}


def _is_link_card_image(url: str) -> bool:
    """twimg 的 ``/card_img/`` 是链接卡片预览图 → 等同 L1 的 ``media.external``，不算自带媒体。"""
    text = str(url or "").lower()
    return "twimg.com" in text and "/card_img/" in text


def _is_media_placeholder_anchor(href: str) -> bool:
    """``<a href="https://video.twimg.com/…mp4">Media</a>`` 这类附件占位锚点。

    判定规则（**按主机分流**，比"主机子串 + 路径白名单"更稳）：
      · 主机必须是 twimg（精确匹配，``example.com/twimg.com/x`` 不算）
      · ``video.twimg.com``：任何路径都算（``/amplify_video/``、``/ext_tw_video/``、
        ``/tweet_video/``、``/vid/`` 都是视频直链——早期只认前三个，
        真实 feed 一旦换成 ``ext_tw_video`` 就会漏删并让 "Media" 泄漏回正文）
      · ``pbs.twimg.com``：只认 ``/media/``（真正的图片附件）；
        ``/card_img/``（链接卡片）、``/profile_images/``、``*_thumb/`` 都不算

    **已知不可区分**：用户自己发正文只有 "Media" 且链接指向 ``pbs/media/`` 时，
    与 fxtwitter 的附件占位在 HTML 层完全同形，仍会被剔除（随后回落到 ``<title>``）。
    """
    text = str(href or "").strip()
    if not text.lower().startswith(("http://", "https://")):
        return False
    try:
        parts = urlsplit(text)
    except Exception:
        return False
    host = (parts.hostname or "").lower()
    if host == "video.twimg.com":
        return True
    if host == "pbs.twimg.com":
        return "/media/" in (parts.path or "").lower()
    return False


def _guess_lang(text: str) -> str:
    """RSS 没有 ``lang`` 字段，用一个廉价启发式补上。

    L1（FxTwitter API）每条都带 ``lang``，``skip_langs`` 因此有效；L2 的 ``lang``
    若恒为空，同一个订阅在换源后"省 LLM 调用"的开关会静默反转（实测真实 feed
    90/90 条 lang 为空）。这里只做**粗粒度**判断，明确比"什么都不填"好：

      · 有假名 → ja；有谚文 → ko；汉字为主且明显多于拉丁 → zh；其余以拉丁字母为主 → en
      · 判不出来就返回空串，交给下游的"中文占比"启发式，不硬猜

    **先剔除 URL / @提及 / #标签再统计**：短中文帖后面挂一条 ``https://t.co/…``
    会被算成"拉丁为主"→ 判成 en，于是 ``skip_langs=["en"]`` 的用户会漏译中文帖。

    **判据不要加"补偿常量"**：曾经写成 ``han*4 >= latin+8`` 来"稳一点"，结果把误判
    镜像地造了回来——``联系我 foo@bar.com``（3 汉字 + 9 拉丁）直接变成 ``en``。
    现在用 ``han*4 >= max(1, latin)``：中文占比只要不落后太多就算中文。

    **已知不精确**：全汉字、无假名的日文（如新闻标题）会被判成 zh —— 这与中文在
    字符层面不可区分，不打算为它引入词典。
    """
    text = str(text or "")
    if not text:
        return ""
    stripped = re.sub(r"https?://\S+", " ", text)
    stripped = re.sub(r"(?<![A-Za-z0-9_])@[A-Za-z0-9_]{1,15}", " ", stripped)
    stripped = re.sub(r"#[^\s#]+", " ", stripped)
    hiragana_katakana = len(re.findall(r"[\u3040-\u30ff]", stripped))
    hangul = len(re.findall(r"[\uac00-\ud7af]", stripped))
    han = len(re.findall(r"[\u4e00-\u9fff]", stripped))
    latin = len(re.findall(r"[A-Za-z]", stripped))
    if hiragana_katakana:
        return "ja"
    if hangul:
        return "ko"
    if han and han * 4 >= max(1, latin):
        return "zh"
    if latin >= 8 and latin * 3 >= max(1, han):
        return "en"
    return ""


def repair_xml_text(text: str) -> str:
    """把"几乎合法"的 feed 修成合法 XML。**只在严格解析失败后才调用。**

    第三方 RSS（RSSHub / Nitter fork）常见三类瑕疵，每一种都会让
    ``ElementTree`` 直接抛异常 → 整份 feed 被判成"该源不可用" → 水位停摆：

    1. HTML 命名实体（``&nbsp;`` ``&rsquo;`` …）——XML 只认 5 个预定义实体
    2. 裸 ``&``（URL 里的查询串最常见）
    3. 用了 ``media:`` / ``dc:`` 前缀却没声明 ``xmlns:``

    CDATA 段落**原样保留**：里面的内容本来就该被当成字面文本，不该被我们改写。
    """
    parts = re.split(r"(<!\[CDATA\[.*?\]\]>)", str(text or ""), flags=re.S)

    def fix(segment: str) -> str:
        # 1) 已知 HTML 实体 → 字面字符；未知命名实体 → 转义，避免解析器报错
        def entity(match: re.Match) -> str:
            name = match.group(1)
            if name in XML_PREDEFINED_ENTITIES:
                # XML 本来就认这 5 个：原样保留。
                # 二次转义会把 `&amp;` 变成字面 `&amp;`，属性值（图片/视频直链）会因此坏掉。
                return match.group(0)
            if name in HTML_NAMED_ENTITIES:
                return HTML_NAMED_ENTITIES[name]
            return f"&amp;{name};"

        segment = re.sub(r"&([A-Za-z][A-Za-z0-9]{1,31});", entity, segment)
        # 2) 裸 &（后面不是实体/数字引用）→ 转义
        return re.sub(
            r"&(?!(?:[A-Za-z][A-Za-z0-9]*|#[0-9]+|#x[0-9a-fA-F]+);)", "&amp;", segment
        )

    for index, part in enumerate(parts):
        if part.startswith("<![CDATA["):
            continue
        parts[index] = fix(part)
    repaired = "".join(parts)

    # 3) 补齐未声明但常用的命名空间前缀（注入到根元素上，不动内容）
    declared = {name.lower() for name in re.findall(r"xmlns:([A-Za-z_][\w.-]*)\s*=", repaired)}
    used = {name.lower() for name in re.findall(r"</?([A-Za-z_][\w.-]*):", repaired)}
    missing = [
        name for name in sorted(used)
        if name not in declared and name in COMMON_XML_NAMESPACES
    ]
    if missing:
        attrs = "".join(f' xmlns:{name}="{COMMON_XML_NAMESPACES[name]}"' for name in missing)
        repaired = re.sub(
            r"<(?!\?|!)([A-Za-z_][\w.-]*)", lambda m: f"<{m.group(1)}{attrs}", repaired, count=1
        )
    return repaired


def _xml_local(tag) -> str:
    """``{http://purl.org/dc/elements/1.1/}creator`` → ``creator``。"""
    text = str(tag or "")
    return text.rsplit("}", 1)[-1].lower() if "}" in text else text.lower()
def _xml_children(node) -> list:
    return list(node) if node is not None else []


def _xml_creator_handle(item) -> str:
    """从 ``dc:creator`` / ``author`` 里取 ``@handle``（RSSHub 常填此项）。"""
    for child in _xml_children(item):
        if _xml_local(child.tag) not in ("creator", "author"):
            continue
        raw = str(child.text or "").strip()
        if not raw:
            continue
        if "@" in raw:
            candidate = raw.rsplit("@", 1)[-1].strip()
        else:
            candidate = raw.lstrip("@").strip()
        if re.fullmatch(r"[A-Za-z0-9_]{1,15}", candidate):
            return candidate
    return ""


def _rss_post_id(raw: str) -> str:
    """从 guid/link 里抠出数字推文 id。

    优先用纯数字（与 FxTwitter 的 ``id`` 同口径，便于两源之间去重）；否则退化成
    内容哈希——**哈希必须包含来源字符串**，不能只按 text 算，否则同一账号两条
    文本相同的推文会被当成一条。

    **空来源返回空串**（由调用方丢弃该条）：对空串做哈希会得到常量
    ``e3b0c442…``，于是所有缺 link/guid 的条目会塌成同一条，静默丢掉 N-1 条。
    """
    text = str(raw or "").strip()
    if not text:
        return ""
    for match in re.finditer(r"/status(?:es)?/(\d+)", text):
        return match.group(1)
    for match in re.finditer(r"(\d{15,25})", text):
        return match.group(1)
    return hashlib.sha256(text.encode("utf-8", "replace")).hexdigest()[:20]


def _handle_from_url(url: str) -> str:
    """从任何 X/Nitter/RSSHub 链接里取 handle；取不到时返回 ""。

    实测要点：fxtwitter 的 channel ``<link>`` 是 ``https://fxtwitter.com/{handle}``
    （**没有** /feed 后缀），而条目 ``<link>`` 是 ``https://x.com/{handle}/status/{id}``。

    **两处排除规则必须分开**（这是 v1.3.1 修出来的新 bug）：
      · ``/{h}/status/`` 的作者位：那里只有 ``i`` / ``web`` 是真实路由段。
        ``@rss``、``@user``、``@lists`` 这些 handle 是合法存在的，排除它们会把作者判成空，
        于是作者回落到频道主人、转推被误判成原创、``filter_reposts`` 失效。
      · 从 channel link 反推（``/rss``、``/feed.xml``、只有一段路径）：那里才需要完整的
        保留词表，否则 ``nitter.example/rss`` 会反推出 ``rss``。
    """
    text = str(url or "")
    for pattern, excluded in (
        (r"/([A-Za-z0-9_]{1,15})/status(?:es)?/", ROUTE_ONLY_SEGMENTS),
        (r"/(?:twitter|nitter[^/]*)/user/([A-Za-z0-9_]{1,15})", frozenset()),
        (r"/([A-Za-z0-9_]{1,15})/feed(?:\.media)?\.xml", frozenset()),
        (r"/([A-Za-z0-9_]{1,15})/rss", frozenset()),
        # channel link：**整条 URL 只有一段路径**（如 https://fxtwitter.com/elonmusk）。
        # 必须锚定整串，否则会去匹配路径末尾的数字段（x.com/i/status/123 → "123"）。
        (r"^https?://[^/]+/([A-Za-z0-9_]{1,15})/?$", RESERVED_PATH_SEGMENTS),
    ):
        for match in re.finditer(pattern, text, re.IGNORECASE):
            candidate = match.group(1)
            if candidate.lower() not in excluded:
                return candidate
    return ""


def _rss_timestamp(entry: dict, post_id: str = "") -> int:
    """从各种日期字段里取秒级时间戳；**取不到返回 0**（表示"没有日期"）。

    RSS 只有秒级精度，可能与 api 源的 ``created_timestamp`` 差 1 秒，
    因此水位比较用的是严格 ``>`` 之外的**包含**判定。

    刻意**不**回退到 ``time.time()``：那会让无日期条目的时间戳变成"抓取时刻"，
    把 ``newest_any`` 抬到 now、水位随之前移，之后所有真实时间早于 now 的帖子
    都会被 ``min_ts`` 过滤掉——这是永久漏推的经典成因。由 ``parse_feed_xml``
    统一决定无日期条目怎么处理。
    """
    for key in ("published_parsed", "updated_parsed", "created_parsed", "_xmon_ts"):
        value = entry.get(key)
        if isinstance(value, (tuple, list)) and len(value) >= 6:
            try:
                return int(
                    datetime(*[int(part) for part in value[:6]], tzinfo=timezone.utc).timestamp()
                )
            except Exception:
                pass
        elif isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0:
            return int(value)
    for key in ("published", "updated", "pubDate", "date", "created", "lastBuildDate"):
        raw = str(entry.get(key) or "").strip()
        if not raw:
            continue
        try:
            moment = parsedate_to_datetime(raw)
            if moment is not None:
                if moment.tzinfo is None:
                    moment = moment.replace(tzinfo=timezone.utc)
                return int(moment.timestamp())
        except Exception:
            pass
        try:
            iso = raw.replace("Z", "+00:00")
            moment = datetime.fromisoformat(iso)
            if moment.tzinfo is None:
                moment = moment.replace(tzinfo=timezone.utc)
            return int(moment.timestamp())
        except Exception:
            pass
        if raw.isdigit():
            try:
                return int(raw)
            except Exception:
                pass
    return 0


def _entry_to_status(entry: dict, now_ts: int = 0, fallback_author: dict | None = None) -> dict | None:
    """一条 RSS/Atom 条目 → FxTwitter 形状的 status 对象（``to_post_record`` 直接吃）。

    真实 feed 的三个关键事实（都是拿 ``fxtwitter.com/{handle}/feed.xml`` 实测出来的）：

    1. 它是**含转推的时间线**：90 条里 54 条的 ``<link>`` 指向别人。因此作者必须取自
       ``<link>`` 而不是 channel——否则会（a）给转推生成指向频道账号的**错误链接**，
       （b）让 ``filter_reposts`` 一条都拦不住（实测 90/90 被判成 original）。
    2. `<enclosure type="video/mp4">` 是视频帖的**唯一**媒体信号：90 条里 19 条是视频，
       不映射成 media 的话"仅媒体"订阅会静默丢掉它们。
    3. `<blockquote>` 装的是被引用/被回复的那条帖（90 条里 50 条），不属于本帖正文。
    """
    if not isinstance(entry, dict):
        return None
    link = str(entry.get("link") or "").strip()
    guid = str(entry.get("guid") or entry.get("id") or "").strip()
    post_id = _rss_post_id(guid or link)
    if not post_id:
        return None
    raw_body = entry.get("summary") or entry.get("description") or entry.get("content") or ""
    if isinstance(raw_body, list):  # 少数 feed 把 content 做成列表
        raw_body = " ".join(str(part) for part in raw_body)
    raw_body = str(raw_body or "")
    text, quote = _split_post_body(raw_body)

    images: list[str] = []
    for url in _extract_img_urls(raw_body):
        if url and url not in images:
            images.append(url)
    videos: list[str] = []
    for att in entry.get("enclosures") or []:
        if not isinstance(att, dict):
            continue
        href = str(att.get("href") or "").strip()
        mime = str(att.get("type") or "").lower()
        if not href:
            continue
        if _is_link_card_image(href) and not mime.startswith("video"):
            # twimg 的 /card_img/ 是**链接卡片预览图**，不是帖子自带的媒体。
            # L1 把它归在 media.external（不计入 has_media），L2 必须同口径，
            # 否则"仅媒体"订阅在换源后会把纯链接帖推出去。
            continue
        if mime.startswith("image") or (not mime and not href.lower().endswith((".mp4", ".m3u8"))):
            if href not in images:
                images.append(href)
        elif mime.startswith("video") or href.lower().endswith(".mp4"):
            # 只认 mp4：m3u8 交给协议端必然播不了（与 L1 的 pick_video_url 同口径）
            if not href.lower().endswith(".m3u8") and href not in videos:
                videos.append(href)
    images = images[:4]  # X 单帖实测最多 4 张，多出来的必然是模板噪声
    videos = videos[:1]  # X 单帖最多 1 个视频

    # 没有可用日期就不入库：parse_feed_xml 已经给"部分条目缺日期"补过 feed 级回退，
    # 走到这里还是 0 说明这份 feed 的时间语义不可用。
    created = _rss_timestamp(entry)
    if created <= 0:
        return None
    if now_ts and created > now_ts + 86400:  # 明显不合理的未来时间：当抓取时刻处理
        created = now_ts
    handle = str(entry.get("_xmon_handle") or "").strip()
    channel_handle = str((fallback_author or {}).get("screen_name") or "").strip()
    repost_of = str(entry.get("_xmon_repost_of") or "").strip()
    title = _strip_html_text(entry.get("title"))
    is_status = True
    if not text and not images and not videos:
        # 纯文本为空的条目：只有在"像推文"时才当帖子
        looks_like_status = bool(re.search(r"/status(?:es)?/", f"{guid} {link}"))
        if not looks_like_status and (not title or title.lower() in ("untitled", "(no title)")):
            is_status = False
        text = title

    media_items: list[dict] = [{"type": "photo", "url": url} for url in images]
    for url in videos:
        media_items.append(
            {
                "type": "video",
                "url": url,
                "thumbnail_url": str(entry.get("thumbnail") or ""),
            }
        )
    media: dict = {"all": media_items} if media_items else {}

    fallback = fallback_author if isinstance(fallback_author, dict) else {}
    author_handle = handle or channel_handle
    # 显示名只有在"这条帖确实是频道本人发的"时才敢用频道名：
    # 转推的显示名 RSS 里没有，硬填频道名会把人名张冠李戴（宁可用 handle）。
    is_channel_post = bool(author_handle) and author_handle.lower() == channel_handle.lower()
    author = {
        "id": author_handle or str(fallback.get("id") or post_id),
        "screen_name": author_handle,
        "name": str(fallback.get("name") or "") if is_channel_post else "",
        "avatar_url": str(fallback.get("avatar_url") or ""),
    }
    obj = {
        "type": "status" if is_status else "entry",
        "id": post_id,
        "url": link or (f"https://x.com/{author_handle}/status/{post_id}" if author_handle else ""),
        "text": text,
        "created_timestamp": created,
        # RSS 没有 lang 字段，用启发式补：否则 skip_langs 在 L2 上恒不生效
        "lang": _guess_lang(text),
        "author": author,
        "media": media,
    }
    if repost_of:
        obj["reposted_by"] = {
            "id": repost_of,
            "screen_name": repost_of,
            "name": "",
            "avatar_url": "",
        }
    if quote:
        obj["quote"] = quote
    return obj


def _rss_author(channel: dict, handle: str) -> dict:
    """从 feed 的 channel/feed 元数据拼一个**尽力而为**的作者对象。

    RSS 里没有 ``avatar_url``、``author.id`` 这类字段，取不到就留空——
    调用方 ``upsert_author`` 本来就是"字段缺了不覆盖"的语义。

    真实 fxtwitter feed 的 channel ``<title>`` 是 ``Elon Musk (@elonmusk)`` ——
    名字里**已经带了 handle**，直接用会和 ``post_text`` 追加的 ``(@handle)``
    拼成 ``Elon Musk (@elonmusk) (@elonmusk)``。这里把尾部的 ``(@handle)`` 剥掉。
    """
    channel = channel if isinstance(channel, dict) else {}
    channel_author = channel.get("author")
    name = ""
    if isinstance(channel_author, dict):
        name = str(channel_author.get("name") or "").strip()
    if not name:
        name = str(channel.get("name") or "").strip()
    if not name:
        name = str(channel.get("title") or "").strip()
    # **调用方给的 handle 最可信**（它就是订阅的目标账号）：从 channel link 反推
    # 只是兜底，因为 `/elonmusk/rss` 这类 URL 会反推出 "rss"。这个值会被
    # upsert_author 无条件写进 authors.handle，猜错等于把整条订阅改成抓错账号。
    screen = str(handle or "").strip().lstrip("@") or _handle_from_url(str(channel.get("link") or ""))
    if name and screen:
        name = re.sub(
            r"\s*[（(]\s*@?" + re.escape(screen) + r"\s*[)）]\s*$", "", name, flags=re.IGNORECASE
        ).strip()
    return {
        "id": screen or str(handle or ""),
        "screen_name": screen,
        "name": name,
        "avatar_url": "",
    }


def parse_feed_xml(
    content, min_ts: int = 0, handle: str = ""
) -> tuple[list[dict], dict] | None:
    """RSS / Atom 字节 → ``(status 列表, 作者对象)``。

    返回 ``None`` 表示"这根本不是一个能用的 feed"（HTML 验证页、空 body、
    结构不认识、没有可用的 id、一条日期都解析不出来）——**调用方必须把它当成
    源不可用**，而不是"没有新帖"。返回 ``([], author)`` 表示 feed 正常但没有
    新内容，两者语义完全不同。

    ``min_ts`` 的边界是**排他**的（``created_timestamp > min_ts`` 才是新帖）。
    曾经考虑放宽成"包含"，以便接住"与水位同一秒的另一条帖"，但那样会破坏
    "首次订阅不补发历史"：基线轮会把最新那条的时间戳写进水位，而基线轮**不投递**，
    于是下一轮会把它当新帖推出去。权衡结果：保住确定性的不补发，接受
    "同秒漏帖"这个**实测为 0/90** 的残余风险（真实 feed 的时间戳互不相同）。
    """
    if isinstance(content, bytes):
        if content[:3] == b"\xef\xbb\xbf":
            content = content[3:]
        text = content.decode("utf-8", "replace")
    else:
        text = str(content or "")
    text = text.strip()
    if not text:
        return None
    root = None
    try:
        root = ET.fromstring(text)
    except Exception:
        # 第三方 RSS 常见瑕疵（HTML 实体 / 裸 & / 未声明前缀）不该让整份 feed 判死
        try:
            root = ET.fromstring(repair_xml_text(text))
        except Exception:
            return None
    if _xml_local(root.tag) not in ("rss", "feed", "rdf"):
        return None

    channel = root.find("channel")
    if channel is None:
        for child in _xml_children(root):
            if _xml_local(child.tag) in ("channel", "feed"):
                channel = child
                break
    if channel is None:
        # 少数 feed 把条目直接挂在根下；只要能看到 entry/item 就不算失败
        if not [c for c in _xml_children(root) if _xml_local(c.tag) in ("item", "entry")]:
            return None
        channel = root

    channel_title = _strip_html_text(channel.findtext("title")) if channel is not None else ""
    channel_link = (channel.findtext("link") or "").strip() if channel is not None else ""
    channel_data = {"title": channel_title, "link": channel_link, "author": None}
    for child in _xml_children(channel):
        if _xml_local(child.tag) == "author":
            name = _strip_html_text(child.findtext("name")) or _strip_html_text(child.text)
            if name:
                channel_data["author"] = {"name": name}
    resolved_handle = str(handle or "").strip().lstrip("@") or _handle_from_url(channel_link)
    author = _rss_author(channel_data, resolved_handle)

    now_ts = int(time.time())
    raw_entries: list[dict] = []
    for node in _xml_children(channel):
        if _xml_local(node.tag) not in ("item", "entry"):
            continue
        entry: dict = {}
        for child in _xml_children(node):
            local = _xml_local(child.tag)
            if local == "link":
                entry["link"] = str(child.get("href") or child.text or "").strip()
            elif local in ("title", "description", "summary", "content", "guid", "id", "pubdate", "published", "updated"):
                key = {"pubdate": "pubDate"}.get(local, local)
                if local in ("description", "summary", "content") and entry.get(key):
                    entry[key] = f"{entry[key]} {child.text or ''}".strip()
                elif not entry.get(key):
                    entry[key] = child.text or ""
            elif local == "enclosure":
                entry.setdefault("enclosures", []).append(
                    {"href": child.get("url") or "", "type": child.get("type") or ""}
                )
            elif local == "thumbnail":
                # <media:thumbnail url="..."> —— 视频帖的封面，L1 在 media.all 里也有这个字段
                if not entry.get("thumbnail"):
                    entry["thumbnail"] = str(child.get("url") or "").strip()
        # 作者取自**条目自己的链接**（结构字段）：该 feed 是含转推的时间线，
        # channel 只是频道主人。条目 handle 与频道 handle 不一致 ⇒ 这是一条转推
        # （RSS 没有 reposted_by 字段，这是唯一可靠信号），据此补出 reposted_by
        # 让 filter_reposts 继续生效。dc:creator 只做兜底——各实现的语义不一致
        # （有的填原作者、有的填频道主），不足以推翻 <link>。
        item_handle = (
            _handle_from_url(entry.get("link") or "")
            or _xml_creator_handle(node)
            or resolved_handle
        )
        entry["_xmon_handle"] = item_handle
        if item_handle and resolved_handle and item_handle.lower() != resolved_handle.lower():
            entry["_xmon_repost_of"] = resolved_handle
        entry["_xmon_ts"] = _rss_timestamp(entry)
        raw_entries.append(entry)

    if not raw_entries:
        return [], author

    dated = [int(e["_xmon_ts"]) for e in raw_entries if int(e.get("_xmon_ts") or 0) > 0]
    if not dated:
        # 一条日期都解析不出来：无法定义增量水位，硬用"抓取时刻"会把水位抬到 now,
        # 之后所有真实时间早于 now 的帖子都会被永久过滤。判定为"该源不可用"。
        logger.warning("[x_monitor] feed 里没有任何可用日期，无法建立增量水位，判定该源不可用")
        return None
    fallback_ts = max(dated)

    posts: list[dict] = []
    converted = 0
    for entry in raw_entries:
        if int(entry.get("_xmon_ts") or 0) <= 0:
            # 个别条目缺日期：用 feed 内最新时间兜底，绝不跳到 now
            entry["_xmon_ts"] = fallback_ts
        status = _entry_to_status(entry, now_ts, author)
        if status is None:
            continue
        converted += 1
        if min_ts and int(status.get("created_timestamp") or 0) <= int(min_ts):
            continue
        posts.append(status)
    if raw_entries and converted == 0:
        # 有条目却一条都转换不出来（例如全部缺 link/guid）：这不是"没有新帖"
        logger.warning("[x_monitor] feed 有条目但全部缺少可用 id，判定该源不可用")
        return None
    return posts, author


# ---------------------------------------------------------------- 数据结构


@dataclass
class FetchResult:
    """一次抓取的结果。

    ``ok=False`` 表示**网络 / 解析失败**，调用方绝不能把它当成"没有新帖"，
    否则会把失败写进去重水位，造成永久漏推。

    ``provider`` 与 ``source`` 是同一件事的两个名字：``source`` 是存量字段
    （测试与日志都在用），``provider`` 是 P5 引入的显式名字，新代码统一读它。
    """

    ok: bool = False
    no_new: bool = False
    not_found: bool = False
    author: dict | None = None
    posts: list[dict] = field(default_factory=list)
    error: str = ""
    source: str = ""
    provider: str = ""

    def __post_init__(self) -> None:
        if not self.provider:
            self.provider = self.source
        elif not self.source:
            self.source = self.provider


# ---------------------------------------------------------------- 存储层


class Store:
    """SQLite 存储：authors / edges / posts / deliveries / groups / meta 六张表。

    四条不变量（有测试守着）：
      1. ``UNIQUE(edge_id, post_id)``  —— 结构上杜绝同一会话重复推送
      2. ``edges.enqueued_ts`` 只增不减 —— 单调水位
      3. 抓取失败**不写** ``authors.last_seen_ts``、**不置** ``baseline_ready``
      4. 首次订阅建立基线，不补发历史
    """

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS authors (
        author_id      TEXT PRIMARY KEY,
        handle         TEXT NOT NULL,
        display_name   TEXT NOT NULL DEFAULT '',
        avatar_url     TEXT NOT NULL DEFAULT '',
        last_seen_ts   INTEGER NOT NULL DEFAULT 0,
        baseline_ready INTEGER NOT NULL DEFAULT 0,
        updated_at     INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS edges (
        edge_id      INTEGER PRIMARY KEY AUTOINCREMENT,
        umo          TEXT NOT NULL,
        group_id     TEXT NOT NULL DEFAULT '',
        author_id    TEXT NOT NULL,
        enabled      INTEGER NOT NULL DEFAULT 1,
        nickname     TEXT NOT NULL DEFAULT '',
        media_only   INTEGER NOT NULL DEFAULT 0,
        enqueued_ts  INTEGER NOT NULL DEFAULT 0,
        created_at   INTEGER NOT NULL DEFAULT 0,
        UNIQUE(umo, author_id)
    );
    CREATE INDEX IF NOT EXISTS idx_edges_author ON edges(author_id);
    CREATE TABLE IF NOT EXISTS posts (
        post_id       TEXT PRIMARY KEY,
        author_id     TEXT NOT NULL,
        created_ts    INTEGER NOT NULL,
        kind          TEXT NOT NULL DEFAULT 'original',
        body_json     TEXT NOT NULL,
        media_json    TEXT NOT NULL DEFAULT '[]',
        has_media     INTEGER NOT NULL DEFAULT 0,
        translated    TEXT NOT NULL DEFAULT '',
        translate_lang TEXT NOT NULL DEFAULT '',
        fetched_at    INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_posts_author ON posts(author_id, created_ts);
    CREATE TABLE IF NOT EXISTS deliveries (
        delivery_id INTEGER PRIMARY KEY AUTOINCREMENT,
        edge_id     INTEGER NOT NULL,
        post_id     TEXT NOT NULL,
        state       TEXT NOT NULL DEFAULT 'QUEUED',
        attempts    INTEGER NOT NULL DEFAULT 0,
        queued_at   INTEGER NOT NULL DEFAULT 0,
        sent_at     INTEGER,
        last_error  TEXT NOT NULL DEFAULT '',
        UNIQUE(edge_id, post_id)
    );
    CREATE INDEX IF NOT EXISTS idx_deliveries_state ON deliveries(state, queued_at);
    CREATE TABLE IF NOT EXISTS meta (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL DEFAULT ''
    );
    CREATE TABLE IF NOT EXISTS groups (
        umo         TEXT PRIMARY KEY,
        dnd_enabled INTEGER NOT NULL DEFAULT 0,
        dnd_ranges  TEXT NOT NULL DEFAULT '',
        updated_at  INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS known_groups (
        umo       TEXT PRIMARY KEY,
        last_seen INTEGER NOT NULL DEFAULT 0
    );
    """

    #: 给老库补列用：(表, 列, 声明)。新装库由 SCHEMA 直接建好，这里是升级路径。
    MIGRATIONS = (
        ("edges", "media_only", "INTEGER NOT NULL DEFAULT 0"),
        ("posts", "has_media", "INTEGER NOT NULL DEFAULT 0"),
    )

    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as conn:
            conn.executescript(self.SCHEMA)
            self._migrate(conn)

    def _migrate(self, conn: sqlite3.Connection) -> None:
        """按 ``PRAGMA table_info`` 探测缺列并 ``ALTER TABLE ADD COLUMN``。

        插件更新会替换插件目录，但**数据目录不会**；所以每次升级都必须能就地补列，
        否则已有订阅数据的老库升级后会直接报 "no such column"。

        **单列失败不能拖垮整个插件**：每列独立 try/except，失败项汇总告警一次，
        插件照常加载（不依赖该列的功能仍然可用），而不是让 AstrBot 报"插件加载失败"。
        """
        failed: list[str] = []
        for table, column, decl in self.MIGRATIONS:
            try:
                existing = {
                    str(r["name"]) for r in conn.execute(f"PRAGMA table_info({table})")
                }
                if column not in existing:
                    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")
                    logger.info(f"[x_monitor] 数据库迁移：{table}.{column} 已补齐")
            except Exception as exc:
                failed.append(f"{table}.{column}")
                logger.error(f"[x_monitor] 数据库迁移失败 {table}.{column}: {exc}")
        if failed:
            logger.error(
                "[x_monitor] 下列列补齐失败，依赖它们的查询会报 no such column；"
                f"请检查数据库文件是否可写或已损坏：{', '.join(failed)}"
            )

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    @contextmanager
    def _conn(self):
        """开连接 → 成功则 commit → 异常则 rollback → **无论如何 close**。

        注意 ``with sqlite3.connect(...)`` 只是事务上下文，**不会关闭连接**；
        在 Windows 上会实打实地占住文件句柄，所以这里显式收口。
        """
        conn = self.connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ------------------------------------------------------------ meta
    def get_meta(self, key: str, default: str = "") -> str:
        with self._conn() as conn:
            row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return str(row["value"]) if row else default

    def set_meta(self, key: str, value: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO meta(key, value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(value)),
            )

    # ------------------------------------------------------------ 群级设置
    def get_group(self, umo: str) -> sqlite3.Row | None:
        with self._conn() as conn:
            return conn.execute("SELECT * FROM groups WHERE umo=?", (umo,)).fetchone()

    def set_group_dnd(self, umo: str, enabled: bool, ranges_text: str) -> None:
        """群级免打扰。免打扰是**按群**的：群管理员只该改自己群的时段。"""
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO groups(umo, dnd_enabled, dnd_ranges, updated_at) "
                "VALUES(?,?,?,?) ON CONFLICT(umo) DO UPDATE SET "
                "dnd_enabled=excluded.dnd_enabled, dnd_ranges=excluded.dnd_ranges, "
                "updated_at=excluded.updated_at",
                (umo, 1 if enabled else 0, str(ranges_text or ""), int(time.time())),
            )

    def clear_group_dnd(self, umo: str) -> int:
        """**删除**该群的免打扰覆盖行，让它回落到全局默认。

        只把 ``dnd_enabled`` 置 0 会留下一条"显式关闭"的行，于是这个群**永久**
        不再继承全局默认值，而且没有任何路径能恢复 —— 删除才是"恢复默认"的正确语义。
        """
        with self._conn() as conn:
            return conn.execute("DELETE FROM groups WHERE umo=?", (umo,)).rowcount

    def touch_group(self, umo: str) -> None:
        """记录见过的会话，供 WebUI 的群列表使用。

        刻意写进**独立的 known_groups 表**而不是 groups 表：groups 表里的一行代表
        "这个群显式设置过免打扰"，若把"见过"也写进去，就会让该群不再回落到全局默认时段。
        """
        if not umo:
            return
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO known_groups(umo, last_seen) VALUES(?,?) "
                "ON CONFLICT(umo) DO UPDATE SET last_seen=excluded.last_seen",
                (umo, int(time.time())),
            )

    def known_groups(self) -> list[str]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT umo FROM known_groups ORDER BY last_seen DESC"
            ).fetchall()
        return [str(r["umo"]) for r in rows]

    # ------------------------------------------------------------ authors
    def upsert_author(
        self,
        author_id: str,
        handle: str,
        display_name: str = "",
        avatar_url: str = "",
    ) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO authors(author_id, handle, display_name, avatar_url, updated_at) "
                "VALUES(?,?,?,?,?) ON CONFLICT(author_id) DO UPDATE SET "
                "handle=excluded.handle, display_name=excluded.display_name, "
                "avatar_url=excluded.avatar_url, updated_at=excluded.updated_at",
                (author_id, handle, display_name, avatar_url, int(time.time())),
            )

    def get_author(self, author_id: str) -> sqlite3.Row | None:
        with self._conn() as conn:
            return conn.execute(
                "SELECT * FROM authors WHERE author_id=?", (author_id,)
            ).fetchone()

    def get_author_by_handle(self, handle: str) -> sqlite3.Row | None:
        """按 handle 查作者行。

        老数据里同一个 handle 可能有两行（v1.3.0 在 L1 不可用时用 handle 兜底建过一行，
        恢复后又建了数字 id 的一行）。没有 ORDER BY 的 ``fetchone()`` 返回哪一行取决于
        插入顺序，会让作者解析结果不稳定。这里明确**优先数字 id 那一行**
        （``author_id`` 与 handle 不同的那一行才是规范 id），其次取最近更新的，
        最后用 ``author_id`` 兜底做**确定性**并列仲裁——否则两条数字行会随轮询的
        ``updated_at`` 刷新而互相翻转。
        """
        with self._conn() as conn:
            return conn.execute(
                "SELECT * FROM authors WHERE lower(handle)=lower(?) "
                "ORDER BY (lower(author_id) = lower(handle)) ASC, updated_at DESC, author_id ASC "
                "LIMIT 1",
                (handle,),
            ).fetchone()

    def count_authors_by_handle(self, handle: str) -> int:
        """同一 handle 有几行（>1 说明库里有历史遗留的重复作者行）。"""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM authors WHERE lower(handle)=lower(?)", (handle,)
            ).fetchone()
        return int(row["n"]) if row is not None else 0

    def list_authors(self) -> list[sqlite3.Row]:
        with self._conn() as conn:
            return conn.execute(
                "SELECT * FROM authors WHERE baseline_ready=1 ORDER BY updated_at DESC"
            ).fetchall()

    def list_authors_for_web(self) -> list[sqlite3.Row]:
        """WebUI 用的账号列表：已建基线的 **∪** 任何有订阅的。

        刚通过 WebUI 添加的账号在首次轮询前 ``baseline_ready=0``，
        若只按基线过滤，页面上就会"加完看不见"，直到下一轮轮询才冒出来。
        """
        with self._conn() as conn:
            return conn.execute(
                "SELECT * FROM authors WHERE baseline_ready=1 "
                "OR author_id IN (SELECT author_id FROM edges) "
                "ORDER BY lower(handle)"
            ).fetchall()

    def mark_baseline(self, author_id: str, seen_ts: int) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE authors SET last_seen_ts=?, baseline_ready=1, updated_at=? "
                "WHERE author_id=?",
                (int(seen_ts), int(time.time()), author_id),
            )

    # ------------------------------------------------------------ edges
    def list_edges(self, umo: str | None = None) -> list[sqlite3.Row]:
        with self._conn() as conn:
            if umo is None:
                return conn.execute("SELECT * FROM edges ORDER BY edge_id").fetchall()
            return conn.execute(
                "SELECT * FROM edges WHERE umo=? ORDER BY edge_id", (umo,)
            ).fetchall()

    def get_edge(self, umo: str, author_id: str) -> sqlite3.Row | None:
        with self._conn() as conn:
            return conn.execute(
                "SELECT * FROM edges WHERE umo=? AND author_id=?", (umo, author_id)
            ).fetchone()

    def get_edge_by_id(self, edge_id: int) -> sqlite3.Row | None:
        with self._conn() as conn:
            return conn.execute(
                "SELECT * FROM edges WHERE edge_id=?", (int(edge_id),)
            ).fetchone()

    def all_group_overrides(self) -> list[str]:
        """显式设置过免打扰的群（groups 表）。"""
        with self._conn() as conn:
            rows = conn.execute("SELECT umo FROM groups ORDER BY updated_at DESC").fetchall()
        return [str(r["umo"]) for r in rows]

    def get_edge_by_handle(self, umo: str, handle: str) -> sqlite3.Row | None:
        with self._conn() as conn:
            return conn.execute(
                "SELECT e.* FROM edges e JOIN authors a ON a.author_id=e.author_id "
                "WHERE e.umo=? AND lower(a.handle)=lower(?)",
                (umo, handle.lstrip("@")),
            ).fetchone()

    def add_edge(
        self,
        umo: str,
        group_id: str,
        author_id: str,
        nickname: str,
        start_ts: int,
        media_only: bool = False,
    ) -> bool:
        """新增订阅。返回 True 表示新建，False 表示已存在。

        新建时把 ``enqueued_ts`` 设成作者当前的 ``last_seen_ts`` —— 这就是
        「首次订阅不补发历史」的实现点。

        **昵称在这里再规范化一次**：调用方（指令 / WebUI）本来就会规范化，但写库层是
        最后一道闸 —— 漏掉它，含孤立代理对的昵称会以 ``UnicodeEncodeError`` 的形式
        在 INSERT 处炸掉，而不是在校验处被挡下（独立验证 N-1：修了调用方不等于修了写库层）。
        """
        with self._conn() as conn:
            cur = conn.execute(
                "INSERT OR IGNORE INTO edges"
                "(umo, group_id, author_id, enabled, nickname, media_only, "
                " enqueued_ts, created_at) "
                "VALUES(?,?,?,1,?,?,?,?)",
                (
                    umo,
                    group_id,
                    author_id,
                    normalize_nick(nickname),
                    1 if media_only else 0,
                    int(start_ts),
                    int(time.time()),
                ),
            )
            return cur.rowcount > 0

    def set_edge_media_only(self, umo: str, author_id: str, media_only: bool) -> int:
        """按订阅设置"只推含多媒体的帖子"（默认关闭 = 拉全部原创帖）。

        刻意放在**订阅边**而不是全局配置：同一个账号可以对 A 群只发媒体、对 B 群发全部。
        """
        with self._conn() as conn:
            cur = conn.execute(
                "UPDATE edges SET media_only=? WHERE umo=? AND author_id=?",
                (1 if media_only else 0, umo, author_id),
            )
            return cur.rowcount

    def remove_edge(self, umo: str, author_id: str) -> int:
        """删除订阅，并**连带删掉它的投递记录**。

        不删 deliveries 会留下孤儿行，而 ``queue_depth(umo=None)`` 不 JOIN edges，
        于是"待投递队列"会永久虚高、免打扰积压上限也跟着失真 —— 而且再也清不掉。
        """
        with self._conn() as conn:
            edge_ids = [
                int(r["edge_id"])
                for r in conn.execute(
                    "SELECT edge_id FROM edges WHERE umo=? AND author_id=?", (umo, author_id)
                ).fetchall()
            ]
            if edge_ids:
                marks = ",".join("?" * len(edge_ids))
                conn.execute(f"DELETE FROM deliveries WHERE edge_id IN ({marks})", edge_ids)
            return conn.execute(
                "DELETE FROM edges WHERE umo=? AND author_id=?", (umo, author_id)
            ).rowcount

    def remove_author(self, author_id: str) -> dict:
        """删除账号及其全部订阅（连带该账号的投递记录与已收录推文）。

        WebUI 上"删账号"必须是这个语义：只删 edges 会留下孤儿 deliveries 与 posts。
        这里除了按当前 edge_id 清理，还额外按 ``posts.author_id`` 兜一次 ——
        历史版本（remove_edge 不清理投递）留下的孤儿也能被收走。
        """
        with self._conn() as conn:
            edge_ids = [
                int(r["edge_id"])
                for r in conn.execute(
                    "SELECT edge_id FROM edges WHERE author_id=?", (author_id,)
                ).fetchall()
            ]
            deliveries = 0
            if edge_ids:
                marks = ",".join("?" * len(edge_ids))
                deliveries += conn.execute(
                    f"DELETE FROM deliveries WHERE edge_id IN ({marks})", edge_ids
                ).rowcount
            # 兜底：清掉该账号所有帖子的投递记录（含已删订阅留下的孤儿）
            deliveries += conn.execute(
                "DELETE FROM deliveries WHERE post_id IN "
                "(SELECT post_id FROM posts WHERE author_id=?)",
                (author_id,),
            ).rowcount
            edges = conn.execute(
                "DELETE FROM edges WHERE author_id=?", (author_id,)
            ).rowcount
            posts = conn.execute(
                "DELETE FROM posts WHERE author_id=?", (author_id,)
            ).rowcount
            authors = conn.execute(
                "DELETE FROM authors WHERE author_id=?", (author_id,)
            ).rowcount
        return {
            "edges": int(edges),
            "posts": int(posts),
            "authors": int(authors),
            "deliveries": int(deliveries),
        }

    def update_edge(
        self,
        umo: str,
        author_id: str,
        nickname: str | None = None,
        enabled: bool | None = None,
        media_only: bool | None = None,
    ) -> int:
        """**一个事务**里更新订阅的可选字段，返回受影响行数。

        分三次 UPDATE 会在中途失败时留下"半更新"，而且丢掉 rowcount 就无法区分
        "改成功"与"没这一行"，页面只能假报成功。
        """
        sets: list[str] = []
        params: list = []
        if nickname is not None:
            sets.append("nickname=?")
            params.append(normalize_nick(nickname))
        if enabled is not None:
            sets.append("enabled=?")
            params.append(1 if enabled else 0)
        if media_only is not None:
            sets.append("media_only=?")
            params.append(1 if media_only else 0)
        if not sets:
            return 0
        params.extend([umo, author_id])
        with self._conn() as conn:
            return conn.execute(
                f"UPDATE edges SET {', '.join(sets)} WHERE umo=? AND author_id=?", params
            ).rowcount

    def set_edge_nickname(self, umo: str, author_id: str, nickname: str) -> int:
        with self._conn() as conn:
            cur = conn.execute(
                "UPDATE edges SET nickname=? WHERE umo=? AND author_id=?",
                (normalize_nick(nickname), umo, author_id),
            )
            return cur.rowcount

    def set_edge_enabled(self, umo: str, author_id: str, enabled: bool) -> int:
        with self._conn() as conn:
            cur = conn.execute(
                "UPDATE edges SET enabled=? WHERE umo=? AND author_id=?",
                (1 if enabled else 0, umo, author_id),
            )
            return cur.rowcount

    def list_edges_with_author(self) -> list[sqlite3.Row]:
        """一次 JOIN 拿齐订阅与其账号信息。

        WebUI 总览原本对每条订阅都 ``get_author``，而 ``_conn()`` 每次都新开连接
        （还带 ``PRAGMA journal_mode=WAL``）—— 订阅上百条时就是几百次开连接。
        """
        with self._conn() as conn:
            return conn.execute(
                "SELECT e.*, a.handle AS author_handle, "
                "a.display_name AS author_display_name "
                "FROM edges e LEFT JOIN authors a ON a.author_id=e.author_id "
                "ORDER BY e.edge_id"
            ).fetchall()

    def pending_counts(self) -> dict[int, int]:
        """每条订阅的待投递数，一次 GROUP BY 拿全（替代逐条 COUNT）。"""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT edge_id, COUNT(*) AS n FROM deliveries WHERE state=? GROUP BY edge_id",
                (STATE_QUEUED,),
            ).fetchall()
        return {int(r["edge_id"]): int(r["n"]) for r in rows}

    def media_post_count(self) -> int:
        """含媒体帖子数 —— ``posts.has_media`` 列的实际用途（状态面板可观测指标）。"""
        with self._conn() as conn:
            return int(
                conn.execute("SELECT COUNT(*) FROM posts WHERE has_media=1").fetchone()[0]
            )

    def enabled_edges_for_author(self, author_id: str) -> list[sqlite3.Row]:
        with self._conn() as conn:
            return conn.execute(
                "SELECT * FROM edges WHERE author_id=? AND enabled=1", (author_id,)
            ).fetchall()

    def subscribed_author_ids(self) -> list[str]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT DISTINCT author_id FROM edges WHERE enabled=1"
            ).fetchall()
        return [str(r["author_id"]) for r in rows]

    # ------------------------------------------------------------ 提交一轮抓取

    def commit_poll(
        self,
        author_id: str,
        posts: list[dict],
        edges: list[sqlite3.Row],
        new_seen_ts: int,
        set_baseline: bool = False,
    ) -> tuple[int, int]:
        """**一个事务**内提交：写 posts → 为每条边入队 → 推进两条水位。

        原子性是关键：任一环节失败都整体回滚，下轮会用同一个 ``since`` 重新拉到，
        既不会漏（水位没推进）也不会重（``UNIQUE(edge_id, post_id)``）。

        返回 ``(新增帖数, 入队数)``。
        """
        inserted = enqueued = 0
        now = int(time.time())
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                for post in posts:
                    has_media = 1 if post.get("has_media") else 0
                    cur = conn.execute(
                        "INSERT OR IGNORE INTO posts"
                        "(post_id, author_id, created_ts, kind, body_json, media_json, "
                        " has_media, fetched_at) "
                        "VALUES(?,?,?,?,?,?,?,?)",
                        (
                            post["post_id"],
                            author_id,
                            int(post["created_ts"]),
                            post.get("kind", KIND_ORIGINAL),
                            post["body_json"],
                            post.get("media_json", "[]"),
                            has_media,
                            now,
                        ),
                    )
                    inserted += cur.rowcount
                    for edge in edges:
                        if int(post["created_ts"]) <= int(edge["enqueued_ts"]):
                            continue
                        # 按订阅的"仅媒体"：**只影响入队，不影响水位推进**。
                        # 因此被跳过的纯文字帖之后不会补发（与"首次订阅不补发"语义一致）。
                        if edge_media_only(edge) and not has_media:
                            continue
                        cur = conn.execute(
                            "INSERT OR IGNORE INTO deliveries"
                            "(edge_id, post_id, state, attempts, queued_at) "
                            "VALUES(?,?,?,0,?)",
                            (int(edge["edge_id"]), post["post_id"], STATE_QUEUED, now),
                        )
                        enqueued += cur.rowcount
                    if edges:
                        conn.execute(
                            "UPDATE edges SET enqueued_ts=? WHERE author_id=? AND enqueued_ts<?",
                            (int(post["created_ts"]), author_id, int(post["created_ts"])),
                        )
                if not posts and edges:
                    # 本轮没有帖可入队（例如全被 filter_* 过滤掉）：仍要把订阅水位推到
                    # 已见位置，否则 edges.enqueued_ts 会永远落后于 authors.last_seen_ts，
                    # 与 docstring 的"推进两条水位"不符。
                    conn.execute(
                        "UPDATE edges SET enqueued_ts=? WHERE author_id=? AND enqueued_ts<?",
                        (int(new_seen_ts), author_id, int(new_seen_ts)),
                    )
                conn.execute(
                    "UPDATE authors SET last_seen_ts=MAX(last_seen_ts,?), updated_at=? "
                    "WHERE author_id=?",
                    (int(new_seen_ts), now, author_id),
                )
                if set_baseline:
                    conn.execute(
                        "UPDATE authors SET baseline_ready=1 WHERE author_id=?", (author_id,)
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return inserted, enqueued

    # ------------------------------------------------------------ posts
    def set_translation(self, post_id: str, text: str, lang: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE posts SET translated=?, translate_lang=? WHERE post_id=?",
                (text, lang, post_id),
            )

    def count_posts(self) -> int:
        with self._conn() as conn:
            return int(conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0])

    # ------------------------------------------------------------ deliveries
    def queue_depth(self, umo: str | None = None) -> int:
        with self._conn() as conn:
            if umo is None:
                return int(
                    conn.execute(
                        "SELECT COUNT(*) FROM deliveries WHERE state=?", (STATE_QUEUED,)
                    ).fetchone()[0]
                )
            return int(
                conn.execute(
                    "SELECT COUNT(*) FROM deliveries d "
                    "JOIN edges e ON e.edge_id=d.edge_id "
                    "WHERE d.state=? AND e.umo=?",
                    (STATE_QUEUED, umo),
                ).fetchone()[0]
            )

    def distinct_umos_with_queue(self) -> list[str]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT DISTINCT e.umo FROM deliveries d "
                "JOIN edges e ON e.edge_id=d.edge_id WHERE d.state=?",
                (STATE_QUEUED,),
            ).fetchall()
        return [str(r["umo"]) for r in rows]

    def next_queued(
        self, limit: int, exclude_umos: list[str] | None = None
    ) -> list[sqlite3.Row]:
        """取队首待投递。

        ``exclude_umos`` 用于**在 SQL 层**排除正处于免打扰的会话。这一步是必须的：
        若只是 ``LIMIT`` 之后再过滤，免打扰会话会一直占住队首，别的会话在整段
        免打扰时间里一条都发不出去（积压越多越严重）。
        """
        with self._conn() as conn:
            sql = (
                "SELECT d.delivery_id, d.edge_id, d.post_id, d.attempts, d.queued_at, "
                "e.umo, e.nickname, e.author_id, p.body_json, p.media_json, p.translated, "
                "a.handle, a.display_name "
                "FROM deliveries d JOIN edges e ON e.edge_id=d.edge_id "
                "JOIN posts p ON p.post_id=d.post_id "
                "JOIN authors a ON a.author_id=e.author_id "
                "WHERE d.state=?"
            )
            params: list = [STATE_QUEUED]
            if exclude_umos:
                marks = ",".join("?" * len(exclude_umos))
                sql += f" AND e.umo NOT IN ({marks})"
                params.extend(exclude_umos)
            sql += " ORDER BY d.queued_at, d.delivery_id LIMIT ?"
            params.append(int(limit))
            return conn.execute(sql, params).fetchall()

    def mark_delivered(self, delivery_id: int) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE deliveries SET state=?, sent_at=?, attempts=attempts+1 "
                "WHERE delivery_id=?",
                (STATE_DELIVERED, int(time.time()), delivery_id),
            )

    def mark_failed(self, delivery_id: int, error: str, retry: bool) -> None:
        state = STATE_QUEUED if retry else STATE_FAILED
        with self._conn() as conn:
            conn.execute(
                "UPDATE deliveries SET state=?, attempts=attempts+1, last_error=? "
                "WHERE delivery_id=?",
                (state, str(error)[:500], delivery_id),
            )

    def drop_oldest_queued(self, count: int, umo: str | None = None) -> list[sqlite3.Row]:
        """丢弃最旧的 ``count`` 条待投递，返回被丢弃的行（用于汇总提示）。"""
        if count <= 0:
            return []
        with self._conn() as conn:
            if umo is None:
                rows = conn.execute(
                    "SELECT d.delivery_id, d.edge_id, d.post_id, e.umo, a.handle "
                    "FROM deliveries d JOIN edges e ON e.edge_id=d.edge_id "
                    "JOIN authors a ON a.author_id=e.author_id "
                    "WHERE d.state=? ORDER BY d.queued_at, d.delivery_id LIMIT ?",
                    (STATE_QUEUED, int(count)),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT d.delivery_id, d.edge_id, d.post_id, e.umo, a.handle "
                    "FROM deliveries d JOIN edges e ON e.edge_id=d.edge_id "
                    "JOIN authors a ON a.author_id=e.author_id "
                    "WHERE d.state=? AND e.umo=? "
                    "ORDER BY d.queued_at, d.delivery_id LIMIT ?",
                    (STATE_QUEUED, umo, int(count)),
                ).fetchall()
            if rows:
                ids = [int(r["delivery_id"]) for r in rows]
                conn.executemany(
                    "UPDATE deliveries SET state=? WHERE delivery_id=?",
                    [(STATE_DROPPED, i) for i in ids],
                )
            return rows

    def recent_deliveries(self, limit: int = 10) -> list[sqlite3.Row]:
        with self._conn() as conn:
            return conn.execute(
                "SELECT d.delivery_id, d.state, d.sent_at, d.last_error, "
                "e.umo, a.handle FROM deliveries d "
                "JOIN edges e ON e.edge_id=d.edge_id "
                "JOIN authors a ON a.author_id=e.author_id "
                "ORDER BY d.delivery_id DESC LIMIT ?",
                (int(limit),),
            ).fetchall()

    def pending_count_for_edge(self, edge_id: int) -> int:
        with self._conn() as conn:
            return int(
                conn.execute(
                    "SELECT COUNT(*) FROM deliveries WHERE edge_id=? AND state=?",
                    (int(edge_id), STATE_QUEUED),
                ).fetchone()[0]
            )


# ---------------------------------------------------------------- 抓取提供者


class FetchProvider:
    """抓取提供者的隐式协议（P5 降级链的插槽）。

    契约只有两条：
      · ``name``（类属性）——稳定的英文标识，同时是配置里写的值
      · ``async fetch_user(handle, since_ts, with_replies) -> FetchResult``
        —— **禁止抛异常**；任何失败都必须收敛成 ``FetchResult(ok=False)``，
        并且必须保证 ``result.provider`` 是可区分的名字（日志与 /x状态 靠它排障）。

    更重要的是一条**语义契约**：``ok=True, no_new=True`` 与 ``ok=False`` 是两回事。
    "这个源解析不出来"必须是后者，否则会被当成"确实没有新帖"而永久漏推。
    """

    name = "provider"

    async def fetch_user(self, handle: str, since_ts: int = 0, with_replies: bool = False) -> FetchResult:
        raise NotImplementedError

    def health_snapshot(self) -> list[dict]:
        """多实例提供者（RSSHub 池）用它汇报各实例状态；单实例返回空列表。"""
        return []

    def same_config(self, other: "FetchProvider") -> bool:
        """与另一个同类提供者的**配置**是否一致（用于避免无谓重建）。

        重建链会把实例健康、熔断统计全部丢掉，所以只要配置没变就必须复用旧对象。
        """
        return type(self) is type(other) and self.name == other.name


class FxTwitterProvider(FetchProvider):
    """FxTwitter / FxEmbed V2 API 提供者（免 key）。

    实测要点：
      - `?since=<秒级时间戳>` 增量拉取；**无新帖返回 HTTP 204 空 body**
      - `count` 参数完全无效（页大小固定 20）
      - `since_id` 被静默忽略，**绝不能用**
      - `with_replies=true` 会插入被回复原帖并产生**重复 id**，必须去重
      - 错误信封两种形态：时间线读 `code`+`results`，单条读 `code`+`message`
    """

    name = PROVIDER_FXTWITTER

    def __init__(self, client: httpx.AsyncClient, base: str = "") -> None:
        self.client = client
        # base 可配置：把 L1 指到一个失效地址就能验证降级链真的换了源
        self.base = (str(base or "").strip() or FXTWITTER_BASE).rstrip("/")

    def same_config(self, other: "FetchProvider") -> bool:
        return (
            type(self) is type(other)
            and self.name == other.name
            and self.base == getattr(other, "base", None)
        )

    async def fetch_user(self, handle: str, since_ts: int = 0, with_replies: bool = False) -> FetchResult:
        handle = str(handle or "").lstrip("@").strip()
        if not handle:
            return FetchResult(ok=False, error="empty handle", source=self.name)
        url = f"{self.base}/2/profile/{handle}/statuses"
        params: dict[str, str] = {}
        if since_ts and int(since_ts) > 0:
            params["since"] = str(int(since_ts))
        if with_replies:
            params["with_replies"] = "true"
        try:
            resp = await self.client.get(url, params=params)
        except Exception as exc:  # 网络层
            return FetchResult(ok=False, error=f"{type(exc).__name__}: {exc}", source=self.name)

        if resp.status_code == 204:
            # 这就是"没有新帖"的确定信号——靠状态码判定，204 时不要解析 body
            return FetchResult(ok=True, no_new=True, source=self.name)
        if resp.status_code == 404:
            return FetchResult(
                ok=False, not_found=True, error="404 not found", source=self.name
            )
        if resp.status_code != 200:
            return FetchResult(
                ok=False,
                error=f"HTTP {resp.status_code}",
                source=self.name,
            )
        try:
            payload = resp.json()
        except Exception as exc:
            return FetchResult(ok=False, error=f"bad json: {exc}", source=self.name)

        if not isinstance(payload, dict):
            return FetchResult(ok=False, error="unexpected payload", source=self.name)
        if payload.get("code") not in (None, 200):
            return FetchResult(
                ok=False,
                error=f"api code={payload.get('code')} {payload.get('message', '')}".strip(),
                source=self.name,
            )

        results = payload.get("results")
        if not isinstance(results, list):
            return FetchResult(ok=False, error="missing results", source=self.name)

        statuses = [
            item for item in results if isinstance(item, dict) and item.get("type") == "status"
        ]
        if not statuses:
            # 200 + 空数组**不是**"没有新帖"的确定信号（真正的信号是 204）。
            # 实测正常的"无新帖"一律走 204；200+空数组只可能来自零帖新账号，
            # 或者把 204/错误改写成 `{"code":200,"results":[]}` 的反代/CDN。
            # 后者若被当成"无新帖"，就是水位停摆式的永久漏推——所以要求
            # 信封里带 cursor（真实响应总有）才算数，否则按源不可用处理。
            if isinstance(payload.get("cursor"), dict):
                return FetchResult(ok=True, no_new=True, source=self.name)
            return FetchResult(
                ok=False,
                error="empty timeline payload (200 without cursor)",
                source=self.name,
            )

        # 顺带取一次作者信息（results 里每条都带完整 author）
        author = None
        for item in statuses:
            candidate = item.get("author")
            if isinstance(candidate, dict) and candidate.get("screen_name"):
                if candidate.get("screen_name", "").lower() == handle.lower():
                    author = candidate
                    break
        if author is None:
            first = statuses[0].get("author")
            author = first if isinstance(first, dict) else None

        return FetchResult(
            ok=True, author=author, posts=statuses, source=self.name
        )


class RssProvider(FetchProvider):
    """通用 RSS 源：``{base}{path}``（fxtwitter.com 的公开 RSS 面）。

    它是 L2，也是 RSSHub 池的基类。**鉴权与请求细节由子类接管**，
    这里只负责"一次 HTTP + 一次防御式解析"。

    增量：feed 本身没有 ``since`` 参数，只能在本地按 ``created_timestamp``
    过滤；feed 的 pubDate 只有秒级精度，边界判定为**排他**（理由见
    ``parse_feed_xml`` 的文档串：放宽成包含会破坏"首次订阅不补发历史"）。
    """

    name = PROVIDER_FXTWITTER_RSS

    #: 依次尝试的 feed 路径模板。这几个面**都只是"可能存在的同一个时间线"**
    #: （实测 fxtwitter 的 ``/feed.xml`` 一个面就带全了图片与视频 enclosure，
    #: 所以不再探测 ``feed.media.xml``）；``/rss`` 是 Nitter fork 的实际路由，
    #: 放最后只为兜底——它在 fxtwitter 上必然 404，成本是一次请求。
    FEED_PATHS = ("/{handle}/feed.xml", "/{handle}/feed", "/{handle}/rss")
    ACCEPT = "application/rss+xml, application/atom+xml, application/xml, text/xml;q=0.9, */*;q=0.5"

    def __init__(self, client: httpx.AsyncClient, base: str = "", name: str = "") -> None:
        self.client = client
        self.base = (str(base or "").strip() or FXTWITTER_RSS_BASE).rstrip("/")
        #: 依次尝试的路径模板；子类（RSSHub）会换掉整组
        self.paths: tuple[str, ...] = tuple(self.FEED_PATHS)
        #: 上次真正解析成功的路径模板：下次先试它，避免每轮都从 404 那个面开始
        self._preferred_path = ""
        if name:
            self.name = name

    def same_config(self, other: "FetchProvider") -> bool:
        return (
            type(self) is type(other)
            and self.name == other.name
            and self.base == getattr(other, "base", None)
            and self.paths == getattr(other, "paths", None)
        )

    def _headers(self) -> dict:
        return {
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": self.ACCEPT,
            "Cache-Control": "no-cache",
        }

    def _ordered_paths(self) -> list[str]:
        """上次成功的路径优先，其余保持配置顺序。"""
        paths = list(self.paths)
        preferred = self._preferred_path
        if preferred in paths:
            paths.remove(preferred)
            paths.insert(0, preferred)
        return paths

    async def fetch_user(
        self, handle: str, since_ts: int = 0, with_replies: bool = False, base: str = ""
    ) -> FetchResult:
        """``base`` 显式传入时用传入值，否则用实例的 base。

        池（``RssHubProvider``）必须走这条路径：把"当前试到哪个实例"放在
        ``self.base`` 上，并发时 A 请求的循环会读到 B 改写的值，URL 落到别的实例、
        而 ``_mark`` 还按局部变量记账 —— 实例健康会归错人。
        """
        handle = str(handle or "").lstrip("@").strip()
        if not handle:
            return FetchResult(ok=False, error="empty handle", source=self.name, provider=self.name)
        root = str(base or self.base or "").rstrip("/")

        last_error = "no feed url worked"
        parsed_any = False
        last_author: dict | None = None
        for template in self._ordered_paths():
            url = f"{root}{template.format(handle=handle)}"
            try:
                resp = await self.client.get(url, headers=self._headers())
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                continue
            if resp.status_code == 404:
                last_error = "404 not found"
                continue
            if resp.status_code != 200:
                last_error = f"HTTP {resp.status_code}"
                continue
            try:
                content = resp.content
            except Exception as exc:  # 极少数替身没有 .content
                last_error = f"no body: {exc}"
                continue
            parsed = parse_feed_xml(content, min_ts=since_ts, handle=handle)
            if parsed is None:
                # 200 但不是 feed：Nitter 类镜像会返回浏览器验证页，
                # 把 200 当成功是同类插件踩过的坑，这里必须当成"该源不可用"。
                last_error = "not a feed (200 but unparsable)"
                continue
            posts, author = parsed
            parsed_any = True
            if author:
                last_author = author
            if not posts:
                # 这个面是**结构合法的 feed**，只是没有水位之后的新条目。
                # **不能就此下"无新帖"的结论**：这些面只是同一服务的不同入口，
                # 但彼此并不保证同步（CDN/反代/镜像卡住、RSSHub 路由存在但为空、
                # Nitter fork 只实现了 /rss）。v1.3.1 曾在这里提前 return，
                # 结果"空但合法的 /feed.xml"会永久遮住 /feed 上的内容：
                # 整源 no_new、首轮还把水位设成 now，之后真实帖全被 min_ts 滤掉。
                last_error = "feed ok but no new item"
                continue
            # 只有**真的出了条目**的面才算"内容面"：把它记成首选，下轮先试它。
            # 用空面覆写首选会让顺序在两个面之间来回摆动。
            self._preferred_path = template
            return FetchResult(
                ok=True,
                author=author,
                posts=posts,
                source=self.name,
                provider=self.name,
            )
        if parsed_any:
            # 至少一个面是**结构合法的 feed**，只是没有新条目 —— 这才是"无新帖"，
            # 与"解析不出来"必须区分开（后者会让水位停摆或永久漏推）。
            # 作者信息仍要带上：否则一旦 L1 长期不可用，账号详情就再也刷不出来了。
            return FetchResult(
                ok=True,
                no_new=True,
                author=last_author,
                source=self.name,
                provider=self.name,
            )
        return FetchResult(
            ok=False,
            not_found="404" in last_error,
            error=last_error,
            source=self.name,
            provider=self.name,
        )


class RssHubProvider(RssProvider):
    """自建 RSSHub / Nitter fork 的**实例池**：多实例健康检查 + 故障转移。

    为什么是"池"而不是"一个地址"：公共 Nitter 实例实测全线不可用
    （xcancel.com HTTP 451、nitter.poast.org DNS 失效、官方仓已归档），
    RSSHub 官方公共实例 302 到 google.com，所以这一层只对**自建实例**有意义，
    而自建实例的用户往往有不止一个。实例顺序按"最近可用"轮换。
    """

    name = PROVIDER_RSSHUB
    #: RSSHub 的 X 时间线路由（需要实例侧配 TWITTER_AUTH_TOKEN 或官方 key）
    RSSHUB_PATHS = ("/twitter/user/{handle}",)
    #: 实例被判定为不健康后的冷却时间（秒）
    INSTANCE_COOLDOWN = 600.0

    def __init__(self, client: httpx.AsyncClient, instances: list[str] | None = None) -> None:
        cleaned: list[str] = []
        for raw in instances or []:
            url = str(raw or "").strip().rstrip("/")
            if url and url not in cleaned:
                cleaned.append(url)
        super().__init__(client, base=cleaned[0] if cleaned else "", name=self.name)
        #: 实例内的尝试顺序：先 RSSHub 路由，再退化到 fxtwitter 风格的 feed 路径
        #: （有些 Nitter fork / RSSHub 实例只提供后者）
        self.paths: tuple[str, ...] = tuple(self.RSSHUB_PATHS) + tuple(self.FEED_PATHS)
        self._instances = cleaned
        self._health: dict[str, dict] = {}
        for url in cleaned:
            self._health[url] = {"failures": 0, "unhealthy_until": 0.0, "last_error": "", "ok": 0}

    def same_config(self, other: "FetchProvider") -> bool:
        # 实例集合相同即视为同一份配置；但**不比较顺序**，
        # 因为运行期会按健康度轮换实例顺序，那是状态不是配置。
        return (
            type(self) is type(other)
            and self.name == other.name
            and sorted(self._instances) == sorted(getattr(other, "_instances", []) or [])
        )

    def ordered_instances(self) -> list[str]:
        """健康的排前面；同一档内保持用户配置顺序（可用性优先，稳定次之）。"""
        now = time.time()
        healthy, unhealthy = [], []
        for url in self._instances:
            state = self._health.get(url) or {}
            if float(state.get("unhealthy_until") or 0) > now:
                unhealthy.append(url)
            else:
                healthy.append(url)
        return healthy + unhealthy

    def _mark(self, url: str, ok: bool, error: str = "") -> None:
        state = self._health.setdefault(
            url, {"failures": 0, "unhealthy_until": 0.0, "last_error": "", "ok": 0}
        )
        if ok:
            state["failures"] = 0
            state["unhealthy_until"] = 0.0
            state["last_error"] = ""
            state["ok"] = int(state.get("ok") or 0) + 1
        else:
            state["failures"] = int(state.get("failures") or 0) + 1
            state["last_error"] = error
            state["unhealthy_until"] = time.time() + self.INSTANCE_COOLDOWN
        self._sort_instances()

    def _sort_instances(self) -> None:
        order = self.ordered_instances()
        if order:
            self._instances = order

    def health_snapshot(self) -> list[dict]:
        now = time.time()
        out: list[dict] = []
        for url in self._instances:
            state = self._health.get(url) or {}
            out.append(
                {
                    "name": url,
                    "detailed": True,
                    "ok": int(state.get("ok") or 0),
                    "failures": int(state.get("failures") or 0),
                    "unhealthy_until": float(state.get("unhealthy_until") or 0),
                    "last_error": str(state.get("last_error") or ""),
                    "healthy": float(state.get("unhealthy_until") or 0) <= now,
                }
            )
        return out

    async def fetch_user(
        self, handle: str, since_ts: int = 0, with_replies: bool = False, base: str = ""
    ) -> FetchResult:
        """``base`` 参数只为满足基类签名：池自己决定用哪个实例，忽略外部传入的 base。"""
        handle = str(handle or "").lstrip("@").strip()
        if not handle:
            return FetchResult(ok=False, error="empty handle", source=self.name, provider=self.name)
        if not self._instances:
            return FetchResult(
                ok=False,
                error="no rsshub instance configured",
                source=self.name,
                provider=self.name,
            )

        last_error = "no instance worked"
        not_found = False
        for instance in list(self._instances):
            # 实例地址**只走参数**，绝不写回 self.base：self.base 是共享可变状态，
            # 并发抓取时会被另一个任务改写，导致请求落到别的实例、而 _mark 还按
            # 局部变量记账（实例健康归错人、坏实例被立刻"复活"到队首）。
            result = await super().fetch_user(handle, since_ts, with_replies, base=instance)
            if result.ok:
                self._mark(instance, True)
                # 注意：池内换实例对上层是**不可见**的（对外仍是 rsshub 这一个源），
                # 实例级细节只在 /x源 里展开
                return FetchResult(
                    ok=True,
                    no_new=result.no_new,
                    author=result.author,
                    posts=result.posts,
                    source=self.name,
                    provider=self.name,
                )
            if result.not_found:
                # 必须把内层的"账号不存在"透传给链：否则一个不存在的 handle 会让
                # 整个 rsshub 源进熔断计数。
                not_found = True
                # **实例不因"账号不存在"被冷却**：404 是账号问题，不是这个实例坏了。
                # 一个 404 响应反而证明实例是活的，所以顺手把它的不健康状态清掉
                # （否则一个打错的 handle 就把每个实例冷却 600s、健康轮换失效）。
                self._mark(instance, True)
                last_error = f"{instance}: {result.error}"
                continue
            last_error = f"{instance}: {result.error}"
            self._mark(instance, False, str(result.error))
        return FetchResult(
            ok=False,
            not_found=not_found,
            error=last_error,
            source=self.name,
            provider=self.name,
        )


class ProviderChain:
    """有序降级链：依次尝试各提供者，谁先成功就用谁。

    四个设计点，全是为了"不要骗自己"：

    1. **降级是显式的**：``ok=False`` 才切下一层；``ok=True, no_new=True`` 是
       结论（真的没新帖），直接返回，不会去问下一层。否则第二层只能给出
       "20 条里没有比水位更新的"这种更弱的结论，反而会把第一层的质量拉低。
       （**仅指层与层之间**；同一个 provider 内部的多条 URL 是"同一份数据的
       不同入口"，由 provider 自己决定。）
    2. **熔断只是延后，不是"探活"**：连续失败达阈值后把该源挪到队尾并冷却；
       因为**找到可用源就立刻返回**，冷却期内若还有别的健康源，这个源不会被
       试到。它会在两种情况下重新被试到：冷却到期回到原顺位，或者其余源全部
       失败的兜底轮次。写文档时不要说成"每轮都会被试到"。
    3. **按账号记归属**：哪个账号当前由哪个源供数，逐个记下来，
       ``/x状态`` 与 WebUI 直接显示，排障不必翻日志。
    4. **"账号不存在"不是"源不可用"**：404 只记 ``not_found``，**不进熔断计数**。
       否则一个打错的 handle 就能把整个健康源冷却 600 秒，拖累所有账号。
    """

    def __init__(
        self,
        providers: list[FetchProvider],
        threshold: int = 1,
        cooldown: float = 600.0,
        per_timeout: float = FETCH_PROVIDER_TIMEOUT,
        probe_handle: str = DEFAULT_PROBE_HANDLE,
    ) -> None:
        self.providers = list(providers)
        self.threshold = max(1, int(threshold))
        self.cooldown = max(0.0, float(cooldown))
        self.per_timeout = max(1.0, float(per_timeout))
        #: 用来区分"账号不存在"与"整个源坏了"的探针账号（必须是一个必然存在的账号）。
        #: 没有它就**绝不**因为 404 降级任何源。
        self.probe_handle = str(probe_handle or "").strip().lstrip("@")
        #: 连续 404 到这个数就做一次健康探测，用探测结果决定"账号问题"还是"源坏了"
        self.not_found_threshold = max(3, self.threshold * 3)
        self._stats: dict[str, dict] = {}
        for provider in self.providers:
            self._stats[provider.name] = {
                "failures": 0,
                "ok": 0,
                "last_error": "",
                "last_ok_ts": 0.0,
                "last_error_ts": 0.0,
                "tripped_until": 0.0,
            }
        #: 配置里声明的名字（保序）。用于检测"配置变了需要重建链"。
        self._names: tuple[str, ...] = tuple(provider.name for provider in self.providers)
        #: /x源 手动指定的源：只影响"下一次抓取的优先顺序"，不改变链的组成
        self.pinned: str = ""

    # ------------------------------------------------------------ 名字与顺序

    @property
    def names(self) -> tuple[str, ...]:
        return self._names

    def active_name(self) -> str:
        """当前"第一顺位"的源名——没有失败记录时的名义生效源。"""
        if self.pinned:
            return self.pinned
        ordered = self.ordered_names()
        return ordered[0] if ordered else ""

    def set_pin(self, name: str) -> bool:
        """把某个源提到队首——只影响**尝试顺序**，不改变链的组成与统计。"""
        candidate = str(name or "").strip().lower()
        if candidate not in self._names:
            return False
        self.pinned = candidate
        return True

    def clear_pin(self) -> None:
        self.pinned = ""

    def _ordered_providers(self, pin: str | None = None) -> list[FetchProvider]:
        """当前尝试顺序：``pin`` 指定的在最前，其余未熔断的按原序，熔断的压到最后。

        注意这里**不重排 self.providers**：顺序是每次算出来的，
        否则一次手动指定就会永久改变链的形状，清除后也回不去。
        """
        now = time.time()
        pinned_name = self.pinned if pin is None else pin
        live: list[FetchProvider] = []
        tripped: list[FetchProvider] = []
        for provider in self.providers:
            if provider.name == pinned_name:
                continue
            self._expire_trip(self._stats.setdefault(provider.name, {}))
            if self.is_tripped(provider.name):
                tripped.append(provider)
            else:
                live.append(provider)
        pinned = [p for p in self.providers if p.name == pinned_name]
        return pinned + live + tripped

    def ordered_names(self) -> list[str]:
        return [provider.name for provider in self._ordered_providers()]

    def health(self, name: str) -> dict:
        return dict(self._stats.get(name) or {})

    def is_tripped(self, name: str) -> bool:
        state = self._stats.get(name) or {}
        return (
            int(state.get("failures") or 0) >= self.threshold
            or bool(state.get("source_broken"))
        ) and float(state.get("tripped_until") or 0) > time.time()

    def _expire_trip(self, state: dict) -> None:
        """冷却到期就把这次"降级窗口"收尾：清掉 404 计数与 broken 标记。

        **不能**写成 ``tripped_until <= now``：``tripped_until`` 初值是 0，
        那样会在第一次失败时就把计数清掉，阈值永远攒不满。
        只清 404 线，不动 ``failures``——真实失败的计数该留着。
        """
        tripped_until = float(state.get("tripped_until") or 0)
        if tripped_until and tripped_until <= time.time():
            state["tripped_until"] = 0.0
            state["not_found"] = 0
            state["source_broken"] = False
            state["needs_probe"] = False

    def degraded(self) -> bool:
        """配置里排第一的源是否正处于熔断状态。

        刻意按**配置顺序**判断，而不是快照的行序：快照按当前尝试顺序输出，
        降级后"当前第一顺位"恰好是健康的那个，用它判断永远是 False。
        """
        if not self._names:
            return False
        return self.is_tripped(self._names[0])

    def snapshot(self) -> list[dict]:
        """给 /x状态、/x源 与 WebUI 用的只读快照，**按当前尝试顺序**输出。

        每项同时给两个序号：
          · ``index`` —— 在 ``fetch_chain`` 里的声明序号（就是文档里的 L1/L2/L3）
          · ``order`` —— 当前实际尝试顺位
        这样降级时既不会把 L1/L2 的含义搞混，也能一眼看出"现在先试哪个"。
        """
        now = time.time()
        ordered = self.ordered_names()
        order_of = {name: position for position, name in enumerate(ordered)}
        index_of = {name: position for position, name in enumerate(self._names)}
        out: list[dict] = []
        for name in ordered:
            provider = next((p for p in self.providers if p.name == name), None)
            if provider is None:
                continue
            state = self._stats.get(name) or {}
            # 替身提供者可能没有 health_snapshot（它只是可选能力），不能因此炸掉状态页
            probe = getattr(provider, "health_snapshot", None)
            out.append(
                {
                    "name": name,
                    "failures": int(state.get("failures") or 0),
                    "ok": int(state.get("ok") or 0),
                    "not_found": int(state.get("not_found") or 0),
                    "broken": bool(state.get("source_broken")),
                    "last_error": str(state.get("last_error") or ""),
                    "last_ok_ts": float(state.get("last_ok_ts") or 0),
                    "tripped": self.is_tripped(name),
                    "retry_in": max(0, int(float(state.get("tripped_until") or 0) - now)),
                    "order": order_of.get(name, 0),
                    "index": index_of.get(name, 0),
                    "instances": probe() if callable(probe) else [],
                }
            )
        return out

    def mark_success(self, name: str) -> None:
        state = self._stats.setdefault(name, {})
        state["failures"] = 0
        state["not_found"] = 0
        state["source_broken"] = False
        state["needs_probe"] = False
        state["tripped_until"] = 0.0
        state["last_ok_ts"] = time.time()
        state["last_ok_mono"] = time.monotonic()
        state["ok"] = int(state.get("ok") or 0) + 1

    def mark_failure(self, name: str, error: str, started: float = 0.0, trip: bool = True) -> None:
        """记一次"源不可用"。

        ``trip=False`` 用于**账号级**结论（404：这个 handle 不存在）。
        单次 404 绝不降级——否则一个打错的账号名会把健康源冷却 600 秒。连续
        ``not_found_threshold`` 次 404 之后**也不直接降级**，而是打一次"健康探测"：
        只有连探针账号（``probe_handle``，必然存在）都拿不到数据，才判定"这个源真的坏了"。
        这样"3 个账号被删/改名"不会冤枉健康源，而"base 配错、对谁都 404"仍能被降级。

        ``started`` 是这次请求的发出时刻（**单调时钟**）：并发抓取时，一个"发出更早、
        回来更晚"的失败不得覆盖之后刚落地的成功。守卫放在函数开头，两个分支共用。
        """
        state = self._stats.setdefault(name, {})
        if started and float(state.get("last_ok_mono") or 0) > float(started):
            return
        self._expire_trip(state)
        if not trip:
            hits = int(state.get("not_found") or 0) + 1
            state["not_found"] = hits
            state["last_error"] = str(error or "")
            state["last_error_ts"] = time.time()
            if self.probe_handle and hits >= self.not_found_threshold:
                state["needs_probe"] = True
            return
        state["failures"] = int(state.get("failures") or 0) + 1
        state["last_error"] = str(error or "")
        state["last_error_ts"] = time.time()
        if state["failures"] >= self.threshold:
            state["tripped_until"] = time.time() + self.cooldown

    def needs_probe(self, name: str) -> bool:
        return bool((self._stats.get(name) or {}).get("needs_probe"))

    def resolve_probe(self, name: str, probe_ok: bool, error: str = "") -> None:
        """落实一次健康探测的结论。

        探针账号成功 ⇒ 说明源是好的，之前那些 404 都是**账号**问题：清掉计数、不降级。
        探针账号也失败 ⇒ 这个源真的坏了：标记 broken 并冷却。
        """
        state = self._stats.setdefault(name, {})
        state["needs_probe"] = False
        if probe_ok:
            state["not_found"] = 0
            state["source_broken"] = False
            logger.info(
                f"[x_monitor] 数据源 {name} 对探针账号 @{self.probe_handle} 正常，"
                f"之前的 404 判定为账号问题（不降级）"
            )
            return
        state["source_broken"] = True
        state["tripped_until"] = time.time() + self.cooldown
        state["last_error"] = f"probe @{self.probe_handle} failed: {error}".strip()
        state["last_error_ts"] = time.time()
        logger.warning(
            f"[x_monitor] 数据源 {name} 连探针账号 @{self.probe_handle} 都拿不到数据"
            f"（{error or 'unknown'}），判定该源不可用并冷却 {int(self.cooldown)}s"
        )

    def same_config(self, other: "ProviderChain") -> bool:
        """链的组成、每个提供者的配置、以及熔断旋钮是否都没变（变了才重建）。

        熔断阈值/冷却时间**必须**参与比较：它们也是配置项，WebUI 保存后应当立刻
        生效；漏掉它们会让用户改了阈值却看不到任何变化。
        """
        if (self.threshold, self.cooldown, self.per_timeout, self.probe_handle) != (
            other.threshold,
            other.cooldown,
            other.per_timeout,
            other.probe_handle,
        ):
            return False
        if len(self.providers) != len(other.providers):
            return False
        for mine, theirs in zip(self.providers, other.providers):
            if not mine.same_config(theirs):
                return False
        return True

    def adopt(self, previous: "ProviderChain") -> None:
        """从旧链继承可复用的东西：同类同配置的提供者实例 + **它自己的**统计。

        两者必须绑定：实例按 ``same_config`` 复用（配置变了就不复用），
        统计也只跟**真的被复用的那一份**走。否则把 ``fxtwitter_api_base`` 换个
        地址后，新地址一次都没请求过，却顶着旧地址的 ``failures/tripped_until``
        显示"熔断 + 旧地址的 HTTP 503"。
        """
        reused: dict[str, FetchProvider] = {}
        for provider in self.providers:
            for old in previous.providers:
                if provider.same_config(old):
                    reused[provider.name] = old
                    break
        if reused:
            self.providers = [reused.get(p.name, p) for p in self.providers]
            self._names = tuple(provider.name for provider in self.providers)
        for name, provider in reused.items():
            if name in self._stats:
                self._stats[name] = dict(previous._stats.get(name) or self._stats[name])
        if previous.pinned in self._names:
            self.pinned = previous.pinned

    # ------------------------------------------------------------ 抓取

    async def fetch_user(
        self, handle: str, since_ts: int = 0, with_replies: bool = False, consume_pin: bool = True
    ) -> FetchResult:
        if not self.providers:
            return FetchResult(ok=False, error="抓取链为空（fetch_chain 未配置）")

        # 手动指定只对"下一次轮询"生效：轮询路径（``poll_author``）会消费它。
        # ``consume_pin=False`` 的调用（``/x订阅`` 的解析、``/x补发``、探测）**既不吃 pin、
        # 也不按 pin 的顺序抓**——否则管理员设完 pin 随手在 WebUI 加个订阅，
        # 那个订阅就会悄悄用一个非默认的数据源去解析账号 id。
        pin = self.pinned if consume_pin else ""
        if consume_pin:
            self.pinned = ""

        tripped_before: list[str] = []
        failures: list[str] = []
        not_found = False
        for provider in self._ordered_providers(pin):
            name = provider.name
            if self.is_tripped(name):
                tripped_before.append(name)
            # 单调时钟：墙上时钟会被 NTP 校时/休眠恢复改动，用它会误判"谁更晚"
            started = time.monotonic()
            try:
                result = await asyncio.wait_for(
                    provider.fetch_user(handle, since_ts, with_replies), timeout=self.per_timeout
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # 提供者内部异常一律当作"该源不可用"
                result = FetchResult(
                    ok=False,
                    error=f"{type(exc).__name__}: {exc}",
                    source=name,
                    provider=name,
                )
            if not isinstance(result, FetchResult):
                result = FetchResult(
                    ok=False, error="provider returned non-FetchResult", source=name, provider=name
                )
            if result.ok:
                self.mark_success(name)
                result.provider = name
                result.source = name
                if not result.author and result.posts:
                    # RSS 源没有 channel 级作者信息时，退回用条目作者（尽力而为）
                    candidates = [
                        post.get("author")
                        for post in result.posts
                        if isinstance(post.get("author"), dict)
                        and post["author"].get("screen_name")
                    ]
                    if candidates:
                        result.author = candidates[0]
                logger.info(
                    f"[x_monitor] @{handle} 命中数据源 {name}"
                    + (
                        f"（此前已熔断、本轮兜底才试到：{', '.join(tripped_before)}）"
                        if tripped_before
                        else ""
                    )
                )
                return result
            if result.not_found:
                not_found = True
            reason = result.error or ("not found" if result.not_found else "unknown error")
            failures.append(f"{name}: {reason}")
            # 404 是"这个账号不存在"的结论，不是"这个源坏了"：只计数、不熔断。
            # 连续攒够阈值后由下面的探测决定要不要真的降级。
            self.mark_failure(name, reason, started=started, trip=not result.not_found)
            if self.needs_probe(name):
                # 用"必然存在的账号"探一次：探针也拿不到 ⇒ 源坏了；探针正常 ⇒ 账号问题
                await self._run_health_probe(provider)
            logger.warning(
                f"[x_monitor] @{handle} 数据源 {name} 失败（{reason}），"
                f"尝试下一层（链：{' > '.join(self.ordered_names())}）"
            )

        return FetchResult(
            ok=False,
            error="；".join(failures) or "所有数据源均失败",
            not_found=not_found,
            source="chain",
            provider="chain",
        )

    async def _run_health_probe(self, provider: FetchProvider) -> None:
        """对某个源做一次**不记账**的健康探测，并落实结论。

        刻意直接用 provider（不走链、不写统计），否则探测自己会把失败/成功计数搅乱。
        """
        name = provider.name
        if not self.probe_handle:
            return
        try:
            probe = await asyncio.wait_for(
                provider.fetch_user(self.probe_handle, 0, False), timeout=self.per_timeout
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.resolve_probe(name, False, f"{type(exc).__name__}: {exc}")
            return
        ok = bool(getattr(probe, "ok", False))
        self.resolve_probe(name, ok, "" if ok else str(getattr(probe, "error", "") or "probe failed"))


# ---------------------------------------------------------------- 媒体缓存


class MediaCache:
    """有界媒体缓存：sha256 命名、原子写、in-flight 合并、按 mtime 做 LRU 淘汰。"""

    #: 两次缓存淘汰之间的最小间隔，避免每下载一张就全目录 stat 一遍
    EVICT_INTERVAL = 60.0

    def __init__(
        self,
        root: Path,
        quota_mb: int = 512,
        ttl_days: int = 30,
    ) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.quota_bytes = max(16, int(quota_mb)) * 1024 * 1024
        self.ttl_seconds = max(1, int(ttl_days)) * 86400
        self._inflight: dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()
        self._last_evict = 0.0

    def path_for(self, url: str, ext: str) -> Path:
        digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
        return self.root / f"{digest}{ext}"

    def find_existing(self, url: str) -> Path | None:
        """按 URL 找缓存文件；多个候选时取**最新写入**的那个（确定性，且与 LRU 一致）。"""
        digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
        candidates = [
            candidate
            for candidate in self.root.glob(f"{digest}.*")
            if candidate.is_file() and candidate.stat().st_size > 0
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda path: path.stat().st_mtime)

    @staticmethod
    def _fix_extension(path: Path) -> Path:
        """缓存命中后校正扩展名。

        v1.3.0 的 ``sniff_ext`` 不认 ``ftyp``，视频被存成了 ``.jpg``；这类**老缓存**
        会一直被复用，光修嗅探器救不回来（查找用的是 ``sha256(url)`` 前缀，两种扩展名都能命中）。
        这里读头部 16 字节重新推断，必要时改名——顺带也让"同一 URL 换了 content-type"能自愈。

        用 ``Path.replace`` 而不是 ``rename``：Windows 上目标已存在时 ``rename`` 会抛
        ``OSError``，若那个目标是 0 字节残留或被占位，就会形成"永远改不过来"的卡死。
        """
        try:
            with open(path, "rb") as handle:
                head = handle.read(16)
        except OSError:
            return path
        want = sniff_ext(head, path.suffix or ".jpg")
        if not want or want == path.suffix:
            return path
        target = path.with_suffix(want)
        try:
            if target.is_file() and target.stat().st_size > 0:
                return target
            path.replace(target)
            return target
        except OSError:
            return path

    async def fetch(
        self,
        url: str,
        headers: dict | None = None,
        client=None,
    ) -> Path | None:
        """下载并返回本地路径；失败返回 None（调用方降级为发链接）。

        **本方法不对 URL 做任何改写**：画质档位由调用方先用 ``apply_twimg_quality``
        算好再传进来。这样缓存键就是"最终要发的那个 URL"，画质改配置后不会命中旧文件。

        ``client`` 传入插件共享的 httpx 客户端时复用连接并自动带上代理配置；
        不传则临时建一个。
        """
        if not url:
            return None
        existing = self.find_existing(url)
        if existing is not None:
            try:
                os.utime(existing, None)
            except OSError:
                pass
            return self._fix_extension(existing)

        async with self._lock:
            pending = self._inflight.get(url)
            if pending is None:
                loop = asyncio.get_running_loop()
                pending = loop.create_future()
                self._inflight[url] = pending
                owner = True
            else:
                owner = False

        if not owner:
            # 同一 URL 的并发请求合并到一次下载
            try:
                return await pending
            except Exception:
                return None

        try:
            result = await self._download(url, headers or {}, client)
        except asyncio.CancelledError:
            # 被取消时也要给并发等待方一个了断，否则它们会永远挂在这个 future 上
            if not pending.done():
                pending.set_result(None)
            raise
        except Exception as exc:
            if not pending.done():
                pending.set_exception(exc)
                # 没有并发等待方时这个 exception 永远不会被 await，asyncio 会在
                # 垃圾回收时打印 "Future exception was never retrieved" 的 traceback。
                # 主动读一次把它标记为已取回，避免每张下载失败的图都刷一条噪声日志。
                pending.exception()
            logger.warning(f"[x_monitor] 媒体下载失败 {url}: {exc}")
            return None
        finally:
            self._inflight.pop(url, None)

        if not pending.done():
            pending.set_result(result)
        now = time.time()
        if now - self._last_evict >= self.EVICT_INTERVAL:
            self._last_evict = now
            await self._evict()
        return result

    async def _download(self, url: str, headers: dict, client=None) -> Path | None:
        hdrs = {"User-Agent": DEFAULT_USER_AGENT, "Referer": "https://x.com/"}
        hdrs.update(headers or {})
        if client is not None:
            return await self._stream_to_file(client, url, hdrs, url)
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0), follow_redirects=True
        ) as own_client:
            return await self._stream_to_file(own_client, url, hdrs, url)

    async def _stream_to_file(self, client, target_url: str, hdrs: dict, url: str) -> Path | None:
        async with client.stream("GET", target_url, headers=hdrs) as resp:
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code}")
            try:
                # 在连接还开着的时候读：响应头比 URL 后缀更可信
                content_type = str(resp.headers.get("content-type") or "").lower()
            except Exception:
                content_type = ""
            chunks: list[bytes] = []
            total = 0
            async for chunk in resp.aiter_bytes():
                total += len(chunk)
                if total > MAX_MEDIA_BYTES:
                    raise RuntimeError(f"媒体超过上限 {MAX_MEDIA_BYTES} 字节")
                chunks.append(chunk)
        data = b"".join(chunks)
        if not data:
            return None
        ext = sniff_ext(data[:16], ".jpg")
        if "video/mp4" in content_type or url.lower().endswith(".mp4"):
            ext = ".mp4"
        elif "image/png" in content_type or url.lower().endswith(".png"):
            ext = ".png"
        elif "image/gif" in content_type:
            ext = ".gif"
        elif "image/webp" in content_type:
            ext = ".webp"
        dest = self.path_for(url, ext)
        await asyncio.to_thread(self._atomic_write, dest, data)
        return dest

    @staticmethod
    def _atomic_write(dest: Path, data: bytes) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=str(dest.parent), suffix=".part")
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
            os.replace(tmp, dest)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    async def _evict(self) -> None:
        """按 mtime 淘汰，直到总量回到配额内。"""
        try:
            files = [p for p in self.root.iterdir() if p.is_file()]
        except OSError:
            return
        now = time.time()
        alive: list[tuple[float, int, Path]] = []
        total = 0
        for path in files:
            try:
                stat = path.stat()
            except OSError:
                continue
            if self.ttl_seconds > 0 and now - stat.st_mtime > self.ttl_seconds:
                _unlink_quiet(path)
                continue
            alive.append((stat.st_mtime, stat.st_size, path))
            total += stat.st_size
        if total <= self.quota_bytes:
            return
        alive.sort()
        for _mtime, size, path in alive:
            if total <= self.quota_bytes:
                break
            _unlink_quiet(path)
            total -= size

    def usage_bytes(self) -> int:
        total = 0
        for path in self.root.iterdir():
            try:
                if path.is_file():
                    total += path.stat().st_size
            except OSError:
                continue
        return total


def _unlink_quiet(path: Path) -> None:
    try:
        path.unlink()
    except OSError:
        pass


def apply_twimg_quality(url: str, quality: str) -> str:
    """只改 twimg 的 ``name=`` 参数，不做任何转码。"""
    if "pbs.twimg.com" not in url:
        return url
    mapping = {"orig": "orig", "large": "large", "small": "small", "medium": "medium"}
    want = mapping.get(str(quality or "orig").lower(), "orig")
    if re.search(r"[?&]name=", url):
        return re.sub(r"([?&]name=)[^&]*", lambda m: m.group(1) + want, url)
    joiner = "&" if "?" in url else "?"
    return f"{url}{joiner}name={want}"


# ---------------------------------------------------------------- image_pool 兼容层


class ImagePoolBridge:
    """x_monitor → astrbot_plugin_image_pool 的单向兼容层（能力探测 + 全降级）。

    设计约束 —— 每一条都对应一个真实的失败模式：

    1. **每次调用前重新解析实例**。``image_pool`` 被重载 / 停用 / 重新启用后，
       ``StarMetadata.star_cls`` 会换成新对象（``star_manager`` 重新实例化）；
       缓存旧对象会往一个已经停用的插件里写数据。
    2. **必须同时判 ``activated``**：停用时 ``star_cls`` 仍可能指向旧实例。
    3. **``hasattr`` 能力探测**：老版本 image_pool 没有 ``ingest_files`` →
       记一次告警后静默跳过；x_monitor 自身照常工作（单向依赖，缺对端不影响推送）。
    4. **绝不抛异常、绝不阻塞**：入库是"附加归档"。任何失败都不得影响抓取、
       水位推进与推送 —— 钩子放在 ``commit_poll`` **之后**（归档集合 == 落库集合），
       且全程兜底。入库失败是**有损**的（水位已推进），只能如实记录，不能假装可重试。
    5. **图片由本插件自己下载**（复用 ``MediaCache``）：``media_mode=link`` 的用户也能归档。
       URL + 画质相同则命中同一份缓存文件，不会重复下载。
    6. **只提交照片**：image_pool 的入库路径按图片处理，视频传进去会被它存成图片后缀。
    7. **有界**：单轮入库有总预算（``image_pool_sync_budget_sec``），tag 数有上限
       （``image_pool_max_tags_per_post``）—— 一条推文不能放大成几十次跨插件写入。
    8. **失败必须可见**（对抗审核的反例）：下载失败 / 超时 / 预算用尽 / 单个 tag 失败
       以前都是"静默少归档几张"，现在各自计数 + 一次性提示，并在 ``/x状态`` 与 WebUI 上汇总。
    9. **每个 tag 独立隔离**：一个坏 tag（``NUL``、孤立代理、对端内部异常）不能让
       同一条帖子的其余 tag 一起丢；同步实现的对端放到线程里调，绝不阻塞事件循环。
    """

    def __init__(self, plugin: "XMonitorPlugin") -> None:
        self.plugin = plugin
        #: 对端回执的累计计数（/x状态 与 WebUI 的可观测面）
        self.attempts = 0        # 实际发起过几次 ingest_files 调用
        self.posts = 0           # 触发过入库的帖子数（含失败）
        self.added = 0           # 对端回执"新增"
        self.skipped = 0         # 对端回执"跳过"（重复 / 黑名单 / 超大 / 非图片 / 名字被拒）
        self.failed = 0          # 超时或异常（汇总口径，= timeouts + tag_failures + 其它异常）
        self.timeouts = 0        # 入库超时次数
        self.tag_failures = 0    # 单个 tag 抛异常的次数（其余 tag 继续）
        self.download_failed = 0  # 照片没能落到本地的张数（403/限流/断网）
        self.budget_exhausted = 0  # 预算用尽导致跳过的次数
        self.rejected_tags = 0   # 被本地守卫拒绝、根本没交给对端的昵称数
        self.rejected_names: list[str] = []  # 上面那些昵称（有界，供状态页展示）
        self.unavailable = 0     # 探测不到可用对端的次数
        self.last_error = ""
        self.last_at = 0.0
        #: 已经提示过的事件 key（原因 / 昵称 / tag），避免每轮刷屏
        self._warned: dict[str, float] = {}

    # ------------------------------------------------------------ 探测

    def enabled(self) -> bool:
        """联动总开关。默认 **false**：跨插件耦合 + 影响图库磁盘，必须显式打开。"""
        return self.plugin.bool_cfg("image_pool_sync_enabled", False)

    def resolve(self):
        """解析对端插件实例。返回 ``(实例或 None, 原因)``，**纯查询、不改任何计数**。

        ``star_cls`` 按 AstrBot 源码注释写的是"类对象"，实际持有的是**插件实例**
        （``star_manager`` 加载时执行 ``metadata.star_cls = metadata.star_cls_type(...)``）。
        """
        context = getattr(self.plugin, "context", None)
        getter = getattr(context, "get_registered_star", None)
        if not callable(getter):
            return None, "no_context"
        try:
            star = getter(IMAGE_POOL_PLUGIN_NAME)
        except Exception as exc:
            self.last_error = f"解析实例失败: {type(exc).__name__}: {exc}"
            return None, "resolve_error"
        if star is None:
            return None, "not_installed"
        if not bool(getattr(star, "activated", False)):
            # 停用的插件 star_cls 可能还是旧对象 —— 绝不能往里写
            return None, "plugin_off"
        impl = getattr(star, "star_cls", None)
        if impl is None:
            return None, "not_installed"
        if inspect.isclass(impl):
            # 旧版 / 异常装配下 star_cls 可能是**类**：那时 getattr 拿到的是未绑定函数，
            # callable 判定会误报可用，真调用时 paths 会被当成 self。
            return None, "bad_contract"
        if not callable(getattr(impl, IMAGE_POOL_INGEST_METHOD, None)):
            return None, "old_version"
        return impl, "ok"

    def quality(self) -> str:
        """归档画质（只改 twimg 的 ``name=`` 参数）。

        非法值回落 ``large`` 而不是 ``orig``：``apply_twimg_quality`` 的兜底是 ``orig``
        （原图、最贵），而"配置写错 → 悄悄按最贵档下载"与默认省磁盘的承诺正好相反。
        """
        raw = str(self.plugin.cfg("image_pool_quality", "large") or "").strip().lower()
        if raw in IMAGE_POOL_QUALITIES:
            return raw
        self.warn_once(
            "bad_quality",
            f"图库归档画质取值 {raw!r} 非法，已回落 large（合法值：{', '.join(IMAGE_POOL_QUALITIES)}）。",
        )
        return "large"

    def snapshot(self) -> dict:
        """给 /x状态 与 WebUI 的只读快照（调用它本身不产生任何副作用）。"""
        enabled = self.enabled()
        if enabled:
            impl, reason = self.resolve()
        else:
            impl, reason = None, "sync_off"
        return {
            "enabled": enabled,
            "available": impl is not None,
            "reason": reason,
            "reason_text": IMAGE_POOL_REASON_TEXT.get(reason, reason),
            "posts": self.posts,
            "attempts": self.attempts,
            "added": self.added,
            "skipped": self.skipped,
            "failed": self.failed,
            "timeouts": self.timeouts,
            "tag_failures": self.tag_failures,
            "download_failed": self.download_failed,
            "budget_exhausted": self.budget_exhausted,
            "rejected_tags": self.rejected_tags,
            "rejected_names": list(self.rejected_names[:5]),
            "unavailable": self.unavailable,
            "last_error": self.last_error,
            "last_at": self.last_at,
        }

    # ------------------------------------------------------------ tag

    def tags_for(self, edges, scope: str = "") -> list[str]:
        """把某个账号的**启用订阅**折算成 tag 列表。

        ``scope`` 是调用方给的维度（这里传 handle）：**截断告警必须按账号分开**
        —— 用同一个 key 时，第二个账号的截断会被第一个账号的告警永久吞掉，
        而且日志里点名的是第一个账号的 tag（独立验证 H-5 / J-N2）。

        规则（D1 + 用户确认的多订阅策略）：

        - 只有**显式设置了自定义昵称**的订阅才产生 tag；没设昵称（转发节点回落推主名）→ 不归档。
        - 多个群用不同昵称订阅同一账号 → **每个去重后的非空昵称各入一份 tag**
          （image_pool 天然支持一图多 tag；代价是同一张图按 tag 数各存一份文件）。
        - **仅大小写不同**的昵称（``abc`` / ``ABC``）只取第一个：图库的文件系统不区分大小写，
          两个 tag 会共用同一个目录并互相覆盖文件（v1.7.0 对抗审核实测过这条数据损坏路径），
          所以本地先折叠掉，并且对端也补了同一道闸。
        - 顺序按 ``edge_id`` 稳定排序，保证"截断到上限"时留下的是确定的那几个。
        """
        tags: list[str] = []
        taken: dict[str, str] = {}  # casefold -> 已采用的 tag
        try:
            ordered = sorted(list(edges or []), key=lambda row: int(row["edge_id"]))
        except Exception:
            ordered = list(edges or [])
        for edge in ordered:
            try:
                raw = str(edge["nickname"] or "")
            except Exception:
                continue
            nick = normalize_nick(raw)
            if not nick:
                continue
            tag = pool_tag(nick)
            if not tag:
                self.rejected_tags += 1
                if nick not in self.rejected_names and len(self.rejected_names) < 20:
                    self.rejected_names.append(nick)
                self.warn_once(
                    f"rejected:{nick}",
                    f"昵称「{nick}」不能用作图库分类名（Windows 保留设备名 / 含无法编码的字符 / "
                    "归一化后为空），不会入库；换一个昵称即可。",
                )
                continue
            fold = os.path.normcase(tag)
            if fold in taken:
                if taken[fold] != tag:
                    self.warn_once(
                        f"fold:{fold}",
                        f"昵称「{tag}」与「{taken[fold]}」在文件系统看来是同一个目录名"
                        f"（Windows 不区分大小写）：两者会互相覆盖图片文件，"
                        f"因此只归档「{taken[fold]}」。",
                    )
                continue
            taken[fold] = tag
            if nick != tag:
                self.warn_once(
                    f"nick:{nick}",
                    f"昵称「{nick}」含图库会调整的字符，入库 tag 用「{tag}」（转发节点昵称不受影响）。",
                )
            if any(ch.isspace() for ch in tag):
                # 图库的 /<分类名> 快捷路由按空白切词，带空格的 tag 只能从 /图库 列表 与 WebUI 操作
                self.warn_once(
                    f"space:{tag}",
                    f"tag「{tag}」含空格：图库的 /<分类名> 快捷路由按空白切词，"
                    "这个分类只能用 /图库 列表 与 WebUI 管理（建议昵称里不要带空格）。",
                )
            tags.append(tag)
        limit = max(1, self.plugin.int_cfg("image_pool_max_tags_per_post", 5))
        if len(tags) > limit:
            self.warn_once(
                f"truncate:{scope}:{limit}",
                f"@{scope or '?'} 有 {len(tags)} 个不同昵称，按 image_pool_max_tags_per_post={limit} "
                f"只归档前 {limit} 个：{'、'.join(tags[limit:])} 被跳过。",
            )
            tags = tags[:limit]
        return tags

    # ------------------------------------------------------------ 入库

    async def ingest_post(
        self,
        record: dict,
        tags: list[str],
        uploader: str = "",
        budget: float | None = None,
    ) -> dict:
        """把一条帖子里**照片**归档到图库。返回统计字典，**永不抛异常**。

        ``budget`` 是这次调用允许消耗的秒数（由调用方按整轮总预算递减），
        为 None 时用 ``image_pool_sync_budget_sec``。它同时覆盖**下载**与**入库**两段。
        """
        result = {"added": 0, "skipped": 0, "reason": ""}
        if not self.enabled():
            result["reason"] = "sync_off"
            return result
        if not tags:
            result["reason"] = "no_tag"
            return result
        photos = self._photo_items(record)
        if not photos:
            # 纯文字帖 / 纯视频帖：不碰对端，也不记账（绝大多数帖子走这条路径）
            result["reason"] = "no_photo"
            return result

        impl, reason = self.resolve()
        if impl is None:
            self.unavailable += 1
            result["reason"] = reason
            self.warn_once(
                reason,
                f"图库联动已开启但不可用：{IMAGE_POOL_REASON_TEXT.get(reason, reason)}。"
                "推文推送不受影响。",
            )
            self.last_at = time.time()
            return result

        if budget is None:
            budget = self.plugin.float_cfg("image_pool_sync_budget_sec", 15.0)
        deadline = time.monotonic() + max(0.5, float(budget))

        self.posts += 1
        try:
            added, skipped = await self._ingest(photos, tags, uploader, deadline)
        except asyncio.CancelledError:
            # 关停时必须原样抛出，否则插件停不下来
            raise
        except Exception as exc:
            # _ingest 内部已逐项兜底，这里只兜最后一道：绝不让它影响抓取与推送
            self.failed += 1
            result["reason"] = "error"
            self.last_error = f"{type(exc).__name__}: {exc}"
            logger.warning(f"[x_monitor] 图库入库失败（不影响推送）: {self.last_error}")
        else:
            self.added += added
            self.skipped += skipped
            result["added"] = added
            result["skipped"] = skipped
        self.last_at = time.time()
        return result

    async def _ingest(
        self, photos: list[dict], tags: list[str], uploader: str, deadline: float
    ) -> tuple[int, int]:
        """先落盘再逐个 tag 调对端。返回 ``(added, skipped)``。

        三条硬约束（都来自对抗审核的反例）：

        1. **每个 tag 独立隔离**：``NUL`` 这类坏 tag 会让对端抛异常，若共用一个 try，
           同一条帖子的其余 tag 会一起丢。现在一个 tag 失败只记一次失败，其余继续。
        2. **每个 tag 单独 ``wait_for`` + 每个 tag 前重新解析实例**：一个 tag 卡住不会让
           已完成的计数丢失，也不会继续往已被停用的旧实例写。
        3. **下载失败必须可见**：``_materialize`` 拿不到文件时以前是"静默空操作"，
           与"本来就没图"无法区分 —— 现在计入 ``download_failed`` 并提示一次。
        """
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            self.budget_exhausted += 1
            self.warn_once(
                "budget_exhausted",
                "图库入库预算已用尽，这条帖子未归档（水位已推进，之后不会再补归档）。",
            )
            return (0, 0)
        try:
            paths, missing = await asyncio.wait_for(
                self._materialize(photos), timeout=max(0.5, remaining)
            )
        except asyncio.TimeoutError:
            missing, paths = len(photos), []
        if missing:
            self.download_failed += missing
            self.warn_once(
                "download_failed",
                f"{missing} 张照片没能下载到本地（403 / 限流 / 断网？），这些图不会入库；"
                "推文发送与水位不受影响。",
            )
        if not paths:
            return (0, 0)

        added = skipped = 0
        for tag in tags:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self.budget_exhausted += 1
                self.warn_once(
                    "budget_exhausted",
                    "图库入库预算已用尽，本账号本轮剩余帖子未归档"
                    "（水位已推进，之后不会再补归档）。",
                )
                break
            impl, reason = self.resolve()
            if impl is None:
                self.unavailable += 1
                self.warn_once(
                    reason,
                    f"图库联动已开启但不可用：{IMAGE_POOL_REASON_TEXT.get(reason, reason)}。"
                    "推文推送不受影响。",
                )
                break
            self.attempts += 1
            try:
                out = await asyncio.wait_for(
                    self._call_peer(impl, paths, tag, uploader),
                    timeout=max(0.5, remaining),
                )
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                self.failed += 1
                self.timeouts += 1
                self.last_error = f"tag「{tag}」入库超时"
                self.warn_once(
                    "timeout",
                    "图库入库超时：对端在线程里跑 SQLite，可能稍后才真正写入，"
                    "所以本次计数不含它（水位已推进，不会再补归档）。",
                )
                continue
            except Exception as exc:
                self.failed += 1
                self.tag_failures += 1
                self.last_error = f"tag「{tag}」: {type(exc).__name__}: {exc}"
                self.warn_once(
                    f"tag_failed:{tag}",
                    f"图库入库 tag「{tag}」失败（{type(exc).__name__}: {exc}）；"
                    "同帖其余 tag 继续。",
                )
                continue
            got_added, got_skipped = self._counts(out, tag)
            added += got_added
            skipped += got_skipped
            if not got_added and got_skipped:
                # 对端明确拒收：最常见的是 tag 撞上图库的内置指令名（help / name / 图库…）
                self.warn_once(
                    f"refused:{tag}",
                    f"图库拒收了 tag「{tag}」（{got_skipped} 张跳过）：常见原因是该名字是图库的"
                    "内置指令名（如 help / name / 图库）或与已有分类仅大小写不同；换一个昵称即可。",
                )
        return (added, skipped)

    async def _call_peer(self, impl, paths: list[str], tag: str, uploader: str):
        """调一次对端 ``ingest_files``。

        **同步实现必须放线程**：``wait_for`` 只能取消 await 点，取消不了已经进入的同步调用；
        如果对端是阻塞的同步函数（接口契约不符），直接在事件循环里调会卡死整个 AstrBot。
        """
        method = getattr(impl, IMAGE_POOL_INGEST_METHOD)
        kwargs = {
            "tag": tag,
            "group_id": "",  # D2：X 归档图**不归属任何群**，只有全局管理员能管
            "uploader": uploader or "",
            "source": IMAGE_POOL_SOURCE,
        }
        if inspect.iscoroutinefunction(method):
            return await method(list(paths), **kwargs)
        self.warn_once(
            "sync_peer",
            "image_pool 的 ingest_files 不是协程函数（接口契约不符），已放到线程里调用，"
            "以免阻塞事件循环。",
        )
        out = await asyncio.to_thread(method, list(paths), **kwargs)
        if inspect.isawaitable(out):
            out = await out
        return out

    @staticmethod
    def _counts(out, tag: str) -> tuple[int, int]:
        """解析对端回执。契约是 ``(added, skipped)``，但跨插件调用必须容错。

        对端是**别人维护的插件**：它可能改成 dict、多返回一个值、或返回 None。
        这里不认识就记一次告警并当 0 处理 —— 绝不能因此把整轮轮询炸掉。
        """
        try:
            if isinstance(out, dict):
                return int(out.get("added", 0) or 0), int(out.get("skipped", 0) or 0)
            if isinstance(out, (tuple, list)) and len(out) >= 2:
                return int(out[0] or 0), int(out[1] or 0)
        except Exception:
            pass
        logger.warning(
            f"[x_monitor] image_pool.ingest_files(tag={tag!r}) 回执格式不认识: {out!r}"
        )
        return (0, 0)

    def _photo_items(self, record: dict) -> list[dict]:
        """从帖子记录里挑出**照片**（视频/GIF 不入图库）。"""
        try:
            media = json.loads(record.get("media_json") or "[]")
        except Exception:
            return []
        if not isinstance(media, list):
            return []
        return [
            item
            for item in media
            if isinstance(item, dict)
            and str(item.get("type") or "") == "photo"
            and str(item.get("url") or "")
        ]

    async def _materialize(self, photos: list[dict]) -> tuple[list[str], int]:
        """按 ``image_pool_quality`` 把照片落到本地。

        返回 ``(存在的文件路径, 失败张数)``，路径**保持原顺序**。失败张数必须回传：
        以前"下载失败 → 空列表 → 静默返回"会让最常见的失败（403 / 限流 / 断网）
        在 `/x状态` 上与"本来就没图"完全无法区分。

        与发送共用 ``MediaCache``：画质档位相同则直接命中 ``_prepare_media`` 下好的那份，
        不同档位才会多下一次（缓存键 = 最终 URL，所以互不覆盖）。
        """
        quality = self.quality()
        semaphore = asyncio.Semaphore(2)
        results: list[str | None] = [None] * len(photos)

        async def one(index: int, item: dict) -> None:
            async with semaphore:
                raw = str(item.get("url") or "")
                url = apply_twimg_quality(raw, quality)
                if not url:
                    return
                cached = str(item.get("path") or "")
                if cached and url == raw and Path(cached).is_file():
                    # 与发送用的是同一档画质：直接复用 _prepare_media 下好的那份
                    results[index] = cached
                    return
                path = None
                try:
                    path = await self.plugin.media_cache.fetch(
                        url, client=getattr(self.plugin, "_http", None)
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(f"[x_monitor] 图库入库下载失败 {url}: {exc}")
                if path is not None:
                    results[index] = str(path)

        await asyncio.gather(
            *(one(index, item) for index, item in enumerate(photos)),
            return_exceptions=True,
        )
        paths = [path for path in results if path]
        return (paths, len(photos) - len(paths))

    #: "一次性告警"缓存的容量上限。key 里会嵌 handle / 昵称 / tag，
    #: 长期运行（尤其昵称频繁变动）会缓慢增长；满了就丢最旧的一半，只影响"会不会再提示一次"，
    #: **不影响任何计数**（独立验证 H-6 / J-N7）。
    WARN_CACHE_MAX = 500

    def warn_once(self, key: str, message: str) -> None:
        """同一件事只记一条日志（按 key 去重），之后静默 —— 否则每轮轮询都刷一遍。

        **只静默日志，不静默计数**：调用方该涨的计数照涨，`/x状态` 按计数汇总。
        （早先这里还写了一个 ``note`` 字段声称"供状态页解释原因"，但状态页与页面都不读它 ——
        已删除，避免"文档承诺了但从不显示"这种假接口。）

        **真实保证**（别过度声称，独立验证 L-⑥/N-3 实测过）：
        命中会刷新时间戳，所以"**每轮都在命中**"的持续故障不会被淘汰；
        但淘汰是在**插入新 key**时按时间戳丢最旧一半触发的，所以一个"很久没再命中"的 key
        迟早会被淘汰并允许再次提示 —— 这与"只提示一次"不同，是"每约半个容量次新事件内至多提示一次"。
        """
        if key in self._warned:
            # **命中也要刷新时间戳**：淘汰按时间戳丢最旧的一半，若命中不刷新，
            # 越频繁命中的告警越先被淘汰 → 持续故障（预算用尽 / 下载失败 / 无昵称…）
            # 会从"只说一次"退化成周期性刷屏（独立验证 L-⑥）。
            self._warned[key] = time.time()
            return
        if len(self._warned) >= self.WARN_CACHE_MAX:
            for stale in sorted(self._warned, key=self._warned.get)[: self.WARN_CACHE_MAX // 2]:
                self._warned.pop(stale, None)
        self._warned[key] = time.time()
        logger.warning(f"[x_monitor] {message}")


# ---------------------------------------------------------------- 翻译


class Translator:
    """复用 AstrBot 已配置的 LLM Provider 做翻译。

    - **不向 FxTwitter 请求 Grok 译文**（已决策：省掉每条约一次额外请求）
    - 超时预算 + 连续失败熔断 + 跳过判定（按免费的 ``lang`` 字段，不按语言分 prompt）
    """

    def __init__(self, plugin: "XMonitorPlugin") -> None:
        self.plugin = plugin
        self._consecutive_failures = 0
        self._tripped = False

    def reset_round(self) -> None:
        """每轮轮询开始前重置熔断状态。"""
        self._consecutive_failures = 0
        self._tripped = False

    @property
    def tripped(self) -> bool:
        return self._tripped

    def should_translate(self, text: str, source_lang: str) -> bool:
        """介入判定（纯函数式，便于测试）。"""
        cfg = self.plugin
        if not cfg.bool_cfg("translate_enabled", False):
            return False
        if self._tripped:
            return False
        body = (text or "").strip()
        if len(body) < int(cfg.int_cfg("min_len", 4)):
            return False
        target = str(cfg.cfg("target_lang", "zh-CN") or "zh-CN").lower()
        lang = str(source_lang or "").strip().lower()
        if lang and lang == target.split("-")[0]:
            return False
        skip = {s.lower() for s in to_str_list(cfg.cfg("skip_langs", []))}
        if lang and lang in skip:
            return False
        if chinese_ratio(body) >= float(cfg.cfg("min_chinese_ratio_skip", 0.35) or 0.35):
            return False
        return True

    def build_prompt(self, text: str, source_lang: str) -> str:
        target = str(self.plugin.cfg("target_lang", "zh-CN") or "zh-CN")
        glossary = to_str_list(self.plugin.cfg("translate_glossary", []))
        by_lang = {}
        for item in to_str_list(self.plugin.cfg("translate_prompts_by_lang", [])):
            if "=" in item:
                key, value = item.split("=", 1)
                by_lang[key.strip().lower()] = value.strip()
        template = by_lang.get(str(source_lang or "").lower()) or str(
            self.plugin.cfg(
                "translate_prompt",
                "把下面的推文翻译成{target_lang}。只输出译文本身，不要解释、不要加引号、"
                "保留原意与语气，专有名词按通用译法处理。",
            )
        )
        prompt = template.replace("{target_lang}", target).replace(
            "{source_lang}", str(source_lang or "auto")
        )
        if glossary:
            prompt += "\n术语对照（优先采用）：" + "；".join(glossary)
        return f"{prompt}\n\n---\n{text}"

    async def translate(self, umo: str, text: str, source_lang: str) -> str:
        if not self.should_translate(text, source_lang):
            return ""
        timeout = float(self.plugin.cfg("translate_timeout_sec", 30) or 30)
        try:
            result = await asyncio.wait_for(
                self._call_provider(umo, self.build_prompt(text, source_lang)), timeout
            )
        except asyncio.TimeoutError:
            self._on_failure("timeout")
            return ""
        except Exception as exc:
            self._on_failure(f"{type(exc).__name__}: {exc}")
            return ""
        body = (result or "").strip()
        if not body:
            self._on_failure("empty")
            return ""
        self._consecutive_failures = 0
        return body

    def _on_failure(self, reason: str) -> None:
        self._consecutive_failures += 1
        logger.warning(
            f"[x_monitor] 翻译失败（{reason}），连续 {self._consecutive_failures} 次"
        )
        limit = int(self.plugin.cfg("translate_max_consecutive_failures", 2) or 2)
        if self._consecutive_failures >= limit:
            self._tripped = True
            logger.warning("[x_monitor] 本轮翻译已熔断，后续推文只发原文")

    async def _call_provider(self, umo: str, prompt: str) -> str:
        context = self.plugin.context
        provider = None
        provider_id = str(self.plugin.cfg("translation_provider_id", "") or "").strip()
        try:
            if provider_id:
                getter = getattr(context, "get_provider_by_id", None)
                if callable(getter):
                    provider = getter(provider_id)
                    if inspect.isawaitable(provider):
                        provider = await provider
            if provider is None:
                getter = getattr(context, "get_using_provider_async", None)
                if callable(getter):
                    provider = await getter(umo)
            if provider is None:
                # 老内核只有同步版 get_using_provider（已废弃但仍在）；没有异步版时兜底，
                # 否则翻译会静默全失效，用户只看到"永远只有原文"。
                getter = getattr(context, "get_using_provider", None)
                if callable(getter):
                    try:
                        provider = getter(umo)
                    except TypeError:
                        provider = getter()
                    if inspect.isawaitable(provider):
                        provider = await provider
        except Exception as exc:
            raise RuntimeError(f"provider lookup failed: {exc}") from exc
        if provider is None:
            raise RuntimeError("no provider available")
        chat = getattr(provider, "text_chat", None)
        if not callable(chat):
            raise RuntimeError("provider has no text_chat")
        response = await chat(
            prompt=prompt,
            system_prompt="你是专业翻译，只输出译文。",
        )
        for attr in ("completion_text", "text", "content"):
            value = getattr(response, attr, None)
            if value:
                return str(value)
        if isinstance(response, str):
            return response
        raise RuntimeError("empty provider response")


def normalize_chain_names(value) -> list[str]:
    """把 ``fetch_chain`` 配置归一成"去空、去重、保序、剔除未知项"的名字列表。

    保序是关键：降级链的顺序就是用户表达优先级的方式。
    """
    out: list[str] = []
    for item in to_str_list(value):
        name = item.strip().lower()
        if name in PROVIDER_NAMES and name not in out:
            out.append(name)
    return out


def build_provider_chain(
    client,
    config_getter,
    int_getter,
    float_getter,
) -> ProviderChain | None:
    """按配置装配降级链。``client`` 为 None（未初始化）时返回 None。

    刻意**每次读配置**而不是把参数固化进 ProviderChain：链的构造很轻，
    重建比重载插件便宜得多，而"改了 fetch_chain 必须重启"是纯粹的坏体验。
    """
    if client is None:
        return None
    names = normalize_chain_names(config_getter("fetch_chain", DEFAULT_FETCH_CHAIN))
    for raw in to_str_list(config_getter("fetch_chain", [])):
        if raw.strip().lower() not in PROVIDER_NAMES:
            logger.warning(f"[x_monitor] fetch_chain 里有未知数据源「{raw}」，已忽略")
    if not names:
        logger.error(
            "[x_monitor] fetch_chain 为空或全是未知项，抓取将完全停摆；"
            f"可选项：{', '.join(PROVIDER_NAMES)}"
        )
        return None

    providers: list[FetchProvider] = []
    for name in names:
        if name == PROVIDER_FXTWITTER:
            providers.append(
                FxTwitterProvider(client, base=config_getter("fxtwitter_api_base", FXTWITTER_BASE))
            )
        elif name == PROVIDER_FXTWITTER_RSS:
            providers.append(
                RssProvider(client, base=config_getter("rss_base", FXTWITTER_RSS_BASE))
            )
        elif name == PROVIDER_RSSHUB:
            providers.append(
                RssHubProvider(client, instances=to_str_list(config_getter("rsshub_instances", [])))
            )
    return ProviderChain(
        providers,
        threshold=int_getter("provider_failover_threshold", 1),
        cooldown=float_getter("provider_cooldown_sec", 600),
        probe_handle=str(config_getter("probe_handle", DEFAULT_PROBE_HANDLE) or ""),
    )


# ---------------------------------------------------------------- 插件主体


@register(PLUGIN_NAME, "califorliu", "X（原 Twitter）帖子监控与转发", PLUGIN_VERSION)
class XMonitorPlugin(Star):
    #: WebUI 路由表：(子路径, 处理方法名, HTTP 方法, 说明)。
    #: 注册时会自动加上插件名前缀 —— 路由**必须**带前缀，而页面侧的 endpoint 不能带。
    ROUTES = (
        ("overview", "api_overview", ["GET"], "总览：账号 / 群 / 订阅 / 运行状态"),
        ("config", "api_config_get", ["GET"], "读取配置 schema 与当前值"),
        ("config/save", "api_config_save", ["POST"], "保存全局配置"),
        ("edge/add", "api_edge_add", ["POST"], "新增订阅（账号 × 群）"),
        ("edge/update", "api_edge_update", ["POST"], "修改订阅：昵称 / 开关 / 仅媒体"),
        ("edge/remove", "api_edge_remove", ["POST"], "删除订阅"),
        ("author/remove", "api_author_remove", ["POST"], "删除账号及其全部订阅"),
        ("group/dnd", "api_group_dnd", ["POST"], "设置某群免打扰时段"),
        ("test/send", "api_test_send", ["POST"], "发送一条链路自检消息"),
        ("test/replay", "api_test_replay", ["POST"], "回放某账号最近 n 条"),
    )

    def __init__(self, context: Context, config: AstrBotConfig) -> None:
        super().__init__(context)
        self.config = config
        self.base_dir = Path(StarTools.get_data_dir(PLUGIN_NAME))
        self.db_path = self.base_dir / "xmonitor.sqlite3"
        self._init_db()
        self._register_web_api()

    def _init_db(self) -> None:
        """建库并装配所有同步可用的组件。

        刻意把「无需事件循环即可完成」的初始化都收在这里（而不是 ``__init__``）：
        离线自检会绕过 ``__init__`` 直接调本方法，收敛后两条路径行为一致，
        也避免出现「某个属性只在真实构造时才存在」这种只在测试里暴露的脆弱点。
        """
        base = Path(getattr(self, "base_dir", Path(self.db_path).parent))
        self.base_dir = base
        self.media_dir = base / "media"
        self.media_dir.mkdir(parents=True, exist_ok=True)
        self.store = Store(Path(self.db_path))
        self.media_cache = MediaCache(
            self.media_dir,
            quota_mb=self.int_cfg("cache_quota_mb", 512),
            ttl_days=self.int_cfg("cache_ttl_days", 30),
        )
        #: image_pool 联动兼容层（P7）。构造期只装配，不解析对端实例 ——
        #: 对端可能在本插件之后才加载完成，所以解析必须推迟到每次调用前。
        self._pool_bridge = ImagePoolBridge(self)
        self._translator = Translator(self)
        self._http = getattr(self, "_http", None)
        self._provider = getattr(self, "_provider", None)
        self._chain_names = getattr(self, "_chain_names", ())
        #: handle -> (数据源名, 时间戳)。排障用："这个账号现在靠哪个源供数"。
        #: 只保留最近若干条，避免长期运行后无界增长。
        self._source_map: dict[str, tuple[str, float]] = getattr(self, "_source_map", {})
        self._tasks = getattr(self, "_tasks", [])
        # 轮询循环与投递循环各用各的唤醒事件：共用一个会互相吞掉对方的唤醒
        self._poll_wake = asyncio.Event()
        self._deliver_wake = asyncio.Event()
        self._stop = asyncio.Event()
        self._draining = False
        self._last_send_ts = 0.0
        self._force_poll = False
        #: 积压丢弃通知的冷却（umo -> 上次通知时间），防止持续超限时刷屏
        self._backlog_notified: dict[str, float] = {}

    async def initialize(self) -> None:
        self._http = self._build_http_client()
        self._provider = self._ensure_chain()
        self._stop.clear()
        self._force_poll = True  # 启动后立刻跑一轮，不必等满一个间隔
        self._tasks = [
            asyncio.create_task(self._poller_loop(), name="x_monitor_poller"),
            asyncio.create_task(self._delivery_loop(), name="x_monitor_delivery"),
        ]
        logger.info(
            f"[x_monitor] 已启动：轮询间隔 {self.int_cfg('poll_interval_minutes', 5)} 分钟，"
            f"订阅 {len(self.store.list_edges())} 条，"
            f"数据源链 {' > '.join(self._provider.names) if self._provider else '（空！）'}"
        )

    # ------------------------------------------------------------ 数据源降级链

    def _config_chain_names(self) -> list[str]:
        return normalize_chain_names(self.cfg("fetch_chain", DEFAULT_FETCH_CHAIN))

    def _ensure_chain(self) -> ProviderChain | None:
        """按需构建 / 重建降级链（配置改了无需重启）。

        两条不变量：

        1. 只有拿到**真正的 HTTP 客户端**才重建：离线自检会把替身链直接塞进
           ``_provider``（此时 ``_http`` 是 None 或一个哨兵对象），绝不能被覆盖。
        2. 配置没变的提供者**复用旧实例**。重建会丢掉 RSSHub 的实例健康状态；
           而 WebUI 每保存一次配置就会走一遍这里，不复用的话"实例池冷却"
           会被每一次保存悄悄清空。
        """
        if not callable(getattr(self._http, "get", None)):
            return self._provider
        candidate = build_provider_chain(
            self._http, self.cfg, self.int_cfg, self.float_cfg
        )
        current = self._provider
        if candidate is None:
            self._provider = None
            self._chain_names = ()
            return None
        if isinstance(current, ProviderChain) and current.same_config(candidate):
            return current
        # 重建时把**同名同配置**的提供者实例留给新链，且尽量保留它的熔断统计：
        # 否则 WebUI 每保存一次配置就会把"某源正在降级中"这件事悄悄抹掉，
        # 而重建本身只是因为链的组成变了（增删了某一层）。
        if isinstance(current, ProviderChain):
            candidate.adopt(current)
        self._provider = candidate
        self._chain_names = tuple(candidate.names)
        return candidate

    def _record_source(self, handle: str, provider_name: str) -> None:
        if not handle or not provider_name:
            return
        self._source_map[handle.lower().lstrip("@")] = (str(provider_name), time.time())
        if len(self._source_map) > 200:
            oldest = min(self._source_map, key=lambda key: self._source_map[key][1])
            self._source_map.pop(oldest, None)

    def record_result_sources(self, handle: str, result: FetchResult) -> None:
        """把一次抓取的生效源记到归属表上（``ok`` 为假时不记）。"""
        if result is None or not result.ok:
            return
        name = str(result.provider or result.source or "")
        if name and name != "chain":
            self._record_source(handle, name)

    def source_for(self, handle: str) -> str:
        """某个账号当前生效的数据源；没抓过则返回链的名义第一顺位。"""
        entry = self._source_map.get(str(handle or "").lower().lstrip("@"))
        if entry:
            return str(entry[0])
        chain = self._provider
        return chain.active_name() if isinstance(chain, ProviderChain) else ""

    def provider_health(self) -> dict:
        """给 /x状态、/x源 与 WebUI 用的数据源健康快照。"""
        chain = self._provider
        if not isinstance(chain, ProviderChain):
            return {
                "configured": self._config_chain_names(),
                "chain": [],
                "active": "",
                "pinned": "",
                "degraded": False,
                "message": "抓取链为空（fetch_chain 里没有可用的数据源）"
                if self._http is not None
                else "插件尚未初始化完成",
            }
        snapshot = chain.snapshot()
        configured = self._config_chain_names()
        return {
            "configured": configured,
            "chain": snapshot,
            "active": chain.active_name(),
            "pinned": chain.pinned,
            "degraded": chain.degraded(),
            "message": "",
        }

    def _chain_unavailable_message(self) -> str:
        """链不可用时的**准确**文案：区分"没初始化"与"配置里没有源"。

        两者原因和处置完全不同：前者等几秒，后者要去改 `fetch_chain`。
        一律回"尚未初始化完成"会让管理员一直等重启，永远想不到是配置问题。
        """
        if self._http is None:
            return "插件尚未初始化完成，请稍后再试"
        return "抓取链为空（fetch_chain 里没有可用的数据源），请检查插件配置"

    def _build_http_client(self):
        """构造共享的 httpx 客户端。

        代理参数名在不同 httpx 版本间变过（``proxies`` → ``proxy``），
        因此**只在确实配置了代理时**才传，并做一次兼容回退，
        避免旧版本上因未知关键字直接抛 TypeError。
        """
        kwargs = {
            "timeout": httpx.Timeout(20.0, connect=10.0),
            "follow_redirects": True,
            "headers": {"User-Agent": DEFAULT_USER_AGENT, "Accept": "application/json"},
        }
        proxy = self._proxy()
        if not proxy:
            return httpx.AsyncClient(**kwargs)
        try:
            return httpx.AsyncClient(proxy=proxy, **kwargs)
        except TypeError:
            return httpx.AsyncClient(proxies=proxy, **kwargs)

    async def terminate(self) -> None:
        self._stop.set()
        self._poll_wake.set()
        self._deliver_wake.set()
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._tasks = []
        if self._http is not None:
            try:
                await self._http.aclose()
            except Exception:
                pass
            self._http = None
        logger.info("[x_monitor] 已停止")

    def _proxy(self) -> str | None:
        value = str(self.cfg("http_proxy", "") or "").strip()
        return value or None

    # ------------------------------------------------------------ 配置读取

    def cfg(self, key: str, default):
        try:
            value = self.config.get(key, default)
        except Exception:
            return default
        return default if value is None else value

    def bool_cfg(self, key: str, default: bool) -> bool:
        value = self.cfg(key, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in ("1", "true", "yes", "on", "是", "开")
        return bool(value)

    def int_cfg(self, key: str, default: int) -> int:
        try:
            return int(float(self.cfg(key, default)))
        except Exception:
            return default

    def float_cfg(self, key: str, default: float) -> float:
        try:
            return float(self.cfg(key, default))
        except Exception:
            return default

    # ------------------------------------------------------------ 权限模型
    # 与 astrbot_plugin_image_pool 同一套：handler 内自校验，不用 PermissionType。

    def _in_id_list(self, event: AstrMessageEvent, key: str, allow_group: bool = True) -> bool:
        """名单匹配：发送者 ID / 完整 umo，可选再匹配裸群号。

        ``allow_group`` 只对**群名单**为真。管理员名单绝不能匹配裸群号——
        否则「全局管理员」里误填一个群号，那个群的**所有人**都成了全局管理员。
        """
        ids = set(to_str_list(self.cfg(key, [])))
        if not ids:
            return False
        if str(event.get_sender_id()) in ids or event.unified_msg_origin in ids:
            return True
        return allow_group and str(event.get_group_id() or "") in ids

    def _is_global_admin(self, event: AstrMessageEvent) -> bool:
        """全局管理员：插件配置的 plugin_admins（不借道 AstrBot 全局管理员）。"""
        return self._in_id_list(event, "plugin_admins", allow_group=False)

    def _is_group_admin(self, event: AstrMessageEvent) -> bool:
        """群级管理员：``群号:UID``；``*:UID`` 表示所有群。"""
        uid = str(event.get_sender_id())
        gid = str(event.get_group_id() or "")
        pairs = set(to_str_list(self.cfg("group_admins", [])))
        return f"{gid}:{uid}" in pairs or f"*:{uid}" in pairs

    def group_mode(self, event: AstrMessageEvent) -> str:
        """解析当前会话的权限模式：whitelist | readonly | none。"""
        if event.get_message_type() != MessageType.GROUP_MESSAGE:
            mode = str(self.cfg("private_mode", "readonly")).strip().lower()
            return mode if mode in VALID_MODES else "readonly"
        if self._in_id_list(event, "open_groups"):
            return "whitelist"
        if self._in_id_list(event, "readonly_groups"):
            return "readonly"
        mode = str(self.cfg("default_group_mode", "none")).strip().lower()
        return mode if mode in VALID_MODES else "none"

    def can(self, event: AstrMessageEvent, action: str) -> bool:
        """action: subscribe | view。"""
        if self._is_global_admin(event):
            return True
        mode = self.group_mode(event)
        return bool(MODE_CAPS.get(mode, {}).get(action, False))

    def manage_scope(self, event: AstrMessageEvent) -> str:
        """管理范围：all（整库）/ local（仅本群）/ none。"""
        if self._is_global_admin(event):
            return "all"
        if self._is_group_admin(event):
            return "local"
        if self.bool_cfg("readonly_allow_manage", False) and self.group_mode(event) == "whitelist":
            return "local"
        return "none"

    def can_manage_edge(self, event: AstrMessageEvent, edge_row) -> tuple[bool, str]:
        """群级管理员只能碰本群的订阅。"""
        scope = self.manage_scope(event)
        if scope == "none":
            return False, self.denied_hint(event)
        if scope == "local" and str(edge_row["umo"]) != str(event.unified_msg_origin):
            return False, "⛔ 群级管理员只能管理本群订阅的账号（这一条属于其他群）。"
        return True, ""

    def denied_hint(self, event: AstrMessageEvent) -> str:
        admins = to_str_list(self.cfg("plugin_admins", [])) + to_str_list(
            self.cfg("group_admins", [])
        )
        if not admins:
            return (
                "⛔ 本插件尚未配置任何管理员。\n"
                "请在 WebUI 插件配置里填「全局管理员」（UID，用 /x在哪 查看）"
                "或「群级管理员」（群号:UID）。\n"
                "该权限与 AstrBot 全局管理员互相独立，避免权限越界。"
            )
        return (
            f"⛔ 你的 ID（{event.get_sender_id()}）没有管理权限。\n"
            "群级管理员只能管理本群订阅；群模式与授权仅限全局管理员。"
        )

    def mode_hint(self, event: AstrMessageEvent) -> str:
        mode = self.group_mode(event)
        if mode == "readonly":
            return "⛔ 本群是「只读群」：只有管理员能增删订阅，其他人可以查看。"
        if mode == "none":
            return (
                "⛔ 本群未配置权限模式（默认仅管理员可操作）。\n"
                "管理员可把本群加入插件配置的「白名单群」或「只读群」以放开权限，"
                "也可以用 /x群模式 调整。"
            )
        return "⛔ 你没有权限执行该操作。"

    # ------------------------------------------------------------ 推文解析

    def post_passes_filters(self, obj: dict) -> bool:
        kind = classify_post(obj)
        if kind == KIND_REPOST and self.bool_cfg("filter_reposts", True):
            return False
        if kind == KIND_REPLY and self.bool_cfg("filter_replies", True):
            return False
        if kind == KIND_QUOTE and self.bool_cfg("filter_quotes", False):
            return False
        return True

    def to_post_record(self, obj: dict) -> dict | None:
        """把 FxTwitter 的 status 对象转成入库记录。返回 None 表示被过滤。"""
        if not self.post_passes_filters(obj):
            return None
        post_id = str(obj.get("id") or "")
        if not post_id:
            return None
        media = extract_media(obj)
        return {
            "post_id": post_id,
            "created_ts": post_sort_key(obj),
            "kind": classify_post(obj),
            "body_json": json.dumps(obj, ensure_ascii=False),
            "media_json": json.dumps(media, ensure_ascii=False),
            "has_media": 1 if media else 0,
        }

    # ------------------------------------------------------------ 轮询

    def _poll_interval_seconds(self) -> float:
        minutes = max(1, self.int_cfg("poll_interval_minutes", 5))
        return float(minutes) * 60.0

    async def _poller_loop(self) -> None:
        """固定心跳 + 到点判断，而不是 ``sleep(间隔)``：改配置无需重启即可生效。

        必须**自己**守住间隔：``poll_once`` 结束后唤醒事件很可能已被置位（刚入队就会置位），
        若无条件重跑就会退化成贴地连续轮询，``poll_interval_minutes`` 形同虚设。
        """
        tick = 5.0
        last_poll = 0.0
        while not self._stop.is_set():
            interval = self._poll_interval_seconds()
            if self._force_poll or time.time() - last_poll >= interval:
                self._force_poll = False
                last_poll = time.time()
                try:
                    await self.poll_once()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.error(f"[x_monitor] 轮询异常: {exc}", exc_info=True)
            try:
                await asyncio.wait_for(self._poll_wake.wait(), timeout=tick)
            except asyncio.TimeoutError:
                pass
            self._poll_wake.clear()

    async def poll_once(self) -> dict:
        """跑一轮：抓取所有被订阅的账号。返回统计，便于测试与排障。"""
        chain = self._ensure_chain()
        if chain is None:
            # 区分"没初始化"与"链配空了"：后者是用户配置问题，必须能看出来
            return {"skipped": "not_initialized" if self._http is None else "empty_chain"}
        self._translator.reset_round()
        author_ids = self.store.subscribed_author_ids()
        if not author_ids:
            return {"authors": 0, "fetched": 0, "enqueued": 0}

        semaphore = asyncio.Semaphore(max(1, self.int_cfg("fetch_concurrency", 3)))
        stats = {"authors": 0, "fetched": 0, "enqueued": 0, "errors": 0}

        async def one(author_id: str) -> None:
            async with semaphore:
                try:
                    result = await self.poll_author(author_id)
                except Exception as exc:
                    # 单个账号的异常不能拖垮整轮；gather 里返回异常对象会让后面的
                    # result.get(...) 直接炸掉，所以在这里就收敛成错误字典。
                    logger.error(
                        f"[x_monitor] 账号 {author_id} 轮询异常: {exc}", exc_info=True
                    )
                    result = {"error": f"{type(exc).__name__}: {exc}"}
            stats["authors"] += 1
            stats["fetched"] += result.get("fetched", 0)
            stats["enqueued"] += result.get("enqueued", 0)
            stats["errors"] += 1 if result.get("error") else 0

        await asyncio.gather(*(one(a) for a in author_ids), return_exceptions=True)
        if stats["enqueued"]:
            self._deliver_wake.set()
        return stats

    async def poll_author(self, author_id: str) -> dict:
        row = self.store.get_author(author_id)
        if row is None:
            return {"error": "unknown_author"}
        chain = self._ensure_chain()
        if chain is None:
            return {"error": "empty_chain"}
        handle = str(row["handle"])
        since_ts = int(row["last_seen_ts"] or 0)
        baseline_ready = bool(row["baseline_ready"])

        result = await chain.fetch_user(
            handle,
            since_ts=since_ts,
            with_replies=self.bool_cfg("include_replies", False),
        )
        if not result.ok:
            # 关键：网络失败**绝不**写水位、**绝不**置基线
            reason = result.error or ("not found" if result.not_found else "fetch failed")
            logger.warning(
                f"[x_monitor] @{handle} 抓取失败（{reason}），本轮保留水位 {since_ts}"
            )
            return {"error": reason}
        self.record_result_sources(handle, result)

        # RSS 源可能拿不到 channel 级作者信息，退回用条目作者（尽力而为）
        if not result.author and result.posts:
            for item in result.posts:
                candidate = item.get("author") if isinstance(item, dict) else None
                if isinstance(candidate, dict) and candidate.get("screen_name"):
                    result.author = candidate
                    break

        if result.author:
            author = result.author
            self.store.upsert_author(
                author_id,
                str(author.get("screen_name") or handle),
                str(author.get("name") or ""),
                str(author.get("avatar_url") or ""),
            )

        # 按 id 去重：with_replies=true 会插入被回复原帖，实测同一 id 出现两次
        statuses = dedupe_by_id(result.posts)
        records = [rec for rec in (self.to_post_record(o) for o in statuses) if rec]
        # 水位必须按**过滤前**的原始推文算：否则一个"只发转推"的账号在
        # filter_reposts=true 下会一条记录都不剩，水位被置成 now，之后永久漏帖。
        newest_any = max((post_sort_key(o) for o in statuses), default=0)

        if not baseline_ready:
            # 首次成功抓取：只建立基线，不补发历史
            seen = newest_any if newest_any > 0 else int(time.time())
            self.store.mark_baseline(author_id, seen)
            logger.info(f"[x_monitor] @{handle} 已建立基线（不补发历史），水位 {seen}")
            return {"fetched": 0, "enqueued": 0, "baseline": seen}

        edges = self.store.enabled_edges_for_author(author_id)
        if not edges:
            return {"fetched": 0, "enqueued": 0}

        if not records:
            # 全被过滤掉了：不投递，但水位仍要推进，否则每轮都重复拉同一批
            if newest_any > since_ts:
                await asyncio.to_thread(
                    self.store.commit_poll, author_id, [], list(edges), newest_any, False
                )
            return {"fetched": 0, "enqueued": 0}
        if not edges:
            return {"fetched": 0, "enqueued": 0}

        if self.media_download_enabled():
            try:
                records = await self._prepare_media(records)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # 本地下载只是"更好的发送方式"，它失败绝不能让这一轮的水位与入队一起丢——
                # 那些记录仍然可以按直链发送。
                logger.warning(f"[x_monitor] 媒体预处理失败，本轮按直链发送: {exc}")

        new_seen = max(since_ts, newest_any)
        inserted, enqueued = await asyncio.to_thread(
            self.store.commit_poll, author_id, records, list(edges), new_seen, False
        )
        if inserted or enqueued:
            logger.info(
                f"[x_monitor] @{handle} 新增 {inserted} 条，入队 {enqueued} 条投递"
            )

        # P7 图库联动：**在 commit_poll 之后**归档。
        # 放在后面是为了"归档集合 == 真正落库的集合"：commit 失败时本轮一张都不归档，
        # 下轮重抓（水位没推进）会补上；反过来先归档再 commit 失败，就白白归档了一批
        # 永远不会入库的帖子（虽然会被 sha256 去重，但仍是白干的活）。
        # 入库本身失败在两种顺序下都是**有损**的（水位已推进），所以失败必须写进
        # 日志与 /x状态，而不是假装能自动重试。
        await self._ingest_to_pool(records, edges, handle, inserted, enqueued)
        return {"fetched": inserted, "enqueued": enqueued}

    async def _ingest_to_pool(
        self,
        records: list[dict],
        edges: list,
        handle: str,
        inserted: int = 0,
        enqueued: int = 0,
    ) -> None:
        """把本轮新帖的照片归档进 image_pool（P7 联动）。**绝不抛异常**。

        - 与投递 / 免打扰完全解耦（D3）：图库是归档，免打扰时段照常入库；
          是否推送、是否「仅媒体」都不影响它。
        - **有界**：本账号本轮共享 ``image_pool_sync_budget_sec`` 的预算，用尽即停。
          一条推文不能把持着 ``fetch_concurrency`` 名额把其它账号饿死。
        - 预算用尽 / 入库失败都是**有损**的（水位已推进，之后不会再重抓），
          所以必须把这件事说出来（一次性日志 + 累计计数 + `/x状态`），而不是静默少归档几张。
        - ``inserted`` 与 ``enqueued`` **都**为 0 时直接返回：本轮既没有新帖落库、
          也没有新入队（重启后重抓同一窗口等），再调对端只是白干的活。
          **只看 ``inserted`` 是错的**（独立验证的反例 N1）：同一个 status id 会出现在
          两个被监控账号的抓取里（A 原创、B 转推，``filter_reposts=false`` 时），
          B 轮 ``inserted=0`` 但 ``enqueued=1`` —— 帖子确实会投递，图却一张都不归档。

        **返回值刻意是 ``None``**：调用方只关心"有没有影响主流程"，而"入库了多少 / 为什么
        没入库"由 ``ImagePoolBridge`` 的累计计数、一次性日志与 `/x状态` 承担。
        早先这里返回过一个 ``stats`` 字典，但没有任何调用方读它 —— 其中的 ``reason``
        于是成了与已删除的 ``note`` 同类的**死字段**（独立验证 H-8）。
        """
        bridge = getattr(self, "_pool_bridge", None)
        if bridge is None or not bridge.enabled():
            return
        if not inserted and not enqueued:
            return
        rejected_before = bridge.rejected_tags
        tags = bridge.tags_for(edges, scope=handle)
        if not tags:
            if bridge.rejected_tags > rejected_before:
                # 用户**设了**昵称，只是不能用作图库分类名（NUL / 孤立代理…）。
                # 具体原因已由 tags_for 的 warn_once 说过，这里不再重复、也不能覆盖成"没设昵称"。
                return
            # 该账号没有任何设置了自定义昵称的订阅 → 按 D1 不入库。
            # 这条以前完全静默：用户看到"累计入库 0 张"却没有任何解释，属于静默失败。
            bridge.warn_once(
                f"no_tag:{handle}",
                f"@{handle} 的订阅都没有设置自定义昵称 → 图库联动不会归档任何图；"
                "用 /x昵称 <handle> <昵称> 设置后才入库。",
            )
            return
        added = skipped = posts = 0
        try:
            budget = max(0.5, self.float_cfg("image_pool_sync_budget_sec", 15.0))
            deadline = time.monotonic() + budget
            for record in records:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    logger.warning(
                        f"[x_monitor] @{handle} 图库入库预算（{budget:.0f}s）已用尽，"
                        f"剩余帖子本轮不入库；这些图之后不会再被重抓，将永久不归档。"
                    )
                    break
                result = await bridge.ingest_post(
                    record, tags, uploader=handle, budget=remaining
                )
                got_added = int(result.get("added") or 0)
                got_skipped = int(result.get("skipped") or 0)
                if got_added or got_skipped:
                    posts += 1
                    added += got_added
                    skipped += got_skipped
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # 兜底：兼容层里任何未预料的异常都不许影响抓取、水位与推送
            logger.warning(f"[x_monitor] 图库联动异常（不影响推送）: {exc}")
        if added or skipped:
            logger.info(
                f"[x_monitor] @{handle} 图库入库：新增 {added} 张，"
                f"跳过 {skipped} 张（{posts} 条帖子）"
            )

    def pool_tag_state_and_rejected(self) -> tuple[str, list[str]]:
        """**一次遍历**同时给出状态与被本地守卫拒绝的昵称。返回 ``(状态, 被拒昵称)``。

        `/x状态` 与 WebUI 两处都要这两样东西，分两次调用会把 edges 全表扫两遍
        （独立验证 N-5 实测该路径扫描翻倍）。

        每个状态都对应一种"看起来一样、原因完全不同"的情形，必须分开说，否则状态页会撒谎：

        - ``no_nickname``：所有**启用**的订阅都没设昵称；
        - ``rejected_only``：设了昵称，但没有一个能用作图库分类名（``NUL`` / 孤立代理…）；
        - ``paused_only``：昵称只存在于**暂停推送**（``enabled=0``）的订阅上 ——
          直接归成 ``no_nickname`` 会让用户去"设昵称"，而其实该做的是 `/x推送 开`
          （独立验证的反例 H-4 / J-N3）；
        - ``unknown``：读订阅表失败 —— 直接归成 ``no_nickname`` 同样是撒谎（H-2 / J-N6）。
        """
        try:
            rows = self.store.list_edges()
        except Exception as exc:
            logger.warning(f"[x_monitor] 读取订阅列表失败，无法判断图库联动状态: {exc}")
            return "unknown", []
        enabled_nick = usable = paused_nick = False
        rejected: list[str] = []
        for row in rows:
            try:
                nick = normalize_nick(row["nickname"])
                if not nick:
                    continue
                if int(row["enabled"] or 0):
                    enabled_nick = True
                    if pool_tag(nick):
                        usable = True
                    elif nick not in rejected:
                        rejected.append(nick)
                else:
                    paused_nick = True
            except Exception:
                continue
        if usable:
            state = "ready"
        elif enabled_nick:
            state = "rejected_only"
        elif paused_nick:
            state = "paused_only"
        else:
            state = "no_nickname"
        return state, rejected

    def pool_tag_state(self) -> str:
        """联动能不能出图（``ready`` / ``no_nickname`` / ``rejected_only`` / ``paused_only`` / ``unknown``）。"""
        return self.pool_tag_state_and_rejected()[0]

    def pool_rejected_nicknames(self) -> list[str]:
        """启用的订阅里"设了昵称但不能用作图库分类名"的那些（去重保序）。"""
        return self.pool_tag_state_and_rejected()[1]

    def _effective_media_url(self, item: dict) -> str:
        """算出这条媒体最终要用的 URL —— **画质档位只在这里决定一次**。

        合并转发会把图片 base64 内联进 payload，实测 X 的 ``orig`` 原图比普通采集图
        大一个数量级，所以 forward 模式默认降到 ``large``。

        同时这个 URL 就是缓存键：改画质档位后不会误命中旧档位的文件。
        """
        url = str(item.get("url") or "")
        if not url:
            return ""
        if str(item.get("type")) != "photo":
            return url
        if str(self.cfg("send_mode", "forward")).lower() == "forward":
            quality = str(self.cfg("forward_image_quality", "large") or "large")
        else:
            quality = str(self.cfg("image_quality", "orig") or "orig")
        return apply_twimg_quality(url, quality)

    def media_download_enabled(self) -> bool:
        """``media_mode == "download"`` 吗。

        **曾经写成 ``self.bool_cfg("media_mode", "download") == "download"``**：
        ``bool_cfg`` 会把字符串解析成布尔，``False == "download"`` 恒为假 →
        本地下载这条路径**从来没被调用过**，选"本地下载（推荐）"的用户实际一直在走直链。
        这里统一成一个 helper，避免两处调用点再各写一遍、再错一遍。
        """
        return str(self.cfg("media_mode", "download") or "").strip().lower() == "download"

    async def _prepare_media(self, records: list[dict]) -> list[dict]:
        """按最终画质下载媒体，并把最终 URL 写回记录。

        下载失败只是没有本地路径，发送时会自动回退为直链组件，不阻塞入库。

        **视频只在 ``send_video`` 打开时才下载**：``_video_items`` 在关闭时直接返回空，
        下载下来的文件永远不会被发送，只会白占缓存与带宽（一条 4K 视频几十 MB，
        而 X 时间线里视频帖占比可以很高——实测某个真实 feed 是 19/90）。

        同一条帖内的多张图**并发下载**（有并发上限）：``poll_author`` 是串行 await 的，
        而它占着 ``fetch_concurrency`` 的名额——4 张图串行能把这一个账号的轮询拉长到
        秒级，进而饿死其它账号。
        """
        want_video = self.bool_cfg("send_video", False)
        semaphore = asyncio.Semaphore(3)

        async def _one(item: dict) -> None:
            final_url = self._effective_media_url(item)
            if not final_url:
                return
            item["url"] = final_url
            if str(item.get("type") or "") != "photo" and not want_video:
                return
            async with semaphore:
                path = await self.media_cache.fetch(final_url, client=self._http)
            if path is not None:
                item["path"] = str(path)

        for record in records:
            media = json.loads(record.get("media_json") or "[]")
            if not media:
                continue
            await asyncio.gather(*(_one(item) for item in media), return_exceptions=True)
            record["media_json"] = json.dumps(media, ensure_ascii=False)
        return records

    # ------------------------------------------------------------ 投递队列

    def dnd_settings(self, umo: str | None = None) -> tuple[bool, str]:
        """取某个会话的免打扰设置，返回 ``(是否启用, 时段原文)``。

        **免打扰是按群（按 umo）的**：群级设置存在 groups 表里；没有该群的行时才回落到
        全局配置。这样群管理员改自己的时段不会影响别的群。
        """
        if umo:
            row = self.store.get_group(umo)
            if row is not None:
                return bool(int(row["dnd_enabled"])), str(row["dnd_ranges"] or "")
        return self.bool_cfg("dnd_enabled", False), ",".join(
            to_str_list(self.cfg("dnd_ranges", []))
        )

    def in_quiet_hours(self, umo: str | None = None) -> bool:
        enabled, ranges_text = self.dnd_settings(umo)
        if not enabled:
            return False
        ranges = parse_dnd_ranges([ranges_text])
        if not ranges:
            return False
        tz = str(self.cfg("dnd_timezone", "Asia/Shanghai") or "Asia/Shanghai")
        return in_quiet_hours(now_minute_in(tz), ranges)

    async def _delivery_loop(self) -> None:
        tick = 5.0
        while not self._stop.is_set():
            try:
                await self.drain_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(f"[x_monitor] 投递异常: {exc}", exc_info=True)
            try:
                await asyncio.wait_for(self._deliver_wake.wait(), timeout=tick)
            except asyncio.TimeoutError:
                pass
            self._deliver_wake.clear()

    async def drain_once(self) -> int:
        """排空投递队列。

        免打扰是**按会话**判断的：处于免打扰的会话会被跳过（消息留在队列里），
        其它会话照常发送。积压裁剪在免打扰期间**也要执行**，否则窗口内会无上限增长。
        """
        if self._draining:
            return 0
        self._draining = True
        try:
            self._translator.reset_round()
            await self._enforce_backlog_limit()
            rate = max(1, self.int_cfg("dnd_drain_rate", 5))
            batch = max(1, min(rate, 20))
            # 在 SQL 层排除正处于免打扰的会话。只靠 LIMIT 之后再过滤是不够的：
            # 免打扰会话会一直占住队首，别的会话在整段免打扰时间里一条都发不出去。
            dnd_umos = [
                umo
                for umo in await asyncio.to_thread(self.store.distinct_umos_with_queue)
                if self.in_quiet_hours(umo)
            ]
            rows = await asyncio.to_thread(self.store.next_queued, batch, dnd_umos)
            sent = 0
            for row in rows:
                # 批次执行期间可能刚好跨过免打扰边界，这里再确认一次
                if self.in_quiet_hours(str(row["umo"])):
                    continue
                await self._respect_rate(rate)
                try:
                    ok = await self.deliver(row)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    # 单条投递异常必须就地收敛并记账，否则这一行会永远霸占队首
                    logger.error(f"[x_monitor] 投递异常: {exc}", exc_info=True)
                    ok = False
                if ok:
                    await asyncio.to_thread(
                        self.store.mark_delivered, int(row["delivery_id"])
                    )
                    sent += 1
                else:
                    attempts = int(row["attempts"] or 0) + 1
                    retry = attempts < max(1, self.int_cfg("max_send_attempts", 3))
                    await asyncio.to_thread(
                        self.store.mark_failed,
                        int(row["delivery_id"]),
                        "send failed",
                        retry,
                    )
            return sent
        finally:
            self._draining = False

    async def _respect_rate(self, rate: int) -> None:
        """把相邻两次发送的间隔压到 ``>= 60/rate`` 秒。

        以「上次发送时刻」为基准计算，因此跨批次、跨 ``drain_once`` 调用同样成立。
        这正是原来 ``sleep(60/rate*0.6)`` 做不到的：它会系统性超速约 1.5 倍，
        而且在只有 1 条时（``len(rows) > 1`` 不成立）完全不 sleep。
        """
        gap = 60.0 / max(1, rate)
        if self.bool_cfg("send_jitter", True):
            gap *= random.uniform(1.0, 1.25)
        wait = self._last_send_ts + gap - time.time()
        if wait > 0:
            await asyncio.sleep(wait)
        self._last_send_ts = time.time()

    async def _enforce_backlog_limit(self) -> None:
        """**按会话**裁剪积压，超出上限就丢弃最旧的并汇总告知。

        刻意放在 ``drain_once`` 的最前面、免打扰判断之前：免打扰期间虽然不发，
        但积压仍必须有上限，否则一个长时段就能把队列撑爆。

        通知带冷却：队列持续超限时每个 drain tick 都会触发裁剪，不加冷却会变成刷屏。
        """
        limit = max(0, self.int_cfg("dnd_backlog_limit", 50))
        if limit <= 0:
            return
        cooldown = max(0, self.int_cfg("backlog_notify_cooldown_minutes", 30)) * 60
        now = time.time()
        umos = await asyncio.to_thread(self.store.distinct_umos_with_queue)
        for umo in umos:
            depth = await asyncio.to_thread(self.store.queue_depth, umo)
            if depth <= limit:
                continue
            dropped = await asyncio.to_thread(
                self.store.drop_oldest_queued, depth - limit, umo
            )
            if not dropped:
                continue
            logger.warning(f"[x_monitor] {umo} 积压超限，已丢弃最旧 {len(dropped)} 条")
            if not self.bool_cfg("dnd_notify_on_drop", True):
                continue
            if cooldown and now - self._backlog_notified.get(umo, 0.0) < cooldown:
                continue
            self._backlog_notified[umo] = now
            await self._safe_send(
                umo,
                MessageChain(
                    chain=[
                        Plain(
                            f"⚠️ 待发送队列积压过多，已丢弃最旧的 {len(dropped)} 条"
                            f"（当前上限 {limit} 条）。"
                        )
                    ]
                ),
            )

    # ------------------------------------------------------------ 发送

    def node_uin(self, event: AstrMessageEvent | None = None, umo: str = "") -> str:
        """合并转发节点的 ``uin``：机器人自身 ID（沿用 image_pool 的做法）。

        主动推送没有 event，所以把学到的 self_id **按平台**缓存
        （多平台 / 多 bot 部署时不能只存一个全局值）；都没有时用 ``"0"``
        （AstrBot 的 ``Node`` 默认值）—— 节点显示靠的是 ``name`` 而不是 ``uin``。
        """
        if event is not None:
            umo = umo or str(getattr(event, "unified_msg_origin", "") or "")
        platform_id = platform_id_of(umo) if umo else ""
        if event is not None:
            try:
                self_id = str(event.get_self_id() or "")
            except Exception:
                self_id = ""
            if self_id and platform_id:
                try:
                    self.store.set_meta(f"self_id:{platform_id}", self_id)
                except Exception:
                    pass
                return self_id
        if platform_id:
            try:
                cached = self.store.get_meta(f"self_id:{platform_id}", "")
            except Exception:
                cached = ""
            if cached:
                return cached
        return "0"

    def post_text(self, body: dict, translated: str = "", nickname: str = "") -> str:
        """构造正文块。纯媒体帖的 ``text`` 可能是空串，不要假定有正文。"""
        author = body.get("author") or {}
        handle = str(author.get("screen_name") or "")
        name = str(author.get("name") or handle or "未知用户")
        created = post_sort_key(body)
        tz = str(self.cfg("display_timezone", "Asia/Shanghai") or "Asia/Shanghai")
        # 自定义昵称同时体现在正文头部：chain 模式没有节点昵称，靠这里才能看出别名
        label = str(nickname or "").strip()
        head = f"🐦 {label}｜{name}" if label and label != name else f"🐦 {name}"
        if handle:
            head += f" (@{handle})"
        if created:
            head += f"\n🕒 {format_ts(created, tz)}"
        if body.get("reposted_by"):
            reposter = str((body.get("reposted_by") or {}).get("screen_name") or "")
            if reposter:
                head = f"🔁 {reposter} 转推\n" + head

        kind = classify_post(body)
        if kind == KIND_REPLY:
            target = str((body.get("replying_to") or {}).get("screen_name") or "")
            if target:
                head += f"\n↩️ 回复 @{target}"

        parts = [head]
        text = str(body.get("text") or "").strip()
        if text:
            parts.append(text)
        if translated:
            if self.bool_cfg("hide_original_when_translated", False):
                parts = [head, f"【译】{translated}"]
            else:
                parts.append(f"【译】{translated}")
        quote = body.get("quote")
        if isinstance(quote, dict) and quote.get("type") != "tombstone":
            quote_text = str(quote.get("text") or "").strip()
            quote_author = str((quote.get("author") or {}).get("screen_name") or "")
            if quote_text:
                snippet = quote_text if len(quote_text) <= 200 else quote_text[:200] + "…"
                parts.append(f"📎 引用 @{quote_author}：{snippet}")
        link = build_share_url(body)
        if link:
            parts.append(f"🔗 {link}")
        return "\n\n".join(parts)

    def _image_items(self, media: list[dict]) -> list[tuple[Path | None, object]]:
        """构造图片组件列表，返回 ``[(本地路径或 None, 组件)]``。

        本地优先（可控制 payload 体积），取不到本地文件时回退为直链组件 ——
        ``media_mode=link`` 时全部走直链。绝不让图片静默消失。
        """
        items: list[tuple[Path | None, object]] = []
        for item in media:
            if item.get("type") != "photo":
                continue
            raw = str(item.get("path") or "")
            if raw and Path(raw).exists():
                path = Path(raw)
                items.append((path, Image.fromFileSystem(str(path))))
                continue
            # link 模式（或下载失败）回退直链；这里同样过一遍画质档位，
            # 保证"发出去的画质"和配置一致
            url = self._effective_media_url(item)
            if not url:
                continue
            try:
                items.append((None, Image.fromURL(url)))
            except Exception as exc:
                logger.warning(f"[x_monitor] 构造图片直链失败 {url}: {exc}")
        return items

    def _video_items(self, media: list[dict]) -> list[tuple[Path | None, object]]:
        if not self.bool_cfg("send_video", False):
            return []
        items: list[tuple[Path | None, object]] = []
        for item in media:
            if item.get("type") != "video":
                continue
            raw = str(item.get("path") or "")
            if raw and Path(raw).exists():
                path = Path(raw)
                items.append((path, Video.fromFileSystem(str(path))))
                continue
            url = str(item.get("url") or "")
            if url:
                try:
                    items.append((None, Video.fromURL(url)))
                except Exception as exc:
                    logger.warning(f"[x_monitor] 构造视频直链失败 {url}: {exc}")
        return items

    async def deliver(self, row) -> bool:
        """投递一条推文。返回 True 表示已送出。

        降级阶梯：合并转发（按 payload 拆批）→ 普通消息链 → 纯文字 + 链接。
        视频始终独立另发，不放进合并转发节点。
        """
        body = json.loads(row["body_json"] or "{}")
        media = json.loads(row["media_json"] or "[]")
        translated = str(row["translated"] or "")
        umo = str(row["umo"])
        nickname = normalize_nick(row["nickname"]) or ""

        if not translated and self.bool_cfg("translate_enabled", False):
            translated = await self._translate_for(umo, body)

        text = self.post_text(body, translated, nickname)
        images = self._image_items(media)
        videos = self._video_items(media)
        return await self._deliver_payload(umo, body, text, images, videos, nickname)

    async def _deliver_payload(
        self,
        umo: str,
        body: dict,
        text: str,
        images: list[tuple],
        videos: list[tuple],
        edge_nickname: str,
    ) -> bool:
        """发送阶梯：合并转发（按 payload 拆批）→ 普通消息链 → 纯文字 + 链接。

        视频始终独立另发，不放进合并转发节点。
        `/x测试`、`/x补发` 与正常投递共用这一段，避免三处各写一遍降级逻辑而漂移。
        """
        mode = str(self.cfg("send_mode", "forward") or "forward").strip().lower()

        if mode == "forward":
            node_nick = self._node_nickname(body, edge_nickname)
            ok, error, partial = await self._send_forward(umo, text, images, node_nick)
            if ok:
                await self._send_videos(umo, videos)
                return True
            if partial:
                # 已经发出去了一部分，再整贴降级会造成重复推送；宁可少发也不重复
                logger.warning(
                    f"[x_monitor] 合并转发部分成功，放弃整贴降级以免重复：{error}"
                )
                return True
            logger.warning(f"[x_monitor] 合并转发失败，降级为普通消息链: {error}")

        ok = await self._send_chain(umo, text, images)
        if ok:
            await self._send_videos(umo, videos)
            return True
        # 最后一层：纯文字 + 链接，绝不让内容静默消失
        return await self._send_chain(umo, text, [])

    def _node_nickname(self, body: dict, edge_nickname: str) -> str:
        """合并转发节点的显示昵称：**自定义昵称优先，否则用推主名**（需求 4）。

        没有订阅级昵称时用推主名，而不是笼统的兜底值——否则所有账号的转发都会显示
        成同一个名字，合并转发就失去了"这是谁发的"这个信息。
        """
        custom = normalize_nick(edge_nickname)
        if custom:
            return custom
        author = body.get("author") or {}
        name = normalize_nick(author.get("name"))
        if name:
            return name
        handle = normalize_nick(author.get("screen_name"))
        if handle:
            return handle
        return normalize_nick(self.cfg("forward_nickname_fallback", "X")) or "X"

    async def _translate_for(self, umo: str, body: dict) -> str:
        text = str(body.get("text") or "").strip()
        if not text:
            return ""
        source_lang = str(body.get("lang") or "")
        translated = await self._translator.translate(umo, text, source_lang)
        post_id = str(body.get("id") or "")
        if translated and post_id:
            try:
                await asyncio.to_thread(
                    self.store.set_translation,
                    post_id,
                    translated,
                    str(self.cfg("target_lang", "zh-CN")),
                )
            except Exception:
                pass
        return translated

    async def _send_chain(self, umo: str, text: str, images: list[tuple]) -> bool:
        """同一条消息链：文本块接图片（视频不在此列）。"""
        chain: list = [Plain(text)] if text else []
        chain.extend(component for _path, component in images)
        if not chain:
            return False
        return await self._safe_send(umo, MessageChain(chain=chain))

    async def _send_forward(
        self, umo: str, text: str, images: list[tuple], nickname: str
    ) -> tuple[bool, str, bool]:
        """合并转发：一张图一个节点，节点昵称 = 自定义昵称或推主名，uin = self_id。

        文本块单独成节点，图片各自成节点，整体按 payload 预算拆批。

        返回 ``(是否全部成功, 失败原因, 是否已发出部分内容)``。
        第三项很关键：分批发送时若第 2 批失败，第 1 批其实已经送达，
        调用方据此**放弃整贴降级**，否则同一贴会被发两遍。
        """
        if not text and not images:
            return False, "empty", False
        uin = self.node_uin(umo=umo)
        nick = nickname or normalize_nick(self.cfg("forward_nickname_fallback", "X")) or "X"
        budget = max(64 * 1024, self.float_cfg("max_forward_payload_mb", 5.0) * 1024 * 1024)
        max_images = max(1, self.int_cfg("max_images_per_forward", 10))
        sent_any = False
        try:
            for chunk, batch in self._plan_forward_batches(text, images, budget, max_images):
                nodes: list = []
                if chunk:
                    nodes.append(Node(content=[Plain(chunk)], name=nick, uin=uin))
                for _path, component in batch:
                    nodes.append(Node(content=[component], name=nick, uin=uin))
                if not nodes:
                    continue
                if not await self._safe_send(umo, MessageChain(chain=[Nodes(nodes)])):
                    return False, "send returned False", sent_any
                sent_any = True
            return True, "", sent_any
        except Exception as exc:
            return False, f"{type(exc).__name__}: {exc}", sent_any

    def _plan_forward_batches(
        self, text: str, images: list[tuple], budget: float, max_images: int
    ) -> list[tuple[str, list[tuple]]]:
        """把「文本 + 图片」规划成若干批合并转发。

        体积估算：图片按 ``1.34 x 文件字节``（``Node.to_dict`` 会把图片转成 base64 内联），
        文本按 UTF-8 字节。**不做真实 base64** —— 避免为估算付出正是要省的开销。
        直链图片没有本地文件，体积未知，按 0 计（只受 ``max_images`` 约束）。
        """
        text = text or ""
        if not images:
            return [(text, [])] if text else []
        batches: list[tuple[str, list[tuple]]] = []
        cur_text: str = text
        cur_images: list[tuple] = []
        used = len(text.encode("utf-8")) + 64 if text else 0
        for item in images:
            path = item[0] if item else None
            size = 0
            if path is not None:
                try:
                    size = path.stat().st_size
                except OSError:
                    size = 0
            estimate = int(size * 1.34) + 64
            if cur_images and (used + estimate > budget or len(cur_images) >= max_images):
                batches.append((cur_text, cur_images))
                cur_text, cur_images, used = "", [], 0
            cur_images.append(item)
            used += estimate
        if cur_images or cur_text:
            batches.append((cur_text, cur_images))
        return batches

    async def _send_videos(self, umo: str, videos: list[tuple]) -> None:
        """视频独立另发，不放进合并转发节点。"""
        for index, (_path, component) in enumerate(videos, start=1):
            label = "🎬 视频" if len(videos) == 1 else f"🎬 视频 {index}/{len(videos)}"
            try:
                await self._safe_send(umo, MessageChain(chain=[Plain(label), component]))
            except Exception as exc:
                logger.warning(f"[x_monitor] 视频发送失败: {exc}")

    async def _safe_send(self, umo: str, chain: MessageChain) -> bool:
        """发送并校验结果。

        ``Context.send_message`` 返回 False 是**静默失败**（无异常），必须升级成失败，
        否则整条降级阶梯会失效。
        """
        sender = getattr(self.context, "send_message", None)
        if not callable(sender):
            logger.warning("[x_monitor] context 不支持 send_message（离线环境？）")
            return False
        try:
            result = await sender(umo, chain)
        except Exception as exc:
            logger.warning(f"[x_monitor] 发送异常 -> {umo}: {exc}")
            return False
        if result is False:
            logger.warning(f"[x_monitor] 发送返回 False -> {umo}")
            return False
        return True

    # ------------------------------------------------------------ WebUI（Plugin Pages）
    #
    # 安全假设：**能登录 Dashboard 的人本就受信任**，因此这个页面等同于"全局管理员面"，
    # 这里不做额外的插件级鉴权。技能文档明确建议不要把 Dashboard 用户名映射到聊天用户 ID，
    # 而是直接声明这个假设 —— 页面上也写了同样一句话。
    #
    # 输入一律当不可信处理：umo 卡格式、配置按 schema 校验、数字做范围钳制。

    def _schema_path(self) -> Path:
        return Path(__file__).resolve().parent / "_conf_schema.json"

    def load_schema(self) -> dict:
        """读插件自带的配置 schema（WebUI 的配置编辑器由它驱动）。"""
        try:
            with open(self._schema_path(), encoding="utf-8-sig") as handle:
                data = json.load(handle)
            return data if isinstance(data, dict) else {}
        except Exception as exc:
            logger.warning(f"[x_monitor] 读取 _conf_schema.json 失败: {exc}")
            return {}

    def _page_index_path(self) -> Path:
        """WebUI 页面入口。AstrBot 是扫 ``pages/<名字>/index.html`` 来发现页面的
        （``dashboard/services/plugin_page_service.py``），且缺 index.html 时**静默跳过**。"""
        return Path(__file__).resolve().parent / "pages" / "manage" / "index.html"

    def _register_web_api(self) -> None:
        """注册 WebUI 路由。旧内核没有 register_web_api，必须优雅降级。

        这里刻意打足日志：AstrBot 的页面服务在跳过非法页面目录时**一行日志都不打**，
        所以"装完看不到页面"如果只靠框架侧，是完全无从查起的。
        """
        page_ok = self._page_index_path().is_file()
        if not page_ok:
            logger.warning(
                f"[x_monitor] 未找到 WebUI 页面文件 {self._page_index_path()}，"
                "插件包可能不完整（pages/ 未随包落盘）。命令侧功能不受影响。"
            )
        if not WEB_API_AVAILABLE:
            logger.warning(
                "[x_monitor] 当前 AstrBot 不支持 Plugin Pages（需 >= v4.24.1），"
                "WebUI 管理页不可用；命令侧功能不受影响。"
            )
            return
        registered = 0
        for route, handler_name, methods, desc in self.ROUTES:
            handler = getattr(self, handler_name, None)
            if not callable(handler):
                # 名字拼错就静默跳过，与"刻意打足日志"自相矛盾
                logger.error(
                    f"[x_monitor] WebUI 路由 {route} 的处理函数 {handler_name} 不存在，已跳过"
                )
                continue
            try:
                self.context.register_web_api(
                    f"/{PLUGIN_NAME}/{route}", handler, list(methods), desc
                )
                registered += 1
            except Exception as exc:
                logger.warning(f"[x_monitor] 注册 WebUI 路由 {route} 失败: {exc}")
        if page_ok and registered:
            logger.info(
                f"[x_monitor] WebUI 管理页就绪：pages/manage/index.html + {registered} 条路由。"
                "若插件详情页的侧边栏里没出现，请对 Dashboard 整页硬刷新（Ctrl+F5）——"
                "侧边栏列表在一次前端会话里只拉取一次。"
            )

    # -------------------------------------------------- WebUI：会话解析

    def list_platforms(self) -> list[dict]:
        """列出 AstrBot 真实的平台实例。

        ``meta().id`` 才是 umo 第一段（框架就是拿它比对：``context.py`` 里
        ``if platform.meta().id == session.platform_name``），``meta().name`` 只是
        适配器类型名（如 ``aiocqhttp``）。取不到 ``platform_manager`` 时返回空表，
        调用方据此降级为"只允许完整 umo"。
        """
        manager = getattr(self.context, "platform_manager", None)
        insts = getattr(manager, "platform_insts", None) if manager is not None else None
        out: list[dict] = []
        for inst in insts or []:
            try:
                meta = inst.meta()
            except Exception:
                continue
            pid = str(getattr(meta, "id", "") or "")
            if not pid:
                continue
            out.append(
                {
                    "id": pid,
                    "name": str(getattr(meta, "name", "") or ""),
                    "desc": str(getattr(meta, "description", "") or ""),
                }
            )
        return out

    def known_umos(self) -> list[str]:
        """已知会话 = 登记过的群 ∪ 有订阅的群 ∪ 设过免打扰的群。"""
        umos: list[str] = []
        for umo in (
            self.store.known_groups()
            + [str(row["umo"]) for row in self.store.list_edges()]
            + self.store.all_group_overrides()
        ):
            if umo and umo not in umos:
                umos.append(umo)
        return umos

    def resolve_umo(self, raw) -> tuple[bool, str, str]:
        """把用户输入解析成**可用**的 umo，返回 ``(ok, umo, error)``。

        三条路径，**全程不猜平台前缀**：

        1. 完整 umo → 用平台实例表校验第一段真实存在
        2. 纯数字群号 → 在已知 umo 里按群号**唯一**匹配
        3. 其它 → 报错

        第 2 条是这次踩坑的正解：以前前端把裸群号硬编码拼成 ``aiocqhttp:...``，
        而真实实例 id 可能是任意名字（如「mybot」），拼出来的 umo 永远发不出去。
        """
        if not isinstance(raw, str):
            return False, "", "会话标识必须是字符串"
        value = raw.strip()
        if not value:
            return False, "", "请填写群号或完整会话标识"
        if any(ch.isspace() for ch in value):
            return False, "", "会话标识不能包含空白字符"

        if ":" in value:
            ok, err = validate_umo(value)
            if not ok:
                return False, "", err
            platforms = {p["id"] for p in self.list_platforms()}
            # 平台表取不到时不做存在性校验（旧内核降级），只保留格式校验
            if platforms and platform_id_of(value) not in platforms:
                return False, "", (
                    f"平台实例「{platform_id_of(value)}」不存在。"
                    f"可用：{', '.join(sorted(platforms))}。"
                    "注意 umo 第一段是平台**实例 id**，不是适配器名（如 aiocqhttp）。"
                )
            return True, value, ""

        if value.isdigit():
            hits = [u for u in self.known_umos() if group_id_of(u) == value]
            if len(hits) == 1:
                return True, hits[0], ""
            if not hits:
                return False, "", (
                    f"未知群号 {value}：本插件还没见过这个群。"
                    "请先在该群里执行 /x在哪（会登记该会话），"
                    "或直接填写完整会话标识 <平台实例ID>:GroupMessage:<群号>。"
                )
            return False, "", (
                f"群号 {value} 在多个平台上都存在，请填写完整会话标识：{', '.join(hits)}"
            )

        return False, "", (
            "无法识别。请填群号，或完整会话标识 <平台实例ID>:GroupMessage:<群号>"
        )

    # -------------------------------------------------- WebUI：数据组装

    def web_overview_payload(self) -> dict:
        authors = []
        for row in self.store.list_authors_for_web():
            handle = str(row["handle"])
            authors.append(
                {
                    "author_id": str(row["author_id"]),
                    "handle": handle,
                    "display_name": str(row["display_name"]),
                    "baseline_ready": bool(int(row["baseline_ready"] or 0)),
                    "last_seen_ts": int(row["last_seen_ts"] or 0),
                    # 该账号当前由哪个数据源供数（P5 降级链归属）
                    "source": self.source_for(handle),
                }
            )

        edges = []
        pending = self.store.pending_counts()
        for row in self.store.list_edges_with_author():
            edges.append(
                {
                    "edge_id": int(row["edge_id"]),
                    "umo": str(row["umo"]),
                    "group_id": str(row["group_id"]),
                    "author_id": str(row["author_id"]),
                    "handle": str(row["author_handle"] or row["author_id"]),
                    "display_name": str(row["author_display_name"] or ""),
                    "nickname": str(row["nickname"] or ""),
                    "enabled": bool(int(row["enabled"] or 0)),
                    "media_only": edge_media_only(row),
                    "pending": pending.get(int(row["edge_id"]), 0),
                }
            )

        # 群列表 = 已知会话 ∪ 有订阅的 umo ∪ 显式设过免打扰的 umo
        umos: list[str] = []
        for umo in (
            self.store.known_groups()
            + [e["umo"] for e in edges]
            + self.store.all_group_overrides()
        ):
            if umo and umo not in umos:
                umos.append(umo)
        groups = []
        for umo in umos:
            enabled, ranges_text = self.dnd_settings(umo)
            groups.append(
                {
                    "umo": umo,
                    "label": group_id_of(umo),
                    "dnd_enabled": enabled,
                    "dnd_ranges": ranges_text,
                    "in_dnd": self.in_quiet_hours(umo),
                    "queue": self.store.queue_depth(umo),
                    "edge_count": len([e for e in edges if e["umo"] == umo]),
                }
            )

        recent = self.store.recent_deliveries(20)
        failed = [r for r in recent if str(r["state"]) == STATE_FAILED]
        health = self.provider_health()
        # 联动状态只在这里查一次（状态 + 被拒昵称一次遍历给全，见 N-5）
        pool_state, pool_rejected = self.pool_tag_state_and_rejected()
        return {
            "plugin": PLUGIN_NAME,
            "version": PLUGIN_VERSION,
            "authors": authors,
            "edges": edges,
            "groups": groups,
            "platforms": self.list_platforms(),
            "provider_chain": health,
            "stats": {
                "edges": len(edges),
                "authors": len(authors),
                "posts": self.store.count_posts(),
                "queue": self.store.queue_depth(),
                "cache_bytes": self.media_cache.usage_bytes(),
                "cache_quota_bytes": self.media_cache.quota_bytes,
                "recent_total": len(recent),
                "recent_failed": len(failed),
                "last_error": str(failed[0]["last_error"]) if failed else "",
                "last_error_handle": str(failed[0]["handle"]) if failed else "",
                # provider：名义第一顺位的数据源（老字段，保持向后兼容）
                "provider": str(health.get("active") or ""),
                "provider_chain": health.get("configured") or [],
                "provider_degraded": bool(health.get("degraded")),
                "initialized": self._http is not None,
                "send_mode": str(self.cfg("send_mode", "forward")),
                "translate_enabled": self.bool_cfg("translate_enabled", False),
                "media_posts": self.store.media_post_count(),
                "replay_max": max(1, self.int_cfg("replay_max", 10)),
                # P7：图库联动状态（页面「总览」卡片直接用这个字段）
                "image_pool": (
                    dict(
                        self._pool_bridge.snapshot(),
                        # 只查一次库：分别调两个函数会让同一份 payload 自相矛盾（H-3）。
                        tag_state=pool_state,
                        # 文案的唯一来源是服务端（app.js 不再自己拷一份，L-C2）
                        tag_state_text=IMAGE_POOL_TAG_STATE_TEXT.get(pool_state, ""),
                        # 全局状态覆盖不到的角落：另有这些昵称不会入库（L-②）
                        rejected_nicknames=pool_rejected,
                    )
                    if getattr(self, "_pool_bridge", None)
                    else {}
                ),
            },
        }

    def web_config_payload(self) -> dict:
        schema = self.load_schema()
        values = {key: self.cfg(key, entry.get("default")) for key, entry in schema.items()}
        return {"schema": schema, "values": values}

    # -------------------------------------------------- WebUI：路由处理
    # 注意：处理函数**不能有位置参数**，一切都从 web_api.request 读。

    async def _json_body(self) -> dict:
        """安全读取 JSON 请求体，**保证返回 dict**。

        ``request.json(default={})`` 只在**解析异常**时返回 default；合法 JSON 但非对象
        （``[1,2]`` / ``"x"`` / ``5`` 都是真值）会原样返回，紧接着的 ``body.get`` 就是
        AttributeError → 500。这里统一收敛。
        """
        try:
            body = await web_api.request.json(default={})
        except Exception as exc:
            logger.warning(f"[x_monitor] 读取 WebUI 请求体失败: {exc}")
            return {}
        return body if isinstance(body, dict) else {}

    async def api_overview(self):
        return web_api.json_response(self.web_overview_payload())

    async def api_config_get(self):
        return web_api.json_response(self.web_config_payload())

    async def api_config_save(self):
        body = await self._json_body()
        incoming = body.get("values")
        if not isinstance(incoming, dict):
            return web_api.error_response("请求体缺少 values 对象", status_code=400)
        schema = self.load_schema()
        if not schema:
            # 区分"schema 读不出来"与"键不存在"，否则会把它伪装成"未知配置项"
            return web_api.error_response(
                "读取 _conf_schema.json 失败，已拒绝写入", status_code=500
            )
        saved: dict = {}
        errors: dict = {}
        for key, raw in incoming.items():
            entry = schema.get(key)
            if not isinstance(entry, dict):
                errors[str(key)] = "未知配置项"
                continue
            ok, value, err = validate_config_value(entry, raw)
            if not ok:
                errors[str(key)] = err
                continue
            saved[str(key)] = value
        if errors:
            # 全有或全无：不做部分落盘，避免"改了一半"这种最难排查的状态
            detail = "；".join(f"{k}：{v}" for k, v in errors.items())
            return web_api.error_response(f"配置未通过校验，未做任何修改 —— {detail}", status_code=400)

        # 校验通过后仍可能落盘失败：这时必须回滚内存并如实报错，
        # 否则会回 200 "已保存"而磁盘没写，重启后静默回退、运行期与磁盘配置分叉。
        before = {key: self.config.get(key, _MISSING) for key in saved}
        for key, value in saved.items():
            self.config[key] = value
        save = getattr(self.config, "save_config", None)
        if callable(save):
            try:
                save()
            except Exception as exc:
                for key, old in before.items():
                    if old is _MISSING:
                        self.config.pop(key, None)
                    else:
                        self.config[key] = old
                logger.error(f"[x_monitor] 配置落盘失败，已回滚内存改动: {exc}")
                return web_api.error_response(f"配置写入磁盘失败，已回滚：{exc}", status_code=500)
        # 配置变了：唤醒两个循环，让新间隔 / 新开关立即生效，不必重启
        self._force_poll = True
        self._poll_wake.set()
        self._deliver_wake.set()
        return web_api.json_response({"saved": sorted(saved), "count": len(saved)})

    async def api_edge_add(self):
        body = await self._json_body()
        handle = str(body.get("handle") or "").strip().lstrip("@")
        nick_ok, nickname, nick_err = parse_nickname_input(body.get("nickname"))
        if not nick_ok:
            return web_api.error_response(
                f"昵称不合法：{nick_err}。"
                "（不可含 \\ / : * ? \" < > | 与控制字符；也不可含无法编码的字符。）",
                status_code=400,
            )
        media_only = to_bool(body.get("media_only"))
        ok, umo, err = self.resolve_umo(body.get("umo"))
        if not ok:
            return web_api.error_response(err, status_code=400)
        if not handle:
            return web_api.error_response("缺少 handle", status_code=400)
        if self._ensure_chain() is None:
            return web_api.error_response(self._chain_unavailable_message(), status_code=503)
        resolved = await self._resolve_author(handle)
        if resolved is None:
            return web_api.error_response(f"找不到 X 账号 @{handle}，或抓取服务暂时不可用", status_code=404)
        author_id, clean, name, _avatar = resolved
        author_row = self.store.get_author(author_id)
        start_ts = int(author_row["last_seen_ts"] or 0) if author_row else 0
        created = self.store.add_edge(
            umo, group_id_of(umo), author_id, nickname, start_ts, media_only
        )
        updated = False
        if not created:
            # 已存在时**真的更新**昵称与仅媒体，而不是回一句"已更新"却什么都没做
            # （add_edge 是 INSERT OR IGNORE，冲突时不会改任何字段）
            updated = self.store.update_edge(
                umo, author_id, nickname=nickname, media_only=media_only
            ) > 0
        self.store.touch_group(umo)
        self._force_poll = True
        self._poll_wake.set()
        return web_api.json_response(
            {
                "created": bool(created),
                "updated": bool(updated),
                "author_id": author_id,
                "handle": clean,
                "display_name": name,
                "umo": umo,
            }
        )

    async def api_edge_update(self):
        body = await self._json_body()
        try:
            edge_id = int(body.get("edge_id") or 0)
        except (TypeError, ValueError):
            return web_api.error_response("edge_id 非法", status_code=400)
        edge = self.store.get_edge_by_id(edge_id)
        if edge is None:
            return web_api.error_response("订阅不存在", status_code=404)
        umo, author_id = str(edge["umo"]), str(edge["author_id"])
        changed: list[str] = []
        nickname = None
        enabled = None
        media_only = None
        if "nickname" in body:
            nick_ok, nickname, nick_err = parse_nickname_input(body.get("nickname"))
            if not nick_ok:
                # 不能把"非法输入"当成"清空昵称"（那会静默抹掉旧值并回 200 成功，L-N3）
                return web_api.error_response(
                    f"昵称不合法：{nick_err}。"
                    "（不可含 \\ / : * ? \" < > | 与控制字符；也不可含无法编码的字符。）",
                    status_code=400,
                )
            changed.append("nickname")
        if "enabled" in body:
            enabled = to_bool(body.get("enabled"), True)
            changed.append("enabled")
        if "media_only" in body:
            media_only = to_bool(body.get("media_only"))
            changed.append("media_only")
        if not changed:
            return web_api.error_response("没有可更新的字段", status_code=400)
        # 一次事务改完三个字段，避免"半更新"
        rows = self.store.update_edge(
            umo, author_id, nickname=nickname, enabled=enabled, media_only=media_only
        )
        if rows <= 0:
            return web_api.error_response("更新未生效（订阅可能已被删除）", status_code=409)
        row = self.store.get_edge(umo, author_id)
        return web_api.json_response(
            {
                "changed": changed,
                "nickname": str(row["nickname"] or "") if row else "",
                "enabled": bool(int(row["enabled"] or 0)) if row else False,
                "media_only": edge_media_only(row) if row else False,
            }
        )

    async def api_edge_remove(self):
        body = await self._json_body()
        try:
            edge_id = int(body.get("edge_id") or 0)
        except (TypeError, ValueError):
            return web_api.error_response("edge_id 非法", status_code=400)
        edge = self.store.get_edge_by_id(edge_id)
        if edge is None:
            return web_api.error_response("订阅不存在", status_code=404)
        removed = self.store.remove_edge(str(edge["umo"]), str(edge["author_id"]))
        return web_api.json_response({"removed": int(removed)})

    async def api_author_remove(self):
        body = await self._json_body()
        author_id = str(body.get("author_id") or "").strip()
        if not author_id:
            return web_api.error_response("缺少 author_id", status_code=400)
        if self.store.get_author(author_id) is None:
            return web_api.error_response("账号不存在", status_code=404)
        result = self.store.remove_author(author_id)
        return web_api.json_response(result)

    async def api_group_dnd(self):
        body = await self._json_body()
        ok, umo, err = self.resolve_umo(body.get("umo"))
        if not ok:
            return web_api.error_response(err, status_code=400)
        enabled = to_bool(body.get("enabled"))
        ranges = to_str_list(body.get("ranges"))
        if not enabled:
            # "关" = 删除本群覆盖行、回落全局默认（只置 0 会让该群永久不再继承默认）
            self.store.clear_group_dnd(umo)
            self._deliver_wake.set()
            return web_api.json_response(
                {"umo": umo, "enabled": False, "ranges": [], "cleared": True}
            )
        # 逐条校验，不静默吞掉格式错的时段
        bad = [item for item in ranges if not parse_dnd_ranges([item])]
        if bad:
            return web_api.error_response(
                f"时段格式不对：{', '.join(bad)}（示例 23:00-08:00，支持跨天）", status_code=400
            )
        if not ranges:
            return web_api.error_response("启用免打扰时至少要给一个时段", status_code=400)
        self.store.set_group_dnd(umo, True, ",".join(ranges))
        self._deliver_wake.set()
        return web_api.json_response(
            {
                "umo": umo,
                "enabled": True,
                "ranges": ranges,
                "in_dnd": self.in_quiet_hours(umo),
            }
        )

    async def api_test_send(self):
        body = await self._json_body()
        ok, umo, err = self.resolve_umo(body.get("umo"))
        if not ok:
            return web_api.error_response(err, status_code=400)
        handle = str(body.get("handle") or "").strip().lstrip("@")
        edge = self.store.get_edge_by_handle(umo, handle) if handle else None
        edge_nick = normalize_nick(edge["nickname"]) if edge is not None else ""
        if edge is not None and not edge_nick:
            author_row = self.store.get_author(str(edge["author_id"]))
            if author_row is not None:
                edge_nick = normalize_nick(author_row["display_name"])
        text = "🧪 X 监控链路自检（来自 WebUI）\n\n看到这条消息说明发送通道正常。"
        sent = False
        if str(self.cfg("send_mode", "forward")).lower() == "forward":
            sent, _error, _partial = await self._send_forward(umo, text, [], edge_nick)
            if not sent:
                sent = await self._send_chain(umo, text, [])
        else:
            sent = await self._send_chain(umo, text, [])
        return web_api.json_response({"sent": bool(sent)})

    async def api_test_replay(self):
        body = await self._json_body()
        handle = str(body.get("handle") or "").strip().lstrip("@")
        ok, umo, err = self.resolve_umo(body.get("umo"))
        if not ok:
            return web_api.error_response(err, status_code=400)
        if not handle:
            return web_api.error_response("缺少 handle", status_code=400)
        if self._ensure_chain() is None:
            return web_api.error_response(self._chain_unavailable_message(), status_code=503)
        try:
            count = int(body.get("count") or self.int_cfg("replay_default", 3))
        except (TypeError, ValueError):
            count = self.int_cfg("replay_default", 3)
        count = max(1, min(count, max(1, self.int_cfg("replay_max", 10))))
        result = await self.replay_recent(umo, handle, count)
        if not result.get("ok"):
            return web_api.error_response(str(result.get("message") or "回放失败"), status_code=400)
        return web_api.json_response(result)

    # ------------------------------------------------------------ 命令：订阅

    def _require_view(self, event: AstrMessageEvent) -> str:
        if self.can(event, "view"):
            return ""
        return self.mode_hint(event)

    def _require_subscribe(self, event: AstrMessageEvent) -> str:
        if self.can(event, "subscribe"):
            return ""
        return self.mode_hint(event)

    async def _resolve_author(self, handle: str) -> tuple[str, str, str, str] | None:
        """解析 handle -> (author_id, handle, display_name, avatar_url)。"""
        clean = str(handle or "").lstrip("@").strip()
        if not clean:
            return None
        row = self.store.get_author_by_handle(clean)
        if row is not None and str(row["author_id"]) != clean:
            return (
                str(row["author_id"]),
                str(row["handle"]),
                str(row["display_name"]),
                str(row["avatar_url"]),
            )
        chain = self._ensure_chain()
        if chain is None:
            return None
        result = await chain.fetch_user(clean, since_ts=0, consume_pin=False)
        if not result.author and result.posts:
            # L1 挂掉时由 RSS 源兜底：RSS 没有 channel 级作者信息，
            # 但条目里的 author 一样能用；实在没有就用 handle 兜底，
            # 免得"抓取服务坏了"连订阅都建不了。
            for item in result.posts:
                candidate = item.get("author") if isinstance(item, dict) else None
                if isinstance(candidate, dict) and candidate.get("screen_name"):
                    result.author = candidate
                    break
        if not result.ok:
            return None
        self.record_result_sources(clean, result)
        author = result.author if isinstance(result.author, dict) else {}
        if not author.get("screen_name") and not author.get("id"):
            return None
        # 同一 handle 已有一行时必须**复用它**：上次 L1 不可用时可能已用 handle 兜底
        # 建过行（author_id = "elonmusk"），而这次拿到了数字 id。若直接 upsert 新行，
        # 同一个 X 账号会以 handle 型与数字型并存 —— WebUI 账号表出现两行同名账号、
        # /x状态 账号数虚高，而且**每轮对同一个账号抓两次**（请求翻倍）。
        existing = self.store.get_author_by_handle(clean)
        if existing is not None:
            author_id = str(existing["author_id"])
            if self.store.count_authors_by_handle(clean) > 1:
                # 历史遗留：同一 handle 多行（v1.3.0 的 handle 兜底 + 数字 id 各一行）。
                # 不做自动迁移（author_id 是 edges 的外键，迁移有丢订阅的风险），
                # 只把它暴露出来，便于人工在 WebUI 里清理。
                logger.warning(
                    f"[x_monitor] @{clean} 在 authors 表里有多行（历史遗留），"
                    f"本次复用 id={author_id}；如需彻底清理请在 WebUI 删除多余账号"
                )
            logger.info(
                f"[x_monitor] @{clean} 已有作者行（id={author_id}），复用它而不新建"
                f"（本次数据源给出 id={author.get('id') or clean}）"
            )
        else:
            author_id = str(author.get("id") or clean)
        if author_id == clean:
            # 用 handle 兜底建出来的 id：与 FxTwitter 的数字 id 不同口径，
            # 但去重键是 (author_id)，换成数字 id 后旧订阅会变成孤儿，
            # 所以这里只做日志提醒，不做自动改写。
            logger.info(
                f"[x_monitor] @{clean} 未取到数字型作者 id（数据源 "
                f"{result.provider or result.source}），暂以 handle 作为 author_id"
            )
        name = str(author.get("name") or "")
        avatar = str(author.get("avatar_url") or "")
        if author_id:
            self.store.upsert_author(author_id, clean, name, avatar)
        return (author_id, clean, name, avatar)

    @staticmethod
    def _clean_raw(raw) -> str:
        """归一化指令的原始参数。

        老版本内核的 ``CommandFilter`` 未使用 ``eval_str=True`` 解析注解时，
        会把注解字面量 ``"GreedyStr"`` 当成缺省值塞进来。这里统一消掉，
        免得拿它去请求一个叫 "GreedyStr" 的 X 账号。
        """
        text = str(raw or "").strip()
        return "" if text == "GreedyStr" else text

    @classmethod
    def _split_first(cls, raw) -> tuple[str, str]:
        """把「第一个词 + 其余全部」切开。

        指令参数一律收成一个 GreedyStr 再在这里切分：``CommandFilter`` 只在参数
        **没有默认值**时才认得 ``GreedyStr``（``init_handler_md`` 里"有默认值则存默认值"），
        而"给 handle 一个默认值、后面再跟无默认值的 GreedyStr"在语法上也不成立。
        收成一个贪婪参数既绕开这两点，又能让昵称、链接里的空格被完整保留。
        """
        parts = cls._clean_raw(raw).split(None, 1)
        if not parts:
            return "", ""
        if len(parts) == 1:
            return parts[0], ""
        return parts[0], parts[1].strip()

    @filter.command("x订阅", alias={"x关注", "xadd"})
    async def cmd_subscribe(self, event: AstrMessageEvent, raw: GreedyStr):
        """订阅一个 X 账号：/x订阅 <handle> [昵称]"""
        handle, nickname = self._split_first(raw)
        if not handle:
            yield event.plain_result(
                "用法：/x订阅 <handle> [昵称]\n"
                "例如 /x订阅 elonmusk 马斯克\n"
                "昵称会同时用作合并转发节点名与图库 tag；不填则显示推主名且不入库。"
            )
            event.stop_event()
            return
        if not self._is_global_admin(event):
            hint = self._require_subscribe(event)
            if hint:
                yield event.plain_result(hint)
                event.stop_event()
                return
        self.node_uin(event)
        resolved = await self._resolve_author(handle)
        if resolved is None:
            yield event.plain_result(
                f"⛔ 找不到 X 账号「{handle}」，或抓取服务暂时不可用，请稍后再试。"
            )
            event.stop_event()
            return
        author_id, clean, name, _avatar = resolved
        umo = event.unified_msg_origin
        group_id = str(event.get_group_id() or "")
        nick_raw = str(nickname or "").strip()
        # 与 /x昵称 共用同一套"清空"词表：以前这里没有，`/x订阅 elonmusk 清空`
        # 会把字面量「清空」写进库（独立验证 O-B7）。
        if nick_raw in ("清空", "清除", "默认", "none", "-"):
            nick_raw = ""
        nick_ok, nick, nick_err = parse_nickname_input(nick_raw or None)
        if not nick_ok:
            yield event.plain_result(
                f"⛔ 昵称不合法：{nick_err}。"
                "（不可含 \\ / : * ? \" < > | 与控制字符，也不可含无法编码的字符。）"
            )
            event.stop_event()
            return
        author_row = self.store.get_author(author_id)
        start_ts = int(author_row["last_seen_ts"] or 0) if author_row else 0
        created = self.store.add_edge(umo, group_id, author_id, nick, start_ts)
        if not created:
            self.store.set_edge_nickname(umo, author_id, nick)
            yield event.plain_result(f"ℹ️ 已订阅过 @{clean}，已更新昵称为「{nick or '（默认）'}」。")
        else:
            yield event.plain_result(
                f"✅ 已订阅 @{clean}（{name}）。\n"
                f"昵称：{nick or '（未设置，使用推主名，且不会同步到图库）'}\n"
                f"首次订阅不补发历史，从现在开始监控。"
            )
        self._force_poll = True
        self._poll_wake.set()
        event.stop_event()

    @filter.command("x取消订阅", alias={"x取关", "xdel"})
    async def cmd_unsubscribe(self, event: AstrMessageEvent, handle: str = ""):
        """取消订阅：/x取消订阅 <handle>"""
        if not str(handle).strip():
            yield event.plain_result("用法：/x取消订阅 <handle>")
            event.stop_event()
            return
        umo = event.unified_msg_origin
        edge = self.store.get_edge_by_handle(umo, handle)
        if edge is None:
            # 「本群没有订阅 X」不敏感，先如实回答比先甩权限错误更有用；
            # 真正的权限校验留到下面实际要改动数据时。
            yield event.plain_result(f"ℹ️ 本群没有订阅 @{handle.lstrip('@')}。")
            event.stop_event()
            return
        ok, why = self.can_manage_edge(event, edge)
        if not ok:
            yield event.plain_result(why)
            event.stop_event()
            return
        self.store.remove_edge(umo, str(edge["author_id"]))
        yield event.plain_result(f"✅ 已取消订阅 @{handle.lstrip('@')}。")
        event.stop_event()

    @filter.command("x订阅列表", alias={"x列表", "xlist"})
    async def cmd_list(self, event: AstrMessageEvent):
        """列出本群订阅：/x订阅列表"""
        self.node_uin(event)
        hint = self._require_view(event)
        if hint:
            yield event.plain_result(hint)
            event.stop_event()
            return
        edges = self.store.list_edges(event.unified_msg_origin)
        if not edges:
            yield event.plain_result("本群还没有订阅任何 X 账号。用 /x订阅 <handle> 添加。")
            event.stop_event()
            return
        lines = []
        for edge in edges:
            author = self.store.get_author(str(edge["author_id"]))
            handle = str(author["handle"]) if author else str(edge["author_id"])
            name = str(author["display_name"]) if author else ""
            nick = str(edge["nickname"] or "")
            flag = "▶️" if int(edge["enabled"]) else "⏸"
            pending = self.store.pending_count_for_edge(int(edge["edge_id"]))
            extra = f"，昵称「{nick}」" if nick else ""
            if edge_media_only(edge):
                extra += "，仅媒体"
            if pending:
                extra += f"，待发 {pending}"
            lines.append(f"{flag} @{handle}（{name}）{extra}")
        mode = self.group_mode(event)
        yield event.plain_result(
            f"📋 本群订阅（{len(edges)} 个，群模式 {mode}）：\n" + "\n".join(lines)
        )
        event.stop_event()

    @filter.command("x昵称", alias={"xnick"})
    async def cmd_nickname(self, event: AstrMessageEvent, raw: GreedyStr):
        """设置/清空订阅昵称：/x昵称 <handle> <昵称|清空>

        昵称同时用作**合并转发节点名**与**图库 tag**，因此在这里统一校验。
        """
        handle, nickname = self._split_first(raw)
        if not handle:
            yield event.plain_result("用法：/x昵称 <handle> <昵称|清空>")
            event.stop_event()
            return
        umo = event.unified_msg_origin
        edge = self.store.get_edge_by_handle(umo, handle)
        if edge is None:
            yield event.plain_result(f"ℹ️ 本群没有订阅 @{handle.lstrip('@')}。")
            event.stop_event()
            return
        ok, why = self.can_manage_edge(event, edge)
        if not ok:
            yield event.plain_result(why)
            event.stop_event()
            return
        raw = str(nickname or "").strip()
        if raw in ("清空", "清除", "默认", "none", "-"):
            raw = ""
        if raw:
            nick_ok, nick, nick_err = parse_nickname_input(raw)
            if not nick_ok:
                yield event.plain_result(
                    f"⛔ 昵称不合法：{nick_err}。"
                    "（不可含 \\ / : * ? \" < > | 与控制字符，也不可含无法编码的字符。）"
                )
                event.stop_event()
                return
        else:
            nick = ""
        self.store.set_edge_nickname(umo, str(edge["author_id"]), nick)
        if nick:
            yield event.plain_result(
                f"✅ 已把 @{handle.lstrip('@')} 的昵称设为「{nick}」。\n"
                f"该昵称同时用于合并转发节点名与图库 tag。"
            )
        else:
            yield event.plain_result(
                f"✅ 已清空 @{handle.lstrip('@')} 的昵称。\n"
                "未设置昵称时：合并转发显示推主名，且**不会**同步到图库。"
            )
        event.stop_event()

    @filter.command("x仅媒体", alias={"xmedia", "xonlymedia"})
    async def cmd_media_only(self, event: AstrMessageEvent, raw: GreedyStr):
        """按订阅设置"只推含多媒体的帖子"：/x仅媒体 <handle> <开|关>

        默认关闭 = 拉取全部原创帖；开启后只推含**图片 / 视频 / GIF** 的帖子，
        纯文字推会被跳过。转推 / 回复 / 引用的过滤依然生效 —— 本开关是额外的收窄条件。
        """
        handle, action = self._split_first(raw)
        if not handle or not action:
            yield event.plain_result(
                "用法：/x仅媒体 <handle> <开|关>\n"
                "例如 /x仅媒体 elonmusk 开 —— 该订阅只推含图片/视频的帖子。\n"
                "默认关闭（拉取全部原创帖）。"
            )
            event.stop_event()
            return
        umo = event.unified_msg_origin
        edge = self.store.get_edge_by_handle(umo, handle)
        if edge is None:
            yield event.plain_result(f"ℹ️ 本群没有订阅 @{handle.lstrip('@')}。")
            event.stop_event()
            return
        ok, why = self.can_manage_edge(event, edge)
        if not ok:
            yield event.plain_result(why)
            event.stop_event()
            return
        word = action.strip().lower()
        if word in ("开", "开启", "on", "1", "true", "是", "yes"):
            flag = True
        elif word in ("关", "关闭", "off", "0", "false", "否", "no"):
            flag = False
        else:
            yield event.plain_result("⛔ 第二个参数只接受「开」或「关」。")
            event.stop_event()
            return
        self.store.set_edge_media_only(umo, str(edge["author_id"]), flag)
        clean = handle.lstrip("@")
        if flag:
            yield event.plain_result(
                f"✅ 已把 @{clean} 设为「仅媒体」：只推含图片 / 视频 / GIF 的帖子，"
                "纯文字推跳过。\n"
                "转推 / 回复 / 引用的过滤依然生效；已跳过的纯文字帖不会补发。"
            )
        else:
            yield event.plain_result(f"✅ 已把 @{clean} 恢复为「拉取全部原创帖」。")
        event.stop_event()

    @filter.command("x推送", alias={"xpush"})
    async def cmd_toggle(
        self, event: AstrMessageEvent, first: str = "", second: str = ""
    ):
        """开关推送：/x推送 开|关 [handle]（省略 handle 表示本群全部订阅）"""
        umo = event.unified_msg_origin
        words_on = ("开", "on", "1", "true", "启用", "开启")
        words_off = ("关", "off", "0", "false", "停用", "关闭")
        head = str(first or "").strip().lower()
        tail = str(second or "").strip()
        if head in words_on:
            enabled, target_handle = True, tail
        elif head in words_off:
            enabled, target_handle = False, tail
        elif tail.strip().lower() in words_on:
            enabled, target_handle = True, str(first).strip()
        elif tail.strip().lower() in words_off:
            enabled, target_handle = False, str(first).strip()
        else:
            yield event.plain_result("用法：/x推送 开|关 [handle]（省略 handle 表示本群全部订阅）")
            event.stop_event()
            return

        edges = self.store.list_edges(umo)
        if not edges:
            yield event.plain_result("本群还没有订阅任何 X 账号。")
            event.stop_event()
            return
        target = self.store.get_edge_by_handle(umo, target_handle) if target_handle else None
        if target_handle and target is None:
            yield event.plain_result(f"ℹ️ 本群没有订阅 @{target_handle.lstrip('@')}。")
            event.stop_event()
            return
        changed = 0
        for edge in edges:
            if target is not None and edge["edge_id"] != target["edge_id"]:
                continue
            ok, _why = self.can_manage_edge(event, edge)
            if not ok:
                continue
            self.store.set_edge_enabled(umo, str(edge["author_id"]), enabled)
            changed += 1
        if not changed:
            yield event.plain_result(self.denied_hint(event))
            event.stop_event()
            return
        yield event.plain_result(
            f"✅ 已{'开启' if enabled else '暂停'}本群 {changed} 个订阅的推送。"
        )
        event.stop_event()

    @filter.command("x状态", alias={"xstatus"})
    async def cmd_status(self, event: AstrMessageEvent):
        """查看运行状态：/x状态"""
        self.node_uin(event)
        if not self.can(event, "view"):
            yield event.plain_result(self.mode_hint(event))
            event.stop_event()
            return
        try:
            edges = self.store.list_edges()
        except Exception as exc:
            # 读库失败时以前会直接把异常抛出 handler（用户"发了没反应"，独立验证 O-N8）
            logger.warning(f"[x_monitor] /x状态 读取订阅列表失败: {exc}")
            yield event.plain_result("⚠️ 读取订阅数据失败，暂时无法显示状态（详见日志）。")
            event.stop_event()
            return
        mine = [e for e in edges if str(e["umo"]) == event.unified_msg_origin]
        authors = self.store.list_authors()
        depth = self.store.queue_depth(event.unified_msg_origin)
        dnd = self.in_quiet_hours(event.unified_msg_origin)
        lines = [
            "📊 X 监控状态",
            f"· 订阅总数 {len(edges)}（本群 {len(mine)}）",
            f"· 监控账号 {len(authors)}",
            f"· 已收录推文 {self.store.count_posts()}（含媒体 {self.store.media_post_count()}）",
            f"· 待投递队列 {depth}",
            f"· 本群模式 {self.group_mode(event)}｜管理范围 {self.manage_scope(event)}",
            f"· 免打扰 {'生效中 ⏸' if dnd else '未生效 ▶️'}",
            f"· 翻译 {'开启' if self.bool_cfg('translate_enabled', False) else '关闭'}"
            f"｜发送 {'合并转发' if str(self.cfg('send_mode', 'forward')).lower() == 'forward' else '普通链'}",
        ]
        pool = self._pool_bridge.snapshot() if getattr(self, "_pool_bridge", None) else None
        if pool is not None:
            # 快照是本插件自己的，但一律 .get()：硬索引会让"将来删掉一个键"
            # 直接打挂整条 /x状态（删 note 那次就是这类改动，H-7 / L-C6）。
            if "enabled" not in pool:
                # .get() 消灭了 KeyError，但别把"字段缺失"说成"功能关闭"（N-3）
                lines.append("· 图库联动 状态未知（快照缺字段，详见日志）")
            elif not pool.get("enabled"):
                lines.append("· 图库联动 关闭（image_pool_sync_enabled）")
            elif pool.get("available"):
                lines.append(
                    f"· 图库联动 开启｜累计入库 {pool.get('added', 0)} 张"
                    f"（跳过 {pool.get('skipped', 0)}）"
                )
                # **当前态与历史计数分开说**：把它们拼在一行、共用一个"当前配置下"前缀
                # 会造成作用域错误 —— 只有第一段是当前态，后面几段全是历史累计
                # （独立验证 L-④ 用真实路径复现过这句自相矛盾的话）。
                tag_state, rejected = self.pool_tag_state_and_rejected()
                state_text = IMAGE_POOL_TAG_STATE_TEXT.get(tag_state, "")
                if state_text:
                    lines.append(f"· ⚠️ 当前配置下不会入库：{state_text}")
                # **只用"当前配置里被拒的昵称个数"**：rejected_tags 是 tags_for 的调用次数，
                # 不是昵称个数，拿来兜底会输出"另有 5 个昵称（NUL）"而实际只有 1 个
                # 且可能早已清空（独立验证 N-1）。
                if tag_state != "rejected_only" and rejected:
                    lines.append(
                        f"· ⚠️ 另有 {len(rejected)} 个昵称不能用作图库分类名"
                        f"（{'、'.join(rejected[:3])}）→ 这些订阅不会入库"
                    )
                history: list[str] = []
                for key, label in (
                    ("download_failed", "张照片下载失败"),
                    ("timeouts", "次入库超时（对端可能稍后才写入）"),
                    ("tag_failures", "次 tag 写入失败"),
                    ("budget_exhausted", "次预算用尽（当轮剩余帖子未归档）"),
                    ("unavailable", "次对端不可用"),
                    ("rejected_tags", "次昵称被本地守卫拒绝"),
                ):
                    value = pool.get(key, 0)
                    if value:
                        history.append(f"{value} {label}")
                # failed 是超时 + tag 失败 + 其它异常的合计，只把"其它"单列，避免超集-子集并列
                others = max(0, int(pool.get("failed", 0) or 0)
                             - int(pool.get("timeouts", 0) or 0)
                             - int(pool.get("tag_failures", 0) or 0))
                if others:
                    history.append(f"{others} 次其它异常")
                if history:
                    lines.append(
                        "· ⚠️ 图库入库历史故障（累计）：" + "；".join(history)
                        + "。这些图不会补归档。"
                    )
            else:
                reason = pool.get("reason_text") or pool.get("reason") or "原因未知"
                lines.append(f"· ⚠️ 图库联动已开启但不可用：{reason}")
        recent = self.store.recent_deliveries(20)
        failed = [r for r in recent if str(r["state"]) == STATE_FAILED]
        if failed:
            last = failed[0]
            lines.append(
                f"· ⚠️ 最近 {len(recent)} 次投递中失败 {len(failed)} 条"
                f"（最近一次 @{last['handle']}：{str(last['last_error'])[:40]}）"
            )
        health = self.provider_health()
        if health.get("chain"):
            lines.append(f"· 数据源 {' > '.join(health['configured'])}（当前 {health['active']}）")
            if health.get("pinned"):
                lines.append(f"· 📌 已手动指定数据源 {health['pinned']}（下一次轮询优先用它）")
            if health.get("degraded"):
                tripped = [item["name"] for item in health["chain"] if item.get("tripped")]
                lines.append(
                    f"· ⚠️ 降级中：{'、'.join(tripped)} 连续失败已被挪到队尾"
                    f"（其余源都失败时仍会兜底试它）"
                )
            lines.append("· 各账号生效源与实时探测：/x源（仅全局管理员）")
        else:
            # 空链/未初始化时也必须把话说出来：否则抓取全停而 /x状态 看似一切正常
            lines.append(f"· ⚠️ 数据源不可用：{health.get('message') or '抓取链为空'}")
        yield event.plain_result("\n".join(lines))
        event.stop_event()

    @filter.command("x源", alias={"xsource"})
    async def cmd_source(self, event: AstrMessageEvent, arg: str = ""):
        """查看 / 指定数据源：/x源 [源名|auto|探测]（仅全局管理员）"""
        self.node_uin(event)
        if not self._is_global_admin(event):
            yield event.plain_result(
                "⛔ 只有全局管理员可以查看或指定数据源"
                "（会改变抓取行为并可能产生第三方费用）。"
            )
            event.stop_event()
            return

        chain = self._ensure_chain()
        health = self.provider_health()
        action = str(arg or "").strip().lower()
        lines: list[str] = []

        if action in ("", "列表", "list"):
            lines.append("🔌 数据源降级链")
            if not health.get("chain"):
                lines.append(f"· ⚠️ {health.get('message') or '抓取链为空'}")
            if health.get("chain"):
                lines.append(f"· 配置顺序：{' > '.join(health['configured'])}")
                lines.append(f"· 当前尝试顺序：{' > '.join(item['name'] for item in health['chain'])}")
            for item in health.get("chain") or []:
                mark = "熔断" if item.get("tripped") else "正常"
                # L 序号用**配置序号**（就是文档里的 L1/L2/L3），当前顺位另说，
                # 否则降级后"L2 排在 L1 上面"会让人读不懂
                index = int(item.get("index", 0)) + 1
                order = int(item.get("order", 0)) + 1
                where = "" if order == index else f"｜当前第 {order} 顺位"
                missing = (
                    f"｜账号不存在 {item['not_found']} 次" if int(item.get("not_found") or 0) else ""
                )
                broken = "｜源探测失败" if item.get("broken") else ""
                lines.append(
                    f"· L{index} {item['name']}｜{mark}{where}｜成功 {item['ok']}｜"
                    f"连续失败 {item['failures']}{missing}{broken}"
                    + (f"｜{int(item.get('retry_in', 0))}s 后重试" if item.get("tripped") else "")
                )
                if item.get("last_error"):
                    lines.append(f"    最近错误：{str(item['last_error'])[:80]}")
                for instance in item.get("instances") or []:
                    state = "可用" if instance.get("healthy") else "熔断"
                    lines.append(
                        f"    ↳ 实例 {instance['name']}｜{state}｜"
                        f"失败 {instance.get('failures', 0)}"
                        + (f"｜{str(instance.get('last_error'))[:60]}" if instance.get("last_error") else "")
                    )
            if health.get("pinned"):
                lines.append(f"📌 已手动指定 {health['pinned']}（只对下一次抓取生效）")
            lines.append("用法：/x源 探测｜/x源 <源名>｜/x源 auto")
            lines.append(f"可选项：{', '.join(PROVIDER_NAMES)}")
        elif action in ("探测", "probe", "test"):
            if chain is None:
                lines.append(f"⚠️ {health.get('message') or '抓取链为空，无法探测'}")
            else:
                lines.append(f"🩺 数据源实时探测（handle = {self._probe_handle()}）")
                for item in await self.probe_providers():
                    mark = "✅ 可用" if item["ok"] else "❌ 不可用"
                    extra = f"（{item['items']} 条）" if item.get("items") else ""
                    detail = "" if item["ok"] else f"：{str(item.get('error') or '')[:80]}"
                    lines.append(
                        f"· {item['name']}（{item.get('kind') or '?'}）：{mark}{extra}{detail}"
                    )
        elif action == "auto":
            if chain is not None:
                chain.clear_pin()
            lines.append("✅ 已取消手动指定，恢复按 fetch_chain 顺序自动降级")
        else:
            if chain is None:
                lines.append(f"⚠️ {health.get('message') or '抓取链为空'}")
            elif action in (health.get("configured") or []):
                chain.set_pin(action)
                lines.append(
                    f"📌 已把 {action} 提到队首；**只对下一次轮询生效**，之后自动恢复原顺序。"
                )
                lines.append("（/x源 auto 可立即取消；订阅解析与回放不会消耗它，也不会受它影响）")
            else:
                lines.append(f"❌ 未知数据源「{arg}」。可选项：{', '.join(PROVIDER_NAMES)}")

        yield event.plain_result("\n".join(lines))
        event.stop_event()

    async def probe_providers(self, handle: str = "") -> list[dict]:
        """对链上每个源做一次独立的可用性探测。

        刻意**不**复用主链实例，也不动主链的熔断计数：
        探测是诊断动作，绝不能因为它失败就把正常抓取的顺序搞乱。
        """
        self._ensure_chain()
        target = str(handle or "").strip().lstrip("@") or self._probe_handle()
        if self._http is None:
            return []
        results: list[dict] = []
        for name in self._config_chain_names():
            if name == PROVIDER_FXTWITTER:
                provider: FetchProvider = FxTwitterProvider(
                    self._http, base=self.cfg("fxtwitter_api_base", FXTWITTER_BASE)
                )
                kind = self.cfg("fxtwitter_api_base", FXTWITTER_BASE)
            elif name == PROVIDER_FXTWITTER_RSS:
                provider = RssProvider(self._http, base=self.cfg("rss_base", FXTWITTER_RSS_BASE))
                kind = self.cfg("rss_base", FXTWITTER_RSS_BASE)
            elif name == PROVIDER_RSSHUB:
                provider = RssHubProvider(
                    self._http, instances=to_str_list(self.cfg("rsshub_instances", []))
                )
                kind = "、".join(to_str_list(self.cfg("rsshub_instances", []))) or "未配置实例"
            else:
                continue
            try:
                result = await asyncio.wait_for(
                    provider.fetch_user(target, since_ts=0), timeout=FETCH_PROVIDER_TIMEOUT
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                result = FetchResult(ok=False, error=f"{type(exc).__name__}: {exc}")
            results.append(
                {
                    "name": name,
                    "kind": str(kind),
                    "ok": bool(result.ok),
                    "no_new": bool(result.no_new),
                    "items": len(result.posts or []),
                    "error": str(result.error or ""),
                }
            )
        return results

    def _probe_handle(self) -> str:
        return str(self.cfg("probe_handle", DEFAULT_PROBE_HANDLE) or DEFAULT_PROBE_HANDLE).lstrip("@")

    def role_of(self, event: AstrMessageEvent) -> str:
        """把调用者的身份翻译成人话，用于 /x帮助 与拒绝提示。"""
        if self._is_global_admin(event):
            return "全局管理员"
        if self._is_group_admin(event):
            return "群级管理员"
        mode = self.group_mode(event)
        if mode == "whitelist":
            return "普通成员（白名单群）"
        if mode == "readonly":
            return "普通成员（只读群）"
        return "普通成员（未配置群）"

    @filter.command("x帮助", alias={"xhelp", "x指令", "x说明"})
    async def cmd_help(self, event: AstrMessageEvent):
        """查看本插件全部指令与用法：/x帮助

        AstrBot 内置的 /help 只列 reserved（内置）指令，插件指令不会出现在里面，
        所以这里提供插件自己的帮助。对所有人可见（只是文档，不含数据）。
        """
        self.node_uin(event)
        self.store.touch_group(event.unified_msg_origin)
        lines = [
            "📖 X 监控转发 · 指令帮助",
            "",
            "/x帮助                        就是本条（列出全部指令与权限分层）",
            "",
            "【订阅管理】",
            "/x订阅 <handle> [昵称]        订阅账号",
            "    昵称同时用作合并转发节点名与图库 tag；不填则显示推主名且不入图库",
            "/x取消订阅 <handle>           取消订阅",
            "/x订阅列表                    列出本群订阅、昵称、仅媒体、待发条数",
            "/x昵称 <handle> <昵称|清空>    设置 / 清空昵称",
            "/x仅媒体 <handle> <开|关>      只推含图片 / 视频 / GIF 的帖子（默认关）",
            "/x推送 开|关 [handle]         开关本群（或单个）订阅的推送",
            "",
            "【查看】",
            "/x状态                        运行状态（订阅数、队列、模式、翻译、数据源、图库联动）",
            "/x在哪                        本群 umo / 群号 / 你的 UID",
            "/x解析 <url>                  解析一条 X 链接",
            "",
            "【免打扰】（按群生效，群管理员可改）",
            "/x免打扰 <时段|关>             如 /x免打扰 23:00-08:00（支持跨天、多段）",
            "/x时段                        查看本群时段与积压",
            "",
            "【图库联动】（可选，默认关闭；配置项 image_pool_sync_enabled）",
            "开启后把推文照片归档进 astrbot_plugin_image_pool，tag = 该订阅的自定义昵称；",
            "只有设置了昵称的订阅才归档；X 归档图不归属任何群，只有图库的全局管理员能管理。",
            "",
            "【测试与回放】",
            "/x测试 [handle]               链路自检；带 handle 会显示节点昵称来源",
            "/x补发 <handle> [n]           回放最近 n 条真实推文（不污染水位与投递记录）",
            "",
            "【管理员】",
            "/x源 [源名|auto|探测]          查看 / 指定数据源，探测各源可用性（仅全局管理员）",
            "/x群模式 <whitelist|readonly|none>   设置本群权限模式（仅全局管理员）",
            "/x授权 <群号:UID>              添加群级管理员（仅全局管理员）",
            "/x撤权 <群号:UID>              移除群级管理员（仅全局管理员）",
            "",
            f"【本群】模式 {self.group_mode(event)}｜你的身份 {self.role_of(event)}",
            "· whitelist 白名单群：全员可订阅、可查看",
            "· readonly 只读群：全员可查看，仅管理员可订阅",
            "· none 未配置：仅管理员可操作（默认）",
            "",
            "提示：/x在哪 可查自己的 UID，用于填写管理员名单（WebUI 插件配置页）。",
            "WebUI：插件页「X 监控转发 → 管理」可图形化编辑账号↔群映射、免打扰与全局配置。",
        ]
        yield event.plain_result("\n".join(lines))
        event.stop_event()

    @filter.command("x在哪", alias={"xwhere"})
    async def cmd_where(self, event: AstrMessageEvent):
        """输出会话三要素与自身 ID，便于填写配置：/x在哪"""
        self.node_uin(event)
        # 顺带登记这个群，WebUI 的群列表才能发现它（不影响免打扰回落）
        self.store.touch_group(event.unified_msg_origin)
        umo = event.unified_msg_origin
        yield event.plain_result(
            "📍 会话信息（用于填写插件配置）\n"
            f"· UMO：{umo}\n"
            f"· 平台实例 id：{platform_id_of(umo)}\n"
            "  ⚠️ umo 第一段是**平台实例 id**（你在 AstrBot 里给这个适配器实例起的名字），"
            "不是适配器名（如 aiocqhttp）；写错会得到 "
            "\"cannot find platform for session\"\n"
            f"· 群号：{event.get_group_id() or '（私聊）'}\n"
            f"· 你的 UID：{event.get_sender_id()}\n"
            f"· 群模式：{self.group_mode(event)}\n"
            "填「全局管理员」写 UID；填「群级管理员」写 群号:UID。"
        )
        event.stop_event()

    @filter.command("x解析", alias={"xurl"})
    async def cmd_parse(self, event: AstrMessageEvent, url: GreedyStr):
        """解析一条 X 链接并就地回显：/x解析 <url>"""
        self.node_uin(event)
        hint = self._require_view(event)
        if hint:
            yield event.plain_result(hint)
            event.stop_event()
            return
        text = self._clean_raw(url)
        match = re.search(r"(?:x|twitter)\.com/[^/\s]+/status/(\d+)", text)
        if not match:
            match = re.search(r"\b(\d{15,25})\b", text)
        if not match:
            yield event.plain_result("用法：/x解析 <X 推文链接>")
            event.stop_event()
            return
        post_id = match.group(1)
        if self._http is None:
            yield event.plain_result("⛔ 插件尚未初始化完成，请稍后再试。")
            event.stop_event()
            return
        try:
            resp = await self._http.get(f"{FXTWITTER_BASE}/2/status/{post_id}")
            payload = resp.json() if resp.status_code == 200 else {}
        except Exception as exc:
            yield event.plain_result(f"⛔ 解析失败：{exc}")
            event.stop_event()
            return
        status = payload.get("status") if isinstance(payload, dict) else None
        if not isinstance(status, dict):
            yield event.plain_result("⛔ 这条推文不存在或已被删除。")
            event.stop_event()
            return
        body = self.post_text(status, "")
        yield event.plain_result(body)
        event.stop_event()

    # ------------------------------------------------------------ 命令：管理

    @filter.command("x群模式", alias={"xmode"})
    async def cmd_group_mode(self, event: AstrMessageEvent, mode: str = ""):
        """设置本群（或指定群）的权限模式：/x群模式 <whitelist|readonly|none> [群号]"""
        if not self._is_global_admin(event):
            yield event.plain_result("⛔ 群模式仅限全局管理员（plugin_admins）修改。")
            event.stop_event()
            return
        target = str(mode or "").strip().lower()
        if target not in VALID_MODES:
            yield event.plain_result(
                "用法：/x群模式 <whitelist|readonly|none>\n"
                "· whitelist 白名单群：全员可订阅、可查看\n"
                "· readonly 只读群：全员可查看，仅管理员可订阅\n"
                "· none 未配置：仅管理员可操作（默认）"
            )
            event.stop_event()
            return
        gid = str(event.get_group_id() or "")
        if not gid:
            yield event.plain_result("⛔ 请在目标群里执行本指令（私聊无法确定目标群）。")
            event.stop_event()
            return
        open_list = [x for x in to_str_list(self.cfg("open_groups", []))]
        readonly_list = [x for x in to_str_list(self.cfg("readonly_groups", []))]
        for lst in (open_list, readonly_list):
            while gid in lst:
                lst.remove(gid)
        if target == "whitelist":
            open_list.append(gid)
        elif target == "readonly":
            readonly_list.append(gid)
        is_open = self.bool_cfg("readonly_allow_manage", False)
        try:
            self.config["open_groups"] = open_list
            self.config["readonly_groups"] = readonly_list
            save = getattr(self.config, "save_config", None)
            if callable(save):
                save()
        except Exception as exc:
            yield event.plain_result(f"⛔ 保存配置失败：{exc}")
            event.stop_event()
            return
        yield event.plain_result(
            f"✅ 已把本群（{gid}）设为「{target}」模式。"
            + ("（只读群管理能力开关：" + ("开" if is_open else "关") + "）")
        )
        event.stop_event()

    @filter.command("x授权", alias={"xgrant"})
    async def cmd_grant(self, event: AstrMessageEvent, pair: str = ""):
        """添加群级管理员：/x授权 <群号:UID>"""
        if not self._is_global_admin(event):
            yield event.plain_result("⛔ 授权仅限全局管理员。")
            event.stop_event()
            return
        value = str(pair or "").strip()
        if ":" not in value:
            yield event.plain_result("用法：/x授权 <群号:UID>（例如 123456:10001，或 *:10001 表示所有群）")
            event.stop_event()
            return
        current = to_str_list(self.cfg("group_admins", []))
        if value in current:
            yield event.plain_result(f"ℹ️ 「{value}」已经是群级管理员。")
            event.stop_event()
            return
        current.append(value)
        self.config["group_admins"] = current
        save = getattr(self.config, "save_config", None)
        if callable(save):
            save()
        yield event.plain_result(f"✅ 已授权群级管理员：{value}（只能管理本群订阅）")
        event.stop_event()

    @filter.command("x撤权", alias={"xrevoke"})
    async def cmd_revoke(self, event: AstrMessageEvent, pair: str = ""):
        """移除群级管理员：/x撤权 <群号:UID>"""
        if not self._is_global_admin(event):
            yield event.plain_result("⛔ 撤权仅限全局管理员。")
            event.stop_event()
            return
        value = str(pair or "").strip()
        current = to_str_list(self.cfg("group_admins", []))
        if value not in current:
            yield event.plain_result(f"ℹ️ 「{value}」不在群级管理员名单里。")
            event.stop_event()
            return
        current = [x for x in current if x != value]
        self.config["group_admins"] = current
        save = getattr(self.config, "save_config", None)
        if callable(save):
            save()
        yield event.plain_result(f"✅ 已移除群级管理员：{value}")
        event.stop_event()

    # ------------------------------------------------------------ 命令：免打扰

    @filter.command("x免打扰", alias={"xdnd"})
    async def cmd_dnd(self, event: AstrMessageEvent, arg: GreedyStr):
        """配置**本群**的免打扰：/x免打扰 <时段|关>

        支持跨天（如 23:00-08:00），可多段用逗号分隔。免打扰期间只入队不发送。

        免打扰按会话存储：群管理员改的是自己群的时段，不会波及其它群。
        """
        if self.manage_scope(event) == "none":
            yield event.plain_result(self.denied_hint(event))
            event.stop_event()
            return
        umo = event.unified_msg_origin
        raw = self._clean_raw(arg)
        if not raw:
            enabled, ranges_text = self.dnd_settings(umo)
            ranges = parse_dnd_ranges([ranges_text])
            readable = (
                ", ".join(f"{s // 60:02d}:{s % 60:02d}-{e // 60:02d}:{e % 60:02d}" for s, e in ranges)
                or "（未设置）"
            )
            yield event.plain_result(
                f"🌙 本群免打扰：{'开启' if enabled else '关闭'}\n"
                f"时段：{readable}\n"
                f"时区：{self.cfg('dnd_timezone', 'Asia/Shanghai')}｜"
                f"速率：{self.int_cfg('dnd_drain_rate', 5)} 条/分｜"
                f"积压上限：{self.int_cfg('dnd_backlog_limit', 50)}\n"
                f"当前：{'生效中 ⏸（只入队不发送）' if self.in_quiet_hours(umo) else '未生效 ▶️'}\n"
                "用法：/x免打扰 23:00-08:00 或 /x免打扰 关"
            )
            event.stop_event()
            return
        if raw.lower() in ("关", "关闭", "off", "0", "false", "none", "清空"):
            # 删除覆盖行而不是置 0：置 0 会让这个群**永久**不再继承全局默认值，
            # 而且没有任何恢复入口。
            self.store.clear_group_dnd(umo)
            still, ranges_now = self.dnd_settings(umo)
            if still:
                tail = (
                    f"已清除本群设置，回落到全局默认「{ranges_now}」"
                    "（当前仍在免打扰中；如需彻底关闭请改全局配置）"
                )
            else:
                tail = "推文将即时发送。"
            yield event.plain_result(f"✅ 本群免打扰设置已清除。{tail}")
            self._deliver_wake.set()
            event.stop_event()
            return
        if not parse_dnd_ranges([raw]):
            yield event.plain_result(
                "⛔ 时段格式不对。示例：/x免打扰 23:00-08:00（支持跨天，多段用逗号分隔）"
            )
            event.stop_event()
            return
        parts = [r.strip() for r in re.split(r"[,\n]", raw) if r.strip()]
        self.store.set_group_dnd(umo, True, ",".join(parts))
        self._deliver_wake.set()
        yield event.plain_result(
            f"✅ 本群免打扰已开启：{', '.join(parts)}\n"
            f"时段内获取到的推文会逐条排队，时段结束后按 "
            f"{self.int_cfg('dnd_drain_rate', 5)} 条/分发送。"
        )
        event.stop_event()

    @filter.command("x时段", alias={"xdndinfo"})
    async def cmd_dnd_info(self, event: AstrMessageEvent):
        """查看免打扰状态与积压：/x时段"""
        if not self.can(event, "view"):
            yield event.plain_result(self.mode_hint(event))
            event.stop_event()
            return
        umo = event.unified_msg_origin
        enabled, ranges_text = self.dnd_settings(umo)
        ranges = parse_dnd_ranges([ranges_text])
        readable = ", ".join(f"{s // 60:02d}:{s % 60:02d}-{e // 60:02d}:{e % 60:02d}" for s, e in ranges)
        yield event.plain_result(
            f"🌙 本群免打扰：{'开启' if enabled else '关闭'}"
            f"（{readable or '未设置'}，时区 {self.cfg('dnd_timezone', 'Asia/Shanghai')}）\n"
            f"当前：{'生效中 ⏸' if self.in_quiet_hours(umo) else '未生效 ▶️'}\n"
            f"待投递队列：{self.store.queue_depth(umo)} 条（上限 "
            f"{self.int_cfg('dnd_backlog_limit', 50)}）"
        )
        event.stop_event()

    # ------------------------------------------------------------ 命令：回放

    async def replay_recent(self, umo: str, handle: str, count: int) -> dict:
        """回放某账号最近 ``count`` 条推文到指定会话。

        命令 `/x补发` 与 WebUI 的"回放"按钮**共用这一段**，避免两处各写一遍
        过滤与发送阶梯而漂移。

        刻意**不推进水位、不写投递记录**：可以反复执行，不会污染去重状态，
        也不会与后续真实推送重复。

        返回 ``{ok, sent, total, handle, nickname, subscribed, media_only, message}``。
        """
        clean = str(handle or "").lstrip("@").strip()
        if not clean:
            return {"ok": False, "message": "缺少 handle"}
        chain = self._ensure_chain()
        if chain is None:
            return {"ok": False, "message": self._chain_unavailable_message()}
        count = max(1, int(count))

        result = await chain.fetch_user(
            clean, since_ts=0, with_replies=self.bool_cfg("include_replies", False), consume_pin=False
        )
        if not result.ok:
            return {
                "ok": False,
                "message": f"抓取 @{clean} 失败：{result.error or '未知错误'}",
            }
        self.record_result_sources(clean, result)

        edge = self.store.get_edge_by_handle(umo, clean)
        nickname = normalize_nick(edge["nickname"]) if edge is not None else ""
        # 回放与正常推送保持同一套过滤：订阅开了「仅媒体」，回放也只回放带媒体的
        media_only = edge is not None and edge_media_only(edge)

        records = [
            rec for rec in (self.to_post_record(o) for o in dedupe_by_id(result.posts)) if rec
        ]
        if media_only:
            records = [rec for rec in records if rec.get("has_media")]
        records.sort(key=lambda r: r["created_ts"])
        picked = records[-count:]
        if not picked:
            hint = (
                "该订阅开启了「仅媒体」，而该账号近期没有带媒体的帖子"
                if media_only
                else "该账号可能只发转推 / 回复"
            )
            return {
                "ok": False,
                "message": f"@{clean} 最近没有符合当前过滤规则的推文（{hint}）",
            }

        if self.media_download_enabled():
            picked = await self._prepare_media(picked)

        rate = max(1, self.int_cfg("dnd_drain_rate", 5))
        sent = 0
        for index, record in enumerate(picked):
            if index:
                await self._respect_rate(rate)
            body = json.loads(record["body_json"] or "{}")
            media = json.loads(record["media_json"] or "[]")
            translated = ""
            if self.bool_cfg("translate_enabled", False):
                translated = await self._translator.translate(
                    umo, str(body.get("text") or ""), str(body.get("lang") or "")
                )
            text = self.post_text(body, translated, nickname)
            try:
                ok = await self._deliver_payload(
                    umo,
                    body,
                    text,
                    self._image_items(media),
                    self._video_items(media),
                    nickname,
                )
            except Exception as exc:
                logger.error(f"[x_monitor] 回放失败: {exc}", exc_info=True)
                ok = False
            sent += 1 if ok else 0

        return {
            "ok": True,
            "sent": sent,
            "total": len(picked),
            "handle": clean,
            "nickname": nickname,
            "subscribed": edge is not None,
            "media_only": media_only,
        }

    @filter.command("x补发", alias={"x回放", "xreplay"})
    async def cmd_replay(self, event: AstrMessageEvent, raw: GreedyStr):
        """回放该账号最近的 n 条推文：/x补发 <handle> [n]

        用于**验证生产链路**（图片、翻译、合并转发、自定义昵称）以及补看漏掉的推文，
        不需要自己发帖。
        """
        if self.manage_scope(event) == "none":
            yield event.plain_result(self.denied_hint(event))
            event.stop_event()
            return
        handle, count_text = self._split_first(raw)
        if not handle:
            yield event.plain_result(
                "用法：/x补发 <handle> [条数]\n"
                "例如 /x补发 elonmusk 3 —— 回放最近 3 条，用于验证发送链路。\n"
                "该操作不推进水位、不写投递记录，可反复执行。"
            )
            event.stop_event()
            return
        default_n = self.int_cfg("replay_default", 3)
        try:
            count = int(str(count_text).strip()) if str(count_text).strip() else default_n
        except ValueError:
            count = default_n
        count = max(1, min(count, max(1, self.int_cfg("replay_max", 10))))

        self.node_uin(event)
        result = await self.replay_recent(event.unified_msg_origin, handle, count)
        if not result.get("ok"):
            yield event.plain_result(f"⛔ {result.get('message')}")
            event.stop_event()
            return

        clean = str(result.get("handle") or handle)
        if not result.get("subscribed"):
            tail = f"\nℹ️ 本群未订阅 @{clean}，节点昵称按推主名显示（用 /x订阅 可设自定义昵称）。"
        elif not result.get("nickname"):
            tail = "\nℹ️ 该订阅未设自定义昵称，节点昵称按推主名显示（用 /x昵称 可设置）。"
        else:
            tail = f"\n节点昵称使用本群自定义昵称「{result['nickname']}」。"
        yield event.plain_result(
            f"✅ 已回放 @{clean} 最近 {result['sent']}/{result['total']} 条。{tail}\n"
            "回放不推进水位、不写投递记录，可反复执行。"
        )
        event.stop_event()

    # ------------------------------------------------------------ 命令：测试

    @filter.command("x测试", alias={"xtest"})
    async def cmd_test(self, event: AstrMessageEvent, handle: str = ""):
        """测试推送链路：/x测试 [handle]"""
        self.node_uin(event)
        umo = event.unified_msg_origin
        lines = [
            "🧪 X 监控链路自检",
            "",
            "看到这条消息说明发送通道正常。",
        ]
        if handle:
            lines.append(f"目标账号：@{str(handle).lstrip('@')}")
        edges = self.store.list_edges(umo)
        lines.append(f"本群订阅：{len(edges)} 个")
        lines.append(
            "发送形态："
            + ("合并转发" if str(self.cfg("send_mode", "forward")).lower() == "forward" else "普通消息链")
        )
        lines.append(f"免打扰：{'生效中 ⏸' if self.in_quiet_hours(umo) else '未生效 ▶️'}")

        # 带了 handle 就用该订阅的自定义昵称；没设昵称则回退推主名 —— 与真实推送一致，
        # 这样 /x测试 才能真正验证"节点昵称会显示成什么"
        edge = self.store.get_edge_by_handle(umo, handle) if handle else None
        edge_nick = normalize_nick(edge["nickname"]) if edge is not None else ""
        if edge is not None and not edge_nick:
            author_row = self.store.get_author(str(edge["author_id"]))
            if author_row is not None:
                edge_nick = normalize_nick(author_row["display_name"])
        if edge_nick:
            source = "本群自定义" if normalize_nick(edge["nickname"]) else "推主名"
            lines.append(f"节点昵称：{edge_nick}（{source}）")
        else:
            fallback = normalize_nick(self.cfg("forward_nickname_fallback", "X")) or "X"
            lines.append(
                f"节点昵称：{fallback}（兜底值）"
                + ("，该订阅未设自定义昵称" if edge is not None else "")
            )
        text = "\n".join(lines)
        if str(self.cfg("send_mode", "forward")).lower() == "forward":
            ok, error, _partial = await self._send_forward(umo, text, [], edge_nick)
            if not ok:
                logger.warning(f"[x_monitor] 测试合并转发失败，降级：{error}")
                ok = await self._send_chain(umo, text, [])
        else:
            ok = await self._send_chain(umo, text, [])
        if ok:
            yield event.plain_result("✅ 发送通道正常。")
        else:
            yield event.plain_result("⛔ 发送失败，请查看 AstrBot 日志。")
        event.stop_event()
