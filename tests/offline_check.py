"""离线自检：不装 AstrBot / httpx 也能验证插件的纯逻辑、存储层、权限层与降级阶梯。

跑法：
    python plugins/astrbot_plugin_x_monitor/tests/offline_check.py

设计要点（沿用 skill 的 offline harness）：
  - 把 astrbot.* 与 httpx 全部 stub 掉，再用 importlib 按文件路径加载 main.py
  - **load_plugin 里必须保留 `sys.modules[name] = module`**：main.py 用了
    `from __future__ import annotations` + `@dataclass`，dataclass 处理字符串注解时
    会回查 `sys.modules[cls.__module__]`，缺了就会 AttributeError
  - 每个测试组用不同的 tag，各自独立一份 SQLite 与媒体目录，避免互相污染
  - 直接用 make_plugin 绕过 __init__ 构造实例，因此 __init__ 里做的事要手动补
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import importlib.util
import io
import json
import os
import re
import sqlite3
import sys
import tempfile
import time
import types
from pathlib import Path

HERE = Path(__file__).resolve().parent
PLUGIN_DIR = HERE.parent
# 注意：**不要用 tempfile.mkdtemp()** —— 在 Windows 沙箱下它建的目录无法再建子目录
# （新建目录未继承父目录 ACL），用 mkdir(parents=True) 自建则正常。
#
# 目录名带 PID：早期用固定名时，两个进程（例如并发跑变异测试）会在 import 阶段
# 互相 rmtree 对方的库，产生 `sqlite3.OperationalError: unable to open database file`
# 这类与代码无关的假崩溃，而且会在中途 abort、后半段断言静默不跑。
TMP_ROOT = Path(tempfile.gettempdir()) / f"x_monitor_offline_tests_{os.getpid()}"
if TMP_ROOT.exists():
    import shutil

    shutil.rmtree(TMP_ROOT, ignore_errors=True)
TMP_ROOT.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------------------- #
# stub：httpx（本机未安装；插件只在运行时用它，离线测试不需要真网络）
# --------------------------------------------------------------------------- #


class _FakeTimeout:
    def __init__(self, *args, **kwargs) -> None:
        pass


class _FakeAsyncClient:
    def __init__(self, *args, **kwargs) -> None:
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, *args, **kwargs):
        raise RuntimeError("offline: no network")

    async def aclose(self):
        return None

    def stream(self, *args, **kwargs):
        raise RuntimeError("offline: no network")


class WebQuery:
    """``web_api.request.query`` 的替身，支持 get(..., type=...) 与 getlist。"""

    def __init__(self, data: dict | None = None) -> None:
        self._data = dict(data or {})

    def get(self, key, default=None, type=None):  # noqa: A002 - 与真实签名一致
        value = self._data.get(key, default)
        if type is not None and value is not None:
            try:
                return type(value)
            except Exception:
                return default
        return value

    def getlist(self, key):
        value = self._data.get(key)
        if isinstance(value, list):
            return value
        return [] if value is None else [value]


class WebRequest:
    """``web_api.request`` 的替身。

    WebUI 的处理函数**不能有位置参数**，一切都从 ``web_api.request`` 读，
    所以测试只需要在调用前把 ``module.web_api.request`` 换成想要的实例。
    """

    def __init__(self, query: dict | None = None, body: dict | None = None) -> None:
        self.query = WebQuery(query)
        self._body = body
        self.method = "GET"
        self.path = ""
        self.plugin_name = "astrbot_plugin_x_monitor"
        self.username = "tester"
        self.headers: dict = {}
        self.cookies: dict = {}

    async def json(self, default=None):
        return self._body if self._body is not None else (default or {})

    async def form(self):
        return {}

    async def files(self):
        return {}

    async def body(self):
        return b""


def install_stubs() -> None:
    """注册全部 astrbot.* 与 httpx 模块，供 main.py 导入。"""
    root = TMP_ROOT
    root.mkdir(parents=True, exist_ok=True)

    def register(name: str, **attrs) -> types.ModuleType:
        module = types.ModuleType(name)
        for key, value in attrs.items():
            setattr(module, key, value)
        sys.modules[name] = module
        return module

    class _Component:
        """所有消息组件的宽松替身。"""

        def __init__(self, *args, **kwargs) -> None:
            self.args, self.kwargs = args, kwargs
            for key, value in kwargs.items():
                setattr(self, key, value)

        @classmethod
        def fromFileSystem(cls, path=None, **kwargs):
            obj = cls()
            obj.path = path
            return obj

        @classmethod
        def fromURL(cls, url=None, **kwargs):
            obj = cls()
            obj.url = url
            return obj

        @classmethod
        def fromBytes(cls, data=None, **kwargs):
            obj = cls()
            obj.data = data
            return obj

        async def convert_to_file_path(self):
            local = getattr(self, "path", None) or getattr(self, "file", None)
            if local:
                return local
            raise RuntimeError("no local path")

        async def convert_to_base64(self):
            return base64.b64encode(Path(self.path).read_bytes()).decode()

        def __repr__(self) -> str:
            return f"<{type(self).__name__}>"

    class _Dummy(_Component):
        pass

    def _decorator_factory(*args, **kwargs):
        def deco(func):
            func.__astrbot_filter__ = True
            return func

        return deco

    def _command_factory(name, alias=None, **kwargs):
        """@filter.command 专用：把指令名记下来，供"帮助清单是否覆盖全部指令"的断言使用。"""

        def deco(func):
            func.__astrbot_filter__ = True
            func.__astrbot_command__ = str(name)
            return func

        return deco

    class _RegisteringCommandable:
        """与真实对象一致：可作装饰器、可继续链式注册（否则二级子指令会 AttributeError）。"""

        def __init__(self, parents: tuple[str, ...] = ()) -> None:
            self.parents = parents

        def command(self, name, alias=None, **kwargs):
            return _decorator_factory()

        def group(self, name, alias=None, **kwargs):
            def deco(func):
                func.__astrbot_filter__ = True
                return _RegisteringCommandable(self.parents + (name,))

            return deco

        def custom_filter(self, *a, **k):
            return _decorator_factory()

        def __call__(self, func):
            return self

    class _Filter:
        permission_type = staticmethod(_decorator_factory)
        command = staticmethod(_command_factory)
        command_group = staticmethod(lambda *a, **k: _RegisteringCommandable())
        regex = staticmethod(_decorator_factory)
        event_message_type = staticmethod(_decorator_factory)
        platform_adapter_type = staticmethod(_decorator_factory)
        custom_filter = staticmethod(_decorator_factory)
        llm_tool = staticmethod(_decorator_factory)
        on_llm_request = staticmethod(_decorator_factory)
        on_llm_response = staticmethod(_decorator_factory)
        on_decorating_result = staticmethod(_decorator_factory)
        on_astrbot_loaded = staticmethod(_decorator_factory)
        after_message_sent = staticmethod(_decorator_factory)

        class PermissionType:
            ADMIN = "ADMIN"
            SHARED_GROUP_ADMIN = "SHARED_GROUP_ADMIN"

        class EventMessageType:
            ALL = "ALL"
            GROUP_MESSAGE = "GROUP_MESSAGE"
            PRIVATE_MESSAGE = "PRIVATE_MESSAGE"

        class PlatformAdapterType:
            ALL = "ALL"
            AIOCQHTTP = "AIOCQHTTP"

    class _MessageType:
        GROUP_MESSAGE = "GroupMessage"
        FRIEND_MESSAGE = "FriendMessage"
        OTHER_MESSAGE = "OtherMessage"

    class _MessageChain:
        def __init__(self, chain=None, **kwargs) -> None:
            self.chain = list(chain) if chain is not None else []

    def _silent_logger():
        noop = lambda *a, **k: None  # noqa: E731
        return types.SimpleNamespace(
            info=noop, warning=noop, error=noop, exception=noop, debug=noop, critical=noop
        )

    class StarTools:
        @classmethod
        def get_data_dir(cls, plugin_name=None):
            path = root / "plugin_data" / str(plugin_name or "unknown")
            path.mkdir(parents=True, exist_ok=True)
            return path

    class Star:
        def __init__(self, context=None) -> None:
            self.context = context

    register("httpx", AsyncClient=_FakeAsyncClient, Timeout=_FakeTimeout, HTTPError=Exception)

    api = register(
        "astrbot.api", logger=_silent_logger(), AstrBotConfig=dict, sp=types.SimpleNamespace()
    )
    # 每个组件必须是**独立**的类：插件用 isinstance 区分组件，共用一个类会让所有判断都成立
    names = (
        "Plain", "At", "AtAll", "Image", "Face", "Reply", "Record", "Video",
        "File", "Forward", "Node", "Nodes", "Json", "Poke",
    )
    components = {name: type(f"_Stub{name}", (_Component,), {}) for name in names}

    class _NodesStub(_Component):
        """真实 Nodes 的第一个参数是位置参数 nodes，替身要如实保存，否则断言看不到内容。"""

        def __init__(self, nodes=None, **kwargs) -> None:
            super().__init__(**kwargs)
            self.nodes = list(nodes or [])

    components["Nodes"] = _NodesStub
    register("astrbot", api=api)
    register(
        "astrbot.api.event",
        AstrMessageEvent=_Dummy,
        filter=_Filter,
        MessageChain=_MessageChain,
        MessageEventResult=_MessageChain,
    )
    register("astrbot.api.message_components", **components)
    register("astrbot.api.platform", MessageType=_MessageType, AstrBotMessage=_Dummy)
    register(
        "astrbot.api.star",
        Context=_Dummy,
        Star=Star,
        StarTools=StarTools,
        register=lambda *a, **k: (lambda cls: cls),
    )
    # Plugin Pages 的 web 模块。必须把它挂成 astrbot.api 的属性：
    # main.py 用 `from astrbot.api import web as web_api`，而手工注入 sys.modules
    # 不会自动给父模块设属性，getattr 会失败。
    web_module = register(
        "astrbot.api.web",
        json_response=lambda data=None, **k: data,
        error_response=lambda message="", status_code=400: {
            "error": message,
            "status_code": status_code,
        },
        file_response=lambda *a, **k: None,
        stream_response=lambda *a, **k: None,
        request=WebRequest(),
        PluginUploadFile=_Dummy,
    )
    api.web = web_module

    register("astrbot.core", logger=_silent_logger())
    register("astrbot.core.message")
    core_components = dict(components)
    core_components["BaseMessageComponent"] = _Component
    register("astrbot.core.message.components", **core_components)
    register(
        "astrbot.core.message.message_event_result",
        MessageChain=_MessageChain,
        MessageEventResult=_MessageChain,
    )
    register("astrbot.core.star")
    register("astrbot.core.star.filter")
    register(
        "astrbot.core.star.filter.command",
        GreedyStr=type("GreedyStr", (str,), {}),
        CommandFilter=_Dummy,
    )


def load_plugin(main_py: Path):
    install_stubs()
    name = f"__offline__.{main_py.parent.name}.main"
    spec = importlib.util.spec_from_file_location(name, main_py)
    module = importlib.util.module_from_spec(spec)
    # 关键：dataclass 解析字符串注解时会回查 sys.modules[cls.__module__]
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


# --------------------------------------------------------------------------- #
# 事件 / 上下文替身
# --------------------------------------------------------------------------- #


class FakeEvent:
    def __init__(self, uid="u1", gid="100", is_group=True, raw="", text="", messages=None):
        self.uid, self.gid, self.is_group = uid, gid, is_group
        self.message_str = text
        self._messages = list(messages or [])
        self.message_obj = types.SimpleNamespace(
            message_str=raw, message=self._messages, sender=None
        )
        self.stopped = False
        self.result = None
        self._extras: dict = {}

    def get_sender_id(self):
        return self.uid

    def get_group_id(self):
        return self.gid

    def get_self_id(self):
        return "bot1"

    def get_sender_name(self):
        return "tester"

    @property
    def unified_msg_origin(self):
        if self.is_group:
            return f"aiocqhttp:GroupMessage:{self.gid}"
        return f"aiocqhttp:FriendMessage:{self.uid}"

    def get_messages(self):
        return self._messages

    def get_message_type(self):
        return "GroupMessage" if self.is_group else "FriendMessage"

    def is_private_chat(self):
        return not self.is_group

    def set_extra(self, key, value):
        self._extras[key] = value

    def get_extra(self, key=None, default=None):
        return self._extras.get(key, default) if key else dict(self._extras)

    def stop_event(self):
        self.stopped = True

    def is_stopped(self):
        return self.stopped

    def plain_result(self, text):
        self.result = text
        return text

    def chain_result(self, chain):
        self.result = chain
        return chain


class FakePlatformMeta:
    """平台实例的元数据：id 才是 umo 第一段，name 只是适配器类型名。"""

    def __init__(self, pid, name="aiocqhttp", description=""):
        self.id, self.name, self.description = pid, name, description


class FakePlatformInst:
    def __init__(self, pid, name="aiocqhttp"):
        self._meta = FakePlatformMeta(pid, name)

    def meta(self):
        return self._meta


class SendContext:
    """记录发送内容的 Context 替身，同时记录 WebUI 路由注册与平台实例。"""

    def __init__(self, result=True, astrbot_config=None, platform_ids=None):
        self.sent: list[tuple] = []
        self.result = result
        self.astrbot_config = astrbot_config or {"wake_prefix": ["/"]}
        self.web_apis: list[tuple[str, list[str], str]] = []
        # 默认空表：resolve_umo 会据此降级为"只校验格式、不校验平台存在性"
        self.platform_manager = types.SimpleNamespace(
            platform_insts=[FakePlatformInst(pid) for pid in (platform_ids or [])]
        )

    def get_config(self, umo=None):
        return self.astrbot_config

    def register_web_api(self, route, handler, methods, desc=""):
        self.web_apis.append((route, list(methods), desc))

    async def send_message(self, umo, chain):
        self.sent.append((umo, chain))
        return self.result


class PluginConfig(dict):
    def save_config(self):
        return None


class FlakyContext(SendContext):
    """第 N 次发送之后开始失败，用来验证"分批部分成功"的处理。"""

    def __init__(self, fail_after: int = 1):
        super().__init__()
        self.fail_after = fail_after
        self.calls = 0

    async def send_message(self, umo, chain):
        self.calls += 1
        if self.calls > self.fail_after:
            return False
        self.sent.append((umo, chain))
        return True


def with_provider(plugin, provider, module):
    """把一个替身包进单元素降级链（抓取一律走 ProviderChain）。

    返回替身本身，便于继续读它的 ``calls`` / ``posts`` 记录。
    """
    chain = module.ProviderChain([provider])
    plugin._provider = chain
    plugin._chain_names = tuple(chain.names)
    return provider


def make_plugin(module, tag: str, context=None, provider=None, **config):
    # 兼容早期写法 make_plugin(module, tag, module=...)：那不是配置项
    config.pop("module", None)
    plugin_cls = None
    star_base = sys.modules["astrbot.api.star"].Star
    for obj in vars(module).values():
        if (
            isinstance(obj, type)
            and obj is not star_base
            and issubclass(obj, star_base)
            and getattr(obj, "__module__", "").startswith("__offline__")
        ):
            plugin_cls = obj
    plugin = plugin_cls.__new__(plugin_cls)
    base = TMP_ROOT / tag
    base.mkdir(parents=True, exist_ok=True)
    plugin.config = PluginConfig(config)
    plugin.context = context if context is not None else SendContext()
    plugin.base_dir = base
    plugin.db_path = base / "index.sqlite3"
    plugin._init_db()
    plugin._http = None
    plugin._provider = None
    if provider is not None:
        with_provider(plugin, provider, module)
    return plugin


async def _drive(agen):
    return [item async for item in agen]


def drive(agen):
    return asyncio.run(_drive(agen))


def run(coro):
    return asyncio.run(coro)


# --------------------------------------------------------------------------- #
# 断言
# --------------------------------------------------------------------------- #


class Report:
    def __init__(self) -> None:
        self.passed = 0
        self.failures: list[str] = []

    def check(self, label: str, got, want) -> bool:
        ok = got == want
        if ok:
            self.passed += 1
        else:
            self.failures.append(f"{label}: got={got!r} want={want!r}")
        print(f"{'PASS' if ok else 'FAIL'}  {label}: {got!r}")
        return ok

    def true(self, label: str, got) -> bool:
        return self.check(label, bool(got), True)

    def false(self, label: str, got) -> bool:
        return self.check(label, bool(got), False)

    def contains(self, label: str, haystack, needle) -> bool:
        return self.true(label, needle in str(haystack))

    def finish(self) -> int:
        print()
        if self.failures:
            print(f"FAILED: {len(self.failures)} of {self.passed + len(self.failures)}")
            for item in self.failures:
                print("  -", item)
            return 1
        print(f"ALL PASSED ({self.passed} checks)")
        return 0


# --------------------------------------------------------------------------- #
# 测试数据
# --------------------------------------------------------------------------- #

PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
    b"\x08\x06\x00\x00\x00\x1f\x15\xc4\x89" + b"\x00" * 64
)


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def make_status(
    post_id="1900000000000000001",
    created=1700000000,
    text="hello world",
    lang="en",
    replying_to=None,
    reposted_by=None,
    quote=None,
    media=None,
    handle="elonmusk",
    name="Elon Musk",
):
    obj = {
        "type": "status",
        "id": post_id,
        "url": f"https://x.com/{handle}/status/{post_id}",
        "text": text,
        "created_at": "Thu Oct 08 10:44:20 +0000 2026",
        "created_timestamp": created,
        "lang": lang,
        "author": {
            "id": "44196397",
            "screen_name": handle,
            "name": name,
            "avatar_url": "https://pbs.twimg.com/a.jpg",
        },
        "replies": 1,
        "likes": 2,
        "views": 3,
    }
    if replying_to is not None:
        obj["replying_to"] = replying_to
    if reposted_by is not None:
        obj["reposted_by"] = reposted_by
    if quote is not None:
        obj["quote"] = quote
    obj["media"] = media if media is not None else {}
    return obj


class FakeProvider:
    """可控的抓取提供者替身。"""

    name = "fake"

    def __init__(self, module, author=None, posts=None, ok=True, no_new=False):
        self.module = module
        self.author = author
        self.posts = list(posts or [])
        self.ok = ok
        self.no_new = no_new
        self.calls: list[tuple] = []

    async def fetch_user(self, handle, since_ts=0, with_replies=False):
        self.calls.append((handle, since_ts, with_replies))
        return self.module.FetchResult(
            ok=self.ok,
            no_new=self.no_new,
            author=self.author,
            posts=list(self.posts),
            source="fake",
        )


AUTHOR = {
    "id": "44196397",
    "screen_name": "elonmusk",
    "name": "Elon Musk",
    "avatar_url": "https://pbs.twimg.com/a.jpg",
}


# --------------------------------------------------------------------------- #
# 离线 HTTP 替身（多源降级链要用真实 provider 逻辑跑，不能只喂假 provider）
# --------------------------------------------------------------------------- #


class FakeResponse:
    def __init__(self, status_code=200, content=b"", json_data=None):
        self.status_code = status_code
        self._content = content if isinstance(content, (bytes, bytearray)) else str(content).encode("utf-8")
        self._json = json_data

    @property
    def content(self) -> bytes:
        return bytes(self._content)

    @property
    def text(self) -> str:
        return self._content.decode("utf-8", "replace")

    def json(self):
        if self._json is None:
            raise ValueError("no json")
        return self._json


class ScriptedClient:
    """按 URL 子串派发响应；没有匹配的规则就抛 OSError（模拟网络不可达）。

    规则按插入顺序**先匹配先返回**，因此把更具体的子串写在前面。
    """

    def __init__(self, rules=None):
        self.rules: list[tuple[str, object]] = list(rules or [])
        self.calls: list[tuple[str, dict]] = []

    def add(self, needle: str, response) -> None:
        self.rules.append((needle, response))

    async def get(self, url, params=None, headers=None, **kwargs):
        self.calls.append((str(url), dict(params or {})))
        for needle, response in self.rules:
            if needle in str(url):
                if callable(response):
                    return response(url)
                return response
        raise OSError(f"offline: no route for {url}")

    def urls(self) -> list[str]:
        return [url for url, _ in self.calls]


def ok_feed_xml(handle="elonmusk", item_id=1234, pub_date="Wed, 15 Nov 2023 22:13:20 GMT"):
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<rss version="2.0"><channel>'
        f"<title>{handle}</title>"
        f'<link>https://fxtwitter.com/{handle}</link>'
        "<item>"
        f"<title>hello from rss</title>"
        f"<link>https://fxtwitter.com/{handle}/status/{item_id}</link>"
        f"<guid>https://fxtwitter.com/{handle}/status/{item_id}</guid>"
        f"<description>&lt;p&gt;hello from rss&lt;/p&gt;</description>"
        f"<pubDate>{pub_date}</pubDate>"
        "</item>"
        "</channel></rss>"
    ).encode("utf-8")


class NoNetworkClient:
    """任何请求都不可达——用于验证"链上全是坏源"时的收敛行为。"""

    async def get(self, *args, **kwargs):
        raise OSError("offline: no network")


# --------------------------------------------------------------------------- #
# 主流程
# --------------------------------------------------------------------------- #


def main() -> int:  # noqa: C901 - 自检脚本，按主题分段
    module = load_plugin(PLUGIN_DIR / "main.py")
    rep = Report()

    # =========================================================== 纯函数
    rep.check("normalize_nick 去非法字符", module.normalize_nick('a/b:c*d?e"f<g>h|i'), "abcdefghi")
    rep.check("normalize_nick 压空白", module.normalize_nick("  a   b  "), "a b")
    rep.check("normalize_nick 截断 32", len(module.normalize_nick("x" * 50)), 32)
    rep.check("normalize_nick 空值", module.normalize_nick(None), "")

    rep.check("to_str_list 逗号分隔", module.to_str_list("a, b ,c"), ["a", "b", "c"])
    rep.check("to_str_list 空", module.to_str_list(None), [])

    split = module.XMonitorPlugin._split_first
    rep.check("_split_first 单参数", split("elonmusk"), ("elonmusk", ""))
    rep.check(
        "_split_first handle + 含空格昵称",
        split("elonmusk 马 斯 克"),
        ("elonmusk", "马 斯 克"),
    )
    rep.check("_split_first 空串", split(""), ("", ""))
    rep.check("_split_first 压空白", split("  a   b  "), ("a", "b"))

    rep.check(
        "parse_dnd_ranges 基本", module.parse_dnd_ranges(["23:00-08:00"]), [(1380, 480)]
    )
    rep.check(
        "parse_dnd_ranges 多段 + 全角",
        module.parse_dnd_ranges(["23:00-08:00, 12：30-13:30"]),
        [(1380, 480), (750, 810)],
    )
    rep.check("parse_dnd_ranges 非法条目跳过", module.parse_dnd_ranges(["乱写"]), [])
    rep.check("parse_dnd_ranges 越界跳过", module.parse_dnd_ranges(["25:00-08:00"]), [])

    ranges = module.parse_dnd_ranges(["23:00-08:00"])
    rep.true("dnd 23:30 在内", module.in_quiet_hours(23 * 60 + 30, ranges))
    rep.true("dnd 00:30 在内（跨天）", module.in_quiet_hours(30, ranges))
    rep.true("dnd 07:59 在内", module.in_quiet_hours(7 * 60 + 59, ranges))
    rep.false("dnd 08:00 在外（右开区间）", module.in_quiet_hours(8 * 60, ranges))
    rep.false("dnd 12:00 在外", module.in_quiet_hours(12 * 60, ranges))
    day = module.parse_dnd_ranges(["12:30-13:30"])
    rep.true("dnd 同日区间内", module.in_quiet_hours(13 * 60, day))
    rep.false("dnd 同日区间外", module.in_quiet_hours(14 * 60, day))

    rep.check("classify 原创", module.classify_post({}), "original")
    rep.check("classify 转推", module.classify_post({"reposted_by": {"screen_name": "a"}}), "repost")
    rep.check(
        "classify 回复",
        module.classify_post({"replying_to": {"screen_name": "a", "status": "1"}}),
        "reply",
    )
    rep.check("classify 引用", module.classify_post({"quote": {"type": "status", "text": "x"}}), "quote")
    rep.check(
        "classify 被删引用按原创",
        module.classify_post({"quote": {"type": "tombstone"}}),
        "original",
    )
    rep.check(
        "classify 回复优先于引用",
        module.classify_post({"replying_to": {"screen_name": "a"}, "quote": {"type": "status"}}),
        "reply",
    )

    media_empty = module.extract_media({"media": {}})
    rep.check("extract_media 空 media（无 all 键）", media_empty, [])
    photo_media = {
        "media": {
            "all": [
                {"type": "photo", "url": "https://pbs.twimg.com/media/a.jpg?name=orig"},
            ]
        }
    }
    rep.check("extract_media 单图", len(module.extract_media(photo_media)), 1)
    video_media = {
        "media": {
            "all": [
                {
                    "type": "video",
                    "url": "https://video.twimg.com/top.mp4",
                    "thumbnail_url": "https://pbs.twimg.com/t.jpg",
                    "formats": [
                        {"url": "https://video.twimg.com/x.m3u8", "container": "m3u8"},
                        {"url": "https://video.twimg.com/480.mp4", "container": "mp4", "bitrate": 256000},
                        {"url": "https://video.twimg.com/1080.mp4", "container": "mp4", "bitrate": 10368000},
                    ],
                }
            ]
        }
    }
    picked = module.extract_media(video_media)
    rep.check("extract_media 视频取最高码率 mp4", picked[0]["url"], "https://video.twimg.com/1080.mp4")
    rep.check("extract_media 视频类型归一为 video", picked[0]["type"], "video")
    gif_media = {"media": {"all": [{"type": "gif", "url": "https://video.twimg.com/g.mp4"}]}}
    rep.check("extract_media gif 当视频", module.extract_media(gif_media)[0]["type"], "video")
    rep.check(
        "pick_video_url 无 mp4 时回退 url",
        module.pick_video_url({"url": "https://v/x.mp4", "formats": [{"url": "a", "container": "m3u8"}]}),
        "https://v/x.mp4",
    )

    dup = module.dedupe_by_id([{"id": "1"}, {"id": "2"}, {"id": "1"}])
    rep.check("dedupe_by_id 去重（with_replies 会重复）", len(dup), 2)

    rep.check(
        "apply_twimg_quality orig->large",
        module.apply_twimg_quality("https://pbs.twimg.com/media/a.jpg?name=orig", "large"),
        "https://pbs.twimg.com/media/a.jpg?name=large",
    )
    rep.check(
        "apply_twimg_quality 追加参数",
        module.apply_twimg_quality("https://pbs.twimg.com/media/a.jpg", "small"),
        "https://pbs.twimg.com/media/a.jpg?name=small",
    )
    rep.check(
        "apply_twimg_quality 非 twimg 不变",
        module.apply_twimg_quality("https://example.com/a.jpg", "large"),
        "https://example.com/a.jpg",
    )

    rep.check("chinese_ratio 全中文", module.chinese_ratio("中文测试"), 1.0)
    rep.check("chinese_ratio 空串", module.chinese_ratio(""), 0.0)

    pmedia = make_plugin(
        module,
        tag="mediaurl",
        send_mode="forward",
        forward_image_quality="large",
        image_quality="orig",
    )
    photo = {"type": "photo", "url": "https://pbs.twimg.com/media/a.jpg?name=orig"}
    rep.check(
        "forward 模式图片降级为 large",
        pmedia._effective_media_url(photo),
        "https://pbs.twimg.com/media/a.jpg?name=large",
    )
    pmedia.config["send_mode"] = "chain"
    rep.check(
        "chain 模式用原始画质 orig",
        pmedia._effective_media_url(photo),
        "https://pbs.twimg.com/media/a.jpg?name=orig",
    )
    pmedia.config["send_mode"] = "forward"
    rep.check(
        "画质改写幂等（不会二次改写）",
        pmedia._effective_media_url({"type": "photo", "url": "https://pbs.twimg.com/media/a.jpg?name=large"}),
        "https://pbs.twimg.com/media/a.jpg?name=large",
    )
    rep.check(
        "视频 URL 不做 twimg 画质改写",
        pmedia._effective_media_url({"type": "video", "url": "https://video.twimg.com/x.mp4"}),
        "https://video.twimg.com/x.mp4",
    )
    rep.check("空 URL 返回空", pmedia._effective_media_url({"type": "photo"}), "")

    # =========================================================== 存储层
    p = make_plugin(module, tag="store")
    p.store.upsert_author("44196397", "elonmusk", "Elon Musk")
    p.store.mark_baseline("44196397", 1700000000)
    rep.check("author 基线已建立", int(p.store.get_author("44196397")["baseline_ready"]), 1)
    rep.check("author 水位", int(p.store.get_author("44196397")["last_seen_ts"]), 1700000000)

    created = p.store.add_edge("aiocqhttp:GroupMessage:100", "100", "44196397", "马斯克", 1700000000)
    rep.true("add_edge 新建成功", created)
    again = p.store.add_edge("aiocqhttp:GroupMessage:100", "100", "44196397", "别的", 1700000000)
    rep.false("add_edge 重复返回 False", again)
    edge = p.store.get_edge("aiocqhttp:GroupMessage:100", "44196397")
    rep.check("首次订阅把 enqueued_ts 设为作者水位（不补发）", int(edge["enqueued_ts"]), 1700000000)
    rep.check("昵称已存", edge["nickname"], "马斯克")

    rep.true("get_edge_by_handle", p.store.get_edge_by_handle("aiocqhttp:GroupMessage:100", "@elonmusk") is not None)

    rep.check("set_edge_nickname 命中 1 行", p.store.set_edge_nickname("aiocqhttp:GroupMessage:100", "44196397", "新/昵称"), 1)
    rep.check(
        "set_edge_nickname 自动规范化",
        p.store.get_edge("aiocqhttp:GroupMessage:100", "44196397")["nickname"],
        "新昵称",
    )

    post_a = {
        "post_id": "1900000000000000001",
        "created_ts": 1700000100,
        "kind": "original",
        "body_json": json.dumps(make_status(created=1700000100), ensure_ascii=False),
        "media_json": "[]",
    }
    post_b = {
        "post_id": "1900000000000000002",
        "created_ts": 1700000200,
        "kind": "original",
        "body_json": json.dumps(make_status(post_id="1900000000000000002", created=1700000200), ensure_ascii=False),
        "media_json": "[]",
    }
    edges = p.store.list_edges("aiocqhttp:GroupMessage:100")
    inserted, enqueued = p.store.commit_poll("44196397", [post_a, post_b], list(edges), 1700000200)
    rep.check("commit_poll 新增帖数", inserted, 2)
    rep.check("commit_poll 入队数", enqueued, 2)
    rep.check("队列深度", p.store.queue_depth(), 2)

    inserted2, enqueued2 = p.store.commit_poll("44196397", [post_a, post_b], list(edges), 1700000200)
    rep.check("commit_poll 幂等：新增 0", inserted2, 0)
    rep.check("commit_poll 幂等：入队 0（UNIQUE(edge_id,post_id)）", enqueued2, 0)
    rep.check("队列深度未变", p.store.queue_depth(), 2)
    rep.check("posts 计数", p.store.count_posts(), 2)

    old_post = {
        "post_id": "1900000000000000000",
        "created_ts": 1600000000,
        "kind": "original",
        "body_json": "{}",
        "media_json": "[]",
    }
    edges = p.store.list_edges("aiocqhttp:GroupMessage:100")
    _ins, enq_old = p.store.commit_poll("44196397", [old_post], list(edges), 1700000200)
    rep.check("早于水位的老帖不入队", enq_old, 0)

    queued = p.store.next_queued(10)
    rep.check("next_queued 按 queued_at 升序", [q["post_id"] for q in queued][0], "1900000000000000001")
    p.store.mark_delivered(int(queued[0]["delivery_id"]))
    rep.check("mark_delivered 后队列深度", p.store.queue_depth(), 1)
    p.store.mark_failed(int(queued[1]["delivery_id"]), "boom", retry=True)
    rep.check("mark_failed retry=True 仍排队", p.store.queue_depth(), 1)
    p.store.mark_failed(int(queued[1]["delivery_id"]), "boom", retry=False)
    rep.check("mark_failed retry=False 出队", p.store.queue_depth(), 0)
    rep.check("pending_count_for_edge", p.store.pending_count_for_edge(int(edge["edge_id"])), 0)

    # 积压丢弃
    p2 = make_plugin(module, tag="backlog")
    p2.store.upsert_author("1", "h", "H")
    p2.store.mark_baseline("1", 100)
    p2.store.add_edge("aiocqhttp:GroupMessage:1", "1", "1", "", 100)
    posts = [
        {
            "post_id": f"p{i}",
            "created_ts": 100 + i,
            "kind": "original",
            "body_json": "{}",
            "media_json": "[]",
        }
        for i in range(1, 6)
    ]
    p2.store.commit_poll("1", posts, list(p2.store.list_edges()), 105)
    rep.check("积压准备 5 条", p2.store.queue_depth(), 5)
    dropped = p2.store.drop_oldest_queued(2)
    rep.check("drop_oldest_queued 返回 2 条", len(dropped), 2)
    rep.check("丢弃的是最旧的", [d["post_id"] for d in dropped], ["p1", "p2"])
    rep.check("丢弃后队列深度", p2.store.queue_depth(), 3)

    # 游标单调：不允许回退
    p3 = make_plugin(module, tag="cursor")
    p3.store.upsert_author("1", "h", "H")
    p3.store.mark_baseline("1", 100)
    p3.store.add_edge("u", "1", "1", "", 100)
    p3.store.commit_poll("1", [{"post_id": "x1", "created_ts": 200, "kind": "original", "body_json": "{}", "media_json": "[]"}], list(p3.store.list_edges()), 200)
    rep.check("游标推进到 200", int(p3.store.list_edges()[0]["enqueued_ts"]), 200)
    p3.store.commit_poll("1", [{"post_id": "x2", "created_ts": 150, "kind": "original", "body_json": "{}", "media_json": "[]"}], list(p3.store.list_edges()), 200)
    rep.check("游标不因更旧的帖子回退", int(p3.store.list_edges()[0]["enqueued_ts"]), 200)

    rep.check("remove_edge 命中 1 行", p3.store.remove_edge("u", "1"), 1)
    rep.check("remove_edge 后无订阅", len(p3.store.list_edges()), 0)

    # =========================================================== 权限模型
    pa = make_plugin(
        module,
        tag="perms",
        plugin_admins=["root"],
        group_admins=["100:ga1", "*:ga2"],
        open_groups=["200"],
        readonly_groups=["300"],
        default_group_mode="none",
        private_mode="readonly",
    )
    rep.true("全局管理员", pa._is_global_admin(FakeEvent("root", "100")))
    rep.false("普通成员非全局管理员", pa._is_global_admin(FakeEvent("u9", "100")))
    rep.true("群级管理员（本群）", pa._is_group_admin(FakeEvent("ga1", "100")))
    rep.false("群级管理员跨群失效", pa._is_group_admin(FakeEvent("ga1", "200")))
    rep.true("通配群级管理员", pa._is_group_admin(FakeEvent("ga2", "999")))

    rep.check("白名单群模式", pa.group_mode(FakeEvent("u9", "200")), "whitelist")
    rep.check("只读群模式", pa.group_mode(FakeEvent("u9", "300")), "readonly")
    rep.check("未配置群回落 none", pa.group_mode(FakeEvent("u9", "400")), "none")
    rep.check("私聊用 private_mode", pa.group_mode(FakeEvent("u9", gid="", is_group=False)), "readonly")

    rep.true("白名单群全员可订阅", pa.can(FakeEvent("u9", "200"), "subscribe"))
    rep.false("只读群普通成员不可订阅", pa.can(FakeEvent("u9", "300"), "subscribe"))
    rep.true("只读群普通成员可查看", pa.can(FakeEvent("u9", "300"), "view"))
    rep.false("未配置群普通成员不可查看", pa.can(FakeEvent("u9", "400"), "view"))
    rep.true("全局管理员在未配置群也可订阅", pa.can(FakeEvent("root", "400"), "subscribe"))

    rep.check("scope: 全局管理员 all", pa.manage_scope(FakeEvent("root", "400")), "all")
    rep.check("scope: 群级管理员 local", pa.manage_scope(FakeEvent("ga1", "100")), "local")
    rep.check("scope: 普通成员 none", pa.manage_scope(FakeEvent("u9", "400")), "none")

    own_edge = {"umo": "aiocqhttp:GroupMessage:100"}
    other_edge = {"umo": "aiocqhttp:GroupMessage:200"}
    ok_own, _ = pa.can_manage_edge(FakeEvent("ga1", "100"), own_edge)
    ok_other, why_other = pa.can_manage_edge(FakeEvent("ga1", "100"), other_edge)
    rep.true("群级管理员可管本群订阅", ok_own)
    rep.false("群级管理员不可管他群订阅", ok_other)
    rep.contains("越权提示含「其他群」", why_other, "其他群")

    rep.contains("无管理员时给出引导", pa.denied_hint, "")
    pb = make_plugin(module, tag="perms2")
    rep.contains("管理员名单为空时的引导文案", pb.denied_hint(FakeEvent()), "尚未配置任何管理员")
    pe = make_plugin(module, tag="perms5", plugin_admins=["root"])
    rep.contains("名单非空时回显自己的 ID", pe.denied_hint(FakeEvent(uid="u42")), "u42")

    pc = make_plugin(module, tag="perms3", default_group_mode="bad-value")
    rep.check("非法群模式回落 none", pc.group_mode(FakeEvent("u", "1")), "none")
    pd = make_plugin(module, tag="perms4", default_group_mode="whitelist", readonly_allow_manage=True)
    rep.check("白名单 + 放开管理 -> local", pd.manage_scope(FakeEvent("u", "1")), "local")

    # =========================================================== 过滤
    pf = make_plugin(module, tag="filters", filter_reposts=True, filter_replies=True, filter_quotes=False)
    rep.false("转推被过滤", pf.post_passes_filters({"reposted_by": {"screen_name": "a"}}))
    rep.false("回复被过滤", pf.post_passes_filters({"replying_to": {"screen_name": "a"}}))
    rep.true("引用帖保留", pf.post_passes_filters({"quote": {"type": "status"}}))
    rep.true("原创通过", pf.post_passes_filters({}))
    rep.check("被过滤的帖子不入库", pf.to_post_record({"id": "1", "replying_to": {"screen_name": "a"}}), None)
    rec = pf.to_post_record(make_status())
    rep.true("原创帖生成记录", rec is not None)
    rep.check("记录里的 kind", rec["kind"], "original")
    rep.check("记录里的 post_id", rec["post_id"], "1900000000000000001")

    # =========================================================== 翻译
    pt = make_plugin(
        module,
        tag="translate",
        translate_enabled=True,
        target_lang="zh-CN",
        min_len=4,
        skip_langs=["ja"],
        min_chinese_ratio_skip=0.35,
        translate_glossary=["Starlink=星链"],
    )
    tr = pt._translator
    rep.true("英文长文本需要翻译", tr.should_translate("hello starlink world", "en"))
    rep.false("目标语言跳过", tr.should_translate("hello world", "zh"))
    rep.false("skip_langs 跳过", tr.should_translate("こんにちは世界", "ja"))
    rep.false("过短跳过", tr.should_translate("ok", "en"))
    rep.false("中文占比高跳过", tr.should_translate("这是一段足够长的中文内容测试", "en"))
    rep.true("lang 未知时保守翻译", tr.should_translate("hello world friends", ""))
    tr._consecutive_failures = 0
    tr._on_failure("x")
    rep.false("一次失败不熔断", tr.tripped)
    tr._on_failure("x")
    rep.true("连续两次失败熔断", tr.tripped)
    rep.false("熔断后不再翻译", tr.should_translate("hello starlink world", "en"))
    tr.reset_round()
    rep.true("新一轮重置熔断", tr.should_translate("hello starlink world", "en"))

    prompt = tr.build_prompt("hello", "en")
    rep.contains("prompt 含目标语言", prompt, "zh-CN")
    rep.contains("prompt 含术语表", prompt, "Starlink=星链")
    pt_disabled = make_plugin(module, tag="translate_off")
    rep.false("未开启翻译时不翻", pt_disabled._translator.should_translate("hello world", "en"))

    # =========================================================== 发送层
    ps = make_plugin(module, tag="sender", send_mode="forward", forward_nickname_fallback="X")
    rep.check("无图时只有一批", len(ps._plan_forward_batches("hi", [], 1024, 10)), 1)
    rep.check("无图批次带文本", ps._plan_forward_batches("hi", [], 1024, 10)[0][0], "hi")

    img_dir = TMP_ROOT / "sender_imgs"
    img_dir.mkdir(parents=True, exist_ok=True)
    img_paths = []
    for i in range(4):
        f = img_dir / f"{i}.png"
        f.write_bytes(PNG + b"\x00" * (2 * 1024 * 1024))
        img_paths.append((f, module.Plain(f"img{i}")))

    batches = ps._plan_forward_batches("hello", img_paths, 5 * 1024 * 1024, 10)
    rep.true("预算不足时拆成多批", len(batches) >= 2)
    rep.true("首批带文本", batches[0][0] == "hello")
    rep.true("后续批次不带文本", all(b[0] == "" for b in batches[1:]))

    capped = ps._plan_forward_batches("", img_paths, 100 * 1024 * 1024, 2)
    rep.check("max_images 上限生效", [len(b[1]) for b in capped], [2, 2])

    ctx = SendContext()
    ps.context = ctx
    rep.check("未学习 self_id 时 uin 回退为 0", ps.node_uin(), "0")
    ps.node_uin(FakeEvent())  # 真实场景：任何一条指令事件都会把 self_id 学下来
    rep.check(
        "按平台学到 self_id",
        ps.node_uin(umo="aiocqhttp:GroupMessage:100"),
        "bot1",
    )
    rep.check("未学过的平台仍回退为 0", ps.node_uin(umo="other:GroupMessage:1"), "0")
    ok, err, _partial = run(
        ps._send_forward("aiocqhttp:GroupMessage:100", "正文", img_paths[:1], "马斯克")
    )
    rep.true("合并转发发送成功", ok)
    rep.check("合并转发只发一条消息", len(ctx.sent), 1)
    chain = ctx.sent[0][1].chain
    rep.check("链中只有 Nodes（不与普通段混发）", len(chain), 1)
    nodes = chain[0].nodes
    rep.check("节点数 = 文本 + 图片", len(nodes), 2)
    rep.check("节点昵称 = 自定义昵称", nodes[0].name, "马斯克")
    rep.check("节点 uin = self_id", nodes[0].uin, "bot1")
    rep.check("图片节点昵称同样生效", nodes[1].name, "马斯克")

    ctx2 = SendContext(result=False)
    ps.context = ctx2
    ok2, err2, partial2 = run(ps._send_forward("umo", "正文", [], "nick"))
    rep.false("send_message 返回 False 被识别为失败", ok2)
    rep.false("一条都没发出去时 partial=False（可以安全降级）", partial2)
    rep.contains("失败原因可见", err2, "False")

    # 分批合并转发：第 2 批失败时，第 1 批其实已经送达 -> partial=True
    flaky = FlakyContext(fail_after=1)
    ps.context = flaky
    okp, errp, partialp = run(ps._send_forward("umo", "正文", img_paths, "nick"))
    rep.false("第 2 批失败 -> 整体失败", okp)
    rep.true("第 2 批失败 -> partial=True", partialp)

    media_json = json.dumps(
        [
            {"type": "photo", "url": f"https://pbs.twimg.com/media/{i}.jpg", "path": str(p)}
            for i, (p, _c) in enumerate(img_paths[:3])
        ]
    )
    row_partial = {
        "body_json": json.dumps(make_status(text="hi"), ensure_ascii=False),
        "media_json": media_json,
        "translated": "",
        "umo": "aiocqhttp:GroupMessage:100",
        "nickname": "",
    }
    flaky2 = FlakyContext(fail_after=1)
    ps.context = flaky2
    ps.config["send_mode"] = "forward"
    rep.true("部分成功后 deliver 仍算成功", run(ps.deliver(row_partial)))
    rep.check("部分成功后不再整贴重发（只调用 2 次）", flaky2.calls, 2)

    ctx3 = SendContext()
    ps.context = ctx3
    ps.config["send_mode"] = "forward"
    ps.config["forward_nickname_fallback"] = "X"
    row = {
        "body_json": json.dumps(make_status(text="hello"), ensure_ascii=False),
        "media_json": "[]",
        "translated": "",
        "umo": "aiocqhttp:GroupMessage:100",
        "nickname": "",
    }
    sent_ok = run(ps.deliver(row))
    rep.true("deliver 走合并转发成功", sent_ok)
    rep.check(
        "未设昵称时节点名回退为推主名（不是笼统兜底值）",
        ctx3.sent[0][1].chain[0].nodes[0].name,
        "Elon Musk",
    )

    ps.config["send_mode"] = "chain"
    ctx4 = SendContext()
    ps.context = ctx4
    sent_ok2 = run(ps.deliver(row))
    rep.true("chain 模式发送成功", sent_ok2)
    rep.check("chain 是一条消息链", len(ctx4.sent), 1)
    rep.true("chain 首段是文本", type(ctx4.sent[0][1].chain[0]).__name__ == "_StubPlain")

    text_out = ps.post_text(make_status(text="hello"), "", "")
    rep.contains("正文含作者", text_out, "Elon Musk")
    rep.contains("正文含 handle", text_out, "@elonmusk")
    rep.contains("正文含链接", text_out, "https://x.com/elonmusk/status/")
    rep.contains("正文含原文", text_out, "hello")

    repost_text = ps.post_text(make_status(reposted_by={"screen_name": "someone"}), "", "")
    rep.contains("转推前缀", repost_text, "转推")
    reply_text = ps.post_text(make_status(replying_to={"screen_name": "bob", "status": "1"}), "", "")
    rep.contains("回复标记", reply_text, "@bob")
    quote_text = ps.post_text(make_status(quote={"type": "status", "text": "引用内容", "author": {"screen_name": "carl"}}), "", "")
    rep.contains("引用摘要", quote_text, "引用内容")
    tomb_text = ps.post_text(make_status(quote={"type": "tombstone"}), "", "")
    rep.false("tombstone 引用不渲染", "引用" in tomb_text)
    empty_text = ps.post_text(make_status(text=""), "", "")
    rep.true("纯媒体帖（text 为空）仍能构造正文", bool(empty_text))
    trans_text = ps.post_text(make_status(text="hello"), "你好", "")
    rep.contains("译文入正文", trans_text, "你好")
    nick_text = ps.post_text(make_status(text="hello"), "", "马斯克")
    rep.contains("自定义昵称体现在正文", nick_text, "马斯克")
    rep.contains("昵称下仍保留真实名字", nick_text, "Elon Musk")
    same_nick_text = ps.post_text(make_status(text="hello"), "", "Elon Musk")
    rep.false("昵称与真名相同时不重复显示", "｜" in same_nick_text)

    # 合并转发节点昵称的优先级：自定义昵称 > 推主名 > handle > 兜底
    body_named = make_status()
    rep.check("节点昵称：自定义优先", ps._node_nickname(body_named, "马斯克"), "马斯克")
    rep.check("节点昵称：回退推主名", ps._node_nickname(body_named, ""), "Elon Musk")
    no_name = make_status()
    no_name["author"] = {"screen_name": "someone", "name": ""}
    rep.check("节点昵称：无名时用 handle", ps._node_nickname(no_name, ""), "someone")
    bare = make_status()
    bare["author"] = {}
    ps.config["forward_nickname_fallback"] = "X"
    rep.check("节点昵称：最后兜底", ps._node_nickname(bare, ""), "X")

    # =========================================================== 轮询
    pp = make_plugin(module, tag="poll", include_replies=False, filter_replies=True)
    fake = with_provider(
        pp,
        FakeProvider(module, author=AUTHOR, posts=[make_status(created=1700000500)]),
        module,
    )
    pp.store.upsert_author("44196397", "elonmusk", "Elon Musk")
    pp.store.add_edge("aiocqhttp:GroupMessage:100", "100", "44196397", "马斯克", 0)
    first = run(pp.poll_author("44196397"))
    rep.true("首次抓取建立基线", "baseline" in first)
    rep.check("首次不补发（无入队）", first.get("enqueued", 0), 0)
    rep.check("基线水位 = 最新帖时间", int(pp.store.get_author("44196397")["last_seen_ts"]), 1700000500)
    rep.check("首次不写 posts", pp.store.count_posts(), 0)

    fake = with_provider(
        pp,
        FakeProvider(
            module, author=AUTHOR, posts=[make_status(post_id="2000000000000000001", created=1700000600)]
        ),
        module,
    )
    second = run(pp.poll_author("44196397"))
    rep.check("第二轮入队 1 条", second.get("enqueued", 0), 1)
    rep.check("第二轮写入 1 帖", pp.store.count_posts(), 1)
    rep.check("抓取时 since 用的是水位", fake.calls[0][1], 1700000500)
    rep.false("默认不请求回复", fake.calls[0][2])

    # 回归：基线轮必须同步推进订阅边水位 —— 否则第一条新帖会把整页历史一起投递
    # （数据源的 ?since= 只决定 200/204，不裁剪页面；见 data-sources.md §2）
    pr = make_plugin(module, tag="poll_baseline_replay")
    pr.store.upsert_author("44196397", "elonmusk", "Elon Musk")
    pr.store.add_edge("aiocqhttp:GroupMessage:100", "100", "44196397", "", 0)  # 全新账号 start_ts=0
    with_provider(
        pr,
        FakeProvider(
            module,
            author=AUTHOR,
            posts=[make_status(post_id="1900000000000000001", created=1700000500)],
        ),
        module,
    )
    rep.true("基线轮建立基线", "baseline" in run(pr.poll_author("44196397")))
    rep.check(
        "基线轮同步推进订阅边水位",
        int(pr.store.get_edge("aiocqhttp:GroupMessage:100", "44196397")["enqueued_ts"]),
        1700000500,
    )
    # 第二轮模拟源端返回整页：更早的历史 + 基线那条 + 唯一新帖
    with_provider(
        pr,
        FakeProvider(
            module,
            author=AUTHOR,
            posts=[
                make_status(post_id="1900000000000000002", created=1700000300),
                make_status(post_id="1900000000000000001", created=1700000500),
                make_status(post_id="1900000000000000003", created=1700000600),
            ],
        ),
        module,
    )
    replay = run(pr.poll_author("44196397"))
    rep.check("基线后首轮只入队新帖（不补发历史）", replay.get("enqueued", 0), 1)
    rep.check("历史帖不入队", pr.store.queue_depth(), 1)

    pp.store.upsert_author("44196397", "elonmusk", "Elon Musk")
    pp.store.mark_baseline("44196397", 1700000600)
    with_provider(pp, FakeProvider(module, ok=False), module)
    before = int(pp.store.get_author("44196397")["last_seen_ts"])
    fail = run(pp.poll_author("44196397"))
    rep.true("抓取失败返回非空 error", fail.get("error"))
    rep.check("抓取失败不推进水位", int(pp.store.get_author("44196397")["last_seen_ts"]), before)

    pp2 = make_plugin(module, tag="poll_reply", include_replies=True)
    fake2 = with_provider(pp2, FakeProvider(module, author=AUTHOR, posts=[]), module)
    pp2.store.upsert_author("44196397", "elonmusk", "Elon Musk")
    run(pp2.poll_author("44196397"))
    rep.true("开启后请求 with_replies", fake2.calls[0][2])

    pp3 = make_plugin(
        module,
        tag="poll_dedupe",
        provider=FakeProvider(
            module,
            author=AUTHOR,
            posts=[
                make_status(post_id="3000000000000000001", created=1700000900),
                make_status(post_id="3000000000000000001", created=1700000900),
            ],
        ),
    )
    rep.check("重复 id 被去重", len(pp3._provider.providers[0].posts), 2)
    pp3.store.upsert_author("44196397", "elonmusk", "Elon Musk")
    pp3.store.add_edge("aiocqhttp:GroupMessage:100", "100", "44196397", "", 0)
    r3_first = run(pp3.poll_author("44196397"))  # 首轮只建基线
    rep.true("重复 id 的首轮仍建立基线", "baseline" in r3_first)
    # 基线轮已把边水位推到 1700000900；**与水位同秒的帖不入队是预期语义**
    # （入队判据是排他边界 created_ts > edges.enqueued_ts），不是去重失效。
    r3_second = run(pp3.poll_author("44196397"))
    rep.check("水位内（同秒）的重复帖不入队", r3_second.get("enqueued", 0), 0)
    rep.check("水位内的帖不进队列", pp3.store.queue_depth(), 0)
    # 水位**之后**的新帖带重复 id：去重后必须只入队一次
    with_provider(
        pp3,
        FakeProvider(
            module,
            author=AUTHOR,
            posts=[
                make_status(post_id="3000000000000000002", created=1700001000),
                make_status(post_id="3000000000000000002", created=1700001000),
            ],
        ),
        module,
    )
    r3_third = run(pp3.poll_author("44196397"))
    rep.check("重复 id 去重后只入队一次", r3_third.get("enqueued", 0), 1)
    rep.check("去重后队列只有一条", pp3.store.queue_depth(), 1)

    # 单账号异常不得拖垮整轮
    class BoomProvider:
        name = "boom"

        async def fetch_user(self, *args, **kwargs):
            raise RuntimeError("boom")

    pp4 = make_plugin(module, tag="poll_boom")
    pp4._http = object()  # poll_once 要求已初始化
    with_provider(pp4, BoomProvider(), module)
    pp4.store.upsert_author("1", "h", "H")
    pp4.store.mark_baseline("1", 100)
    pp4.store.add_edge("aiocqhttp:GroupMessage:1", "1", "1", "", 100)
    stats4 = run(pp4.poll_once())
    rep.check("单账号异常被收敛为本次错误", stats4.get("errors"), 1)
    rep.check("整轮仍统计到该账号", stats4.get("authors"), 1)

    pp5 = make_plugin(module, tag="poll_empty", provider=FakeProvider(module))
    pp5._http = object()
    rep.check("无订阅时轮询直接返回", run(pp5.poll_once()).get("authors"), 0)

    # =========================================================== 多源降级链
    # ---- RSS 防御式解析（纯函数）
    rep.check(
        "_strip_html_text 还原实体与换行",
        module._strip_html_text("&lt;p&gt;a &amp;amp; b&lt;br&gt;c&lt;/p&gt;"),
        "a & b\nc",
    )
    rep.check("_strip_html_text 丢弃 script", module._strip_html_text("<script>evil()</script>real"), "real")
    # 自闭合 <br/> 在 HTMLParser 里可能只走 handle_startendtag，
    # 若只实现 handle_starttag 会漏掉换行——这是"看着对但排版糊成一团"的经典坑
    rep.check(
        "_strip_html_text 自闭合 br 也换行",
        module._strip_html_text("&lt;p&gt;a&lt;br/&gt;b&lt;/p&gt;"),
        "a\nb",
    )
    rep.check("_strip_html_text 空值", module._strip_html_text(None), "")
    rep.check(
        "_extract_img_urls 保序去重",
        module._extract_img_urls('<img src="a.jpg"><img src="b.jpg"><img src="a.jpg">'),
        ["a.jpg", "b.jpg"],
    )
    rep.check("_rss_post_id 优先数字", module._rss_post_id("https://x.com/a/status/1900000000000000001"), "1900000000000000001")
    rep.check("_rss_post_id 非数字退化为哈希", len(module._rss_post_id("tag:whatever")), 20)
    rep.check(
        "_handle_from_url 取 feed 路径",
        module._handle_from_url("https://fxtwitter.com/elonmusk/feed.xml"),
        "elonmusk",
    )

    # 200 但根本不是 feed（Nitter 类镜像的浏览器验证页）→ 必须判定为不可用
    rep.check("解析 HTML 验证页返回 None", module.parse_feed_xml(b"<html>verify</html>"), None)
    rep.check("解析空 body 返回 None", module.parse_feed_xml(b""), None)
    rep.check("解析非 XML 文本返回 None", module.parse_feed_xml(b"not xml at all"), None)

    parsed = module.parse_feed_xml(ok_feed_xml(), handle="elonmusk")
    rep.true("正常 RSS 能解析出条目", parsed is not None and len(parsed[0]) == 1)
    rss_post, rss_author = parsed
    rep.check("RSS 条目 id 来自 /status 链", rss_post[0]["id"], "1234")
    rep.check("RSS 条目正文已去标签", rss_post[0]["text"], "hello from rss")
    rep.check("RSS 作者 handle 来自 channel link", rss_author["screen_name"], "elonmusk")
    rep.check("RSS 条目带作者", rss_post[0]["author"]["screen_name"], "elonmusk")
    rep.check("RSS 条目 created_timestamp 为秒级", rss_post[0]["created_timestamp"], 1700086400)
    rep.check("RSS 条目被识别为 status", rss_post[0]["type"], "status")

    # 图片：description 里的 <img> 要变成 media.all，否则"仅媒体"订阅在 RSS 源上会漏推
    img_feed = (
        '<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel>'
        "<title>elonmusk</title><link>https://fxtwitter.com/elonmusk</link>"
        "<item><title>pic</title>"
        "<link>https://fxtwitter.com/elonmusk/status/2001</link>"
        "<guid>https://fxtwitter.com/elonmusk/status/2001</guid>"
        "<description>&lt;p&gt;look&lt;img src=\"https://pbs.twimg.com/media/aaa.jpg\"/&gt;&lt;/p&gt;</description>"
        "<pubDate>Wed, 15 Nov 2023 22:13:20 GMT</pubDate>"
        "</item></channel></rss>"
    ).encode("utf-8")
    img_parsed = module.parse_feed_xml(img_feed, handle="elonmusk")
    rep.true(
        "RSS 里的 <img> 变成 media.all",
        bool(img_parsed and module.post_has_media(img_parsed[0][0])),
    )
    rep.check(
        "RSS 图文帖正文只留文字",
        img_parsed[0][0]["text"] if img_parsed else "",
        "look",
    )

    # since 过滤：feed 侧没有服务端增量，必须本地过滤旧条目。
    # 边界是**排他**的：等于水位的条目不再返回——放宽成"包含"会重新推一遍
    # 基线那条（基线轮不投递，见 poll_author 的首次订阅语义）。
    rep.check(
        "since 过滤掉等于水位的条目（排他边界）",
        module.parse_feed_xml(ok_feed_xml(), min_ts=1700086400, handle="elonmusk")[0],
        [],
    )
    rep.check(
        "since 过滤后为空列表（不是 None）",
        len(module.parse_feed_xml(ok_feed_xml(), min_ts=1700086400, handle="elonmusk")[0]),
        0,
    )
    rep.check(
        "since 小于条目时间时保留",
        len(module.parse_feed_xml(ok_feed_xml(), min_ts=1700086399, handle="elonmusk")[0]),
        1,
    )

    # 日期语义：整份 feed 一条日期都没有 → 无法定义水位，判定该源不可用
    # （老实现会退回 time.time()，把水位抬到 now 之后永久漏推）
    no_date = ok_feed_xml().replace(b"<pubDate>Wed, 15 Nov 2023 22:13:20 GMT</pubDate>", b"")
    rep.check("全部条目缺日期 → 判该源不可用", module.parse_feed_xml(no_date, handle="elonmusk"), None)

    # 部分条目缺日期 → 用 feed 内最新时间兜底，绝不跳到 now
    mixed = no_date.replace(
        b"</channel>",
        b"<item><title>dated</title>"
        b"<link>https://fxtwitter.com/elonmusk/status/555</link>"
        b"<guid>https://fxtwitter.com/elonmusk/status/555</guid>"
        b"<pubDate>Thu, 16 Nov 2023 00:00:00 GMT</pubDate>"
        b"<description>dated one</description></item></channel>",
    )
    mixed_parsed = module.parse_feed_xml(mixed, handle="elonmusk")
    rep.true("部分条目缺日期时仍能解析", bool(mixed_parsed and len(mixed_parsed[0]) == 2))
    dateless = [p for p in mixed_parsed[0] if p["id"] == "1234"][0]
    rep.check("缺日期条目用 feed 内最新时间兜底", dateless["created_timestamp"], 1700092800)
    rep.true(
        "兜底时间不等于抓取时刻",
        abs(dateless["created_timestamp"] - int(time.time())) > 3600,
    )

    # Atom 形态也要能读（RSSHub 的自建实例可能是 Atom）
    atom = (
        '<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom">'
        "<title>someone</title>"
        '<link href="https://x.com/someone"/>'
        "<entry><title>atom post</title>"
        "<link href=\"https://x.com/someone/status/1900000000000000123\"/>"
        "<id>tag:x:1900000000000000123</id>"
        "<updated>2026-10-08T10:44:20Z</updated>"
        "<content type=\"html\">&lt;p&gt;atom body&lt;/p&gt;</content>"
        "</entry></feed>"
    ).encode("utf-8")
    atom_parsed = module.parse_feed_xml(atom)
    rep.true("Atom 能解析出条目", bool(atom_parsed and len(atom_parsed[0]) == 1))
    rep.check("Atom 正文已去标签", atom_parsed[0][0]["text"], "atom body")

    # ---- RSS 字段形状契约（**合成**样本：5 个条目、内容全虚）
    # tests/fixtures/fxtwitter_feed_sample.xml 按 v2 RSS 面的字段形状手工构造，
    # 固定 5 种形态（本人帖+引用 / 图片 enclosure+<img> / 视频 enclosure+thumbnail /
    # 转推 / CDATA 含实体与 <br />）。
    # 上面那些自造样本只能证明"实现自洽"，只有钉在字段形状上的样本才能证明"契约没读错"。
    fixture = PLUGIN_DIR / "tests/fixtures/fxtwitter_feed_sample.xml"
    rep.true("字段形状样本在库里", fixture.is_file())
    real_raw = fixture.read_bytes()
    real = module.parse_feed_xml(real_raw, handle="demo_feed")
    rep.true("样本能解析出条目", bool(real and len(real[0]) == 5))
    real_posts, real_author = real
    by_id = {p["id"]: p for p in real_posts}
    preal = make_plugin(
        module, tag="p5_real", filter_reposts=True, filter_replies=True, filter_quotes=False
    )

    # channel <title> 是 "Sample Feed (@demo_feed)"，已经带 handle
    rep.check("频道显示名剥掉重复的 (@handle)", real_author["name"], "Sample Feed")

    # 转推：feed 是含转推的时间线，作者必须取自条目 link
    rt = by_id["1900000000000000004"]
    rep.check("转推作者取条目 link 的作者", rt["author"]["screen_name"], "other_author")
    rep.check("转推标出 reposted_by = 频道主人", (rt.get("reposted_by") or {}).get("screen_name"), "demo_feed")
    rep.check("转推被判为 repost", module.classify_post(rt), "repost")
    rep.check("转推链接指向原作者（不再是错误链接）", module.build_share_url(rt), rt["url"])
    rep.false("filter_reposts 在 L2 上真的拦得住", preal.to_post_record(rt))

    # 视频帖：唯一媒体信号是 <enclosure type="video/mp4">
    vp = by_id["1900000000000000003"]
    rep.true("视频 enclosure 被认成媒体（否则仅媒体订阅静默丢帖）", module.post_has_media(vp))
    vmedia = module.extract_media(vp)
    rep.check("视频媒体类型", [m["type"] for m in vmedia], ["video"])
    rep.contains("视频用 mp4 直链", vmedia[0]["url"], ".mp4")
    rep.contains("视频封面来自 media:thumbnail", vmedia[0]["thumbnail"], "amplify_video_thumb")

    # 图片帖：image enclosure + description 里的 <img> 指向同一张，不能重复
    ip = by_id["1900000000000000002"]
    pmedia = module.extract_media(ip)
    rep.check("图片帖媒体类型", [m["type"] for m in pmedia], ["photo"])
    rep.check("图片不因 enclosure+<img> 重复计数", len(pmedia), 1)
    rep.contains("图片 URL 保留 name=orig", pmedia[0]["url"], "name=orig")

    # 引用：<blockquote> 是被引用帖，不属于本帖正文
    qp = by_id["1900000000000000001"]
    rep.check(
        "正文只剩本帖内容",
        qp["text"],
        "Demo post with a quote.",
    )
    rep.true("blockquote 被拆成 quote", isinstance(qp.get("quote"), dict))
    rep.contains("quote 里是被引用帖正文", qp["quote"]["text"], "Quoted post body from a friend.")
    rep.check("quote 作者取自引用链接", qp["quote"]["author"]["screen_name"], "quotefriend")
    rep.false("引用内容不再混进正文", "Quoted post body from a friend." in qp["text"])
    rep.check("带引用的帖被判为 quote", module.classify_post(qp), "quote")
    rendered = preal.post_text(qp)
    rep.contains("正文渲染出引用块", rendered, "📎 引用 @quotefriend")
    rep.false("渲染里作者名不重复拼 handle", "(@demo_feed) (@demo_feed)" in rendered)
    rep.contains("渲染出的链接正确", rendered, "https://x.com/demo_feed/status/1900000000000000001")

    # 引用帖里的 <img> 不算本帖媒体（B 条的 <img> 在正文里、引用里没有，反向验证 A 条）
    rep.false("无媒体的引用帖不被当成图片帖", module.post_has_media(qp))

    # CDATA 里的实体（&apos; 出现在 CDATA 内，必须由 html.unescape 还原）
    ep = by_id["1900000000000000005"]
    rep.contains("CDATA 里的 &apos; 被还原", ep["text"], "It's a demo")
    rep.false("实体没有残留", "&apos;" in ep["text"])
    rep.contains("<br /> 变成换行", ep["text"], "\n")

    # 端到端：字段形状样本走 RssProvider + 水位
    e2e = make_plugin(
        module, tag="p5_real_e2e", fetch_chain=["fxtwitter_rss"], rss_base="https://rss.example"
    )
    e2e._http = ScriptedClient([("feed.xml", FakeResponse(200, real_raw))])
    e2e.store.upsert_author("100000001", "demo_feed", "Sample Feed")
    e2e.store.add_edge("aiocqhttp:GroupMessage:100", "100", "100000001", "", 0)
    first_real = run(e2e.poll_author("100000001"))
    rep.true("样本 feed 首轮建立基线", "baseline" in first_real)
    rep.check("基线水位 = 最新条目时间", int(e2e.store.get_author("100000001")["last_seen_ts"]), 1791495344)
    rep.check("首轮不补发历史", first_real.get("enqueued", 0), 0)
    second_real = run(e2e.poll_author("100000001"))
    rep.check("水位之后无新帖时不入队", second_real.get("enqueued", 0), 0)
    rep.check("无新帖时水位保持不动", int(e2e.store.get_author("100000001")["last_seen_ts"]), 1791495344)
    rep.check("水位之后仍能取到条目（无新帖是 no_new 而非失败）", second_real.get("fetched", 0), 0)

    # 增量：追加一条更新的帖（另一个作者 ⇒ 应被标成转推）
    newer_raw = real_raw.decode("utf-8").replace(
        "</channel>",
        '<item><title>brand new</title>'
        '<link>https://x.com/NewAuthor/status/2109000000000000000</link>'
        '<guid isPermaLink="true">https://x.com/NewAuthor/status/2109000000000000000</guid>'
        "<pubDate>Fri, 09 Oct 2026 00:00:00 GMT</pubDate>"
        "<description><![CDATA[<p>brand new</p>]]></description></item></channel>",
    )
    fresh = module.parse_feed_xml(newer_raw.encode("utf-8"), min_ts=1791495344, handle="demo_feed")
    rep.check("增量只返回水位之后的新条目", [p["id"] for p in fresh[0]], ["2109000000000000000"])
    rep.check("增量条目时间解析正确", fresh[0][0]["created_timestamp"], 1791504000)
    rep.check("增量里的转推同样被标注", fresh[0][0]["reposted_by"]["screen_name"], "demo_feed")

    # ---- 真实 provider 的"不可用 vs 无新帖"语义
    rss_provider = module.RssProvider(
        ScriptedClient(
            [
                ("feed.xml", FakeResponse(200, b"<html>nope</html>")),
                ("/feed", FakeResponse(200, b"<html>nope</html>")),
                ("/rss", FakeResponse(200, b"<html>nope</html>")),
            ]
        ),
        base="https://rss.example",
    )
    bad = run(rss_provider.fetch_user("elonmusk"))
    rep.false("200 但非 feed 判定为不可用", bad.ok)
    rep.true("并给出可读错误", "not a feed" in bad.error)

    rss_provider2 = module.RssProvider(
        ScriptedClient(
            [
                ("feed.xml", FakeResponse(200, ok_feed_xml())),
                ("/feed", FakeResponse(200, ok_feed_xml())),
            ]
        )
    )
    good = run(rss_provider2.fetch_user("elonmusk"))
    rep.true("合法 feed 判定为成功", good.ok)
    rep.check("生效源名写进结果", good.provider, "fxtwitter_rss")
    rep.check("拿到 1 条", len(good.posts), 1)

    empty_feed = b'<?xml version="1.0"?><rss version="2.0"><channel><title>x</title></channel></rss>'
    rss_provider3 = module.RssProvider(
        ScriptedClient([("feed.xml", FakeResponse(200, empty_feed)), ("/feed", FakeResponse(200, empty_feed))])
    )
    empty = run(rss_provider3.fetch_user("elonmusk"))
    rep.true("合法但无条目的 feed 判定为无新帖", empty.ok and empty.no_new)

    xapi = module.FxTwitterProvider(
        ScriptedClient([("/statuses", FakeResponse(204))]), base="https://dead.example"
    )
    xres = run(xapi.fetch_user("elonmusk"))
    rep.true("HTTP 204 仍是无新帖", xres.ok and xres.no_new)
    rep.check("api base 可被覆盖", xapi.base, "https://dead.example")

    # ---- 实例池：第一台坏了要自动切到第二台
    hub = module.RssHubProvider(
        ScriptedClient(
            [
                ("broken.example", FakeResponse(200, b"<html>verify</html>")),
                ("good.example", FakeResponse(200, ok_feed_xml())),
            ]
        ),
        instances=["https://broken.example", "https://good.example"],
    )
    hub_res = run(hub.fetch_user("elonmusk"))
    rep.true("实例池故障转移成功", hub_res.ok)
    rep.check("对外仍只报 rsshub 一个源名", hub_res.provider, "rsshub")
    states = {item["name"]: item for item in hub.health_snapshot()}
    rep.false("坏实例被标记为不健康", states["https://broken.example"]["healthy"])
    rep.true("好实例保持健康", states["https://good.example"]["healthy"])
    rep.true("坏实例被挪到队尾", hub.ordered_instances()[0] == "https://good.example")
    rep.check(
        "未配置实例时明确报错而不是假装成功",
        run(module.RssHubProvider(ScriptedClient(), instances=[]).fetch_user("elonmusk")).ok,
        False,
    )

    # ---- 降级链：L1 挂掉自动切 L2，并留下 source 标签
    chain_client = ScriptedClient(
        [
            ("/2/profile/", FakeResponse(503)),
            ("feed.xml", FakeResponse(200, b"<html>verify</html>")),
            ("/feed", FakeResponse(200, ok_feed_xml(item_id=3001))),
        ]
    )
    chain = module.ProviderChain(
        [
            module.FxTwitterProvider(chain_client, base="https://api.example"),
            module.RssProvider(chain_client, base="https://rss.example"),
        ]
    )
    chain_result = run(chain.fetch_user("elonmusk"))
    rep.true("L1 失败后 L2 顶上", chain_result.ok)
    rep.check("结果标出真正生效的源", chain_result.provider, "fxtwitter_rss")
    rep.check("L1 记下连续失败", chain.health("fxtwitter")["failures"], 1)
    rep.check("L2 记下成功", chain.health("fxtwitter_rss")["ok"], 1)
    rep.check("L1 主动设的值被覆盖", chain_client.urls()[0].startswith("https://api.example"), True)
    rep.check(
        "第一轮试过坏源一次",
        sum(1 for url in chain_client.urls() if "api.example" in url),
        1,
    )
    stats_snapshot = chain.snapshot()
    rep.check(
        "快照按**当前尝试顺序**输出（熔断源排最后）",
        [item["name"] for item in stats_snapshot],
        ["fxtwitter_rss", "fxtwitter"],
    )
    rep.check(
        "快照的 index 保留配置序号（文档里的 L1/L2）",
        [item["index"] for item in stats_snapshot],
        [1, 0],
    )
    by_name = {item["name"]: item for item in stats_snapshot}
    rep.true("失败源被标记为已熔断", by_name["fxtwitter"]["tripped"])
    rep.false("健康源未被标记为熔断", by_name["fxtwitter_rss"]["tripped"])

    # 找到健康的源就立刻返回：熔断的那个**不会**被再试一次（这是有意的——
    # 正常路径上 L1 就已经成功了，链不该为了"顺便探活"多花一次请求）。
    chain_client.calls.clear()
    run(chain.fetch_user("elonmusk"))
    round2_urls = chain_client.urls()
    rep.check(
        "健康源先命中即返回，不再试熔断源",
        [url.split("/")[2] for url in round2_urls],
        ["rss.example"],
    )
    rep.check(
        "第二轮没有再打坏源",
        sum(1 for url in round2_urls if "api.example" in url),
        0,
    )
    rep.check(
        "同一份 feed 只请求一次（记住上次成功的路径，不再白试后面的面）",
        len(round2_urls),
        1,
    )

    # 健康源也坏掉时，熔断源仍会被尝试（否则某源恢复后永远发现不了）
    chain_client.calls.clear()
    chain_client.rules.insert(0, ("rss.example", FakeResponse(500)))
    dead_round = run(chain.fetch_user("elonmusk"))
    rep.false("两层都坏时 ok=False", dead_round.ok)
    rep.check(
        "熔断源仍作为最后兜底被尝试",
        sum(1 for url in chain_client.urls() if "api.example" in url),
        1,
    )
    chain_client.rules.pop(0)

    # 「无新帖」是结论，不该再去问下一层
    only_204 = ScriptedClient([("api.example", FakeResponse(204))])
    chain2 = module.ProviderChain(
        [
            module.FxTwitterProvider(only_204, base="https://api.example"),
            module.RssProvider(only_204, base="https://rss.example"),
        ]
    )
    no_new_result = run(chain2.fetch_user("elonmusk"))
    rep.true("第一层给出无新帖即返回", no_new_result.ok and no_new_result.no_new)
    rep.check("不会白跑第二层", chain2.health("fxtwitter_rss")["ok"], 0)
    rep.check("只请求了一次", len(only_204.calls), 1)

    # 提供者抛异常不能把整轮打挂
    class ExplodingProvider:
        name = "boom"

        async def fetch_user(self, *args, **kwargs):
            raise RuntimeError("kaboom")

    chain3 = module.ProviderChain(
        [ExplodingProvider(), module.RssProvider(chain_client, base="https://rss.example")]
    )
    rep.true("提供者抛异常时链上兜底继续", run(chain3.fetch_user("elonmusk")).ok)
    rep.true("异常被记成该源失败", "RuntimeError" in chain3.health("boom")["last_error"])

    # 链全坏：必须 ok=False（否则会被当成"没有新帖"而永久漏推）
    chain_dead = module.ProviderChain([module.FxTwitterProvider(NoNetworkClient(), base="https://x")])
    dead = run(chain_dead.fetch_user("elonmusk"))
    rep.false("链全坏时 ok=False", dead.ok)
    rep.false("且不能退化成 no_new", dead.no_new)
    rep.true("错误里带上每一层的原因", "fxtwitter:" in dead.error)
    rep.check("空链给出明确错误", "fetch_chain" in run(module.ProviderChain([]).fetch_user("e")).error, True)

    # 404 只有"至少一层明确报 404"时才可信
    only_404 = ScriptedClient([("/2/profile/", FakeResponse(404))])
    chain_404 = module.ProviderChain(
        [module.FxTwitterProvider(only_404, base="https://api.example")]
    )
    nf = run(chain_404.fetch_user("nobody"))
    rep.false("404 时 ok=False", nf.ok)
    rep.true("404 被标成 not_found", nf.not_found)

    # 手动指定：设置后立即换序，清除后回到配置顺序
    fresh_chain = module.ProviderChain(
        [
            module.FxTwitterProvider(chain_client, base="https://api.example"),
            module.RssProvider(chain_client, base="https://rss.example"),
        ]
    )
    rep.check("初始顺序即配置顺序", fresh_chain.ordered_names(), ["fxtwitter", "fxtwitter_rss"])
    fresh_chain.set_pin("fxtwitter_rss")
    rep.check("手动指定后队首即该源", fresh_chain.ordered_names()[0], "fxtwitter_rss")
    rep.false("未知源名不接受", fresh_chain.set_pin("nope"))
    rep.check("未知源名不改变顺序", fresh_chain.ordered_names()[0], "fxtwitter_rss")
    fresh_chain.clear_pin()
    rep.check("清除指定后恢复配置顺序", fresh_chain.ordered_names(), ["fxtwitter", "fxtwitter_rss"])

    # ---- 插件侧接线：配置归一、链重建、归属与状态
    rep.check(
        "normalize_chain_names 去未知项并保序",
        module.normalize_chain_names(["rsshub", "nope", "fxtwitter", "rsshub"]),
        ["rsshub", "fxtwitter"],
    )
    rep.check("normalize_chain_names 接受逗号串", module.normalize_chain_names("fxtwitter, rsshub"), ["fxtwitter", "rsshub"])

    pchain = make_plugin(
        module,
        tag="p5_chain",
        plugin_admins=["root"],
        fetch_chain=["fxtwitter", "fxtwitter_rss"],
        fxtwitter_api_base="https://api.example",
        rss_base="https://rss.example",
    )
    pchain._http = ScriptedClient(
        [
            ("/2/profile/", FakeResponse(503)),
            ("feed.xml", FakeResponse(200, ok_feed_xml(item_id=4004, pub_date="Wed, 15 Nov 2023 22:13:20 GMT"))),
        ]
    )
    built = pchain._ensure_chain()
    rep.check("按配置装配出的链名", list(built.names), ["fxtwitter", "fxtwitter_rss"])
    # 与配置无关的键改一下：不该触发重建，实例必须原样保留
    pchain.config["poll_interval_minutes"] = 30
    rep.true("无关配置变化不重建链", pchain._ensure_chain() is built)

    # 组成变化触发重建时必须**保留 provider 实例与熔断统计**，
    # 否则 WebUI 每保存一次配置就会把"RSSHub 某实例在冷却"悄悄清空。
    old_chain = module.ProviderChain(
        [
            module.FxTwitterProvider(pchain._http, base="https://api.example"),
            module.RssProvider(pchain._http, base="https://rss.example"),
        ]
    )
    rss_before = old_chain.providers[1]
    run(old_chain.fetch_user("elonmusk"))
    candidate = module.ProviderChain([module.RssProvider(pchain._http, base="https://rss.example")])
    candidate.adopt(old_chain)
    rep.check("重建时复用同类同配置的实例", candidate.providers[0] is rss_before, True)
    rep.check("重建时继承该源的统计", candidate.health("fxtwitter_rss")["ok"], old_chain.health("fxtwitter_rss")["ok"])

    pchain.config["fetch_chain"] = ["fxtwitter"]
    rebuilt = pchain._ensure_chain()
    rep.check("改了 fetch_chain 后自动重建并保序", list(rebuilt.names), ["fxtwitter"])
    pchain.config["fetch_chain"] = ["fxtwitter", "fxtwitter_rss"]
    pchain._ensure_chain()
    rep.check(
        "重建后链长恢复",
        list(pchain._provider.names),
        ["fxtwitter", "fxtwitter_rss"],
    )

    pchain.store.upsert_author("44196397", "elonmusk", "Elon Musk")
    pchain.store.add_edge("aiocqhttp:GroupMessage:100", "100", "44196397", "", 0)
    base_round = run(pchain.poll_author("44196397"))
    rep.true("降级到第二层后仍是成功抓取", "baseline" in base_round)
    rep.check("按账号记下生效源", pchain.source_for("elonmusk"), "fxtwitter_rss")
    health = pchain.provider_health()
    rep.check(
        "健康快照列出链上源",
        sorted(item["name"] for item in health["chain"]),
        ["fxtwitter", "fxtwitter_rss"],
    )
    rep.true("健康快照标出降级中", health["degraded"])
    rep.true(
        "失败原因写进快照",
        "HTTP 503" in {item["name"]: item for item in health["chain"]}["fxtwitter"]["last_error"],
    )

    # /x状态 要显示生效源，且不能因为链有问题就报错
    pchain.store.add_edge("aiocqhttp:GroupMessage:100", "100", "44196397", "马斯克", 0)
    st = drive(pchain.cmd_status(FakeEvent(uid="root", gid="100")))
    rep.contains("状态里显示数据源链", st[0], "数据源")
    rep.contains("状态里显示降级告警", st[0], "降级中")

    # /x源：仅全局管理员；列源 / 指定 / 探测
    denied = drive(pchain.cmd_source(FakeEvent(uid="nobody", gid="100"), ""))
    rep.contains("非管理员被拒绝", denied[0], "⛔")
    listed = drive(pchain.cmd_source(FakeEvent(uid="root", gid="100"), ""))
    rep.contains("列出降级链", listed[0], "数据源降级链")
    rep.contains("列出每个源的状态", listed[0], "fxtwitter_rss")
    pinned = drive(pchain.cmd_source(FakeEvent(uid="root", gid="100"), "fxtwitter_rss"))
    rep.contains("手动指定成功", pinned[0], "已把 fxtwitter_rss 提到队首")
    rep.check("指定后链已换序", pchain._provider.ordered_names()[0], "fxtwitter_rss")
    auto = drive(pchain.cmd_source(FakeEvent(uid="root", gid="100"), "auto"))
    rep.contains("取消指定成功", auto[0], "已取消手动指定")
    # 这条链上的 fxtwitter 是真失败过的（被冷却挪后），所以取消指定后队首仍是健康的那个；
    # "恢复配置顺序"应当用没失败过的干净链验证（已在上面 fresh_chain 断言过）。
    rep.check(
        "取消指定后按健康度排序（坏源不回到队首）",
        pchain._provider.ordered_names(),
        ["fxtwitter_rss", "fxtwitter"],
    )
    rep.false("取消指定后 pinned 已清空", pchain._provider.pinned)
    unknown = drive(pchain.cmd_source(FakeEvent(uid="root", gid="100"), "nope"))
    rep.contains("未知源名给出可选项", unknown[0], "未知数据源")

    probed = run(pchain.probe_providers())
    rep.check("探测覆盖链上每个源", [item["name"] for item in probed], ["fxtwitter", "fxtwitter_rss"])
    rep.check("探测如实反映第一层不可用", [item["ok"] for item in probed], [False, True])
    probe_out = drive(pchain.cmd_source(FakeEvent(uid="root", gid="100"), "探测"))
    rep.contains("探测输出 human 可读", probe_out[0], "❌ 不可用")

    # 探测不能污染主链的熔断计数
    before_failures = pchain.provider_health()["chain"][0]["failures"]
    run(pchain.probe_providers())
    rep.check(
        "探测不改变主链熔断计数",
        pchain.provider_health()["chain"][0]["failures"],
        before_failures,
    )

    # 空链不能把插件打挂，也不能静默假装成功
    pempty = make_plugin(module, tag="p5_empty_chain", fetch_chain=[])
    pempty._http = ScriptedClient()
    rep.true("fetch_chain 为空时不构造链", pempty._ensure_chain() is None)
    rep.check("空链时轮询如实报告", run(pempty.poll_once()).get("skipped"), "empty_chain")
    rep.contains("空链时状态查询给出准确原因", pempty.provider_health()["message"], "fetch_chain")

    # 替身链不会被配置重建逻辑覆盖（离线自检与线上路径的行为要一致）
    pkeep = make_plugin(module, tag="p5_keep_fake", fetch_chain=["fxtwitter"])
    pkeep._http = object()
    with_provider(pkeep, FakeProvider(module, author=AUTHOR, posts=[]), module)
    rep.check("哨兵 http 下保留替身链", list(pkeep._ensure_chain().names), ["fake"])

    # =========================================================== 多源降级链的反向断言
    # 下面每条都对应一个被证伪的结论，都是**先有 bug、后有断言**。
    # 断言前的注释写的是"为什么会错"，方便下一个人判断能不能改。

    # ---- 404 是"账号不存在"，不是"这个源坏了"：
    # 若 404 也进熔断计数，默认 threshold=1 时一个打错的 handle 就能把健康源
    # 冷却 600s，让**所有**账号在这段时间里改用更弱的源。
    def _route_by_handle(url):
        if "nobody" in url:
            return FakeResponse(404)
        return FakeResponse(200, json_data={"code": 200, "results": [make_status()]})

    nf_client = ScriptedClient([("/2/profile/", _route_by_handle)])
    nf_chain = module.ProviderChain(
        [
            module.FxTwitterProvider(nf_client, base="https://api.example"),
            module.RssProvider(nf_client, base="https://rss.example"),
        ]
    )
    missing = run(nf_chain.fetch_user("nobody"))
    rep.false("404 时 ok=False", missing.ok)
    rep.true("404 被标为 not_found", missing.not_found)
    rep.check("404 只计入 not_found 计数", nf_chain.health("fxtwitter")["not_found"], 1)
    rep.check("404 **不**计入 failures", nf_chain.health("fxtwitter")["failures"], 0)
    rep.false("404 不会熔断该源", nf_chain.is_tripped("fxtwitter"))
    healthy = run(nf_chain.fetch_user("elonmusk"))
    rep.check("404 之后其它账号仍优先用同一个健康源", healthy.provider, "fxtwitter")
    # 上面那条不够狠：即使 fxtwitter 被熔断，替身里 rss 没有对应路由、最后仍会回落到 fxtwitter。
    # 直接断言顺序才是真正守住"404 不进熔断"的判据。
    rep.check("404 之后尝试顺序不变（fxtwitter 仍排第一）", nf_chain.ordered_names()[0], "fxtwitter")

    # ---- 配置变了不能继承统计：否则新地址一次都没请求过就顶着旧地址的
    #      failures/tripped_until 显示"熔断 + 旧地址的 HTTP 503"。
    adopt_client = ScriptedClient([("/2/profile/", FakeResponse(503))])
    old_chain = module.ProviderChain(
        [module.FxTwitterProvider(adopt_client, base="https://dead.example")]
    )
    run(old_chain.fetch_user("elonmusk"))
    rep.true("旧地址确实熔断了", old_chain.is_tripped("fxtwitter"))

    same_cfg = module.ProviderChain(
        [module.FxTwitterProvider(adopt_client, base="https://dead.example")]
    )
    same_cfg.adopt(old_chain)
    rep.check("同配置重建时继承统计", same_cfg.health("fxtwitter")["failures"], 1)

    new_cfg = module.ProviderChain(
        [module.FxTwitterProvider(adopt_client, base="https://api.example")]
    )
    new_cfg.adopt(old_chain)
    rep.check("换 base 重建时**不**继承统计", new_cfg.health("fxtwitter")["failures"], 0)
    rep.false("换 base 后新源不是熔断状态", new_cfg.is_tripped("fxtwitter"))
    rep.false("换 base 后不保留旧地址的报错", "503" in new_cfg.health("fxtwitter")["last_error"])

    # ---- 熔断旋钮也必须触发重建（否则 WebUI 保存后毫无变化）
    pthr = make_plugin(
        module,
        tag="p5_threshold",
        fetch_chain=["fxtwitter"],
        provider_failover_threshold=1,
        provider_cooldown_sec=600,
    )
    pthr._http = ScriptedClient([("/2/profile/", FakeResponse(503))])
    c_before = pthr._ensure_chain()
    rep.check("初始熔断阈值", c_before.threshold, 1)
    pthr.config["provider_failover_threshold"] = 5
    pthr.config["provider_cooldown_sec"] = 30
    c_after = pthr._ensure_chain()
    rep.true("改阈值/冷却必须重建链", c_after is not c_before)
    rep.check("新阈值立即生效", c_after.threshold, 5)
    rep.check("新冷却立即生效", int(c_after.cooldown), 30)

    # ---- 并发：一个"发出更早、回来更晚"的失败不得抹掉刚落地的成功。
    # 若不禁时序，一次网络抖动就能把刚恢复的源踢到队尾并冷却（且成功即返回，
    # 它在冷却期内不会再被试到），是 threshold=1 + 并发最常见的误伤。
    class _GatedProvider(module.FetchProvider):
        name = "gated"

        def __init__(self):
            self.gate = asyncio.Event()
            self.mode = "slow_fail"

        async def fetch_user(self, handle, since_ts=0, with_replies=False):
            if self.mode == "slow_fail":
                await self.gate.wait()
                return module.FetchResult(ok=False, error="late failure", source=self.name)
            if self.mode == "fast_ok":
                return module.FetchResult(ok=True, posts=[], source=self.name)
            return module.FetchResult(ok=False, error="boom", source=self.name)

    gated = _GatedProvider()
    gate_chain = module.ProviderChain([gated])

    async def _race():
        slow = asyncio.create_task(gate_chain.fetch_user("slow"))
        await asyncio.sleep(0.01)  # 让 slow 先发出去
        gated.mode = "fast_ok"
        await gate_chain.fetch_user("fast")  # 后发、先回、成功
        gated.gate.set()
        await slow

    run(_race())
    rep.check("并发下晚到的失败不覆盖成功", gate_chain.health("gated")["failures"], 0)
    rep.false("并发下健康源不被误熔断", gate_chain.is_tripped("gated"))

    gated.mode = "fail_now"  # 成功之后**新发生**的失败仍然要记
    run(gate_chain.fetch_user("x"))
    rep.check("成功之后新发生的失败仍被记录", gate_chain.health("gated")["failures"], 1)

    # ---- RssHub 池：实例地址只走参数，绝不写回 self.base。
    # 若写回共享状态，并发时 A 的 URL 会被 B 改写而落到别的实例，
    # 但 A 结束时仍按自己的局部变量记账 → 实例健康归错人、坏实例被立刻"复活"。
    hub_client = ScriptedClient(
        [
            ("bad.example", FakeResponse(500)),
            ("good.example", FakeResponse(200, ok_feed_xml(item_id=7001))),
        ]
    )
    hub_pool = module.RssHubProvider(
        hub_client, instances=["https://bad.example", "https://good.example"]
    )
    hub_pool.base = "https://sentinel.example"
    pool_result = run(hub_pool.fetch_user("elonmusk"))
    rep.true("池故障转移成功", pool_result.ok)
    rep.check("池内换实例不改写 self.base", hub_pool.base, "https://sentinel.example")
    pool_states = {item["name"]: item for item in hub_pool.health_snapshot()}
    rep.false("失败实例被标记为不健康", pool_states["https://bad.example"]["healthy"])
    rep.true("成功实例保持健康", pool_states["https://good.example"]["healthy"])

    # 真交错：A 卡在 hub1 的第一次请求时，B 已让 hub1 失败并切到 hub2。
    # 老实现里 A 后续的 URL 会被 B 改写到 hub2，A 却仍 `_mark('hub1', 成功)`，
    # 于是刚被判死的 hub1 立刻回到轮换队首。
    class _InterleavingPoolClient:
        def __init__(self):
            self.hub1_calls = 0
            self.hits = []
            self.a_inside = asyncio.Event()
            self.gate = asyncio.Event()

        async def get(self, url, headers=None, **kwargs):
            host = url.split("/")[2]
            self.hits.append(host)
            if host.startswith("hub1"):
                self.hub1_calls += 1
                if self.hub1_calls == 1:
                    self.a_inside.set()
                    await self.gate.wait()
                return FakeResponse(500)
            return FakeResponse(200, ok_feed_xml(item_id=8001))

    inter_client = _InterleavingPoolClient()
    inter_pool = module.RssHubProvider(
        inter_client, instances=["https://hub1.example", "https://hub2.example"]
    )

    async def _interleaved():
        task_a = asyncio.create_task(inter_pool.fetch_user("aa"))
        await inter_client.a_inside.wait()  # A 已进入 hub1
        task_b = asyncio.create_task(inter_pool.fetch_user("bb"))
        await task_b  # B 让 hub1 失败 → 切 hub2 成功
        inter_client.gate.set()
        await task_a

    run(_interleaved())
    inter_states = {item["name"]: item for item in inter_pool.health_snapshot()}
    rep.false("真交错后坏实例仍是不健康（不会被误判复活）", inter_states["https://hub1.example"]["healthy"])
    rep.true("真交错后好实例健康", inter_states["https://hub2.example"]["healthy"])
    rep.check("坏实例的失败次数被如实累计", inter_states["https://hub1.example"]["failures"] >= 1, True)

    # 上面那条"真交错"是补充性的，单靠它区分不出修复：直接把"每个实例的请求都带自己的地址"
    # 断言出来——这才是 URL 与记账同源的判据（旧实现会把第二条路径发到被并发改写的实例上）。
    seen_bases: list[str] = []
    _real_rss_fetch = module.RssProvider.fetch_user

    async def _record_base(self, handle, since_ts=0, with_replies=False, base=""):
        seen_bases.append(base)
        return await _real_rss_fetch(self, handle, since_ts, with_replies, base=base)

    module.RssProvider.fetch_user = _record_base
    try:
        probe_pool = module.RssHubProvider(
            ScriptedClient(
                [
                    ("hub1.example", FakeResponse(500)),
                    ("hub2.example", FakeResponse(200, ok_feed_xml(item_id=9101))),
                ]
            ),
            instances=["https://hub1.example", "https://hub2.example"],
        )
        run(probe_pool.fetch_user("elonmusk"))
    finally:
        module.RssProvider.fetch_user = _real_rss_fetch
    rep.check(
        "每个实例的请求都用自己的地址（URL 与记账同源）",
        seen_bases,
        ["https://hub1.example", "https://hub2.example"],
    )

    # ---- 手动指定：只对**一次**抓取生效（消费即解除），不会被别的源的成功清掉
    pin_provider_ok = FakeProvider(module, author=AUTHOR, posts=[])
    pin_chain_2 = module.ProviderChain(
        [
            FakeProvider(module, author=AUTHOR, posts=[], ok=False),
            pin_provider_ok,
        ]
    )
    pin_chain_2.providers[0].name = "always_fail"
    pin_chain_2._names = ("always_fail", "fake")
    pin_chain_2.set_pin("fake")
    rep.check("指定后队首即该源", pin_chain_2.ordered_names()[0], "fake")
    run(pin_chain_2.fetch_user("elonmusk"))
    rep.check("抓取一次后 pin 自动解除", pin_chain_2.pinned, "")

    pin_chain_3 = module.ProviderChain(
        [
            FakeProvider(module, author=AUTHOR, posts=[], ok=False),
            FakeProvider(module, author=AUTHOR, posts=[]),
        ]
    )
    pin_chain_3.providers[0].name = "always_fail"
    pin_chain_3.providers[1].name = "good"
    pin_chain_3._names = ("always_fail", "good")
    pin_chain_3.set_pin("always_fail")
    run(pin_chain_3.fetch_user("elonmusk"))
    rep.check("被指定的源失败时 pin 也已消费（不会粘着）", pin_chain_3.pinned, "")
    rep.check("兜底命中的是另一个源", pin_chain_3.health("good")["ok"], 1)

    # ---- 快照：index = 配置序号，order = 当前顺位；degraded 按配置第一个源判断
    snap_chain = module.ProviderChain(
        [
            module.FxTwitterProvider(ScriptedClient([("/2/profile/", FakeResponse(503))]), base="https://api.example"),
            module.RssProvider(ScriptedClient([("feed.xml", FakeResponse(200, ok_feed_xml()))]), base="https://rss.example"),
        ]
    )
    run(snap_chain.fetch_user("elonmusk"))
    snap = snap_chain.snapshot()
    rep.check("快照首行是当前最先尝试的源", snap[0]["name"], "fxtwitter_rss")
    snap_by_name = {item["name"]: item for item in snap}
    rep.check("index 保留配置序号（L1 仍是 fxtwitter）", snap_by_name["fxtwitter"]["index"], 0)
    rep.check("order 反映当前顺位", snap_by_name["fxtwitter"]["order"], 1)
    rep.true("degraded 按**配置第一个源**判断", snap_chain.degraded())
    clean_chain = module.ProviderChain(
        [module.RssProvider(ScriptedClient([("feed.xml", FakeResponse(200, ok_feed_xml()))]), base="https://rss.example")]
    )
    rep.false("没有失败时不是降级状态", clean_chain.degraded())

    # ---- 空链：必须说"链为空"，不能一律说"尚未初始化完成"（后者会让人一直等重启）
    pempty2 = make_plugin(module, tag="p5_empty_msg", fetch_chain=[], plugin_admins=["root"])
    pempty2._http = ScriptedClient()
    pempty2.context = SendContext()
    replay_msg = run(pempty2.replay_recent("aiocqhttp:GroupMessage:100", "elonmusk", 1))
    rep.contains("空链时 /x补发 给出可操作原因", replay_msg["message"], "fetch_chain")
    st_empty = drive(pempty2.cmd_status(FakeEvent(uid="root", gid="100")))
    rep.contains("空链时 /x状态 明确告警", st_empty[0], "数据源不可用")
    module.web_api.request = WebRequest(
        query={}, body={"umo": "aiocqhttp:GroupMessage:100", "handle": "elonmusk"}
    )
    edge_resp = run(pempty2.api_edge_add())
    rep.contains("空链时 WebUI 新增订阅给出可操作原因", str(edge_resp.get("error") or ""), "fetch_chain")

    # ---- /x源 的 L 序号用配置序号，当前顺位另说
    src_chain = make_plugin(
        module,
        tag="p5_source_display",
        plugin_admins=["root"],
        fetch_chain=["fxtwitter", "fxtwitter_rss"],
        fxtwitter_api_base="https://api.example",
        rss_base="https://rss.example",
    )
    src_chain._http = ScriptedClient(
        [
            ("/2/profile/", FakeResponse(503)),
            ("feed.xml", FakeResponse(200, ok_feed_xml())),
        ]
    )
    run(src_chain._ensure_chain().fetch_user("elonmusk"))
    src_out = drive(src_chain.cmd_source(FakeEvent(uid="root", gid="100"), ""))
    rep.contains("L 序号仍是配置序号 L1 fxtwitter", src_out[0], "L1 fxtwitter")
    rep.contains("同时标出当前顺位", src_out[0], "当前第 2 顺位")
    rep.contains("列出当前尝试顺序", src_out[0], "当前尝试顺序")
    probe_out2 = drive(src_chain.cmd_source(FakeEvent(uid="root", gid="100"), "探测"))
    rep.contains("探测结果显示条目数", probe_out2[0], "条）")

    # ---- RSS 侧缺陷（纯函数层面）
    rep.check("_rss_post_id 空来源返回空串（不再塌成常量哈希）", module._rss_post_id(""), "")
    rep.check("_rss_post_id 空来源条目被丢弃", module._entry_to_status({"description": "x"}), None)
    idless = (
        b'<?xml version="1.0"?><rss version="2.0"><channel><title>c</title>'
        b"<item><title>no id</title><description>d</description>"
        b"<pubDate>Wed, 15 Nov 2023 22:13:20 GMT</pubDate></item></channel></rss>"
    )
    rep.check("全部条目缺 id → 判该源不可用", module.parse_feed_xml(idless, handle="c"), None)

    rep.check("sniff_ext 认 ftyp 为 mp4", module.sniff_ext(b"\x00\x00\x00\x18ftypmp42xxxx"), ".mp4")
    rep.check("sniff_ext 仍认 PNG", module.sniff_ext(b"\x89PNG\r\n\x1a\nxxxxxxxx"), ".png")
    rep.check("_guess_lang 英文", module._guess_lang("Hello world, this is a test"), "en")
    rep.check("_guess_lang 中文", module._guess_lang("你好，世界"), "zh")
    rep.check("_guess_lang 日文", module._guess_lang("こんにちは世界"), "ja")
    rep.check("_guess_lang 韩文", module._guess_lang("안녕하세요"), "ko")
    rep.check("_guess_lang 空文本返回空", module._guess_lang(""), "")

    # _handle_from_url 必须排除保留路径段：否则作者/转推判定/链接会一起错
    for raw_url, want in (
        ("https://fxtwitter.com/elonmusk", "elonmusk"),
        ("https://fxtwitter.com/elonmusk/feed.xml", "elonmusk"),
        ("https://x.com/elonmusk/status/123", "elonmusk"),
        ("https://nitter.net/elonmusk/rss", "elonmusk"),
        ("https://rsshub.app/twitter/user/elonmusk", "elonmusk"),
        ("https://x.com/i/status/123", ""),
        ("https://twitter.com/i/web/status/123", ""),
        ("", ""),
    ):
        rep.check(f"_handle_from_url({raw_url!r})", module._handle_from_url(raw_url), want)

    # XML 容错：HTML 实体 / 裸 & / 未声明前缀都不该让整份 feed 判死
    loose_feed = (
        b'<?xml version="1.0"?><rss version="2.0"><channel>'
        b"<title>A&nbsp;B</title>"
        b"<item><title>t &amp; t</title><link>https://x.com/a/status/1</link>"
        b"<guid>https://x.com/a/status/1</guid>"
        b"<pubDate>Wed, 15 Nov 2023 22:13:20 GMT</pubDate>"
        b"<description>x &rsquo;y &amp; z</description></item></channel></rss>"
    )
    loose = module.parse_feed_xml(loose_feed, handle="a")
    rep.true("含 HTML 实体的 feed 仍可解析", bool(loose and len(loose[0]) == 1))
    rep.check("&nbsp; 被还原成空格", loose[1]["name"], "A B")
    rep.contains("&rsquo; 被还原", loose[0][0]["text"], "’")
    undeclared_ns = (
        b'<?xml version="1.0"?><rss version="2.0"><channel><title>c</title>'
        b'<item><title>v</title><link>https://x.com/a/status/2</link>'
        b'<media:thumbnail url="https://pbs.twimg.com/t.jpg"/>'
        b"<pubDate>Wed, 15 Nov 2023 22:13:20 GMT</pubDate>"
        b"<enclosure url=\"https://video.twimg.com/v.mp4\" type=\"video/mp4\"/>"
        b"<description>d</description></item></channel></rss>"
    )
    ns_parsed = module.parse_feed_xml(undeclared_ns, handle="a")
    rep.true("未声明 media: 前缀的 feed 仍可解析", bool(ns_parsed and len(ns_parsed[0]) == 1))
    rep.true("并把 media:thumbnail 读成视频封面", bool(ns_parsed[0][0]["media"]["all"][0].get("thumbnail_url")))

    # build_share_url 必须优先用 feed 给的 url：
    # dc:creator 与条目 link 不一致时，靠 author 重拼会发出指向别人账号的错链
    creator_feed = (
        b'<?xml version="1.0"?><rss version="2.0" xmlns:dc="http://purl.org/dc/elements/1.1/">'
        b"<channel><title>channel</title><link>https://fxtwitter.com/elonmusk</link>"
        b"<item><title>rt</title><link>https://twitter.com/benln/status/2000000000000000123</link>"
        b"<dc:creator>elonmusk</dc:creator>"
        b"<guid>https://twitter.com/benln/status/2000000000000000123</guid>"
        b"<pubDate>Wed, 15 Nov 2023 22:13:20 GMT</pubDate>"
        b"<description>d</description></item></channel></rss>"
    )
    creator_parsed = module.parse_feed_xml(creator_feed, handle="elonmusk")
    creator_post = creator_parsed[0][0]
    rep.check("条目 link 的作者优先于 dc:creator", creator_post["author"]["screen_name"], "benln")
    rep.check("链接取自 feed 而不是重拼", module.build_share_url(creator_post), creator_post["url"])
    rep.check("该条被判为转推", module.classify_post(creator_post), "repost")

    # _rss_author 不得用猜出来的 channel handle 覆盖已知 handle：
    # `/elonmusk/rss` 会反推出 "rss"，写进 authors.handle 后整条订阅会去抓错账号
    nitter_channel = (
        b'<?xml version="1.0"?><rss version="2.0"><channel><title>Elon Musk</title>'
        b"<link>https://nitter.example/elonmusk/rss</link>"
        b"<item><title>t</title><link>https://nitter.example/elonmusk/status/9</link>"
        b"<pubDate>Wed, 15 Nov 2023 22:13:20 GMT</pubDate><description>d</description>"
        b"</item></channel></rss>"
    )
    nitter_parsed = module.parse_feed_xml(nitter_channel, handle="elonmusk")
    rep.check(
        "channel link 反推失败时用调用方给的 handle",
        nitter_parsed[1]["screen_name"],
        "elonmusk",
    )

    # 附件占位锚文本（<a href="…mp4">Media</a>）不算正文
    # URL 用真实形态（/amplify_video/…），否则测不出真实契约
    placeholder_video = "https://video.twimg.com/amplify_video/1/vid/avc1/1280x720/x.mp4?tag=29"
    placeholder_feed = (
        '<?xml version="1.0"?><rss version="2.0"><channel><title>c</title>'
        "<item><title>v</title><link>https://x.com/a/status/3</link>"
        "<pubDate>Wed, 15 Nov 2023 22:13:20 GMT</pubDate>"
        "<description>&lt;p&gt;hello&lt;/p&gt;"
        f'&lt;p&gt;&lt;a href="{placeholder_video}"&gt;Media&lt;/a&gt;&lt;/p&gt;'
        "</description></item></channel></rss>"
    )
    placeholder_post = module.parse_feed_xml(placeholder_feed.encode(), handle="a")[0][0]
    rep.check("附件占位锚文本被剔除（真实 /amplify_video/ 形态）", placeholder_post["text"], "hello")
    user_media_link = placeholder_feed.replace(placeholder_video, "https://example.com/my-article")
    rep.contains(
        "用户自己写的 Media 链接不受影响（非 twimg）",
        module.parse_feed_xml(user_media_link.encode(), handle="a")[0][0]["text"],
        "Media",
    )
    fake_host = placeholder_feed.replace(
        placeholder_video, "https://example.com/twimg.com/amplify_video/1/x.mp4"
    )
    rep.contains(
        "主机名不是 twimg 就不算占位（子串匹配不再误判）",
        module.parse_feed_xml(fake_host.encode(), handle="a")[0][0]["text"],
        "Media",
    )
    card_link = placeholder_feed.replace(
        placeholder_video, "https://pbs.twimg.com/card_img/1/abc?format=jpg&name=800x419"
    )
    rep.contains(
        "链接卡片图（/card_img/）不算附件占位，正文保留",
        module.parse_feed_xml(card_link.encode(), handle="a")[0][0]["text"],
        "Media",
    )

    # ---- 视频落盘的扩展名必须是 .mp4（否则发送端按图片处理/发不出去）
    class _FakeStreamResponse:
        def __init__(self, data: bytes, content_type: str):
            self.status_code = 200
            self.headers = {"content-type": content_type}
            self._data = data

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def aiter_bytes(self):
            yield self._data

    class _FakeStreamClient:
        def __init__(self, data: bytes, content_type: str = "video/mp4"):
            self.data = data
            self.content_type = content_type
            self.calls: list[str] = []

        def stream(self, method, url, headers=None, **kwargs):
            self.calls.append(url)
            return _FakeStreamResponse(self.data, self.content_type)

    pvideo = make_plugin(module, tag="p5_video_ext")
    mp4_bytes = b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 64
    video_client = _FakeStreamClient(mp4_bytes, "video/mp4")
    saved = run(
        pvideo.media_cache.fetch(
            "https://video.twimg.com/amplify_video/1/vid/avc1/1280x720/x.mp4?tag=29",
            client=video_client,
        )
    )
    rep.true("视频下载成功落盘", saved is not None)
    rep.check("视频落盘扩展名是 .mp4", saved.suffix if saved else "", ".mp4")

    # 视频只在 send_video 打开时才下载（否则白占缓存与带宽）
    video_record = [
        {
            "post_id": "1",
            "created_ts": 1,
            "kind": "original",
            "body_json": "{}",
            "media_json": json.dumps(
                [{"type": "video", "url": "https://video.twimg.com/v.mp4"}], ensure_ascii=False
            ),
            "has_media": 1,
        }
    ]
    pvoff = make_plugin(module, tag="p5_video_off", send_video=False)
    off_client = _FakeStreamClient(mp4_bytes)
    pvoff._http = off_client
    run(pvoff._prepare_media(video_record))
    rep.check("send_video=false 时不下载视频", off_client.calls, [])

    pvon = make_plugin(module, tag="p5_video_on", send_video=True)
    on_client = _FakeStreamClient(mp4_bytes)
    pvon._http = on_client
    prepared = run(pvon._prepare_media(video_record))
    rep.check("send_video=true 时才下载视频", len(on_client.calls), 1)
    rep.true("下载后写回本地路径", bool(json.loads(prepared[0]["media_json"])[0].get("path")))

    # ---- 同一 handle 不得因"先降级后恢复"而多出一行 authors
    preuse = make_plugin(
        module, tag="p5_author_reuse", fetch_chain=["fxtwitter_rss"], rss_base="https://rss.example"
    )
    preuse._http = ScriptedClient([("feed.xml", FakeResponse(200, real_raw))])

    def _author_rows(plugin):
        with plugin.store._conn() as conn:
            return [str(r["author_id"]) for r in conn.execute("SELECT author_id FROM authors")]

    first_resolve = run(preuse._resolve_author("elonmusk"))
    rep.check("只有 RSS 时 author_id 用 handle 兜底", first_resolve[0], "elonmusk")
    rep.check("此时 authors 只有一行", _author_rows(preuse), ["elonmusk"])
    preuse.config["fetch_chain"] = ["fxtwitter"]
    preuse.config["fxtwitter_api_base"] = "https://api.example"
    preuse._http = ScriptedClient(
        [("/2/profile/", FakeResponse(200, json_data={"code": 200, "results": [make_status()]}))]
    )
    second_resolve = run(preuse._resolve_author("elonmusk"))
    rep.check("L1 恢复后复用已有行（不新建）", second_resolve[0], "elonmusk")
    rep.check("同一 handle 仍然只有一行", _author_rows(preuse), ["elonmusk"])

    # ---- FxTwitter：200 + 空 results 且无 cursor 不能当成"没有新帖"
    # （那是"水位停摆式永久漏推"的残余入口，尤其对自建反代）
    empty_200 = module.FxTwitterProvider(
        ScriptedClient(
            [("/2/profile/", FakeResponse(200, json_data={"code": 200, "results": []}))]
        ),
        base="https://api.example",
    )
    empty_res = run(empty_200.fetch_user("elonmusk"))
    rep.false("200+空 results 且无 cursor → 判源不可用", empty_res.ok)
    rep.contains("错误里点明原因", empty_res.error, "cursor")
    cursor_200 = module.FxTwitterProvider(
        ScriptedClient(
            [
                (
                    "/2/profile/",
                    FakeResponse(
                        200,
                        json_data={"code": 200, "results": [], "cursor": {"top": None, "bottom": None}},
                    ),
                )
            ]
        ),
        base="https://api.example",
    )
    cursor_res = run(cursor_200.fetch_user("elonmusk"))
    rep.true("带 cursor 的空时间线才是无新帖", cursor_res.ok and cursor_res.no_new)

    # ---- 真实语料整体回归（resources/feed.xml 只在开发仓里，缺失就跳过）
    corpus = PLUGIN_DIR.parent.parent / "resources" / "feed.xml"
    if corpus.is_file():
        c_raw = corpus.read_bytes()
        c_parsed = module.parse_feed_xml(c_raw, handle="elonmusk")
        rep.true("真实语料可解析", bool(c_parsed))
        c_posts, _c_author = c_parsed
        rep.check("真实语料条目数", len(c_posts), 90)
        rep.check("真实语料转推数", sum(1 for p in c_posts if p.get("reposted_by")), 54)
        rep.check("真实语料引用数", sum(1 for p in c_posts if p.get("quote")), 50)
        rep.check("真实语料含媒体条目数", sum(1 for p in c_posts if module.post_has_media(p)), 34)
        rep.check(
            "真实语料的链接卡片图不算自带媒体（与 L1 的 media.external 同口径）",
            sum(
                1
                for p in c_posts
                if any("card_img" in m.get("url", "") for m in module.extract_media(p))
            ),
            0,
        )
        rep.check(
            "真实语料视频条目数",
            sum(1 for p in c_posts if any(m["type"] == "video" for m in module.extract_media(p))),
            19,
        )
        rep.check("真实语料链接全部正确", sum(1 for p in c_posts if module.build_share_url(p) == p["url"]), 90)
        # 上面那条其实是 `url == url` 的同义反复；作者与链接作者一致才是真正守住
        # "作者取自 <link>" 的判据（v1.3.0 时只有 36/90 成立）。
        rep.check(
            "真实语料作者与链接作者一致",
            sum(
                1
                for p in c_posts
                if p["author"]["screen_name"] == module._handle_from_url(p["url"])
            ),
            90,
        )
        rep.check(
            "真实语料里不再残留 Media 占位",
            sum(1 for p in c_posts if p["text"].strip().endswith("Media")),
            0,
        )
        rep.check("真实语料作者去重后数量", len({p["author"]["screen_name"] for p in c_posts}), 44)
        rep.check(
            "真实语料入队数（filter_reposts=true）",
            sum(1 for p in c_posts if preal.to_post_record(p)),
            36,
        )
    else:
        print("SKIP  真实语料整体回归：本机没有 resources/feed.xml（发布包里不带）")

    # =========================================================== 抓取与发送层的反向断言
    # 每条都对应"修错了 / 没修好 / 修出新问题"的地方。

    # ---- 404 的处理必须**区分**"账号不存在"与"源坏了"：
    # 只做"404 不熔断"会留下僵尸源；只做"连续 404 就熔断"会冤枉健康源
    # （3 个被删/改名的账号就能让首选源永久降级，而且熔断后它再也拿不到成功、
    #   计数永远清零不了 → 自锁）。这里用"探针账号"作为判据。
    def _probe_ok_route(url):
        if "/statuses" not in url:
            return FakeResponse(404)
        if "/gone" in url:
            return FakeResponse(404)
        return FakeResponse(200, json_data={"code": 200, "results": [make_status()]})

    healthy_probe_client = ScriptedClient([("/2/profile/", _probe_ok_route)])
    healthy_chain = module.ProviderChain(
        [module.FxTwitterProvider(healthy_probe_client, base="https://api.example")],
        probe_handle="X",
    )
    for handle in ("gone1", "gone2", "gone3", "gone4"):
        run(healthy_chain.fetch_user(handle))
    rep.false("（探针正常时）多个已删账号不会熔断健康源", healthy_chain.is_tripped("fxtwitter"))
    rep.check("触发探测后 404 计数被重置（不会无限累计）", healthy_chain.health("fxtwitter")["not_found"], 1)
    rep.check("404 不污染 failures", healthy_chain.health("fxtwitter")["failures"], 0)
    healthy_ok = run(healthy_chain.fetch_user("elonmusk"))
    rep.true("健康源仍能正常供数", healthy_ok.ok)
    rep.check("一次成功就把 404 计数清零", healthy_chain.health("fxtwitter")["not_found"], 0)

    # 对谁都 404（base 配错）⇒ 探针也失败 ⇒ 判定源坏了并降级
    def _probe_fail_route(url):
        return FakeResponse(404)

    zombie_client = ScriptedClient([("/2/profile/", _probe_fail_route)])
    zombie = module.ProviderChain(
        [module.FxTwitterProvider(zombie_client, base="https://wrong-base.example")],
        probe_handle="X",
    )
    run(zombie.fetch_user("elonmusk"))
    rep.false("单次 404 不熔断", zombie.is_tripped("fxtwitter"))
    for _ in range(2):
        run(zombie.fetch_user("elonmusk"))
    zombie_state = zombie.health("fxtwitter")
    rep.true("探针也失败 ⇒ 判定源坏了", bool(zombie_state.get("source_broken")))
    rep.true("源坏了就进入冷却（不再每轮白试）", zombie.is_tripped("fxtwitter"))
    rep.check("判定源坏时不伪造 failures 计数", zombie_state["failures"], 0)
    rep.check("探测用掉后不再重复探测", bool(zombie_state.get("needs_probe")), False)
    rep.check("探测账号命中的是 probe_handle", sum(1 for u in zombie_client.urls() if "/X/" in u) >= 1, True)

    # 冷却到期后 404 计数复位：不能"一次 404 立刻又冷却 600s"
    fresh_404 = ScriptedClient([("/2/profile/", FakeResponse(404))])
    cooldown_chain = module.ProviderChain(
        [module.FxTwitterProvider(fresh_404, base="https://wrong.example")],
        cooldown=0.05,
        probe_handle="X",
    )
    cooldown_chain.not_found_threshold = 2
    run(cooldown_chain.fetch_user("a"))
    run(cooldown_chain.fetch_user("b"))
    rep.true("先被判定源坏", cooldown_chain.is_tripped("fxtwitter"))
    time.sleep(0.1)
    rep.false("冷却到期后不再算熔断", cooldown_chain.is_tripped("fxtwitter"))
    run(cooldown_chain.fetch_user("c"))
    rep.check(
        "冷却到期后 404 计数从 0 重新开始（不会一次 404 就再熔断）",
        cooldown_chain.health("fxtwitter")["not_found"],
        1,
    )

    # 时序守卫必须覆盖 404 分支：晚到的 404 不能熔断刚落地的成功
    class _LateNotFound(module.FetchProvider):
        name = "late404"

        def __init__(self):
            self.gate = asyncio.Event()
            self.mode = "slow404"

        async def fetch_user(self, handle, since_ts=0, with_replies=False):
            if self.mode == "slow404":
                await self.gate.wait()
                return module.FetchResult(ok=False, not_found=True, error="late 404", source=self.name)
            if self.mode == "ok":
                return module.FetchResult(ok=True, posts=[], source=self.name)
            return module.FetchResult(ok=False, not_found=True, error="404", source=self.name)

    late = _LateNotFound()
    late_chain = module.ProviderChain([late], probe_handle="X")
    late_chain.not_found_threshold = 1  # 让它一有机会就探测

    async def _late_race():
        slow = asyncio.create_task(late_chain.fetch_user("gone"))
        await asyncio.sleep(0.01)
        late.mode = "ok"
        await late_chain.fetch_user("elonmusk")
        late.gate.set()
        await slow

    run(_late_race())
    rep.check("晚到的 404 不污染刚落地的成功", late_chain.health("late404")["not_found"], 0)
    rep.false("晚到的 404 不会让源进入探测/降级", late_chain.is_tripped("late404"))

    # RSSHub 池：404（账号不存在）不该冷却实例
    pool404_client = ScriptedClient([("hub.example", FakeResponse(404))])
    pool404 = module.RssHubProvider(pool404_client, instances=["https://hub.example"])
    pool404_res = run(pool404.fetch_user("nobody"))
    rep.true("池透传内层 not_found", pool404_res.not_found)
    pool404_states = {item["name"]: item for item in pool404.health_snapshot()}
    rep.true(
        "404 不再把实例冷却（实例是活的，只是账号不存在）",
        pool404_states["https://hub.example"]["healthy"],
    )

    # /x源 要能看出"源探测失败"
    psrc_broken = make_plugin(
        module,
        tag="p5_src_broken",
        plugin_admins=["root"],
        fetch_chain=["fxtwitter"],
        fxtwitter_api_base="https://wrong.example",
        probe_handle="X",
    )
    psrc_broken._http = ScriptedClient([("/2/profile/", FakeResponse(404))])
    broken_chain = psrc_broken._ensure_chain()
    for _ in range(broken_chain.not_found_threshold):
        run(broken_chain.fetch_user("nobody"))
    out_broken = drive(psrc_broken.cmd_source(FakeEvent(uid="root", gid="100"), ""))
    rep.contains("/x源 显示源探测失败", out_broken[0], "源探测失败")

    # ---- 作者位：X 的真实路由段要被排除，但可能是账号名的词要保留
    for raw_url, want in (
        ("https://x.com/rss/status/1", "rss"),
        ("https://x.com/user/status/1", "user"),
        ("https://x.com/lists/status/1", "lists"),
        ("https://x.com/feed/status/1", "feed"),
        ("https://x.com/home/status/1", ""),
        ("https://x.com/search/status/1", ""),
        ("https://x.com/explore/status/1", ""),
        ("https://x.com/intent/status/1", ""),
        ("https://x.com/settings/status/1", ""),
    ):
        rep.check(f"作者位 {raw_url}", module._handle_from_url(raw_url), want)

    # ---- 附件占位锚：video.twimg.com 的**任何**路径都算（早期只认三个路径，
    # 换成 ext_tw_video 就会漏删、"Media" 泄漏回正文）
    for host_path in (
        "https://video.twimg.com/amplify_video/1/vid/avc1/1/x.mp4",
        "https://video.twimg.com/ext_tw_video/1/pu/vid/1/x.mp4",
        "https://video.twimg.com/tweet_video/1/x.mp4",
        "https://video.twimg.com/vid/1/x.mp4",
    ):
        rep.true(f"占位锚识别 {host_path}", module._is_media_placeholder_anchor(host_path))
    for host_path in (
        "https://pbs.twimg.com/media/x.jpg",
        "https://pbs.twimg.com/card_img/1/x.jpg",
        "https://pbs.twimg.com/profile_images/1/x.jpg",
        "https://example.com/twimg.com/amplify_video/1/x.mp4",
        "https://evil-twimg.com/amplify_video/1/x.mp4",
    ):
        rep.check(
            f"占位锚排除 {host_path}",
            module._is_media_placeholder_anchor(host_path),
            host_path.startswith("https://pbs.twimg.com/media/"),
        )

    # ---- _guess_lang：剔除 URL 之后，短线中文不再被判成 en
    rep.check("短中文 + 拉丁邮箱仍判中文", module._guess_lang("联系我 foo@bar.com"), "zh")
    rep.check("2 汉字 + 8 拉丁仍判中文", module._guess_lang("汉字abcdefgh"), "zh")
    rep.check("单个汉字也判中文", module._guess_lang("中"), "zh")
    rep.check("短中文 + t.co 仍判中文", module._guess_lang("太棒了 https://t.co/AbCdEfGhIj"), "zh")

    # ---- 缓存：同 URL 有多个候选时取最新；目标文件是 0 字节残留时也能改名
    pcache = make_plugin(module, tag="p5_cache_ext")
    cache_url = "https://video.twimg.com/amplify_video/77/vid/avc1/1/x.mp4"
    cache_digest = hashlib.sha256(cache_url.encode()).hexdigest()
    old_file = pcache.media_dir / f"{cache_digest}.jpg"
    old_file.write_bytes(b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 32)
    empty_target = pcache.media_dir / f"{cache_digest}.mp4"
    empty_target.write_bytes(b"")
    fixed_path = run(pcache.media_cache.fetch(cache_url))
    rep.check("目标为 0 字节残留时仍能改名成功", fixed_path.suffix, ".mp4")
    rep.true("改名后文件非空", fixed_path.stat().st_size > 0)

    newest = pcache.media_dir / f"{hashlib.sha256(b'https://x/y.jpg').hexdigest()}.jpg"
    newest.write_bytes(PNG)
    older = pcache.media_dir / f"{hashlib.sha256(b'https://x/y.jpg').hexdigest()}.png"
    older.write_bytes(PNG)
    os.utime(older, (1, 1))
    rep.check(
        "多候选时取最新写入的那个",
        pcache.media_cache.find_existing("https://x/y.jpg").suffix,
        ".jpg",
    )

    # ---- 媒体预处理失败不能拖垮整轮（水位与入队都要保住）
    pmedia_fail = make_plugin(
        module, tag="p5_media_fail", fetch_chain=["fxtwitter_rss"], rss_base="https://rss.example"
    )
    pmedia_fail._http = ScriptedClient([("feed.xml", FakeResponse(200, real_raw))])
    pmedia_fail.store.upsert_author("100000001", "demo_feed", "Sample Feed")
    pmedia_fail.store.mark_baseline("100000001", 0)
    pmedia_fail.store.add_edge("aiocqhttp:GroupMessage:100", "100", "100000001", "", 0)

    async def _boom_prepare(records):
        raise RuntimeError("media prep exploded")

    pmedia_fail._prepare_media = _boom_prepare
    fail_round = run(pmedia_fail.poll_author("100000001"))
    rep.false("媒体预处理异常不会让本次报错", bool(fail_round.get("error")))
    rep.check("媒体预处理异常仍完成入队", fail_round.get("enqueued", 0) > 0, True)
    rep.true(
        "媒体预处理异常仍推进水位",
        int(pmedia_fail.store.get_author("100000001")["last_seen_ts"]) > 0,
    )

    def _typo_route(url):
        if "nobody" in url:
            return FakeResponse(404)
        return FakeResponse(200, json_data={"code": 200, "results": [make_status()]})

    mixed_client = ScriptedClient([("/2/profile/", _typo_route)])
    mixed_chain = module.ProviderChain(
        [module.FxTwitterProvider(mixed_client, base="https://api.example")]
    )
    for _ in range(4):
        run(mixed_chain.fetch_user("nobody"))
        run(mixed_chain.fetch_user("elonmusk"))
    rep.false("健康源夹着打错的 handle 永远不会被降级", mixed_chain.is_tripped("fxtwitter"))
    rep.check("成功会把 not_found 计数清零", mixed_chain.health("fxtwitter")["not_found"], 0)

    # RSSHub 池必须透传内层 404：否则一个不存在的 handle 会熔断整个 rsshub 源
    npool_client = ScriptedClient([("hub.example", FakeResponse(404))])
    npool = module.RssHubProvider(npool_client, instances=["https://hub.example"])
    nres = run(npool.fetch_user("nobody"))
    rep.true("池透传内层 not_found", nres.not_found)
    npool_chain = module.ProviderChain(
        [module.RssHubProvider(npool_client, instances=["https://hub.example"])]
    )
    run(npool_chain.fetch_user("nobody"))
    rep.false("池的 404 不再熔断 rsshub 源", npool_chain.is_tripped("rsshub"))

    # /x源 要把 not_found 显示出来（否则这个计数是死数据）
    psrc404 = make_plugin(
        module,
        tag="p5_src_notfound",
        plugin_admins=["root"],
        fetch_chain=["fxtwitter"],
        fxtwitter_api_base="https://wrong.example",
    )
    psrc404._http = ScriptedClient([("/2/profile/", FakeResponse(404))])
    chain404 = psrc404._ensure_chain()
    for _ in range(chain404.not_found_threshold):
        run(chain404.fetch_user("nobody"))
    out404 = drive(psrc404.cmd_source(FakeEvent(uid="root", gid="100"), ""))
    rep.contains("/x源 显示账号不存在计数", out404[0], "账号不存在")

    # ---- pin：整轮都失败也必须消费（旧实现"成功才解除"会粘住），
    #      且"顺手抓一次"的调用者（/x订阅、/x补发）不得吃掉 pin
    class _AlwaysFail(module.FetchProvider):
        name = "bad"

        async def fetch_user(self, handle, since_ts=0, with_replies=False):
            return module.FetchResult(ok=False, error="nope", source=self.name)

    class _AlwaysOk(module.FetchProvider):
        name = "ok"

        async def fetch_user(self, handle, since_ts=0, with_replies=False):
            return module.FetchResult(ok=True, posts=[], source=self.name)

    pin_fail = module.ProviderChain([_AlwaysFail()])
    pin_fail.set_pin("bad")
    run(pin_fail.fetch_user("x"))
    rep.check("整轮都失败时 pin 也被消费（不会粘住）", pin_fail.pinned, "")

    pin_keep = module.ProviderChain([_AlwaysOk()])
    pin_keep.set_pin("ok")
    run(pin_keep.fetch_user("x", consume_pin=False))
    rep.check("非轮询路径不消费 pin", pin_keep.pinned, "ok")
    run(pin_keep.fetch_user("x"))
    rep.check("轮询路径才消费 pin", pin_keep.pinned, "")

    # ---- 并发守卫用单调时钟：墙上时钟被校时/休眠改动后不得误判"谁更晚"
    mono_chain = module.ProviderChain([_AlwaysOk()])
    mono_chain.mark_success("ok")
    stats_ok = mono_chain.health("ok")
    rep.true("成功时记录单调时钟", float(stats_ok.get("last_ok_mono") or 0) > 0)
    rep.true("同时保留墙上时钟供展示", float(stats_ok.get("last_ok_ts") or 0) > 0)

    # ---- 手机构型：media_mode 的下载开关（v1.3.0 起这句恒假 → 下载路径是死代码）
    pdl = make_plugin(module, tag="p5_media_download", media_mode="download")
    rep.true("media_mode=download 时下载开关为真", pdl.media_download_enabled())
    plink = make_plugin(module, tag="p5_media_link", media_mode="link")
    rep.false("media_mode=link 时下载开关为假", plink.media_download_enabled())
    pdefault = make_plugin(module, tag="p5_media_default")
    rep.true("默认（不配 media_mode）就是本地下载", pdefault.media_download_enabled())

    dl_calls: list[int] = []

    async def _spy_prepare(records):
        dl_calls.append(len(records))
        return records

    feed_one = ok_feed_xml(item_id=6001)
    for plugin, tag, want in ((pdl, "p5_dl_on", 1), (plink, "p5_dl_off", 0)):
        plugin._prepare_media = _spy_prepare
        plugin._http = ScriptedClient([("feed.xml", FakeResponse(200, feed_one))])
        plugin.config["fetch_chain"] = ["fxtwitter_rss"]
        plugin.config["rss_base"] = "https://rss.example"
        plugin.config["include_replies"] = False
        plugin.config["filter_reposts"] = False
        plugin.config["filter_replies"] = False
        plugin.store.upsert_author("44196397", "elonmusk", "Elon Musk")
        plugin.store.mark_baseline("44196397", 0)
        plugin.store.add_edge("aiocqhttp:GroupMessage:100", "100", "44196397", "", 0)
        dl_calls.clear()
        run(plugin.poll_author("44196397"))
        rep.check(f"{tag}：轮询是否调用 _prepare_media", len(dl_calls), want)

    # ---- 老缓存里的 .jpg 视频要能被改名（否则 v1.3.0 存的视频永远修不回来）
    pext = make_plugin(module, tag="p5_ext_fix")
    digest = hashlib.sha256(
        b"https://video.twimg.com/amplify_video/9/vid/avc1/1/x.mp4"
    ).hexdigest()
    stale = pext.media_dir / f"{digest}.jpg"
    stale.write_bytes(b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 32)
    fixed = run(pext.media_cache.fetch("https://video.twimg.com/amplify_video/9/vid/avc1/1/x.mp4"))
    rep.check("老缓存 .jpg 里的 mp4 会被改名成 .mp4", fixed.suffix, ".mp4")
    rep.false("改名后不残留旧扩展名", stale.exists())

    # ftyp 家族要按 brand 区分（早期实现一律 .mp4）
    for brand, want_ext in (
        (b"qt  ", ".mov"),
        (b"M4A ", ".m4a"),
        (b"3gp4", ".3gp"),
        (b"heic", ".heic"),
        (b"avif", ".avif"),
        (b"isom", ".mp4"),
        (b"mp42", ".mp4"),
    ):
        rep.check(
            f"sniff_ext ftyp brand={brand!r}",
            module.sniff_ext(b"\x00\x00\x00\x18ftyp" + brand + b"\x00" * 4),
            want_ext,
        )
    rep.check("无 ftyp 的 webm 不会当成 mp4", module.sniff_ext(b"\x1aE\xdf\xa3" + b"\x00" * 12), ".jpg")

    # ---- _guess_lang：URL/@提及 不能把中文帖判成英文
    rep.check("短中文 + t.co 仍判中文", module._guess_lang("太棒了 https://t.co/AbCdEfGhIj"), "zh")
    rep.check("中文 + 英文链接仍判中文", module._guess_lang("请看这里 https://example.com/a/b/c"), "zh")
    rep.check("只有 @提及 + 链接 → 判不出来", module._guess_lang("@elonmusk https://t.co/x"), "")
    rep.check("汉字为主的日文仍是已知误判（zh）", module._guess_lang("東京大学 入学式 開催 予定"), "zh")

    # ---- 面探测：第一个面"合法但为空/陈旧"不能遮住后面面上的内容
    empty_valid_feed = (
        b'<?xml version="1.0"?><rss version="2.0"><channel><title>c</title></channel></rss>'
    )
    face_client = ScriptedClient(
        [
            ("feed.xml", FakeResponse(200, empty_valid_feed)),
            ("/feed", FakeResponse(200, ok_feed_xml(item_id=9001))),
        ]
    )
    face_res = run(module.RssProvider(face_client, base="https://rss.example").fetch_user("elonmusk"))
    rep.check("第一个面合法但为空时继续试后面的面", len(face_res.posts), 1)
    rep.check("确实把两个面都试了", len(face_client.urls()), 2)

    # ---- repair_xml_text 不得改动本来就正确的 XML 预定义实体
    entity_doc = (
        b'<?xml version="1.0"?><rss version="2.0"><channel><title>A&nbsp;B</title>'
        b"<item><title>t</title>"
        b"<link>https://x.com/a/status/1?x=1&amp;y=2</link>"
        b"<guid>https://x.com/a/status/1?x=1&amp;y=2</guid>"
        b'<enclosure url="https://pbs.twimg.com/media/x.jpg?format=jpg&amp;name=orig" type="image/jpeg"/>'
        b"<pubDate>Wed, 15 Nov 2023 22:13:20 GMT</pubDate>"
        b"<description>d</description></item></channel></rss>"
    )
    entity_post = module.parse_feed_xml(entity_doc, handle="a")[0][0]
    rep.check("修复器不改动正确的 &amp;（链接）", entity_post["url"], "https://x.com/a/status/1?x=1&y=2")
    rep.check(
        "修复器不改动正确的 &amp;（媒体直链）",
        module.extract_media(entity_post)[0]["url"],
        "https://pbs.twimg.com/media/x.jpg?format=jpg&name=orig",
    )
    rep.check("&nbsp; 仍然被正确还原", module.parse_feed_xml(entity_doc, handle="a")[1]["name"], "A B")

    # ---- 作者位上 @rss / @user 这类 handle 必须被当成真实账号
    rep.check("作者位不排除 rss", module._handle_from_url("https://x.com/rss/status/1"), "rss")
    rep.check("作者位不排除 user", module._handle_from_url("https://x.com/user/status/1"), "user")
    rep.check("作者位排除 i（墓碑/不可见帖）", module._handle_from_url("https://x.com/i/status/1"), "")
    rep.check("channel link 仍排除保留词", module._handle_from_url("https://nitter.example/rss"), "")
    reserved_feed = (
        b'<?xml version="1.0"?><rss version="2.0"><channel><title>c</title>'
        b"<link>https://fxtwitter.com/elonmusk</link>"
        b"<item><title>t</title><link>https://x.com/rss/status/2108000000000000999</link>"
        b"<guid>https://x.com/rss/status/2108000000000000999</guid>"
        b"<pubDate>Wed, 15 Nov 2023 22:13:20 GMT</pubDate><description>d</description>"
        b"</item></channel></rss>"
    )
    reserved_post = module.parse_feed_xml(reserved_feed, handle="elonmusk")[0][0]
    rep.check("handle 为保留词的作者不再被误判", reserved_post["author"]["screen_name"], "rss")
    rep.check("并且被正确判为转推", module.classify_post(reserved_post), "repost")

    # ---- _rss_author：channel link 形态不能覆盖调用方给的 handle
    rep.check(
        "channel link=https://x.com/rss 时用调用方 handle",
        module._rss_author({"link": "https://x.com/rss", "title": "T"}, "elonmusk")["screen_name"],
        "elonmusk",
    )
    rep.check(
        "channel link 是 API 路径时不再取到 2",
        module._rss_author(
            {"link": "https://api.fxtwitter.com/2/profile/elonmusk", "title": "T"}, "elonmusk"
        )["screen_name"],
        "elonmusk",
    )

    # ---- 同 handle 两行时，取作者必须确定性地选规范（数字 id）那一行
    pdup = make_plugin(module, tag="p5_dup_author")
    pdup.store.upsert_author("elonmusk", "elonmusk", "旧行")
    pdup.store.upsert_author("44196397", "elonmusk", "新行")
    rep.check(
        "同 handle 两行时优先数字 id 行",
        str(pdup.store.get_author_by_handle("elonmusk")["author_id"]),
        "44196397",
    )

    # ---- 端到端：设了 pin 之后走一次 /x订阅 的解析路径，pin 必须还在
    src_chain._provider.set_pin("fxtwitter")
    run(src_chain._resolve_author("someone"))
    rep.check("订阅/WebUI 的解析路径不吃掉 pin", src_chain._provider.pinned, "fxtwitter")
    src_chain._provider.clear_pin()


    # =========================================================== 免打扰与投递
    pdnd = make_plugin(
        module,
        tag="dnd",
        dnd_enabled=True,
        dnd_ranges=["00:00-23:59"],
        dnd_backlog_limit=50,
    )
    rep.true("全天时段判定为免打扰", pdnd.in_quiet_hours())
    pdnd.store.upsert_author("1", "h", "H")
    pdnd.store.mark_baseline("1", 100)
    pdnd.store.add_edge("aiocqhttp:GroupMessage:100", "100", "1", "", 100)
    pdnd.store.commit_poll(
        "1",
        [
            {
                "post_id": "q1",
                "created_ts": 200,
                "kind": "original",
                "body_json": json.dumps(make_status(handle="h", name="H"), ensure_ascii=False),
                "media_json": "[]",
            }
        ],
        list(pdnd.store.list_edges()),
        200,
    )
    ctx_dnd = SendContext()
    pdnd.context = ctx_dnd
    rep.check("免打扰时 drain 不发", run(pdnd.drain_once()), 0)
    rep.check("免打扰时消息留在队列里", pdnd.store.queue_depth(), 1)
    rep.check("免打扰时确实没有调用发送", len(ctx_dnd.sent), 0)

    # 免打扰按群隔离：一个群设置不影响另一个群
    pdnd_g = make_plugin(module, tag="dnd_group", dnd_backlog_limit=50)
    pdnd_g.store.set_group_dnd("aiocqhttp:GroupMessage:200", True, "00:00-23:59")
    rep.true("群级免打扰生效", pdnd_g.in_quiet_hours("aiocqhttp:GroupMessage:200"))
    rep.false("未设置的群不受影响", pdnd_g.in_quiet_hours("aiocqhttp:GroupMessage:100"))
    rep.true("全局默认仍可作兜底", pdnd_g.in_quiet_hours() is False)

    pdnd2 = make_plugin(module, tag="dnd2", dnd_enabled=False)
    rep.false("未开启免打扰", pdnd2.in_quiet_hours())
    pdnd2.store.upsert_author("1", "h", "H")
    pdnd2.store.mark_baseline("1", 100)
    pdnd2.store.add_edge("aiocqhttp:GroupMessage:100", "100", "1", "昵称", 100)
    pdnd2.store.commit_poll(
        "1",
        [{"post_id": "d1", "created_ts": 200, "kind": "original", "body_json": json.dumps(make_status(handle="h", name="H")), "media_json": "[]"}],
        list(pdnd2.store.list_edges()),
        200,
    )
    ctx5 = SendContext()
    pdnd2.context = ctx5
    sent = run(pdnd2.drain_once())
    rep.check("非免打扰时成功发出 1 条", sent, 1)
    rep.check("队列已清空", pdnd2.store.queue_depth(), 0)
    rep.check("确实调用了发送", len(ctx5.sent), 1)

    pdnd3 = make_plugin(module, tag="dnd3", dnd_backlog_limit=1, dnd_notify_on_drop=True)
    pdnd3.store.upsert_author("1", "h", "H")
    pdnd3.store.mark_baseline("1", 100)
    pdnd3.store.add_edge("aiocqhttp:GroupMessage:100", "100", "1", "", 100)
    pdnd3.store.commit_poll(
        "1",
        [
            {"post_id": f"b{i}", "created_ts": 100 + i, "kind": "original", "body_json": "{}", "media_json": "[]"}
            for i in range(1, 5)
        ],
        list(pdnd3.store.list_edges()),
        105,
    )
    ctx6 = SendContext()
    pdnd3.context = ctx6
    run(pdnd3._enforce_backlog_limit())
    rep.check("积压超限后只剩上限条数", pdnd3.store.queue_depth(), 1)
    rep.true("丢弃有汇总提示（绝不静默）", len(ctx6.sent) >= 1)

    # =========================================================== 命令
    pcmd = make_plugin(
        module,
        tag="cmd",
        plugin_admins=["root"],
        group_admins=["100:ga1"],
        default_group_mode="none",
    )
    with_provider(pcmd, FakeProvider(module, author=AUTHOR, posts=[make_status()]), module)

    ev = FakeEvent(uid="u1", gid="100")
    out = drive(pcmd.cmd_where(ev))
    rep.contains("x在哪 输出 UMO", out[0], "aiocqhttp:GroupMessage:100")
    rep.contains("x在哪 输出 UID", out[0], "u1")
    rep.true("x在哪 后停止事件", ev.stopped)

    ev2 = FakeEvent(uid="root", gid="100")
    out2 = drive(pcmd.cmd_list(ev2))
    rep.contains("空列表提示", out2[0], "还没有订阅")

    ev2b = FakeEvent(uid="u1", gid="100")
    out2b = drive(pcmd.cmd_list(ev2b))
    rep.contains("未配置群普通成员看不到列表", out2b[0], "⛔")

    ev3 = FakeEvent(uid="u1", gid="100")
    out3 = drive(pcmd.cmd_subscribe(ev3, "elonmusk 马斯克"))
    rep.contains("未配置群拒绝订阅", out3[0], "⛔")
    rep.check("拒绝时不建订阅", len(pcmd.store.list_edges()), 0)

    ev4 = FakeEvent(uid="root", gid="100")
    out4 = drive(pcmd.cmd_subscribe(ev4, "elonmusk 马斯克"))
    rep.contains("管理员订阅成功", out4[0], "✅")
    rep.check("订阅已建立", len(pcmd.store.list_edges()), 1)
    rep.true("订阅后唤醒轮询", pcmd._poll_wake.is_set())
    rep.true("订阅后置强制轮询标记", pcmd._force_poll)
    rep.check("self_id 按平台缓存", pcmd.store.get_meta("self_id:aiocqhttp", ""), "bot1")

    ev5 = FakeEvent(uid="root", gid="100")
    out5 = drive(pcmd.cmd_subscribe(ev5, "elonmusk"))
    rep.contains("重复订阅提示", out5[0], "已订阅过")

    ev5b = FakeEvent(uid="root", gid="100")
    out5b = drive(pcmd.cmd_subscribe(ev5b, ""))
    rep.contains("空参数给出用法", out5b[0], "用法")

    ev6 = FakeEvent(uid="u1", gid="100")
    out6 = drive(pcmd.cmd_nickname(ev6, "elonmusk x"))
    rep.contains("普通成员改昵称被拒", out6[0], "⛔")

    ev7 = FakeEvent(uid="ga1", gid="100")
    out7 = drive(pcmd.cmd_nickname(ev7, "elonmusk 马斯/克"))
    rep.contains("群级管理员可改本群昵称", out7[0], "✅")
    rep.check(
        "昵称已规范化落库",
        pcmd.store.get_edge("aiocqhttp:GroupMessage:100", "44196397")["nickname"],
        "马斯克",
    )

    ev7b = FakeEvent(uid="ga1", gid="100")
    out7b = drive(pcmd.cmd_nickname(ev7b, "elonmusk 带 空格 的 昵称"))
    rep.check(
        "贪婪参数保留了昵称里的空格",
        pcmd.store.get_edge("aiocqhttp:GroupMessage:100", "44196397")["nickname"],
        "带 空格 的 昵称",
    )

    ev8 = FakeEvent(uid="ga1", gid="100")
    out8 = drive(pcmd.cmd_nickname(ev8, "elonmusk 清空"))
    rep.contains("清空昵称", out8[0], "清空")
    rep.check(
        "昵称已清空",
        pcmd.store.get_edge("aiocqhttp:GroupMessage:100", "44196397")["nickname"],
        "",
    )

    # 取消订阅：按 umo 查找，因此结构上不可能操作他群的订阅
    pcmd.store.add_edge("aiocqhttp:GroupMessage:200", "200", "44196397", "", 0)
    out9 = drive(pcmd.cmd_unsubscribe(FakeEvent(uid="ga1", gid="100"), "elonmusk"))
    rep.contains("群级管理员可取消本群订阅", out9[0], "已取消订阅")
    rep.check("本群订阅已删", len(pcmd.store.list_edges("aiocqhttp:GroupMessage:100")), 0)
    rep.check(
        "他群订阅未受影响（umo 隔离）",
        len(pcmd.store.list_edges("aiocqhttp:GroupMessage:200")),
        1,
    )

    ev10 = FakeEvent(uid="root", gid="200")
    out10 = drive(pcmd.cmd_unsubscribe(ev10, "elonmusk"))
    rep.contains("全局管理员可取消", out10[0], "已取消订阅")
    rep.check("他群订阅也已删", len(pcmd.store.list_edges("aiocqhttp:GroupMessage:200")), 0)

    ev10b = FakeEvent(uid="u1", gid="100")
    out10b = drive(pcmd.cmd_unsubscribe(ev10b, "elonmusk"))
    rep.contains("不存在时报未订阅", out10b[0], "没有订阅")

    ev11 = FakeEvent(uid="u1", gid="100")
    out11 = drive(pcmd.cmd_dnd(ev11, "23:00-08:00"))
    rep.contains("非管理员配置免打扰被拒", out11[0], "⛔")

    ev12 = FakeEvent(uid="ga1", gid="100")
    out12 = drive(pcmd.cmd_dnd(ev12, "23:00-08:00"))
    rep.contains("群级管理员可配本群免打扰", out12[0], "免打扰已开启")
    group_row = pcmd.store.get_group("aiocqhttp:GroupMessage:100")
    rep.check("免打扰时段写入本群", group_row["dnd_ranges"], "23:00-08:00")
    rep.check("免打扰启用标记写入本群", int(group_row["dnd_enabled"]), 1)
    rep.check(
        "他群未被波及（免打扰按群隔离）",
        pcmd.store.get_group("aiocqhttp:GroupMessage:200"),
        None,
    )
    rep.check("全局配置未被改动", pcmd.config.get("dnd_ranges"), None)

    ev13 = FakeEvent(uid="ga1", gid="100")
    out13 = drive(pcmd.cmd_dnd(ev13, "关"))
    rep.contains("免打扰设置已清除", out13[0], "已清除")
    rep.check(
        "关闭 = 删除覆盖行（回落全局默认，而不是留下永久覆盖）",
        pcmd.store.get_group("aiocqhttp:GroupMessage:100"),
        None,
    )

    ev13b = FakeEvent(uid="ga1", gid="100")
    out13b = drive(pcmd.cmd_dnd(ev13b, "乱写"))
    rep.contains("非法时段被拒", out13b[0], "格式不对")

    ev14 = FakeEvent(uid="u1", gid="100")
    out14 = drive(pcmd.cmd_group_mode(ev14, "whitelist"))
    rep.contains("群模式仅全局管理员", out14[0], "全局管理员")

    ev15 = FakeEvent(uid="root", gid="100")
    out15 = drive(pcmd.cmd_group_mode(ev15, "whitelist"))
    rep.contains("全局管理员可改群模式", out15[0], "whitelist")
    rep.check("群模式写入 open_groups", pcmd.config.get("open_groups"), ["100"])
    rep.check("改模式后生效", pcmd.group_mode(FakeEvent("u1", "100")), "whitelist")

    ev16 = FakeEvent(uid="root", gid="100")
    out16 = drive(pcmd.cmd_group_mode(ev16, "readonly"))
    rep.check("从白名单切到只读", pcmd.config.get("open_groups"), [])
    rep.check("只读名单已写入", pcmd.config.get("readonly_groups"), ["100"])

    ev17 = FakeEvent(uid="root", gid="100")
    out17 = drive(pcmd.cmd_grant(ev17, "100:newadmin"))
    rep.contains("授权成功", out17[0], "✅")
    rep.true("新管理员生效", pcmd._is_group_admin(FakeEvent("newadmin", "100")))
    ev18 = FakeEvent(uid="root", gid="100")
    drive(pcmd.cmd_revoke(ev18, "100:newadmin"))
    rep.false("撤权后失效", pcmd._is_group_admin(FakeEvent("newadmin", "100")))

    ev19 = FakeEvent(uid="u1", gid="100")
    out19 = drive(pcmd.cmd_status(ev19))
    rep.contains("状态含订阅数", out19[0], "订阅总数")

    # =========================================================== 回放 /x补发
    prev = make_plugin(
        module,
        tag="replay",
        plugin_admins=["root"],
        group_admins=["100:ga1"],
        default_group_mode="none",
        send_mode="chain",
        media_mode="link",
        replay_default=3,
        replay_max=5,
    )
    rposts = [
        make_status(post_id=f"50000000000000000{i:02d}", created=1700001000 + i, text=f"post {i}")
        for i in range(1, 8)
    ]
    with_provider(prev, FakeProvider(module, author=AUTHOR, posts=rposts), module)

    ev_r0 = FakeEvent(uid="u1", gid="100")
    out_r0 = drive(prev.cmd_replay(ev_r0, "elonmusk 3"))
    rep.contains("回放需要管理权限", out_r0[0], "⛔")
    rep.check("被拒时没有发送", len(prev.context.sent), 0)

    ev_r1 = FakeEvent(uid="root", gid="100")
    out_r1 = drive(prev.cmd_replay(ev_r1, "elonmusk 3"))
    rep.contains("回放成功", out_r1[0], "已回放")
    rep.check("回放指定条数", len(prev.context.sent), 3)
    rep.check("回放不写投递记录", prev.store.queue_depth(), 0)
    rep.check("回放不写 posts", prev.store.count_posts(), 0)
    rep.check("回放不建作者水位", prev.store.get_author("44196397"), None)

    prev.context.sent.clear()
    out_r2 = drive(prev.cmd_replay(FakeEvent(uid="root", gid="100"), "elonmusk 99"))
    rep.check("回放条数被 replay_max 截断", len(prev.context.sent), 5)

    prev.context.sent.clear()
    drive(prev.cmd_replay(FakeEvent(uid="root", gid="100"), "elonmusk"))
    rep.check("回放默认条数生效", len(prev.context.sent), 3)

    prev.context.sent.clear()
    out_r3 = drive(prev.cmd_replay(FakeEvent(uid="root", gid="100"), ""))
    rep.contains("空参数给出用法", out_r3[0], "用法")

    with_provider(
        prev,
        FakeProvider(
            module, author=AUTHOR, posts=[make_status(reposted_by={"screen_name": "someone"})]
        ),
        module,
    )
    out_r4 = drive(prev.cmd_replay(FakeEvent(uid="root", gid="100"), "elonmusk 3"))
    rep.contains("全被过滤时给出可操作提示", out_r4[0], "没有符合当前过滤规则")

    # 回放要能验证"订阅级自定义昵称"确实落到合并转发节点上
    with_provider(prev, FakeProvider(module, author=AUTHOR, posts=rposts), module)
    prev.store.upsert_author("44196397", "elonmusk", "Elon Musk")
    prev.store.add_edge("aiocqhttp:GroupMessage:100", "100", "44196397", "马斯克", 0)
    prev.config["send_mode"] = "forward"
    prev.context.sent.clear()
    out_r5 = drive(prev.cmd_replay(FakeEvent(uid="root", gid="100"), "elonmusk 1"))
    rep.contains("回放提示使用本群自定义昵称", out_r5[0], "马斯克")
    rep.check("回放节点昵称 = 本群自定义昵称", prev.context.sent[-1][1].chain[0].nodes[0].name, "马斯克")

    prev.context.sent.clear()
    out_t = drive(prev.cmd_test(FakeEvent(uid="root", gid="100"), "elonmusk"))
    rep.check("x测试 节点昵称 = 本群自定义昵称", prev.context.sent[-1][1].chain[0].nodes[0].name, "马斯克")
    rep.contains(
        "x测试 正文标注昵称来源",
        prev.context.sent[-1][1].chain[0].nodes[0].content[0].args[0],
        "马斯克",
    )

    prev.context.sent.clear()
    prev.store.set_edge_nickname("aiocqhttp:GroupMessage:100", "44196397", "")
    drive(prev.cmd_test(FakeEvent(uid="root", gid="100"), "elonmusk"))
    rep.check(
        "x测试 无自定义昵称时回退推主名",
        prev.context.sent[-1][1].chain[0].nodes[0].name,
        "Elon Musk",
    )

    # =========================================================== 媒体缓存
    pmc = make_plugin(module, tag="media_cache", cache_quota_mb=64, cache_ttl_days=30)
    mc = pmc.media_cache
    target = mc.path_for("https://pbs.twimg.com/media/x.jpg", ".jpg")
    mc._atomic_write(target, PNG)
    rep.true("原子写成功", target.exists())
    rep.check("find_existing 命中同一 URL", mc.find_existing("https://pbs.twimg.com/media/x.jpg"), target)
    rep.check("find_existing 不同 URL 未命中", mc.find_existing("https://other/x.jpg"), None)
    rep.true("usage_bytes 统计到文件", mc.usage_bytes() >= len(PNG))

    # =========================================================== 仅媒体订阅
    # 判定口径（已确认）：media.all 非空且为 photo/video/gif 即算；external 外链不算
    rep.true("单图算媒体", module.post_has_media({"media": {"all": [{"type": "photo", "url": "u"}]}}))
    rep.true("视频算媒体", module.post_has_media({"media": {"all": [{"type": "video", "url": "u"}]}}))
    rep.true("gif 算媒体", module.post_has_media({"media": {"all": [{"type": "gif", "url": "u"}]}}))
    rep.true(
        "多图算媒体",
        module.post_has_media(
            {"media": {"all": [{"type": "photo", "url": "a"}, {"type": "photo", "url": "b"}]}}
        ),
    )
    rep.false("无 media 键不算", module.post_has_media({}))
    rep.false("media 为空对象不算", module.post_has_media({"media": {}}))
    rep.false("all 为空数组不算", module.post_has_media({"media": {"all": []}}))
    rep.false(
        "只有 external 外链卡片不算",
        module.post_has_media({"media": {"external": {"url": "https://youtube.com/x"}}}),
    )
    rep.false("只有 mosaic 不算", module.post_has_media({"media": {"mosaic": {"type": "mosaic_photo"}}}))
    rep.false("纯文字帖不算", module.post_has_media(make_status(text="just text")))
    rep.true(
        "纯媒体帖（text 为空）算",
        module.post_has_media(make_status(text="", media={"all": [{"type": "photo", "url": "u"}]})),
    )
    rep.check("记录里 has_media=0（纯文字）", pf.to_post_record(make_status(text="hi"))["has_media"], 0)
    rep.check(
        "记录里 has_media=1（带图）",
        pf.to_post_record(
            make_status(text="hi", media={"all": [{"type": "photo", "url": "u"}]})
        )["has_media"],
        1,
    )
    rep.false("edge_media_only 缺键时按 False", module.edge_media_only({}))
    rep.true("edge_media_only 读 1", module.edge_media_only({"media_only": 1}))
    rep.false("edge_media_only 读 0", module.edge_media_only({"media_only": 0}))

    # 同一账号：A 群要全部、B 群只要媒体 —— 过滤必须按订阅边生效
    pm = make_plugin(module, tag="mediaonly")
    pm.store.upsert_author("1", "h", "H")
    pm.store.mark_baseline("1", 100)
    pm.store.add_edge("aiocqhttp:GroupMessage:100", "100", "1", "全量", 100)
    pm.store.add_edge("aiocqhttp:GroupMessage:200", "200", "1", "仅媒体", 100, media_only=True)
    rep.false(
        "默认订阅不带 media_only",
        module.edge_media_only(pm.store.get_edge("aiocqhttp:GroupMessage:100", "1")),
    )
    rep.true(
        "订阅时可带 media_only",
        module.edge_media_only(pm.store.get_edge("aiocqhttp:GroupMessage:200", "1")),
    )

    text_post = {
        "post_id": "t1",
        "created_ts": 200,
        "kind": "original",
        "body_json": "{}",
        "media_json": "[]",
        "has_media": 0,
    }
    media_post = {
        "post_id": "m1",
        "created_ts": 300,
        "kind": "original",
        "body_json": "{}",
        "media_json": "[{}]",
        "has_media": 1,
    }
    ins, enq = pm.store.commit_poll("1", [text_post, media_post], list(pm.store.list_edges()), 300)
    rep.check("两条都入库", ins, 2)
    rep.check("入队 3 条（纯文字只给全量订阅，媒体帖给两个订阅）", enq, 3)
    rep.check(
        "队列内容符合预期",
        sorted((str(r["umo"]), str(r["post_id"])) for r in pm.store.next_queued(10)),
        [
            ("aiocqhttp:GroupMessage:100", "m1"),
            ("aiocqhttp:GroupMessage:100", "t1"),
            ("aiocqhttp:GroupMessage:200", "m1"),
        ],
    )
    rep.check(
        "仅媒体订阅的水位同样推进（跳过的不补发）",
        int(pm.store.get_edge("aiocqhttp:GroupMessage:200", "1")["enqueued_ts"]),
        300,
    )
    pm.store.set_edge_media_only("aiocqhttp:GroupMessage:200", "1", False)
    _ins2, enq2 = pm.store.commit_poll(
        "1", [text_post, media_post], list(pm.store.list_edges()), 300
    )
    rep.check("关掉仅媒体后不回溯补发被跳过的纯文字帖", enq2, 0)

    # 老库升级路径：缺列的库要能就地补列，且老数据不能丢
    mig_dir = TMP_ROOT / "migration"
    mig_dir.mkdir(parents=True, exist_ok=True)
    mig_db = mig_dir / "old.sqlite3"
    if mig_db.exists():
        mig_db.unlink()
    raw = sqlite3.connect(mig_db)
    raw.executescript(
        """
        CREATE TABLE edges (
            edge_id INTEGER PRIMARY KEY AUTOINCREMENT, umo TEXT NOT NULL,
            group_id TEXT NOT NULL DEFAULT '', author_id TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1, nickname TEXT NOT NULL DEFAULT '',
            enqueued_ts INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL DEFAULT 0,
            UNIQUE(umo, author_id));
        CREATE TABLE posts (
            post_id TEXT PRIMARY KEY, author_id TEXT NOT NULL, created_ts INTEGER NOT NULL,
            kind TEXT NOT NULL DEFAULT 'original', body_json TEXT NOT NULL,
            media_json TEXT NOT NULL DEFAULT '[]', translated TEXT NOT NULL DEFAULT '',
            translate_lang TEXT NOT NULL DEFAULT '', fetched_at INTEGER NOT NULL DEFAULT 0);
        """
    )
    raw.execute(
        "INSERT INTO edges(umo, group_id, author_id, nickname, enqueued_ts) "
        "VALUES('aiocqhttp:GroupMessage:100','100','1','老订阅',0)"
    )
    raw.commit()
    raw.close()

    migrated = module.Store(mig_db)
    with migrated._conn() as mconn:
        edge_cols = {str(r["name"]) for r in mconn.execute("PRAGMA table_info(edges)")}
        post_cols = {str(r["name"]) for r in mconn.execute("PRAGMA table_info(posts)")}
    rep.true("迁移补齐 edges.media_only", "media_only" in edge_cols)
    rep.true("迁移补齐 posts.has_media", "has_media" in post_cols)
    rep.check("迁移后老订阅仍在", len(migrated.list_edges("aiocqhttp:GroupMessage:100")), 1)
    rep.false(
        "迁移后老行的 media_only 默认关闭",
        module.edge_media_only(migrated.list_edges("aiocqhttp:GroupMessage:100")[0]),
    )
    rep.check("迁移后老行昵称保留", migrated.list_edges("aiocqhttp:GroupMessage:100")[0]["nickname"], "老订阅")

    # 命令层
    pmo = make_plugin(
        module,
        tag="mediaonly_cmd",
        plugin_admins=["root"],
        group_admins=["100:ga1"],
        default_group_mode="none",
        send_mode="chain",
        media_mode="link",
    )
    pmo.store.upsert_author("44196397", "elonmusk", "Elon Musk")
    pmo.store.mark_baseline("44196397", 100)
    pmo.store.add_edge("aiocqhttp:GroupMessage:100", "100", "44196397", "马斯克", 100)

    out_m0 = drive(pmo.cmd_media_only(FakeEvent(uid="u1", gid="100"), "elonmusk 开"))
    rep.contains("仅媒体需要管理权限", out_m0[0], "⛔")
    rep.false(
        "被拒时未改动",
        module.edge_media_only(pmo.store.get_edge("aiocqhttp:GroupMessage:100", "44196397")),
    )

    out_m1 = drive(pmo.cmd_media_only(FakeEvent(uid="ga1", gid="100"), "elonmusk 开"))
    rep.contains("群级管理员可开启仅媒体", out_m1[0], "仅媒体")
    rep.true(
        "仅媒体已落库",
        module.edge_media_only(pmo.store.get_edge("aiocqhttp:GroupMessage:100", "44196397")),
    )
    out_m2 = drive(pmo.cmd_media_only(FakeEvent(uid="ga1", gid="100"), "elonmusk 关"))
    rep.contains("可以关闭", out_m2[0], "全部原创帖")
    rep.false(
        "关闭后落库为 0",
        module.edge_media_only(pmo.store.get_edge("aiocqhttp:GroupMessage:100", "44196397")),
    )
    out_m3 = drive(pmo.cmd_media_only(FakeEvent(uid="ga1", gid="100"), "elonmusk 乱写"))
    rep.contains("非法参数被拒", out_m3[0], "只接受")
    out_m4 = drive(pmo.cmd_media_only(FakeEvent(uid="ga1", gid="100"), "nobody 开"))
    rep.contains("未订阅时给出提示", out_m4[0], "没有订阅")
    out_m5 = drive(pmo.cmd_media_only(FakeEvent(uid="ga1", gid="100"), "elonmusk"))
    rep.contains("缺参数给出用法", out_m5[0], "用法")

    drive(pmo.cmd_media_only(FakeEvent(uid="ga1", gid="100"), "elonmusk 开"))
    out_mlist = drive(pmo.cmd_list(FakeEvent(uid="root", gid="100")))
    rep.contains("订阅列表显示仅媒体标记", out_mlist[0], "仅媒体")

    # 回放遵守仅媒体
    with_provider(
        pmo,
        FakeProvider(
            module,
            author=AUTHOR,
            posts=[
                make_status(post_id="6000000000000000001", created=1700002001, text="纯文字帖"),
                make_status(
                    post_id="6000000000000000002",
                    created=1700002002,
                    text="带图帖",
                    media={"all": [{"type": "photo", "url": "https://pbs.twimg.com/media/x.jpg"}]},
                ),
            ],
        ),
        module,
    )
    pmo.context.sent.clear()
    out_rp = drive(pmo.cmd_replay(FakeEvent(uid="root", gid="100"), "elonmusk 5"))
    rep.check("回放遵守仅媒体：只回放带图那条", len(pmo.context.sent), 1)
    rep.contains("回放计数为 1/1", out_rp[0], "1/1")

    drive(pmo.cmd_media_only(FakeEvent(uid="ga1", gid="100"), "elonmusk 关"))
    pmo.context.sent.clear()
    drive(pmo.cmd_replay(FakeEvent(uid="root", gid="100"), "elonmusk 5"))
    rep.check("关掉仅媒体后回放两条都发", len(pmo.context.sent), 2)

    # =========================================================== 帮助与群登记
    ph = make_plugin(
        module,
        tag="help",
        plugin_admins=["root"],
        group_admins=["100:ga1"],
        open_groups=["200"],
        readonly_groups=["300"],
        default_group_mode="none",
        dnd_enabled=True,
        dnd_ranges=["00:00-23:59"],
    )
    ev_h = FakeEvent(uid="u1", gid="100")
    out_h = drive(ph.cmd_help(ev_h))
    rep.contains("帮助含标题", out_h[0], "指令帮助")
    rep.contains("帮助含订阅指令", out_h[0], "/x订阅")
    rep.contains("帮助含回放指令", out_h[0], "/x补发")
    rep.contains("帮助含仅媒体指令", out_h[0], "/x仅媒体")
    rep.contains("帮助含本群模式", out_h[0], "模式 none")
    rep.contains("帮助含身份", out_h[0], "普通成员（未配置群）")
    rep.true("帮助后停止事件", ev_h.stopped)

    out_h2 = drive(ph.cmd_help(FakeEvent(uid="root", gid="100")))
    rep.contains("全局管理员身份可见", out_h2[0], "全局管理员")
    out_h3 = drive(ph.cmd_help(FakeEvent(uid="ga1", gid="100")))
    rep.contains("群级管理员身份可见", out_h3[0], "群级管理员")
    out_h4 = drive(ph.cmd_help(FakeEvent(uid="u9", gid="200")))
    rep.contains("白名单群普通成员身份", out_h4[0], "普通成员（白名单群）")
    rep.check("role_of 只读群", ph.role_of(FakeEvent("u9", "300")), "普通成员（只读群）")

    # 群登记只进 known_groups，绝不建 groups 行 —— 否则会顶掉全局免打扰默认值
    rep.true(
        "帮助已登记本群",
        "aiocqhttp:GroupMessage:100" in ph.store.known_groups(),
    )
    rep.check("登记不会创建免打扰覆盖行", ph.store.get_group("aiocqhttp:GroupMessage:100"), None)
    rep.true(
        "登记后仍回落全局免打扰默认值（回归守卫）",
        ph.in_quiet_hours("aiocqhttp:GroupMessage:100"),
    )
    drive(ph.cmd_where(FakeEvent(uid="u1", gid="777")))
    rep.true("x在哪 也登记群", "aiocqhttp:GroupMessage:777" in ph.store.known_groups())
    rep.check("x在哪 的登记同样不建覆盖行", ph.store.get_group("aiocqhttp:GroupMessage:777"), None)

    # =========================================================== WebUI 后端
    rep.check("validate_umo 合法", module.validate_umo("aiocqhttp:GroupMessage:123"), (True, ""))
    rep.false("validate_umo 段数不对", module.validate_umo("aiocqhttp:123")[0])
    rep.false("validate_umo 空段", module.validate_umo("aiocqhttp::123")[0])
    rep.false("validate_umo 空串", module.validate_umo("")[0])
    rep.check("group_id_of 取最后一段", module.group_id_of("aiocqhttp:GroupMessage:123"), "123")
    rep.check("platform_id_of 取第一段", module.platform_id_of("mybot:GroupMessage:123"), "mybot")

    # =========================================================== umo 解析（本次 bug 的回归守卫）
    # umo 第一段是**平台实例 id**（如「mybot」），不是适配器名（aiocqhttp）。
    pplat = make_plugin(module, tag="umo")
    pplat.context = SendContext(platform_ids=["mybot", "另一个bot"])
    pplat.store.touch_group("mybot:GroupMessage:123456789")

    rep.check(
        "list_platforms 返回实例 id 与适配器名",
        [(p["id"], p["name"]) for p in pplat.list_platforms()],
        [("mybot", "aiocqhttp"), ("另一个bot", "aiocqhttp")],
    )
    rep.check("known_umos 含登记过的群", pplat.known_umos(), ["mybot:GroupMessage:123456789"])

    ok, umo, _err = pplat.resolve_umo("mybot:GroupMessage:123456789")
    rep.check("完整 umo 通过", (ok, umo), (True, "mybot:GroupMessage:123456789"))

    ok, umo, _err = pplat.resolve_umo("123456789")
    rep.check("裸群号唯一匹配到真实平台", (ok, umo), (True, "mybot:GroupMessage:123456789"))

    ok, _umo, err = pplat.resolve_umo("aiocqhttp:GroupMessage:123456789")
    rep.false("把适配器名当平台 id 会被拒（本次 bug 的守卫）", ok)
    rep.contains("错误信息列出可用实例 id", err, "mybot")
    rep.contains("错误信息点明不是适配器名", err, "适配器名")

    ok, _umo, err = pplat.resolve_umo("999999")
    rep.false("未知群号被拒", ok)
    rep.contains("未知群号提示先去 /x在哪", err, "/x在哪")

    pplat.store.touch_group("另一个bot:GroupMessage:123456789")
    ok, _umo, err = pplat.resolve_umo("123456789")
    rep.false("多平台同群号报歧义", ok)
    rep.contains("歧义提示列出候选", err, "另一个bot")

    rep.false("空输入被拒", pplat.resolve_umo("")[0])
    rep.false("无法识别的输入被拒", pplat.resolve_umo("乱写")[0])
    rep.false("段数不对被拒", pplat.resolve_umo("mybot:123")[0])

    # 取不到平台实例表时降级：只校验格式，不校验存在性
    pnoplat = make_plugin(module, tag="umo_noplat")
    pnoplat.context = SendContext()
    rep.check("无平台表时 list_platforms 返回空", pnoplat.list_platforms(), [])
    rep.true(
        "无平台表时完整 umo 仍通过（降级为只校验格式）",
        pnoplat.resolve_umo("任意实例:GroupMessage:1")[0],
    )

    vcv = module.validate_config_value
    rep.check("配置校验 bool 真", vcv({"type": "bool"}, True), (True, True, ""))
    rep.check("配置校验 bool 中文开", vcv({"type": "bool"}, "开"), (True, True, ""))
    rep.check("配置校验 bool 非法", vcv({"type": "bool"}, "maybe"), (False, None, "应为布尔值"))
    rep.check(
        "配置校验 int 钳制上限",
        vcv({"type": "int", "slider": {"min": 1, "max": 10}}, 99),
        (True, 10, ""),
    )
    rep.check(
        "配置校验 int 钳制下限",
        vcv({"type": "int", "slider": {"min": 5, "max": 10}}, 1),
        (True, 5, ""),
    )
    rep.false("配置校验 int 拒绝布尔", vcv({"type": "int"}, True)[0])
    rep.check(
        "配置校验 float",
        vcv({"type": "float", "slider": {"min": 1, "max": 20}}, 5.5),
        (True, 5.5, ""),
    )
    rep.false("配置校验 非法数字", vcv({"type": "int"}, "abc")[0])
    rep.check(
        "配置校验 枚举内合法",
        vcv({"type": "string", "options": ["a", "b"]}, "b"),
        (True, "b", ""),
    )
    rep.false("配置校验 枚举外拒绝", vcv({"type": "string", "options": ["a", "b"]}, "c")[0])
    rep.check("配置校验 list 归一", vcv({"type": "list"}, "a, b"), (True, ["a", "b"], ""))
    rep.false("配置校验 list 选项校验", vcv({"type": "list", "options": ["a"]}, ["a", "z"])[0])

    pweb = make_plugin(module, tag="webui", plugin_admins=["root"], send_mode="chain", media_mode="link")
    pweb.context = SendContext()
    pweb._register_web_api()
    rep.check("注册了 10 条 WebUI 路由", len(pweb.context.web_apis), 10)
    rep.check(
        "路由都带插件名前缀",
        [
            route
            for route, _methods, _desc in pweb.context.web_apis
            if not route.startswith(f"/{module.PLUGIN_NAME}/")
        ],
        [],
    )
    rep.check(
        "GET 2 条 / POST 8 条",
        sorted(m for _r, methods, _d in pweb.context.web_apis for m in methods),
        ["GET", "GET"] + ["POST"] * 8,
    )
    rep.check(
        "每条路由都有说明（desc 是必填第 4 参）",
        [r for r, _m, desc in pweb.context.web_apis if not desc],
        [],
    )

    # 打包守卫：AstrBot 扫 pages/<名>/index.html 发现页面，缺 index.html 时**静默跳过**，
    # 所以"页面文件是否随包存在"必须有断言守着，否则只能靠装完看不到来发现。
    for rel in (
        "pages/manage/index.html",
        "pages/manage/app.js",
        "pages/manage/style.css",
        ".astrbot-plugin/i18n/zh-CN.json",
        ".astrbot-plugin/i18n/en-US.json",
    ):
        rep.true(f"随包存在 {rel}", (PLUGIN_DIR / rel).is_file())
    rep.true("_page_index_path 指向真实存在的入口", pweb._page_index_path().is_file())
    i18n = json.loads(
        (PLUGIN_DIR / ".astrbot-plugin/i18n/zh-CN.json").read_text(encoding="utf-8")
    )
    rep.true(
        "i18n 声明了 pages.manage.title",
        bool(i18n.get("pages", {}).get("manage", {}).get("title")),
    )
    meta_text = (PLUGIN_DIR / "metadata.yaml").read_text(encoding="utf-8")
    rep.false(
        "metadata 未声明 pages/views（框架不消费这两个字段）",
        any(k in meta_text for k in ("pages:", "views:")),
    )

    # 页面脚本必须 type="module"：模块默认 defer，才会晚于宿主注入 SDK 执行。
    # 用普通 <script> 会在解析到那一行立刻执行，此时 window.AstrBotPluginPage 还是
    # undefined，表现为 "Cannot read properties of undefined (reading 'apiGet')"。
    page_html = (PLUGIN_DIR / "pages/manage/index.html").read_text(encoding="utf-8")
    rep.true(
        '页面脚本带 type="module"',
        'type="module"' in page_html and "app.js" in page_html,
    )
    app_js = (PLUGIN_DIR / "pages/manage/app.js").read_text(encoding="utf-8")
    rep.false(
        "app.js 不在顶层同步取 bridge",
        "const bridge = window.AstrBotPluginPage" in app_js,
    )
    rep.true("app.js 会等待 SDK 注入", "waitForBridge" in app_js)

    # 本次 bug 的根因守卫：前端**不得**硬编码平台前缀、也不得自行拼 umo。
    # umo 第一段是平台实例 id（如「mybot」），不是适配器名（aiocqhttp）。
    rep.false(
        "app.js 不硬编码平台前缀",
        "PLUGIN_UMO_PREFIX" in app_js or "aiocqhttp:GroupMessage:" in app_js,
    )
    rep.false("app.js 不自行拼 umo（改由后端 resolve_umo 解析）", "normalizeUmo" in app_js)
    rep.true("app.js 用 groupPicker 选择会话", "groupPicker" in app_js)

    # 账号表要能看出每个账号由哪个数据源供数，否则排障只能翻日志
    rep.true("app.js 账号表有数据源列", '"数据源"' in app_js)
    rep.true("app.js 渲染每账号的生效源", "author.source" in app_js)
    rep.true("app.js 展示降级告警", "provider_degraded" in app_js or "provider_chain" in app_js)

    # 前端守卫（都是"功能静默失效"类问题，必须固化）
    rep.true("手动模式支持私聊（FriendMessage）", "FriendMessage" in app_js)
    rep.true("写操作有防重复提交（withBusy）", "withBusy" in app_js)
    rep.true("SDK 缺失提示是粘性的（state.fatal）", "state.fatal" in app_js)
    rep.true("bridge.ready 有超时（Promise.race）", "Promise.race" in app_js)
    rep.true("配置渲染有代次守卫（renderSeq）", "renderSeq" in app_js)
    rep.true("配置读取有 in-flight 去重（configPromise）", "configPromise" in app_js)
    rep.true("modal 有可重入保护（modalOpen）", "modalOpen" in app_js)

    def call(handler, query=None, body=None):
        module.web_api.request = WebRequest(query=query, body=body)
        return run(handler())

    with_provider(pweb, FakeProvider(module, author=AUTHOR, posts=[]), module)
    pweb.store.upsert_author("44196397", "elonmusk", "Elon Musk")
    pweb.store.mark_baseline("44196397", 100)

    ov = call(pweb.api_overview)
    rep.true("overview 返回 dict", isinstance(ov, dict))
    rep.check("overview 含 stats", "stats" in ov, True)
    rep.check("overview 统计账号数", ov["stats"]["authors"], 1)
    rep.true("overview 含 groups/edges/authors 三段", all(k in ov for k in ("groups", "edges", "authors")))
    rep.true("overview 带数据源健康段", "provider_chain" in ov)
    rep.check(
        "overview 每个账号都带生效源字段",
        [a for a in ov["authors"] if "source" not in a],
        [],
    )

    cfg = call(pweb.api_config_get)
    rep.true("config 返回 schema", isinstance(cfg.get("schema"), dict))
    rep.check("config 的 schema 与 values 键一致", sorted(cfg["schema"]), sorted(cfg["values"]))

    ok_save = call(
        pweb.api_config_save,
        body={"values": {"poll_interval_minutes": 15, "filter_reposts": False}},
    )
    rep.check("配置保存条数", ok_save["count"], 2)
    rep.check("配置已生效", pweb.int_cfg("poll_interval_minutes", 5), 15)
    before = pweb.int_cfg("poll_interval_minutes", 5)
    bad_save = call(
        pweb.api_config_save,
        body={"values": {"poll_interval_minutes": 30, "send_mode": "bogus"}},
    )
    rep.true("非法配置返回错误", "error" in bad_save)
    rep.check("全有或全无：合法项也没落盘", pweb.int_cfg("poll_interval_minutes", 5), before)
    rep.true("未知配置项被拒", "error" in call(pweb.api_config_save, body={"values": {"nope": 1}}))
    rep.true("缺 values 被拒", "error" in call(pweb.api_config_save, body={}))
    call(pweb.api_config_save, body={"values": {"poll_interval_minutes": 999}})
    rep.check("越界值被钳制到上限", pweb.int_cfg("poll_interval_minutes", 5), 60)

    rep.true(
        "非法 umo 被拒",
        "error" in call(pweb.api_edge_add, body={"umo": "bad", "handle": "elonmusk"}),
    )
    rep.true(
        "缺 handle 被拒",
        "error" in call(pweb.api_edge_add, body={"umo": "aiocqhttp:GroupMessage:100"}),
    )
    added = call(
        pweb.api_edge_add,
        body={
            "umo": "aiocqhttp:GroupMessage:100",
            "handle": "elonmusk",
            "nickname": "马斯克",
            "media_only": True,
        },
    )
    rep.check("新增订阅 created=True", added["created"], True)
    edge_row = pweb.store.get_edge("aiocqhttp:GroupMessage:100", "44196397")
    rep.check("昵称已落库", edge_row["nickname"], "马斯克")
    rep.true("仅媒体已落库", module.edge_media_only(edge_row))
    rep.check(
        "重复新增 created=False",
        call(
            pweb.api_edge_add,
            body={"umo": "aiocqhttp:GroupMessage:100", "handle": "elonmusk"},
        )["created"],
        False,
    )
    with_provider(pweb, FakeProvider(module, author=None, posts=[]), module)
    rep.true(
        "找不到账号返回错误",
        "error" in call(
            pweb.api_edge_add,
            body={"umo": "aiocqhttp:GroupMessage:100", "handle": "nobody"},
        ),
    )

    eid = int(edge_row["edge_id"])
    upd = call(
        pweb.api_edge_update,
        body={"edge_id": eid, "nickname": "新名", "media_only": False, "enabled": False},
    )
    rep.check("更新返回变更字段", sorted(upd["changed"]), ["enabled", "media_only", "nickname"])
    rep.check("昵称已更新", upd["nickname"], "新名")
    rep.false("开关已更新", upd["enabled"])
    rep.false("仅媒体已更新", upd["media_only"])
    rep.true(
        "不存在的 edge 报错",
        "error" in call(pweb.api_edge_update, body={"edge_id": 99999, "enabled": True}),
    )
    rep.true(
        "无字段可更新时报错",
        "error" in call(pweb.api_edge_update, body={"edge_id": eid}),
    )

    rep.true(
        "非法时段被拒",
        "error" in call(
            pweb.api_group_dnd,
            body={"umo": "aiocqhttp:GroupMessage:100", "enabled": True, "ranges": ["乱写"]},
        ),
    )
    ok_dnd = call(
        pweb.api_group_dnd,
        body={
            "umo": "aiocqhttp:GroupMessage:100",
            "enabled": True,
            "ranges": ["23:00-08:00"],
        },
    )
    rep.check("免打扰已写入", ok_dnd["ranges"], ["23:00-08:00"])
    rep.check(
        "groups 表已落库",
        pweb.store.get_group("aiocqhttp:GroupMessage:100")["dnd_ranges"],
        "23:00-08:00",
    )

    pweb.context = SendContext()
    ts = call(pweb.api_test_send, body={"umo": "aiocqhttp:GroupMessage:100"})
    rep.check("WebUI 测试发送成功", ts["sent"], True)
    rep.true(
        "测试发送非法 umo 被拒",
        "error" in call(pweb.api_test_send, body={"umo": "bad"}),
    )

    with_provider(
        pweb,
        FakeProvider(module, author=AUTHOR, posts=[make_status(created=1700003000, text="hi")]),
        module,
    )
    rp = call(
        pweb.api_test_replay,
        body={"umo": "aiocqhttp:GroupMessage:100", "handle": "elonmusk", "count": 1},
    )
    rep.check("WebUI 回放成功", rp["ok"], True)
    rep.check("WebUI 回放条数", rp["total"], 1)
    rep.true(
        "WebUI 回放缺 handle 报错",
        "error" in call(pweb.api_test_replay, body={"umo": "aiocqhttp:GroupMessage:100"}),
    )

    rep.check("删除订阅", call(pweb.api_edge_remove, body={"edge_id": eid})["removed"], 1)
    pweb.store.add_edge("aiocqhttp:GroupMessage:100", "100", "44196397", "", 100)
    ar = call(pweb.api_author_remove, body={"author_id": "44196397"})
    rep.check("删账号连带删订阅", ar["edges"], 1)
    rep.check("删账号删掉账号行", ar["authors"], 1)
    rep.true("删账号后作者不存在", pweb.store.get_author("44196397") is None)
    rep.true("删账号后无残留订阅", len(pweb.store.list_edges("aiocqhttp:GroupMessage:100")) == 0)

    # =========================================================== 回归守卫
    rep.check("to_bool('false') 是 False（bool() 会是 True）", module.to_bool("false"), False)
    rep.check("to_bool('0') 是 False", module.to_bool("0"), False)
    rep.check("to_bool('关') 是 False", module.to_bool("关"), False)
    rep.check("to_bool('开') 是 True", module.to_bool("开"), True)
    rep.check("to_bool(True) 是 True", module.to_bool(True), True)
    rep.check("to_bool 未知值走默认", module.to_bool("???", True), True)

    pfix = make_plugin(module, tag="reviewfix", plugin_admins=["root"])
    pfix.context = SendContext(platform_ids=["mybot"])
    with_provider(pfix, FakeProvider(module, author=AUTHOR, posts=[]), module)
    pfix.store.upsert_author("44196397", "elonmusk", "Elon Musk")

    # 非 dict 的合法 JSON body 不得变成 500（body.get → AttributeError）
    for payload in ([1, 2], "x", 5):
        module.web_api.request = WebRequest(body=payload)
        resp = run(pfix.api_edge_add())
        rep.true(f"非 dict body({payload!r}) 被安全拒绝而非崩溃", "error" in resp)

    # 无效平台 id 不得写库
    module.web_api.request = WebRequest(
        body={"umo": "aiocqhttp:GroupMessage:1", "handle": "elonmusk"}
    )
    rep.true("无效平台 id 被拒", "error" in run(pfix.api_edge_add()))
    rep.check("被拒后没有建订阅", len(pfix.store.list_edges()), 0)

    # 重复新增必须**真的更新**，而不是回一句"已更新"却什么都没做
    module.web_api.request = WebRequest(
        body={"umo": "mybot:GroupMessage:1", "handle": "elonmusk", "nickname": "旧"}
    )
    first = run(pfix.api_edge_add())
    rep.check("首次新增 created=True", first["created"], True)
    module.web_api.request = WebRequest(
        body={
            "umo": "mybot:GroupMessage:1",
            "handle": "elonmusk",
            "nickname": "新",
            "media_only": True,
        }
    )
    second = run(pfix.api_edge_add())
    rep.check("重复新增 created=False", second["created"], False)
    rep.check("重复新增真的更新了昵称", second["updated"], True)
    rep.check(
        "昵称已落库",
        pfix.store.get_edge("mybot:GroupMessage:1", "44196397")["nickname"],
        "新",
    )
    rep.true(
        "仅媒体已落库",
        module.edge_media_only(pfix.store.get_edge("mybot:GroupMessage:1", "44196397")),
    )

    # 免打扰：启用写覆盖行，关闭删覆盖行（回落全局默认）
    module.web_api.request = WebRequest(
        body={"umo": "mybot:GroupMessage:1", "enabled": True, "ranges": ["23:00-08:00"]}
    )
    run(pfix.api_group_dnd())
    rep.true("启用后 groups 有覆盖行", pfix.store.get_group("mybot:GroupMessage:1") is not None)
    module.web_api.request = WebRequest(
        body={"umo": "mybot:GroupMessage:1", "enabled": False, "ranges": []}
    )
    off = run(pfix.api_group_dnd())
    rep.check("关闭 = 删除覆盖行", pfix.store.get_group("mybot:GroupMessage:1"), None)
    rep.true("关闭返回 cleared", off.get("cleared"))
    module.web_api.request = WebRequest(
        body={"umo": "mybot:GroupMessage:1", "enabled": True, "ranges": ["乱写", "23:00-08:00"]}
    )
    rep.true("部分时段非法 -> 整批拒绝", "error" in run(pfix.api_group_dnd()))
    rep.check("整批拒绝后没写库", pfix.store.get_group("mybot:GroupMessage:1"), None)

    module.web_api.request = WebRequest(body={"author_id": "nope"})
    rep.true("删不存在的账号返回错误", "error" in run(pfix.api_author_remove()))

    rep.false("非字符串 umo 被拒", pfix.resolve_umo({"a": 1})[0])
    rep.false("含空白的 umo 被拒", pfix.resolve_umo("mybot:Group Message:1")[0])

    # 删除订阅要连带删掉投递记录，否则队列数永久虚高且再也清不掉
    pfix.store.add_edge("mybot:GroupMessage:9", "9", "44196397", "", 0)
    pfix.store.commit_poll(
        "44196397",
        [
            {
                "post_id": "rv1",
                "created_ts": 200,
                "kind": "original",
                "body_json": "{}",
                "media_json": "[]",
                "has_media": 0,
            }
        ],
        list(pfix.store.list_edges()),
        200,
    )
    rep.check("删除前队列有 1 条", pfix.store.queue_depth(), 1)
    pfix.store.remove_edge("mybot:GroupMessage:9", "44196397")
    rep.check("删除订阅后队列归零（不留孤儿投递）", pfix.store.queue_depth(), 0)

    # 队首饥饿：免打扰会话不得挡住别的会话
    pfix.store.add_edge("mybot:GroupMessage:DND", "DND", "44196397", "", 0)
    pfix.store.add_edge("mybot:GroupMessage:LIVE", "LIVE", "44196397", "", 0)
    pfix.store.commit_poll(
        "44196397",
        [
            {
                "post_id": f"rv{i}",
                "created_ts": 300 + i,
                "kind": "original",
                "body_json": "{}",
                "media_json": "[]",
                "has_media": 0,
            }
            for i in range(1, 4)
        ],
        list(pfix.store.list_edges()),
        303,
    )
    rows_excl = pfix.store.next_queued(10, ["mybot:GroupMessage:DND"])
    rep.true(
        "next_queued(exclude_umos) 排除了免打扰会话",
        all(str(r["umo"]) != "mybot:GroupMessage:DND" for r in rows_excl),
    )
    rep.true("排除后仍能取到其它会话的待发消息", len(rows_excl) > 0)

    # 空 posts 也要推进订阅水位（否则 edges.enqueued_ts 永远落后）
    pfix.store.commit_poll("44196397", [], list(pfix.store.list_edges()), 999, False)
    rep.true(
        "空批次也把订阅水位推到已见位置",
        all(int(r["enqueued_ts"]) >= 999 for r in pfix.store.list_edges("mybot:GroupMessage:LIVE")),
    )

    # =========================================================== 结构性回归守卫
    import inspect as _inspect

    greedy_with_default = []
    registered = []
    non_async = []
    for name, fn in vars(module.XMonitorPlugin).items():
        if not callable(fn) or name.startswith("__"):
            continue
        try:
            sig = _inspect.signature(fn)
        except (TypeError, ValueError):
            continue
        for pname, param in sig.parameters.items():
            # CommandFilter 只在参数「没有默认值」时才把它认作 GreedyStr
            if param.annotation == "GreedyStr" and param.default is not _inspect.Parameter.empty:
                greedy_with_default.append(f"{name}.{pname}")
        if getattr(fn, "__astrbot_filter__", False):
            registered.append(name)
            # 命令处理器是 async 生成器（yield 结果），不是普通协程
            if not (_inspect.iscoroutinefunction(fn) or _inspect.isasyncgenfunction(fn)):
                non_async.append(name)
    rep.check("GreedyStr 参数一律不带默认值", greedy_with_default, [])
    rep.true("命令处理器数量 >= 17", len(registered) >= 17)
    rep.check("命令处理器都是异步的", non_async, [])
    rep.true("命令名互不重复", len(set(registered)) == len(registered))

    # /x帮助 的清单是硬编码的，最容易漏项（它自己就曾漏掉）——
    # 用实际注册的指令名反向校验，漏一个就红。
    command_names = sorted(
        getattr(fn, "__astrbot_command__", "")
        for fn in vars(module.XMonitorPlugin).values()
        if callable(fn) and getattr(fn, "__astrbot_filter__", False)
    )
    help_out = drive(ph.cmd_help(FakeEvent(uid="root", gid="100")))
    help_text = help_out[0]
    rep.check(
        "帮助清单覆盖全部已注册指令",
        [name for name in command_names if name and f"/{name}" not in help_text],
        [],
    )
    rep.check("帮助清单里的指令名都真实存在", sorted(set(command_names)), command_names)
    rep.check(
        "群模式常量与 schema 选项一致",
        sorted(module.VALID_MODES),
        ["none", "readonly", "whitelist"],
    )

    schema = json.loads((PLUGIN_DIR / "_conf_schema.json").read_text(encoding="utf-8"))
    rep.check("schema 每个键都有 description", [k for k, v in schema.items() if not v.get("description")], [])
    rep.check(
        "枚举型配置的 default 都在 options 内",
        [
            k
            for k, v in schema.items()
            if "options" in v
            and (
                any(item not in v["options"] for item in v["default"])
                if isinstance(v.get("default"), list)
                else v.get("default") not in v["options"]
            )
        ],
        [],
    )
    rep.check(
        "labels 与 options 长度一致",
        [k for k, v in schema.items() if "labels" in v and len(v["labels"]) != len(v["options"])],
        [],
    )

    # ---- 配置漂移与版本一致性（这两类问题都是"发布后才发现"的典型）
    source_text = (PLUGIN_DIR / "main.py").read_text(encoding="utf-8")
    referenced = sorted(
        set(
            re.findall(
                r'(?:self\.)?(?:cfg|bool_cfg|int_cfg|float_cfg|config_getter|bool_getter'
                r'|int_getter|float_getter)\(\s*["\']([a-z0-9_]+)["\']',
                source_text,
            )
        )
    )
    rep.check("代码用到的配置键都已在 schema 声明", [k for k in referenced if k not in schema], [])
    rep.check("schema 声明的配置键都真的被用到（无死配置）", [k for k in schema if k not in referenced], [])
    rep.true("代码里至少引用了 40 个配置键（正则没跑偏）", len(referenced) >= 40)

    meta_text = (PLUGIN_DIR / "metadata.yaml").read_text(encoding="utf-8")
    meta_version = re.search(r"^version:\s*(\S+)", meta_text, re.M)
    rep.true("metadata.yaml 声明了 version", bool(meta_version))
    rep.check("PLUGIN_VERSION 与 metadata.yaml 一致", module.PLUGIN_VERSION, meta_version.group(1))
    rep.true(
        "README 变更记录里有当前版本",
        f"### v{module.PLUGIN_VERSION}" in (PLUGIN_DIR / "README.md").read_text(encoding="utf-8"),
    )
    rep.check(
        "数据源常量与 schema 可选值一致",
        sorted(module.PROVIDER_NAMES),
        sorted(schema["fetch_chain"]["options"]),
    )
    rep.check(
        "默认降级链只由已知源组成",
        [n for n in module.DEFAULT_FETCH_CHAIN if n not in module.PROVIDER_NAMES],
        [],
    )

    # 逐个名字真的能被装配出来（防止"名字常量加了但 build 分支忘了写"）
    class _AnyClient:
        async def get(self, url, **kwargs):  # pragma: no cover - 只用于装配
            raise OSError("offline")

    built_names = set()
    for provider_name in module.PROVIDER_NAMES:
        built = module.build_provider_chain(
            _AnyClient(),
            lambda key, default=None, _n=provider_name: (
                [_n] if key == "fetch_chain" else default
            ),
            lambda key, default=0: default,
            lambda key, default=0.0: default,
        )
        built_names |= set(built.names) if built else set()
    rep.check("每个声明过的源都能被装配出来", sorted(built_names), sorted(module.PROVIDER_NAMES))

    print()
    print("提示：TMP_ROOT =", TMP_ROOT)
    return rep.finish()

if __name__ == "__main__":
    # 中断必须**看得见**：以前异常会让脚本直接 traceback 退出，后半段断言静默不跑、
    # 也没有汇总行，只看退出码容易误判成"全绿"。
    try:
        code = main()
    except SystemExit:
        raise
    except BaseException:
        import traceback

        traceback.print_exc()
        print()
        print("FAILED: 自检在跑到汇总之前就中断了（见上面的 traceback）；")
        print("        中断点之后的断言**一条都没有执行**，不要当成通过。")
        raise SystemExit(2)
    raise SystemExit(code)
