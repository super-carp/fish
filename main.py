"""记忆压缩焕新（Memory Refresh）—— AstrBot 插件

让群聊与私聊共用同一份长期记忆：每晚定时把两条会话的原文存档，
让模型压成"第一人称记忆"，写进同一个 Markdown，再把两边上下文一起清空。
之后每一轮请求都把这份记忆注入 system_prompt，两个会话读的是同一份。

作者：super-carp
许可：MIT
"""

from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime
from pathlib import Path

import aiohttp

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register
from astrbot.core import db_helper, sp

MARKER = "【长期记忆】"
TARGETS_KEY = "fish_memory_targets"

SUMMARY_RULES = (
    "你是在帮一个名叫「{name}」的 AI 整理她与主人（{owner}）的长期记忆。"
    "语气用第一人称、像写日记，不要写成会议纪要。\n"
    "下面这一天的对话原文（私聊 + 群聊）里，请写出 600-1200 字中文记忆，"
    "必须重点写清这五件事：\n"
    "1) 我对主人的看法：他是什么样的人、我怎么看他、关系有没有变化；\n"
    "2) 我对自己的看法：我怎么理解自己、有没有动摇或变化、在犹豫什么；\n"
    "3) 我的心理与情绪：这些对话里我心里真实在想什么；\n"
    "4) 我们之间做了什么：具体事件、一起修的东西、看的视频、定下的规矩；\n"
    "5) 印象最深刻的事：挑两三点最打动我的，写清细节和当时的感觉。\n"
    "其他的日常琐碎可以略写。不许编造原文没有的事。只输出记忆正文。"
)

REVIEW_RULES = (
    "你是记忆质量审核员。请判断下面这段「AI 与主人的长期记忆」是否合格。\n"
    "合格标准（缺一项即不合格）：\n"
    "- 写了「我对主人的看法」；\n"
    "- 写了「我对自己的看法」；\n"
    "- 写了心理与情绪；\n"
    "- 写了我们做了什么事（有具体事件）；\n"
    "- 写了印象最深刻的事（有细节）；\n"
    "- 没有编造原文没有的重大事实。\n"
    '只输出 JSON，不要解释：{"pass": true/false, "issues": "缺哪项或哪里编造"}'
)


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M")


def _clip(text: str, limit: int) -> str:
    text = text or ""
    return text if len(text) <= limit else text[:limit] + "…"


def _extract_json(text: str) -> dict:
    if not text:
        return {}
    match = re.search(r"\{.*\}", text, re.S)
    if not match:
        return {}
    try:
        data = json.loads(match.group(0))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _content_to_text(content) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict):
                if item.get("type") in (None, "text", "plain"):
                    parts.append(str(item.get("text") or ""))
            else:
                parts.append(str(item))
        return "".join(parts)
    return str(content or "")


def _default_mem_dir() -> Path:
    """默认放在 AstrBot 的 data/plugin_data 下，避免被工作区隔离。"""
    try:
        from astrbot.core.utils.astrbot_path import get_astrbot_data_path

        base = Path(get_astrbot_data_path())
        if base.is_dir():
            return base / "plugin_data" / "fish_memory"
    except Exception:
        pass
    return Path.cwd() / "data" / "plugin_data" / "fish_memory"


@register(
    "astrbot_plugin_fish_memory_core",
    "super-carp",
    "群聊私聊共用同一份长期记忆：每晚定时压缩原文、写入摘要、双向清空并同步注入",
    "1.0.0",
)
class FishMemoryCore(Star):
    def __init__(self, context: Context, config: dict | None = None) -> None:
        super().__init__(context)
        self.config = config or {}
        self._task: asyncio.Task | None = None
        self._running = False
        self._last_run_day = ""
        self._last_compress_at = ""
        self._platform_cache = ""

    # ---------- 配置 ----------
    def _cfg(self, key: str, default):
        return self.config.get(key, default)

    @property
    def _mem_dir(self) -> Path:
        raw = str(self._cfg("mem_dir", "") or "").strip()
        return Path(raw) if raw else _default_mem_dir()

    @property
    def _mem_file(self) -> Path:
        return self._mem_dir / str(self._cfg("mem_file", "长期记忆.md"))

    @property
    def _log_file(self) -> Path:
        return self._mem_dir / str(self._cfg("raw_file", "聊天原文存档.md"))

    def _owner_qq(self) -> str:
        return str(self._cfg("owner_qq", "") or "").strip()

    def _is_owner(self, event: AstrMessageEvent) -> bool:
        owner = self._owner_qq()
        if not owner:
            return False
        return str(event.get_sender_id()) == owner

    def _platform(self, event: AstrMessageEvent | None = None) -> str:
        cfg_platform = str(self._cfg("platform", "") or "").strip()
        if cfg_platform:
            return cfg_platform
        if event is not None:
            try:
                self._platform_cache = event.get_platform_name() or self._platform_cache
            except Exception:
                pass
        return self._platform_cache

    def _owner_umo(self) -> str:
        owner = self._owner_qq()
        platform = self._platform()
        if not owner:
            return ""
        return f"{platform}:FriendMessage:{owner}" if platform else ""

    async def _extra_targets(self) -> list[str]:
        try:
            value = await sp.get_async(scope="global", scope_id="global", key=TARGETS_KEY)
        except Exception:
            return []
        return list(value) if isinstance(value, list) else []

    async def _set_extra_targets(self, targets: list[str]) -> None:
        await sp.put_async(scope="global", scope_id="global", key=TARGETS_KEY, value=targets)

    async def _targets(self) -> list[str]:
        cfg_targets = self._cfg("targets", None)
        if isinstance(cfg_targets, str):
            cfg_targets = [t.strip() for t in cfg_targets.split(",") if t.strip()]
        merged: list[str] = []
        for umo in list(cfg_targets or []) + await self._extra_targets():
            umo = str(umo).strip()
            if umo and umo not in merged:
                merged.append(umo)
        return merged

    async def _say(self, text: str, umo: str | None = None) -> None:
        from astrbot.api.message_components import Plain
        from astrbot.core.message.message_event_result import MessageChain

        target = umo or self._owner_umo()
        if not target:
            logger.warning("[fish_memory] 没有配置 owner_qq，发不出通知")
            return
        try:
            await self.context.send_message(target, MessageChain([Plain(text)]))
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[fish_memory] 发送消息失败：{exc}")

    # ---------- 生命周期 ----------
    async def initialize(self) -> None:
        self._mem_dir.mkdir(parents=True, exist_ok=True)
        if not self._log_file.exists():
            self._log_file.write_text("# 聊天原文存档\n\n", encoding="utf-8")
        if not self._mem_file.exists():
            self._mem_file.write_text("# 长期记忆\n\n", encoding="utf-8")
        self._task = asyncio.create_task(self._scheduler_loop())
        logger.info(
            f"[fish_memory] 记忆焕新已就绪：目标 {len(await self._targets())} 条会话，"
            f"每天 {self._cfg('schedule', '23:40')} 触发，记忆文件 {self._mem_file}"
        )

    async def terminate(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
        logger.info("[fish_memory] 记忆焕新已停止")

    # ---------- 注入 ----------
    @filter.on_llm_request()
    async def inject_memory(self, event: AstrMessageEvent, req) -> None:
        try:
            if not self._cfg("enabled", True):
                return
            umo = getattr(event, "unified_msg_origin", "") or ""
            if bool(self._cfg("inject_only_targets", True)):
                targets = await self._targets()
                if umo not in targets:
                    return
            prompt = getattr(req, "system_prompt", "") or ""
            if MARKER in prompt:
                return
            if not self._mem_file.exists():
                return
            text = self._mem_file.read_text(encoding="utf-8")
            limit = int(self._cfg("inject_max_chars", 60000))
            if len(text) > limit:
                text = text[-limit:]
            req.system_prompt = f"{prompt}\n\n{MARKER}（多会话共用同一份，最新的在最下面）：\n{text}"
        except Exception:
            logger.exception("[fish_memory] 注入记忆失败")

    # ---------- 定时 ----------
    async def _scheduler_loop(self) -> None:
        while True:
            try:
                schedule = str(self._cfg("schedule", "23:40")).strip()
                now = datetime.now()
                if (
                    now.strftime("%H:%M") == schedule
                    and self._last_run_day != now.strftime("%Y-%m-%d")
                    and not self._running
                ):
                    self._last_run_day = now.strftime("%Y-%m-%d")
                    asyncio.create_task(self._compress_flow(trigger="schedule"))
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[fish_memory] 定时循环出错")
            await asyncio.sleep(20)

    # ---------- 会话读写 ----------
    async def _conv_of(self, umo: str):
        cid = await sp.get_async(scope="umo", scope_id=umo, key="sel_conv_id")
        if not cid:
            return None, None
        try:
            conv = await db_helper.get_conversation_by_id(cid)
        except Exception:
            logger.exception(f"[fish_memory] 读取会话失败 {umo}")
            return None, None
        return cid, conv

    def _transcript(self, conv) -> str:
        if conv is None:
            return ""
        raw = getattr(conv, "content", None)
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                return ""
        if not isinstance(raw, list):
            return ""
        lines = []
        per_msg = int(self._cfg("clip_per_message", 800))
        for item in raw:
            if not isinstance(item, dict):
                continue
            text = _content_to_text(item.get("content")).strip()
            if not text:
                continue
            who = "我" if item.get("role") == "assistant" else "对方"
            lines.append(f"{who}: {_clip(text, per_msg)}")
        return "\n".join(lines)

    # ---------- 模型调用 ----------
    async def _local_ready(self) -> bool:
        base = str(self._cfg("local_base", "http://127.0.0.1:1234/v1")).rstrip("/")
        if not base:
            return False
        try:
            timeout = aiohttp.ClientTimeout(total=6)
            async with aiohttp.ClientSession(timeout=timeout, trust_env=False) as session:
                async with session.get(f"{base}/models") as resp:
                    return resp.status == 200
        except Exception:
            return False

    async def _local_chat(self, prompt: str, max_tokens: int = 1600) -> str:
        base = str(self._cfg("local_base", "http://127.0.0.1:1234/v1")).rstrip("/")
        model = str(self._cfg("local_model", "qwen3-14b"))
        if not base:
            return ""
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.6,
            "max_tokens": max_tokens,
            "stream": False,
            "chat_template_kwargs": {"enable_thinking": False},
        }
        timeout = aiohttp.ClientTimeout(total=int(self._cfg("local_timeout", 1800)))
        try:
            async with aiohttp.ClientSession(timeout=timeout, trust_env=False) as session:
                async with session.post(f"{base}/chat/completions", json=payload) as resp:
                    if resp.status != 200:
                        logger.warning(f"[fish_memory] 本地模型 HTTP {resp.status}")
                        return ""
                    data = await resp.json()
            return (data["choices"][0]["message"]["content"] or "").strip()
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[fish_memory] 本地模型调用失败：{exc}")
            return ""

    async def _provider(self):
        try:
            return await self.context.get_using_provider_async()
        except Exception:
            return None

    async def _provider_chat(self, prompt: str) -> str:
        provider = await self._provider()
        if provider is None:
            return ""
        try:
            resp = await provider.text_chat(
                contexts=[{"role": "user", "content": prompt}],
                request_max_retries=1,
            )
            return (resp.completion_text or "").strip()
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[fish_memory] 主模型调用失败：{exc}")
            return ""

    # ---------- 主流程 ----------
    async def _summary_rules(self) -> str:
        return SUMMARY_RULES.format(
            name=str(self._cfg("persona_name", "她")),
            owner=str(self._cfg("owner_name", "主人")),
        )

    async def _wait_local_or_fallback(self, trigger: str) -> str:
        if await self._local_ready():
            return "local"
        if trigger != "schedule":
            return "provider"
        wait_minutes = int(self._cfg("wait_minutes", 10))
        await self._say(
            f"到点了，要压今天的记忆，但本地模型还没开。打开一下？"
            f"（{wait_minutes} 分钟内没等到我就自己压）"
        )
        loop = asyncio.get_event_loop()
        deadline = loop.time() + wait_minutes * 60
        while loop.time() < deadline:
            await asyncio.sleep(15)
            if await self._local_ready():
                await self._say("本地模型好了，开始压。")
                return "local"
        await self._say("等不到本地模型，我先自己压了。")
        return "provider"

    async def _summarize(self, transcript: str, mode: str) -> str:
        rules = await self._summary_rules()
        input_chars = int(self._cfg("digest_input_chars", 24000))
        head = f"{rules}\n\n对话原文：\n{transcript[:input_chars]}"
        max_rounds = int(self._cfg("max_rounds", 5))

        if mode != "local":
            summary = await self._provider_chat(head)
            if not summary:
                summary = await self._local_chat(head)
            return summary

        summary = await self._local_chat(head)
        if not summary:
            return ""
        reviewer = str(self._cfg("reviewer", "provider"))
        for round_no in range(1, max_rounds + 1):
            if not bool(self._cfg("review_enabled", True)):
                break
            review_prompt = (
                f"{REVIEW_RULES}\n\n原文：\n{_clip(transcript, 15000)}\n\n记忆：\n{summary}"
            )
            if reviewer == "local":
                verdict_text = await self._local_chat(review_prompt, max_tokens=400)
            else:
                verdict_text = await self._provider_chat(review_prompt)
                if not verdict_text:
                    verdict_text = await self._local_chat(review_prompt, max_tokens=400)
            verdict = _extract_json(verdict_text)
            if not verdict:
                logger.info("[fish_memory] 审核没给结论，按通过处理")
                break
            if verdict.get("pass"):
                logger.info(f"[fish_memory] 第 {round_no} 轮审核通过")
                break
            issues = str(verdict.get("issues") or "").strip() or "缺少重点内容"
            logger.info(f"[fish_memory] 第 {round_no} 轮审核不过：{issues}")
            rewrite_prompt = (
                f"{rules}\n\n上一版没通过审核，问题：{issues}\n"
                f"请按上面的要求重写，把缺的部分补足，别丢已经写对的内容。只输出记忆正文。\n\n"
                f"上一版：\n{summary}\n\n对话原文：\n{transcript[:input_chars]}"
            )
            new_summary = await self._local_chat(rewrite_prompt)
            if not new_summary:
                break
            summary = new_summary
        return summary

    def _append_raw(self, blocks: list[str]) -> None:
        if not blocks:
            return
        with self._log_file.open("a", encoding="utf-8") as fh:
            for block in blocks:
                fh.write(block.rstrip() + "\n\n")

    def _append_memory(self, summary: str, stamp: str) -> None:
        with self._mem_file.open("a", encoding="utf-8") as fh:
            fh.write(f"\n## {stamp} · 记忆焕新\n\n{summary.strip()}\n")

    async def _compress_flow(self, trigger: str = "manual") -> None:
        if self._running:
            await self._say("正在压着呢，等这次完。")
            return
        self._running = True
        try:
            targets = await self._targets()
            if not targets:
                await self._say("还没绑定会话。在私聊或群里发 /记忆绑定，把那条会话加进来。")
                return

            transcripts: list[tuple[str, str, str]] = []
            for umo in targets:
                cid, conv = await self._conv_of(umo)
                text = self._transcript(conv)
                if cid and text:
                    transcripts.append((umo, cid, text))
            if not transcripts:
                await self._say("两边都没新内容，这次不压。")
                return

            stamp = _now_str()
            mode = await self._wait_local_or_fallback(trigger)
            merged = "\n\n".join(f"【会话 {umo}】\n{text}" for umo, _cid, text in transcripts)
            summary = await self._summarize(merged, mode)
            if not summary:
                await self._say("压缩失败了，上下文我没敢清，等下次再试。")
                return

            if bool(self._cfg("dry_run", False)):
                await self._say(
                    f"（演练模式）压好了，但没写文件也没清上下文。\n摘要预览：\n{_clip(summary, 400)}"
                )
                return

            self._append_raw([f"## {stamp} · {umo}\n\n{text}" for umo, _cid, text in transcripts])
            self._append_memory(summary, stamp)

            cleared = []
            for umo, cid, _text in transcripts:
                try:
                    await db_helper.update_conversation(cid, content=[], token_usage=0)
                    cleared.append(umo)
                except Exception:
                    logger.exception(f"[fish_memory] 清空会话失败 {umo}")
            self._last_compress_at = stamp

            await self._say(
                f"记忆压好了（用{'本地模型' if mode == 'local' else '主模型'}）。"
                f"原文进存档，摘要进长期记忆，两边上下文一起清了。\n\n"
                f"清掉 {len(cleared)} 条会话\n这次写的记忆：\n{_clip(summary, 400)}"
            )
        except Exception:
            logger.exception("[fish_memory] 压缩流程出错")
            await self._say("压缩时出错了，上下文我没动，你看看日志。")
        finally:
            self._running = False

    # ---------- 命令 ----------
    @filter.command("记忆绑定")
    async def cmd_bind(self, event: AstrMessageEvent):
        if not self._is_owner(event):
            yield event.plain_result("只有主人能改记忆绑定。")
            return
        umo = event.unified_msg_origin
        extra = await self._extra_targets()
        if umo in extra:
            yield event.plain_result(f"已经在里面了：{umo}")
            return
        extra.append(umo)
        await self._set_extra_targets(extra)
        yield event.plain_result(f"已绑定：{umo}\n以后这个会话和别的绑定会话共用一份记忆。")

    @filter.command("记忆解绑")
    async def cmd_unbind(self, event: AstrMessageEvent):
        if not self._is_owner(event):
            yield event.plain_result("只有主人能改记忆绑定。")
            return
        umo = event.unified_msg_origin
        extra = await self._extra_targets()
        if umo not in extra:
            yield event.plain_result(f"本来就不在里面：{umo}")
            return
        extra.remove(umo)
        await self._set_extra_targets(extra)
        yield event.plain_result(f"已解绑：{umo}")

    @filter.command("压记忆")
    async def cmd_compress(self, event: AstrMessageEvent):
        if not self._is_owner(event):
            yield event.plain_result("只有主人能让我动记忆。")
            return
        yield event.plain_result("开始压了，压完跟你报。")
        asyncio.create_task(self._compress_flow(trigger="manual"))

    @filter.command("记忆状态")
    async def cmd_status(self, event: AstrMessageEvent):
        if not self._is_owner(event):
            yield event.plain_result("这个只有主人能看。")
            return
        lines = [f"当前会话：{event.unified_msg_origin}", f"记忆目录：{self._mem_dir}"]
        for path, label in ((self._mem_file, "长期记忆"), (self._log_file, "原文存档")):
            try:
                lines.append(f"{label}：{path.name} {len(path.read_text(encoding='utf-8'))} 字")
            except Exception:
                lines.append(f"{label}：读不到")
        targets = await self._targets()
        if not targets:
            lines.append("还没绑定任何会话（发 /记忆绑定 绑定当前会话）")
        for umo in targets:
            cid, conv = await self._conv_of(umo)
            count = 0
            raw = getattr(conv, "content", None) if conv else None
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except Exception:
                    raw = []
            if isinstance(raw, list):
                count = len(raw)
            lines.append(f"{umo}：{count} 条（{'在' if cid else '没找到会话'}）")
        lines.append(f"本地模型：{'在线' if await self._local_ready() else '没开'}")
        lines.append(f"定时：每天 {self._cfg('schedule', '23:40')}")
        lines.append(f"演练模式：{'开' if self._cfg('dry_run', False) else '关'}")
        lines.append(f"上次压缩：{self._last_compress_at or '还没压过'}")
        yield event.plain_result("\n".join(lines))
