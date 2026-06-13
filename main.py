"""
rate-limiter: 消息流速限流插件
解决高活跃群聊中 token 消耗过快的问题
"""

import time
from collections import defaultdict, deque

from core.plugin import BasePlugin, PluginContext, on, Priority, logger
from core.provider import LLMRequest
from core.prompt_manager import Prompt
from core.chat.message_utils import KiraMessageEvent, KiraMessageBatchEvent, MessageChain
from core.chat.message_elements import Text


class RateLimiterPlugin(BasePlugin):

    def __init__(self, ctx: PluginContext, cfg: dict):
        super().__init__(ctx, cfg)
        # 每个 session 的统计数据
        self.session_stats: dict = defaultdict(lambda: {
            "timestamps": deque(),       # 消息到达时间戳
            "wakeups": deque(),          # LLM 唤醒时间戳
            "cooldown_until": None,      # 冷却结束时间
            "dropped_count": 0,          # 冷却期间丢弃的消息数
            "queued_mentions": [],       # 冷却期间排队的被提及消息
        })

    async def initialize(self):
        """插件加载时初始化"""
        self.max_messages = self.plugin_cfg.get("max_messages_per_minute", 15)
        self.max_wakeups = self.plugin_cfg.get("max_wakeups_per_minute", 8)
        self.cooldown_seconds = self.plugin_cfg.get("cooldown_seconds", 300)
        self.queue_mentions = self.plugin_cfg.get("queue_mentions", True)
        # cooldown_reply fields are nested under the "cooldown_reply" section
        cooldown_reply_cfg = self.plugin_cfg.get("cooldown_reply", {})
        self.cooldown_reply_enabled = cooldown_reply_cfg.get("cooldown_reply_enabled", False)
        self.cooldown_reply_text = cooldown_reply_cfg.get("cooldown_reply_text", "我现在比较忙，请稍后再找我~")
        enabled = self.plugin_cfg.get("enabled_sessions", [])
        self.enabled_sessions = set(enabled) if enabled else None  # None 表示全部生效
        # owner_ids 在 schema.json 中是 list 类型
        owner_ids = self.plugin_cfg.get("owner_ids", [])
        self.owner_ids = set(str(uid) for uid in owner_ids) if owner_ids else set()
        logger.info(
            f"[RateLimiter] Initialized | max_msg={self.max_messages}/min, "
            f"max_wakeup={self.max_wakeups}/min, cooldown={self.cooldown_seconds}s, "
            f"sessions={'all' if self.enabled_sessions is None else len(self.enabled_sessions)}, "
            f"owners={len(self.owner_ids)}"
        )

    async def terminate(self):
        """插件卸载时清理"""
        self.session_stats.clear()

    # ----------------------------------------------------------------
    # Hook: 拦截 IM 消息，进行限流判断
    # 优先级 HIGH 确保在默认消息处理之前执行
    # ----------------------------------------------------------------
    @on.im_message(priority=Priority.HIGH + 5)
    async def rate_limit_message(self, event: KiraMessageEvent, *args, **kwargs):
        try:
            await self._rate_limit_message_impl(event)
        except Exception as e:
            logger.error(f"[RateLimiter] Unhandled exception in rate_limit_message: {e}", exc_info=True)

    async def _rate_limit_message_impl(self, event: KiraMessageEvent):
        session_id = event.session.sid

        # 仅对指定会话生效
        if self.enabled_sessions is not None and session_id not in self.enabled_sessions:
            return

        sender = getattr(event.message, "sender", None)
        sender_id = str(sender.user_id) if sender and hasattr(sender, "user_id") else None
        now = time.time()

        # 主人绕过限流
        if sender_id and sender_id in self.owner_ids:
            logger.debug(f"[RateLimiter] Owner bypass: {sender_id} in {session_id}")
            return  # 继续正常流程

        stats = self.session_stats[session_id]

        # 清理过期记录（滑动窗口 60 秒）
        self._clean_window(stats, now)

        # Check cooldown
        logger.debug(f"[RateLimiter] Cooldown check: until={stats['cooldown_until']}, now={now}, dropped={stats['dropped_count']}")
        if stats["cooldown_until"] and now < stats["cooldown_until"]:
            stats["dropped_count"] += 1
            is_mentioned = bool(getattr(event, "is_mentioned", False))
            logger.debug(f"[RateLimiter] In cooldown block: is_mentioned={is_mentioned}, queue_mentions={self.queue_mentions}")
            if self.queue_mentions and is_mentioned:
                stats["queued_mentions"].append(event)
                logger.info(f"[RateLimiter] Mention queued during cooldown: {session_id}")
            event.discard(force=True)
            if self.cooldown_reply_enabled and is_mentioned:
                try:
                    chain = MessageChain([Text(self.cooldown_reply_text)])
                    await self.ctx.message_processor.send_message_chain(session_id, chain)
                    logger.debug(f"[RateLimiter] Cooldown reply sent to {session_id}")
                except Exception as e:
                    logger.error(f"[RateLimiter] Failed to send cooldown reply to {session_id}: {e}")
            return

        # 记录消息时间戳
        stats["timestamps"].append(now)

        # 检查是否超限
        if len(stats["timestamps"]) > self.max_messages:
            stats["cooldown_until"] = now + self.cooldown_seconds
            stats["dropped_count"] = 0
            stats["timestamps"].clear()
            await self._clear_session_buffer(session_id)
            event.discard(force=True)
            logger.warning(f"[RateLimiter] Message rate exceeded in {session_id}, entering cooldown for {self.cooldown_seconds}s")

    # ----------------------------------------------------------------
    # Hook: LLM 请求前注入限流上下文
    # 冷却结束后提醒 AI 有消息被丢弃
    # ----------------------------------------------------------------
    @on.llm_request()
    async def inject_rate_limit_context(self, event: KiraMessageBatchEvent, req: LLMRequest, tag_set, *args, **kwargs):
        session_id = event.sid
        if not session_id:
            return

        # 仅对指定会话生效
        if self.enabled_sessions is not None and session_id not in self.enabled_sessions:
            return

        stats = self.session_stats.get(session_id)
        if not stats:
            return

        now = time.time()

        # 清理过期唤醒记录
        while stats["wakeups"] and now - stats["wakeups"][0] > 60:
            stats["wakeups"].popleft()

        # 刚从冷却期恢复：注入提示词
        if (
            stats["cooldown_until"]
            and now >= stats["cooldown_until"]
            and now - stats["cooldown_until"] < 120  # 恢复后 2 分钟内注入
            and stats["dropped_count"] > 0
        ):
            dropped = stats["dropped_count"]
            queued = len(stats["queued_mentions"])
            notice = (
                f"[限流提示] 刚刚群聊过于活跃，有 {dropped} 条消息因限流被丢弃"
                f"（冷却 {self.cooldown_seconds} 秒）。"
            )
            if queued > 0:
                notice += f"其中 {queued} 条提及你的消息已排队，以下是摘要：\n"
                for evt in stats["queued_mentions"]:
                    nickname = getattr(evt.message.sender, "nickname", "未知用户")
                    notice += f"- [{nickname}]: {evt.message_repr}\n"
            notice += "请简要回应近期话题，不要逐条回复。"

            req.user_prompt.append(Prompt(
                content=notice,
                name="rate_limiter_notice",
                source="rate-limiter",
            ))

            # 重置计数
            stats["dropped_count"] = 0
            stats["queued_mentions"].clear()
            stats["cooldown_until"] = None
            stats["wakeups"].clear()  # 恢复时清空唤醒计数，避免立即再次触发
            logger.info(f"[RateLimiter] Cooldown recovered for {session_id}, injected {dropped} dropped / {queued} queued notice")

        # Cooldown active: block LLM request and send cooldown reply if mentioned
        if stats["cooldown_until"] and now < stats["cooldown_until"]:
            stats["dropped_count"] += 1
            event.stop()
            await self._clear_session_buffer(session_id)
            if self.cooldown_reply_enabled:
                any_mentioned = any(getattr(m, "is_mentioned", False) for m in event.messages)
                if any_mentioned:
                    try:
                        chain = MessageChain([Text(self.cooldown_reply_text)])
                        await self.ctx.message_processor.send_message_chain(session_id, chain)
                        logger.debug(f"[RateLimiter] Cooldown reply sent to {session_id} (via llm_request)")
                    except Exception as e:
                        logger.error(f"[RateLimiter] Failed to send cooldown reply to {session_id}: {e}")
            return

        # 记录唤醒时间戳
        stats["wakeups"].append(now)

        # 检查唤醒次数是否也超限
        if len(stats["wakeups"]) > self.max_wakeups:
            stats["cooldown_until"] = now + self.cooldown_seconds
            stats["dropped_count"] = 0
            stats["wakeups"].clear()
            await self._clear_session_buffer(session_id)
            event.stop()
            logger.warning(f"[RateLimiter] Wakeup rate exceeded in {session_id}, entering cooldown for {self.cooldown_seconds}s, LLM request stopped")
            return

    # ----------------------------------------------------------------
    # 辅助方法
    # ----------------------------------------------------------------
    @staticmethod
    def _clean_window(stats: dict, now: float):
        """清理滑动窗口中过期的时间戳"""
        while stats["timestamps"] and now - stats["timestamps"][0] > 60:
            stats["timestamps"].popleft()
        while stats["wakeups"] and now - stats["wakeups"][0] > 60:
            stats["wakeups"].popleft()

    async def _clear_session_buffer(self, session_id: str):
        """清空会话缓冲区，防止 debounce 循环刷新旧消息"""
        try:
            buffer = self.ctx.message_processor.session_buffer.get_buffer(session_id)
            buffer.flush()
        except Exception:
            pass

