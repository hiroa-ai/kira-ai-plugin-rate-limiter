"""
rate-limiter: 消息流速限流插件
解决高活跃群聊中 token 消耗过快的问题
"""

import time
from collections import defaultdict, deque

from core.plugin import BasePlugin, PluginContext, on, Priority, register
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
        self.cooldown_reply_enabled = self.plugin_cfg.get("cooldown_reply_enabled", False)
        self.cooldown_reply_text = self.plugin_cfg.get("cooldown_reply_text", "我现在比较忙，请稍后再找我~")
        enabled = self.plugin_cfg.get("enabled_sessions", [])
        self.enabled_sessions = set(enabled) if enabled else None  # None 表示全部生效
        # owner_ids 在 schema.json 中是 list 类型
        owner_ids = self.plugin_cfg.get("owner_ids", [])
        self.owner_ids = set(str(uid) for uid in owner_ids) if owner_ids else set()

    async def terminate(self):
        """插件卸载时清理"""
        self.session_stats.clear()

    # ----------------------------------------------------------------
    # Hook: 拦截 IM 消息，进行限流判断
    # 优先级 HIGH 确保在默认消息处理之前执行
    # ----------------------------------------------------------------
    @on.im_message(priority=Priority.HIGH + 5)
    async def rate_limit_message(self, event: KiraMessageEvent, *args, **kwargs):
        session_id = event.session.sid

        # 仅对指定会话生效
        if self.enabled_sessions is not None and session_id not in self.enabled_sessions:
            return

        sender = getattr(event.message, "sender", None)
        sender_id = str(sender.user_id) if sender and hasattr(sender, "user_id") else None
        now = time.time()

        # 主人绕过限流
        if sender_id and sender_id in self.owner_ids:
            return  # 继续正常流程

        stats = self.session_stats[session_id]

        # 清理过期记录（滑动窗口 60 秒）
        self._clean_window(stats, now)

        # 检查冷却期
        if stats["cooldown_until"] and now < stats["cooldown_until"]:
            stats["dropped_count"] += 1
            if self.queue_mentions and event.is_mentioned:
                stats["queued_mentions"].append(event)
            else:
                event.discard()
            if self.cooldown_reply_enabled and event.is_mentioned:
                chain = MessageChain([Text(self.cooldown_reply_text)])
                await self.ctx.message_processor.send_message_chain(session_id, chain)
            return

        # 记录消息时间戳
        stats["timestamps"].append(now)

        # 检查是否超限
        if len(stats["timestamps"]) > self.max_messages:
            stats["cooldown_until"] = now + self.cooldown_seconds
            stats["dropped_count"] = 0
            stats["timestamps"].clear()  # 清空窗口，避免恢复后立即再次触发
            event.discard()

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

        # 冷却期内不记录唤醒（请求实际不会产生有效 LLM 调用）
        if stats["cooldown_until"] and now < stats["cooldown_until"]:
            return

        # 记录唤醒时间戳
        stats["wakeups"].append(now)

        # 检查唤醒次数是否也超限
        if len(stats["wakeups"]) > self.max_wakeups:
            stats["cooldown_until"] = now + self.cooldown_seconds
            stats["dropped_count"] = 0
            stats["wakeups"].clear()

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
