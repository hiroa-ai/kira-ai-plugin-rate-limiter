# Rate Limiter

KiraAI 消息流速限流插件，防止高活跃群聊中 token 过度消耗。

## 工作原理

插件通过两个 Hook 实现双维度限流：

| Hook | 监控维度 | 触发阈值 |
|---|---|---|
| `im_message` | 每分钟消息数 | `max_messages_per_minute` |
| `llm_request` | 每分钟 LLM 唤醒数 | `max_wakeups_per_minute` |

任一维度超限后进入冷却期，期间消息将被丢弃。冷却结束后，插件会向 AI 注入限流提示，包含丢弃消息数和被提及消息摘要，引导 AI 简要回应。

## 配置项

| 配置 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `enabled_sessions` | list | `[]` | 启用限流的会话 ID 列表，留空则对所有会话生效 |
| `max_messages_per_minute` | int | `50` | 每分钟最大消息数 |
| `max_wakeups_per_minute` | int | `20` | 每分钟最大 LLM 唤醒次数 |
| `cooldown_seconds` | int | `300` | 冷却时间（秒） |
| `cooldown_reply_enabled` | switch | `false` | 冷却期间被提及时是否发送回复 |
| `cooldown_reply_text` | string | `我现在比较忙，请稍后再找我~` | 冷却期间的回复内容 |
| `queue_mentions` | switch | `false` | 冷却期内是否保留被提及的消息等待合并处理 |
| `owner_ids` | list | `[]` | 主人 ID 列表，这些用户的消息不触发限流 |

## 安装

通过 WebUI 安装或将插件目录放入 `data/plugins/` 下即可。