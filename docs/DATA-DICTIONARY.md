# 数据字典

| 字段 | 类型 | 可空 | 定义 |
|---|---|---:|---|
| `run_id` | string | 否 | 输入摘要派生的稳定运行编号 |
| `source_line` | integer | 否 | 原文件物理行号 |
| `reason_code` | string | 是 | 隔离或冲突的稳定原因码 |
| `raw_value` | string | 是 | 未归一的原始证据 |
| `normalized_value` | string | 是 | 按项目规则得到的值 |
| `rule_version` | string | 否 | 计算规则版本 |
| `source` | string | 否 | 日志来源标识，代次登记的分区键 |
| `generation` | integer | 否 | 来源内单调递增的文件代次，轮转一次加一 |
| `batch_id` | string | 否 | 代次归属批次，默认 `来源#g代次`，尾行补齐仍归原批次 |
| `state` | string | 否 | 代次状态：`RECEIVING` 接收中 / `PENDING_TAIL` 待补齐 / `COMPLETE` 完成 |
| `received_offset` | integer | 否 | 该代次已接收的累计字节偏移，续传与并发裁决依据 |
| `pending_tail` | string | 是 | 末尾不含换行的半截行，等待后续分片拼接；结尾后为空串 |
| `seq` | integer | 否 | 代次内分片序号，与校验值共同构成幂等键 |
| `checksum` | string | 否 | 分片文本 SHA-256，同序号异内容判 `SEGMENT_CONFLICT` |
| `hour_bucket` | string | 是 | 记录对应的 UTC 小时桶，无法取桶记 `UNBUCKETED` |

字符串编号逐字保留。金额与计量值用 `Decimal`。时间值带偏移并以 UTC 参与比较。区间统一采用左闭右开。

代次状态单向流转：`RECEIVING` →（换代）→ `PENDING_TAIL` →（明确结尾）→ `COMPLETE`；换代时新代次直接以 `RECEIVING` 开立。尾行只在同代次内拼接，禁止跨代拼接。
