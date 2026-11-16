# 深度登记 多时区应用日志小时桶重算

        - 规模：中型
        - 核心规则：事件先归一到 UTC，DST 重叠时保留 fold；小时桶右端开，水位只能前进且迟到事件仍进入修订。
        - 使用者与工作流：导入随仓样例，逐模块复核，最后导出单一证据清单。

        ## 功能模块

        | 模块 | 使用者动作 | 入口 | 核心文件 | 测试 |
        |---|---|---|---|---|
        | M1 日志采信 | 登记来源/代次、分段上传并复核证据 | `/api/logs/intake` `/api/logs/import` / `/logs` | `app/modules/m1_logs/core.py` `app/modules/m1_logs/repository.py` `app/modules/m1_logs/handler.py` | `tests/test_m1_intake.py` `tests/test_m1_intake_http.py` `tests/test_m1_logs.py` |
| M2 时间解析 | 运行样例并复核证据 | `/api/time/normalize` / `/time` | `app/modules/m2_time/core.py` `app/modules/m2_time/service.py` | `tests/test_m2_time.py` |
| M3 事件去重 | 运行样例并复核证据 | `/api/events/dedupe` / `/events` | `app/modules/m3_events/core.py` `app/modules/m3_events/service.py` | `tests/test_m3_events.py` |
| M4 小时分桶 | 运行样例并复核证据 | `/api/buckets/rebuild` / `/buckets` | `app/modules/m4_buckets/core.py` `app/modules/m4_buckets/service.py` | `tests/test_m4_buckets.py` |
| M5 迟到修订 | 运行样例并复核证据 | `/api/watermark/advance` / `/watermark` | `app/modules/m5_watermark/core.py` `app/modules/m5_watermark/service.py` | `tests/test_m5_watermark.py` |
| M6 批次对账 | 运行样例并复核证据 | `/api/export/buckets.csv` / `/runs` | `app/modules/m6_runs/core.py` `app/modules/m6_runs/service.py` | `tests/test_m6_runs.py` |

        ## 陷阱登记

        | TRAP | 现象 | 朴素实现 | 本项目处理 | 处理位置 | 测试 |
        |---|---|---|---|---|---|
        | TRAP-03 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m1_logs/core.py` | `tests/test_m1_logs.py::test_trap_1` |
| TRAP-05 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m2_time/core.py` | `tests/test_m2_time.py::test_trap_1` |
| TRAP-07 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m3_events/core.py` | `tests/test_m3_events.py::test_trap_1` |
| TRAP-09 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m4_buckets/core.py` | `tests/test_m4_buckets.py::test_trap_1` |
| TRAP-11 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m5_watermark/core.py` | `tests/test_m5_watermark.py::test_trap_1` |
| TRAP-12 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m6_runs/core.py` | `tests/test_m6_runs.py::test_trap_1` |
| TRAP-13 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m1_logs/core.py` | `tests/test_m1_logs.py::test_trap_2` |
| TRAP-14 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m2_time/core.py` | `tests/test_m2_time.py::test_trap_2` |
| TRAP-16 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m3_events/core.py` | `tests/test_m3_events.py::test_trap_2` |
| TRAP-23 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m4_buckets/core.py` | `tests/test_m4_buckets.py::test_trap_2` |
| TRAP-25 | 轮转时半行被当坏数据丢弃 | 半截 JSON 直接判 TRUNCATED 入隔离 | 半行保留为 `pending_tail`，与续片拼接后再裁决，物理行号连续 | `app/modules/m1_logs/core.py` | `tests/test_m1_intake.py::test_half_line_is_retained_not_quarantined` |
| TRAP-26 | 续采沿用旧位置漏读新文件 | 新文件等待旧文件写完才接收 | 换代即开立新代次 RECEIVING，旧代次转 PENDING_TAIL 互不阻塞 | `app/modules/m1_logs/core.py` | `tests/test_m1_intake.py::test_rotation_new_generation_receives_immediately` |
| TRAP-27 | 旧文件尾行补齐时堵住新流或两代拼接伪造事件 | 跨代拼接半行或挂到新代批次 | 尾行只在原代次拼接、按原 `batch_id` 归档，两代证据独立 | `app/modules/m1_logs/core.py` `app/modules/m1_logs/repository.py` | `tests/test_m1_intake.py::test_old_generation_tail_completes_into_original_batch` |
| TRAP-28 | 换代与上传并发产生过期推进 | 后到的旧版本覆盖新代次状态 | 版本/偏移校验失败返回 422（STALE_VERSION/POSITION_CONFLICT），锁内裁决 | `app/modules/m1_logs/core.py` `app/modules/m1_logs/handler.py` | `tests/test_m1_intake.py::test_concurrent_rotation_single_winner_no_duplicate_generation` |
| TRAP-29 | 中断恢复漏行、重试重复入桶 | 偏移未落盘或分片重复解析 | 单事务持久化偏移/尾行，`(代次,序号,校验值)` 幂等回放 | `app/modules/m1_logs/repository.py` | `tests/test_m1_intake.py::test_recovery_resumes_without_gap_or_duplicate` |

        ## 对账规则

        - 输入记录数 = 有效记录数 + 隔离记录数 + 空记录数。
        - 核心数值或事件数在转换、判定和导出之间保持守恒。
        - 同一输入摘要重跑复用原运行编号，不增加结果行。
        - 分段采信中：代次物理行数 = 有效行 + 隔离行 + 空行；各代次有效/隔离之和等于来源合计，尾行补齐与结尾冲刷不改变守恒。
        - 已接收字节偏移只随新分片单调推进；幂等回放不改变偏移、不重复产生证据行。

        ## 已知不覆盖

        - 生产系统连接、身份认证和外部密钥不在演示范围内。
