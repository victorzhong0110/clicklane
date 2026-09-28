# PulseBoard 基准测试

数字全部来自同一次 `scripts/benchmark.py` 运行，原始 JSON 在 `docs/measurements.json`。
脚本没有填上的格子写成「未测到」，并附原因。

## 机器与条件

| 项 | 实测 |
| --- | --- |
| uname | `Linux bench 6.12.94+ #1 SMP PREEMPT_DYNAMIC Thu Sep 24 16:04:37 UTC 2026 x86_64 x86_64 x86_64 GNU/Linux` |
| CPU | Intel(R) Xeon(R) Processor，nproc=4 |
| 内存 | MemTotal 16398384 kB，MemAvailable 3343140 kB，SwapTotal 0 kB |
| 系统 | Ubuntu 24.04.4 LTS (Noble Numbat) |
| Java（宿主机） | `openjdk version "21.0.10" 2026-01-20
OpenJDK Runtime Environment (build 21.0.10+7-Ubuntu-124.04)
OpenJDK 64-Bit Server VM (build 21.0.10+7-Ubuntu-124.04, mixed mode, sharing)` |
| Python | Python 3.12.3 |
| Docker | `Server=29.1.3 Driver=vfs NCPU=4 MemTotal=16791945216` |
| 根分区 | `overlay         126G   26G   95G  22% /` |
| ClickHouse | 24.8.14.39 |
| Flink | 1.18.1 |

测量条件（脚本写死的参数，不是事后调整）：

- 样本 200000 条，窗口 600000 ms，水位延迟 120000 ms，Top-N=10。
- Kafka `acks=all`，生产者幂等，linger 20 ms；3 个分区。
- Flink 并行度 2，checkpoint 间隔 10 s，EXACTLY_ONCE 语义，hashmap 状态，文件 checkpoint。
- 水位是按分区的 punctuated watermark（每条事件都发）。
- ClickHouse Sink 批量 1000 行或 200 ms，checkpoint 时再刷一次。查询用 FINAL。
- 乱序实验 seed=7。recovery 限速 4000 条/秒，用来在杀掉 TaskManager 之前留下 checkpoint。
- Docker 存储驱动见上表。驱动若是 vfs，读写会比 overlay2 慢，吞吐要连同驱动一起看。

## 各次运行

### baseline

| 项 | 值 |
| --- | --- |
| 生产条数 | 200000 |
| 生产耗时（秒） | 9.096 |
| 生产速率（条/秒） | 2.199e+04 |
| 乱序回退条数 | 0 |
| 生产端最大回退（毫秒） | 0 |
| 端到端耗时（秒，开打到 ODS 稳定） | 29.66 |
| 端到端吞吐（条/秒，生产条数 / 端到端耗时） | 6742 |
| ODS 是否稳定 | True |
| ODS FINAL 行数 | 200000 |
| PV/UV 窗口数 | 33 |
| 迟到行数（稳定时） | 0 |
| 迟到事件 / 迟到 PV | 未测到 / 未测到 |
| 重复侧输出 | 未测到 |
| ODS count() / count() FINAL | 未测到 / 未测到 |
| 最近一次 checkpoint 耗时（毫秒） | 106 |
| 最近一次 checkpoint 状态大小（字节） | 9137583 |

延迟（仅 baseline 作为标题数字；单位毫秒，inserted_at 减 produce_time，含最多 200 ms 的刷写）：

| 口径 | n | min | p50 | p99 | max |
| --- | --- | --- | --- | --- | --- |
| ods_insert_minus_produce | 200000 | 17 | 98 | 211.1 | 282 |
| ads_insert_minus_max_produce | 33 | 235 | 338 | 520.6 | 534 |

和 Python 离线口径的差（按窗口取绝对差，再求和。UV 可以按窗口比，不能把各窗口 UV 加起来当成总用户数）：

| 指标 | 对比窗口 | 有差异的窗口 | 绝对差之和 | 最大绝对差 | 实时合计 | 离线合计 |
| --- | --- | --- | --- | --- | --- | --- |
| pv | 33 | 0 | 0 | 0 | 193824 | 193824 |
| uv | 33 | 0 | 0 | 0 | 60083 | 60083 |
| cart_cnt | 33 | 0 | 0 | 0 | 2714 | 2714 |
| buy_cnt | 33 | 0 | 0 | 0 | 3462 | 3462 |
| dim_miss | 33 | 0 | 0 | 0 | 66642 | 66642 |
| pv_users | 33 | 0 | 0 | 0 | 60083 | 60083 |
| cart_users | 33 | 0 | 0 | 0 | 1911 | 1911 |
| buy_users | 33 | 0 | 0 | 0 | 3130 | 3130 |
| pv_to_cart_users | 33 | 0 | 0 | 0 | 1684 | 1684 |
| cart_to_buy_users | 33 | 0 | 0 | 0 | 784 | 784 |
| pv_to_buy_users | 33 | 0 | 0 | 0 | 2383 | 2383 |
| topn item/pv | rt 330 / batch 330 | 不一致 0，缺一侧 0 |  |  |  |  |

和 ClickHouse SQL 离线口径的差：

| 指标 | 对比窗口 | 有差异的窗口 | 绝对差之和 | 最大绝对差 | 实时合计 | 离线合计 |
| --- | --- | --- | --- | --- | --- | --- |
| pv | 33 | 0 | 0 | 0 | 193824 | 193824 |
| uv | 33 | 0 | 0 | 0 | 60083 | 60083 |
| cart_cnt | 33 | 0 | 0 | 0 | 2714 | 2714 |
| buy_cnt | 33 | 0 | 0 | 0 | 3462 | 3462 |
| dim_miss | 33 | 0 | 0 | 0 | 66642 | 66642 |
| pv_users | 33 | 0 | 0 | 0 | 60083 | 60083 |
| cart_users | 33 | 0 | 0 | 0 | 1911 | 1911 |
| buy_users | 33 | 0 | 0 | 0 | 3130 | 3130 |
| pv_to_cart_users | 33 | 0 | 0 | 0 | 1684 | 1684 |
| cart_to_buy_users | 33 | 0 | 0 | 0 | 784 | 784 |
| pv_to_buy_users | 33 | 0 | 0 | 0 | 2383 | 2383 |
| topn item/pv | rt 330 / batch 330 | 不一致 0，缺一侧 0 |  |  |  |  |

### disorder-3m

| 项 | 值 |
| --- | --- |
| 生产条数 | 200000 |
| 生产耗时（秒） | 9.117 |
| 生产速率（条/秒） | 2.194e+04 |
| 乱序回退条数 | 50192 |
| 生产端最大回退（毫秒） | 235000 |
| 端到端耗时（秒，开打到 ODS 稳定） | 29.73 |
| 端到端吞吐（条/秒，生产条数 / 端到端耗时） | 6726 |
| ODS 是否稳定 | True |
| ODS FINAL 行数 | 200000 |
| PV/UV 窗口数 | 33 |
| 迟到行数（稳定时） | 1871 |
| 迟到事件 / 迟到 PV | 未测到 / 未测到 |
| 重复侧输出 | 未测到 |
| ODS count() / count() FINAL | 未测到 / 未测到 |
| 最近一次 checkpoint 耗时（毫秒） | 84 |
| 最近一次 checkpoint 状态大小（字节） | 9137592 |

和 Python 离线口径的差（按窗口取绝对差，再求和。UV 可以按窗口比，不能把各窗口 UV 加起来当成总用户数）：

| 指标 | 对比窗口 | 有差异的窗口 | 绝对差之和 | 最大绝对差 | 实时合计 | 离线合计 |
| --- | --- | --- | --- | --- | --- | --- |
| pv | 33 | 31 | 1796 | 210 | 192028 | 193824 |
| uv | 33 | 29 | 349 | 35 | 59734 | 60083 |
| cart_cnt | 33 | 17 | 34 | 8 | 2680 | 2714 |
| buy_cnt | 33 | 16 | 41 | 7 | 3421 | 3462 |
| dim_miss | 33 | 29 | 607 | 71 | 66035 | 66642 |
| pv_users | 33 | 29 | 349 | 35 | 59734 | 60083 |
| cart_users | 33 | 14 | 25 | 6 | 1886 | 1911 |
| buy_users | 33 | 16 | 32 | 7 | 3098 | 3130 |
| pv_to_cart_users | 33 | 14 | 23 | 5 | 1661 | 1684 |
| cart_to_buy_users | 33 | 8 | 9 | 2 | 775 | 784 |
| pv_to_buy_users | 33 | 16 | 32 | 8 | 2351 | 2383 |
| topn item/pv | rt 330 / batch 330 | 不一致 121，缺一侧 0 |  |  |  |  |

和 ClickHouse SQL 离线口径的差：

| 指标 | 对比窗口 | 有差异的窗口 | 绝对差之和 | 最大绝对差 | 实时合计 | 离线合计 |
| --- | --- | --- | --- | --- | --- | --- |
| pv | 33 | 31 | 1796 | 210 | 192028 | 193824 |
| uv | 33 | 29 | 349 | 35 | 59734 | 60083 |
| cart_cnt | 33 | 17 | 34 | 8 | 2680 | 2714 |
| buy_cnt | 33 | 16 | 41 | 7 | 3421 | 3462 |
| dim_miss | 33 | 29 | 607 | 71 | 66035 | 66642 |
| pv_users | 33 | 29 | 349 | 35 | 59734 | 60083 |
| cart_users | 33 | 14 | 25 | 6 | 1886 | 1911 |
| buy_users | 33 | 16 | 32 | 7 | 3098 | 3130 |
| pv_to_cart_users | 33 | 14 | 23 | 5 | 1661 | 1684 |
| cart_to_buy_users | 33 | 8 | 9 | 2 | 775 | 784 |
| pv_to_buy_users | 33 | 16 | 32 | 8 | 2351 | 2383 |
| topn item/pv | rt 330 / batch 330 | 不一致 121，缺一侧 0 |  |  |  |  |

### disorder-15m

| 项 | 值 |
| --- | --- |
| 生产条数 | 200000 |
| 生产耗时（秒） | 9.212 |
| 生产速率（条/秒） | 21710 |
| 乱序回退条数 | 50192 |
| 生产端最大回退（毫秒） | 964000 |
| 端到端耗时（秒，开打到 ODS 稳定） | 29.89 |
| 端到端吞吐（条/秒，生产条数 / 端到端耗时） | 6692 |
| ODS 是否稳定 | True |
| ODS FINAL 行数 | 200000 |
| PV/UV 窗口数 | 33 |
| 迟到行数（稳定时） | 47284 |
| 迟到事件 / 迟到 PV | 未测到 / 未测到 |
| 重复侧输出 | 未测到 |
| ODS count() / count() FINAL | 未测到 / 未测到 |
| 最近一次 checkpoint 耗时（毫秒） | 93 |
| 最近一次 checkpoint 状态大小（字节） | 9137595 |

和 Python 离线口径的差（按窗口取绝对差，再求和。UV 可以按窗口比，不能把各窗口 UV 加起来当成总用户数）：

| 指标 | 对比窗口 | 有差异的窗口 | 绝对差之和 | 最大绝对差 | 实时合计 | 离线合计 |
| --- | --- | --- | --- | --- | --- | --- |
| pv | 33 | 32 | 45856 | 2953 | 147968 | 193824 |
| uv | 33 | 32 | 6235 | 401 | 53848 | 60083 |
| cart_cnt | 33 | 28 | 653 | 58 | 2061 | 2714 |
| buy_cnt | 33 | 31 | 775 | 57 | 2687 | 3462 |
| dim_miss | 33 | 32 | 15698 | 1001 | 50944 | 66642 |
| pv_users | 33 | 32 | 6235 | 401 | 53848 | 60083 |
| cart_users | 33 | 28 | 372 | 36 | 1539 | 1911 |
| buy_users | 33 | 31 | 654 | 50 | 2476 | 3130 |
| pv_to_cart_users | 33 | 31 | 513 | 42 | 1171 | 1684 |
| cart_to_buy_users | 33 | 29 | 287 | 27 | 497 | 784 |
| pv_to_buy_users | 33 | 31 | 753 | 59 | 1630 | 2383 |
| topn item/pv | rt 330 / batch 330 | 不一致 312，缺一侧 0 |  |  |  |  |

和 ClickHouse SQL 离线口径的差：

| 指标 | 对比窗口 | 有差异的窗口 | 绝对差之和 | 最大绝对差 | 实时合计 | 离线合计 |
| --- | --- | --- | --- | --- | --- | --- |
| pv | 33 | 32 | 45856 | 2953 | 147968 | 193824 |
| uv | 33 | 32 | 6235 | 401 | 53848 | 60083 |
| cart_cnt | 33 | 28 | 653 | 58 | 2061 | 2714 |
| buy_cnt | 33 | 31 | 775 | 57 | 2687 | 3462 |
| dim_miss | 33 | 32 | 15698 | 1001 | 50944 | 66642 |
| pv_users | 33 | 32 | 6235 | 401 | 53848 | 60083 |
| cart_users | 33 | 28 | 372 | 36 | 1539 | 1911 |
| buy_users | 33 | 31 | 654 | 50 | 2476 | 3130 |
| pv_to_cart_users | 33 | 31 | 513 | 42 | 1171 | 1684 |
| cart_to_buy_users | 33 | 29 | 287 | 27 | 497 | 784 |
| pv_to_buy_users | 33 | 31 | 753 | 59 | 1630 | 2383 |
| topn item/pv | rt 330 / batch 330 | 不一致 312，缺一侧 0 |  |  |  |  |

### recovery

| 项 | 值 |
| --- | --- |
| 生产条数 | 200000 |
| 生产耗时（秒） | 50.01 |
| 生产速率（条/秒） | 3999 |
| 乱序回退条数 | 0 |
| 生产端最大回退（毫秒） | 0 |
| 端到端耗时（秒，开打到 ODS 稳定） | 70.65 |
| 端到端吞吐（条/秒，生产条数 / 端到端耗时） | 2831 |
| ODS 是否稳定 | True |
| ODS FINAL 行数 | 200000 |
| PV/UV 窗口数 | 33 |
| 迟到行数（稳定时） | 3399 |
| 迟到事件 / 迟到 PV | 未测到 / 未测到 |
| 重复侧输出 | 未测到 |
| ODS count() / count() FINAL | 未测到 / 未测到 |
| 最近一次 checkpoint 耗时（毫秒） | 85 |
| 最近一次 checkpoint 状态大小（字节） | 9137583 |

和 Python 离线口径的差（按窗口取绝对差，再求和。UV 可以按窗口比，不能把各窗口 UV 加起来当成总用户数）：

| 指标 | 对比窗口 | 有差异的窗口 | 绝对差之和 | 最大绝对差 | 实时合计 | 离线合计 |
| --- | --- | --- | --- | --- | --- | --- |
| pv | 33 | 3 | 3320 | 1519 | 190504 | 193824 |
| uv | 33 | 3 | 947 | 454 | 59136 | 60083 |
| cart_cnt | 33 | 3 | 43 | 16 | 2671 | 2714 |
| buy_cnt | 33 | 3 | 36 | 17 | 3426 | 3462 |
| dim_miss | 33 | 3 | 1163 | 535 | 65479 | 66642 |
| pv_users | 33 | 3 | 947 | 454 | 59136 | 60083 |
| cart_users | 33 | 3 | 26 | 12 | 1885 | 1911 |
| buy_users | 33 | 3 | 34 | 17 | 3096 | 3130 |
| pv_to_cart_users | 33 | 3 | 22 | 10 | 1662 | 1684 |
| cart_to_buy_users | 33 | 2 | 8 | 6 | 776 | 784 |
| pv_to_buy_users | 33 | 3 | 23 | 10 | 2360 | 2383 |
| topn item/pv | rt 330 / batch 330 | 不一致 29，缺一侧 0 |  |  |  |  |

和 ClickHouse SQL 离线口径的差：

| 指标 | 对比窗口 | 有差异的窗口 | 绝对差之和 | 最大绝对差 | 实时合计 | 离线合计 |
| --- | --- | --- | --- | --- | --- | --- |
| pv | 33 | 3 | 3320 | 1519 | 190504 | 193824 |
| uv | 33 | 3 | 947 | 454 | 59136 | 60083 |
| cart_cnt | 33 | 3 | 43 | 16 | 2671 | 2714 |
| buy_cnt | 33 | 3 | 36 | 17 | 3426 | 3462 |
| dim_miss | 33 | 3 | 1163 | 535 | 65479 | 66642 |
| pv_users | 33 | 3 | 947 | 454 | 59136 | 60083 |
| cart_users | 33 | 3 | 26 | 12 | 1885 | 1911 |
| buy_users | 33 | 3 | 34 | 17 | 3096 | 3130 |
| pv_to_cart_users | 33 | 3 | 22 | 10 | 1662 | 1684 |
| cart_to_buy_users | 33 | 2 | 8 | 6 | 776 | 784 |
| pv_to_buy_users | 33 | 3 | 23 | 10 | 2360 | 2383 |
| topn item/pv | rt 330 / batch 330 | 不一致 29，缺一侧 0 |  |  |  |  |

TaskManager 恢复：

| 项 | 值 |
| --- | --- |
| checkpoint_before | `{'counts': {'restored': 0, 'total': 1, 'in_progress': 0, 'completed': 1, 'failed': 0}, 'completed': {'id': 1, 'status': 'COMPLETED', 'end_to_end_duration_ms': 35, 'state_size_bytes': 1460567, 'checkpointed_size_bytes': 1460567}, 'completed_raw': {'className': 'completed', 'id': 1, 'status': 'COMPLETED', 'is_savepoint': False, 'savepointFormat': None, 'trigger_timestamp': 1790580815170, 'latest_ack_timestamp': 1790580815205, 'checkpointed_size': 1460567, 'state_size': 1460567, 'end_to_end_duration': 35, 'alignment_buffered': 0, 'processed_data': 5144, 'persisted_data': 0, 'num_subtasks': 16, 'num_acknowledged_subtasks': 16, 'checkpoint_type': 'CHECKPOINT', 'tasks': {}, 'external_path': 'file:/opt/flink/checkpoints/2d364fab703d13df870e9df3c1df4d40/chk-1', 'discarded': False}}` |
| kill_to_running_s | `0.232` |
| states | `[{'after_kill_s': 0.136, 'state': 'RUNNING'}, {'after_kill_s': 0.23, 'state': 'RUNNING'}]` |
| restored | `{'id': 1, 'restore_timestamp': 1790580833740, 'external_path': 'file:/opt/flink/checkpoints/2d364fab703d13df870e9df3c1df4d40/chk-1', 'raw': {'id': 1, 'restore_timestamp': 1790580833740, 'is_savepoint': False, 'external_path': 'file:/opt/flink/checkpoints/2d364fab703d13df870e9df3c1df4d40/chk-1'}}` |
| job_id_unchanged | `True` |
| ods_physical_after | `{'physical': '200000'}` |
| ods_final_after | `{'final_rows': '200000'}` |

## Spark

pyspark 退出码 0。和 Python PV 的对比：`{'pv': {'windows_compared': 33, 'windows_missing_one_side': 0, 'windows_with_diff': 0, 'sum_abs_diff': 0.0, 'max_abs_diff': 0.0, 'sum_rt': 193824.0, 'sum_batch': 193824.0}, 'uv': {'windows_compared': 33, 'windows_missing_one_side': 0, 'windows_with_diff': 0, 'sum_abs_diff': 0.0, 'max_abs_diff': 0.0, 'sum_rt': 60083.0, 'sum_batch': 60083.0}, 'cart_cnt': {'windows_compared': 33, 'windows_missing_one_side': 0, 'windows_with_diff': 0, 'sum_abs_diff': 0.0, 'max_abs_diff': 0.0, 'sum_rt': 2714.0, 'sum_batch': 2714.0}, 'buy_cnt': {'windows_compared': 33, 'windows_missing_one_side': 0, 'windows_with_diff': 0, 'sum_abs_diff': 0.0, 'max_abs_diff': 0.0, 'sum_rt': 3462.0, 'sum_batch': 3462.0}}`。

输出尾部：

```
{"spark": "ok", "out": "/workspace/results/spark"}
26/09/28 07:34:57 WARN Utils: Your hostname, bench resolves to a loopback address: 127.0.0.1; using 172.30.0.2 instead (on interface enp0s4)
26/09/28 07:34:57 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
26/09/28 07:34:57 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable

```

## Python 离线规格

batch/metrics.py 计算耗时 0.474 秒，重复事件 106，窗口数 33。
它和 ClickHouse SQL 的 PV 差：

| 指标 | 对比窗口 | 有差异的窗口 | 绝对差之和 | 最大绝对差 | 实时合计 | 离线合计 |
| --- | --- | --- | --- | --- | --- | --- |
| pv | 33 | 0 | 0 | 0 | 193824 | 193824 |
| uv | 33 | 0 | 0 | 0 | 60083 | 60083 |
| cart_cnt | 33 | 0 | 0 | 0 | 2714 | 2714 |
| buy_cnt | 33 | 0 | 0 | 0 | 3462 | 3462 |
| dim_miss | 33 | 0 | 0 | 0 | 66642 | 66642 |
| pv_users | 33 | 0 | 0 | 0 | 60083 | 60083 |
| cart_users | 33 | 0 | 0 | 0 | 1911 | 1911 |
| buy_users | 33 | 0 | 0 | 0 | 3130 | 3130 |
| pv_to_cart_users | 33 | 0 | 0 | 0 | 1684 | 1684 |
| cart_to_buy_users | 33 | 0 | 0 | 0 | 784 | 784 |
| pv_to_buy_users | 33 | 0 | 0 | 0 | 2383 | 2383 |
| topn item/pv | rt 330 / batch 330 | 不一致 0，缺一侧 0 |  |  |  |  |

## 测量范围

- Flink SQL 只写在 sql/flink_reference.sql，没有提交到集群，也没有进入这组数字。
- 数据集只用了公开 CSV 的前 200000 行，不是全量两个月。
- Grafana 面板依赖 grafana-clickhouse-datasource 插件；若插件查询格式和当前版本不一致，以 ClickHouse 里的表为准。

## 怎么读这些数

脚本表格里的 `kill_to_running_s = 0.232` 不能当成恢复耗时。那两次轮询读到的作业状态都还是 RUNNING，Flink 的心跳还没把 TaskManager 判死。

JobManager 日志里同一次 `pulseboard-recovery`（job `2d364fab703d13df870e9df3c1df4d40`）的时间是：

| 时刻 (UTC) | 日志 |
| --- | --- |
| 07:33:48.724 | 作业从 RUNNING 进入 RESTARTING，原因是 TaskManager 不可达 |
| 07:33:53.732 | 从 checkpoint 1（`file:/opt/flink/checkpoints/2d364fab703d13df870e9df3c1df4d40/chk-1`）恢复 |
| 07:33:56.830 | 最后一个 task 回到 RUNNING |

从进入 RESTARTING 到作业状态回到 RUNNING 是 5.007 秒，和 `fixedDelayRestart` 的 5 秒间隔一致。到最后一个 task RUNNING 是 8.106 秒。

迟到 PV 和实时 PV 相对离线的绝对差之和相等，三次都是：

| run_id | 迟到事件 | 迟到 PV | PV 绝对差之和 |
| --- | --- | --- | --- |
| disorder-3m | 1871 | 1796 | 1796 |
| disorder-15m | 47284 | 45856 | 45856 |
| recovery | 3399 | 3320 | 3320 |

baseline 没有迟到行，33 个窗口的 PV、UV、漏斗和 330 行 Top-N 与 Python、ClickHouse SQL 的差都是 0。recovery 的 ODS `count()` 和 `count() FINAL` 都是 200000，这次查询看不到重复主键。Spark 的 PV、UV、cart、buy 与 Python 的绝对差也是 0。

端到端吞吐用的是「生产条数 / 从开始回放到 ODS 稳定」。baseline 是 200000 / 29.665 秒 = 6741.95 条/秒。生产者自己的速率是 21986.55 条/秒。ODS 延迟是 `inserted_at - produce_time`。脚本当次 `quantile`（近似）写出 p50 98 毫秒、p99 211.09 毫秒；min 17、max 282。聚合新鲜度还要等水位越过窗口右端点，脚本当次 p50 338 毫秒、p99 520.56 毫秒，max 534 毫秒。

`quantile` 是近似函数，隔几秒再查 p99 会变。补查 `quantileExact`（同表、FINAL、run_id=baseline）：ODS p50 98、p99 213；聚合新鲜度 p50 338、p99 534（33 个窗口时 0.99 分位落到最大值）。标题表仍保留脚本当次的近似值。
