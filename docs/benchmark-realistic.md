# 接近生产的基准

这一页只写 `scripts/benchmark_realistic.py` 在这台机器上测到的数。原始记录在 `docs/measurements-realistic.json`。没有测到的项写明原因。延迟用的是 ClickHouse `quantileExact`，不是近似的 `quantile`。

## 机器和配额

- 4 核 Intel Xeon（KVM），`nproc` = 4
- MemTotal 16398384 kB，无 swap
- Docker 29.1.3，存储驱动 vfs，NCPU=4，MemTotal=16791945216
- Flink 1.18.1，ClickHouse 24.8.14.39
- 回放开始时这份 JSON 里的 MemAvailable 是 6955436 kB（当时栈已经起来）

`docker inspect` 的 `HostConfig.NanoCpus` 和容器里的 cgroup `cpu.max`（配额 周期，单位微秒）说明 CPU 配额写上了。内存是 `HostConfig.Memory`。

| 容器 | 请求 CPU | NanoCpus | cgroup cpu.max | Memory 字节 |
| --- | --- | --- | --- | --- |
| pulseboard-kafka | 0.50 | 500000000 | `50000 100000` | 805306368 |
| pulseboard-clickhouse | 1.00 | 1000000000 | `100000 100000` | 3221225472 |
| pulseboard-jobmanager | 0.40 | 400000000 | `40000 100000` | 943718400 |
| pulseboard-taskmanager | 1.50 | 1500000000 | `150000 100000` | 1610612736 |
| pulseboard-grafana | 0.25 | 250000000 | `25000 100000` | 402653184 |

ClickHouse 一开始是 1024 MB 容器、进程上限 800000000 字节。`pace-before` 的 `docker stats` 还显示 `398.8MiB / 1GiB`。两轮 70 万行叠在一起之后，查询和删除 mutation 都报 Code 241，`maximum: 762.94 MiB`（一次是 would use 790.68 MiB，一次是 823.50 MiB）。之后把容器改成 3072 MB、`max_server_memory_usage` 改成 2400000000，并且每轮把指标写入 JSON 后删掉该 `run_id` 的行。`pace-after`、更快的对照和混沌是在这个新上限下测的。`pace-before` 本身是在旧的 1GiB 上限下跑完的，恒等式差额为 0。

## 数据怎么回放

文件 `data/realistic/events.csv`，700000 行，sha256 `5b60424df6fc991100f673c7aada3506034e89649018290c90b7b573d01451b5`。事件时间 1572566400000 到 1572609550000（2019-11-01 00:00:00 UTC 到 11:59:10 UTC），跨度 43150000 ms。这是公开 CSV 的真实前缀，在 70 万行处截断，没有傍晚和夜间。行为：pv 675012，cart 11345，buy 13643。文件里原有自然键重复 457 条，其中 PV 99。

| UTC 小时 | 事件数 |
| --- | --- |
| 00 | 10887 |
| 01 | 14000 |
| 02 | 32498 |
| 03 | 49348 |
| 04 | 61480 |
| 05 | 73450 |
| 06 | 76155 |
| 07 | 79426 |
| 08 | 79818 |
| 09 | 76812 |
| 10 | 75667 |
| 11 | 70459 |

主回放 speedup = 43150000 / 150000 = 287.6667，把这 12 小时压进大约 150 秒墙钟。更快的对照 speedup = 616.4286，大约 70 秒。同一事件秒的记录 `release_s` 相同，连在一起发送。抖动、迟到只推迟发送，不改 `event_time_ms`。

种子 11。注入（三次干净全量回放的计数相同）：data 661405，jitter 35033，late 3562，duplicate 1354，malformed 50（bad_json / bad_user / bad_behavior / bad_time / empty 各 10）。加上重复后 `produced` = 701354。

离线 Python（`batch/metrics.py`，同一份 CSV，不含注入的重复和畸形）用时 2.472 秒，72 个窗口，PV 合计 675012。

有一轮 `pace-after` / `fast-*` 复用了已经写过的 Kafka topic。`OffsetsInitializer.earliest()` 把上一轮再读了一遍，重复和迟到侧输出接近全量。那些数字作废。下面的表是删掉 topic 之后重跑的。

## 持续吞吐

生产端没有被自己的睡眠拖住：`max_behind_s` 都小于 0.2 秒，说明发送跟上了事件时间表。

| 运行 | 水位 | sink | 生产耗时 s | 生产速率 条/秒 | max_behind_s | 端到端 s | 端到端 条/秒 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| pace-before | punctuated | 1000 / 200 ms | 151.862 | 4618.70 | 0.133 | 174.329 | 4023.16 |
| pace-after | periodic 200 ms | 1000 / 200 ms | 151.867 | 4618.55 | 0.161 | 174.468 | 4019.96 |
| fast-before | punctuated | 1000 / 200 ms | 70.891 | 9894.10 | 0.155 | 88.493 | 7925.53 |
| fast-after | periodic 200 ms | 1000 / 200 ms | 70.887 | 9894.68 | 0.153 | 93.466 | 7503.84 |
| fast-sink | punctuated | 4000 / 500 ms | 70.886 | 9894.78 | 0.148 | 93.358 | 7512.52 |

端到端把“等窗口在 ClickHouse 里连续三次稳定”算进去，所以低于生产速率。`settle` 每 5 秒看一次，几秒的端到端差落在这个粒度里。生产速率和 `max_behind_s` 才说明管道有没有跟上回放。

按 ODS `inserted_at` 的每秒条数（`quantileExact`）：

| 运行 | 秒数 | p50 | avg | max |
| --- | --- | --- | --- | --- |
| pace-before | 152 | 5745 | 4614.17 | 6916 |
| pace-after | 153 | 5678 | 4584.01 | 7448 |
| fast-before | 72 | 11932 | 9741.03 | 15505 |
| fast-after | 72 | 12181 | 9741.03 | 16208 |
| fast-sink | 71 | 12365 | 9878.23 | 14832 |

150 秒那档的每秒峰值 6916–7448，对应上午最忙的那几个小时被压短之后的形状。70 秒那档平均大约 9700–9900 条/秒，峰值大约 1.5 万–1.6 万。

## 突发时的端到端延迟

突发秒：该秒 ODS 条数 ≥ 全部秒的 `quantileExact(0.9)`。安静秒：条数 ≤ `quantileExact(0.5)`。延迟是 `inserted_at - produce_time` 的毫秒，`quantileExact`。

`pace-before`：突发门槛 6355 条，16 个突发秒；安静门槛 5745 条，77 个安静秒。

| 运行 | 范围 | n | p50 ms | p99 ms | max ms |
| --- | --- | --- | --- | --- | --- |
| pace-before | 全部 | 701354 | 160 | 292 | 342 |
| pace-before | 突发 | 105048 | 172 | 296 | 327 |
| pace-before | 安静 | 237632 | 170 | 294 | 342 |
| pace-after | 全部 | 701354 | 157 | 294 | 367 |
| pace-after | 突发 | 106622 | 156 | 296 | 324 |
| fast-before | 全部 | 701354 | 122 | 268 | 392 |
| fast-before | 突发 | 115521 | 110 | 258 | 299 |
| fast-sink | 全部 | 701354 | 306 | 584 | 696 |
| fast-sink | 突发 | 112742 | 290 | 569 | 626 |

在这条回放跟上的负载上，突发秒和安静秒的 p99 几乎一样。管道没有在忙时额外排队。

窗口新鲜度（`ads_pv_uv.inserted_at - max_produce_time`，`quantileExact`）`pace-before`：72 个窗口，min 98，p50 306，p99 830，max 830。72 个点的 0.99 分位落在最大值上。

## 和离线比

恒等式：

```text
ads_pv + late_pv - duplicate_pv = 离线 PV - 文件里原有的重复 PV
```

右边是 675012 - 99 = 674913，也就是每个自然键第一次出现的 PV。

| 运行 | ads_pv | late_pv | duplicate_pv | 左边 | 差额 | 齐 |
| --- | --- | --- | --- | --- | --- | --- |
| pace-before | 674838 | 1475 | 1400 | 674913 | 0 | 是 |
| pace-after | 674971 | 1342 | 1400 | 674913 | 0 | 是 |
| fast-before | 674902 | 1411 | 1400 | 674913 | 0 | 是 |
| fast-after | 675227 | 1086 | 1400 | 674913 | 0 | 是 |
| fast-sink | 674907 | 1406 | 1400 | 674913 | 0 | 是 |

`pace-before` 按窗口和离线比：PV 绝对差合计 420，最大 23，72 个窗口都对得上，68 个窗口有差。UV 绝对差合计 171。Top-N 720 对里有 164 对商品或 PV 不一致。这些差来自迟到和注入的重复，总额被上面的恒等式收住。UV 不能把各窗口加起来当总用户数。

ODS：上述干净运行的 `count()` 和 `count() FINAL` 都是 701354，和 `produced` 一致。查询之前没有 `OPTIMIZE`。

## 重复、迟到、畸形

`pace-before`（其余干净运行的重复侧输出同样是 1811）：

- 注入重复 1354。文件原有重复 457。侧输出 1811，等于两者之和。其中 PV 重复 1400。
- 注入“迟到”3562 条（事件时间再推迟 3–10 分钟）。真正进入 `dwd_late_events` 的是 1524 条，其中 PV 1475。其余注入仍然落在 120 秒水位里面，或者靠近文件尾部，后面没有更晚的事件把水位推过它们。
- 畸形 50 条。质量表：bad_behavior 10，bad_time 10，bad_user 10，unparseable 10，empty 1。10 条完全相同的空报文在 `ReplacingMergeTree` 里收成 1 行。非法行没有进入 PV 恒等式。

periodic 200 ms 在 speedup 287.67 下大约对应 57 秒事件时间，仍小于 120 秒水位。`pace-after` 的迟到 PV 是 1342，和 punctuated 的 1475 同一量级，恒等式仍是 0。

## 优化前后

先比水位，一次只改 `--watermark.mode`。150 秒昼夜回放上，periodic 的端到端速率是 punctuated 的 4019.96 / 4023.16 = 0.9992，没有提高 5%。生产速率 4618.55 对 4618.70，`max_behind_s` 0.161 对 0.133。

再把 speedup 提高到 616.43。periodic / punctuated 的端到端比是 7503.84 / 7925.53 = 0.9468，仍然没有变快。两边生产速率都是约 9894 条/秒，`max_behind_s` 都是 0.15 秒左右，每秒摄入的 avg 都是 9741.03。端到端差大约 5 秒，和 `settle` 的 5 秒轮询粒度相同，不能当成吞吐退步。

于是只改 sink：punctuated 不变，batch 1000 / flush 200 ms 改成 batch 4000 / flush 500 ms。

| | fast-before | fast-sink |
| --- | --- | --- |
| 生产速率 | 9894.10 | 9894.78 |
| max_behind_s | 0.155 | 0.148 |
| 端到端条/秒 | 7925.53 | 7512.52 |
| ODS p50 / p99 ms | 122 / 268 | 306 / 584 |
| 突发 p50 / p99 ms | 110 / 258 | 290 / 569 |
| PV 恒等式差额 | 0 | 0 |

吞吐没有提高。延迟变差了：全量 p50 从 122 ms 到 306 ms，p99 从 268 ms 到 584 ms。更大的批次在这条已经跟得上的负载上只是让 ODS 更晚落盘。

## 混沌

三次都用 punctuated、150 秒昼夜回放、干净 topic。恢复秒数从 JobManager 日志读。任务切换那一行不含 job id，脚本没把它收进自动解析；下面的“最后任务”是同一份日志里、作业进入 RUNNING 之后最后一条 `INITIALIZING to RUNNING`。

| | 杀 TaskManager | 重启 ClickHouse | 重启 Kafka |
| --- | --- | --- | --- |
| 作业 | `3c9d39150a641b9dc22eb59d03a2c659` | `80ec21fb7eb888cfe5de9c50286f077f` | `69a73b0e4b90930c5e3b1f3d9b1272cb` |
| RESTARTING 次数 | 1 | 3 | 0 |
| RUNNING→RESTARTING | 08:18:58.330 | 08:21:19.705（第一次） | 日志里没有 |
| 作业回到 RUNNING | 08:19:03.331（5.001 秒） | 08:21:35.885（16.180 秒） | — |
| 最后任务 RUNNING | 08:19:04.982，dedup 2/2（6.652 秒） | 08:21:36.408，dedup 2/2（16.703 秒） | — |
| 恢复的 checkpoint | chk-3 | 三次都是 chk-3 | — |
| 动手前 checkpoint | id 3，24 ms，状态 1417744 字节 | id 3，23 ms，状态 1472732 字节 | id 3，28 ms，状态 1504702 字节 |
| ODS count() / FINAL | 701354 / 701354 | 701354 / 701354 | 701354 / 701354 |
| 重复侧输出 | 1811 | 1811 | 1811 |
| late_pv | 5748 | 4017 | 1400 |
| 恒等式差额 | -3978 | +2 | 0 |
| 结果是否仍齐 | 否 | 否（差 2 个 PV） | 是 |

杀掉 TaskManager 之后，迟到 PV 从昼夜回放的大约 1475 升到 5748，另外还有 3978 个第一次出现的 PV 既不在窗口里也不在迟到侧输出里。ODS 行数仍然等于生产条数，明细没有丢，聚合短了。不能说恢复后结果还是齐的。

重启 ClickHouse 时作业按 5 秒固定延迟重启了 3 次，每次都从同一个 chk-3 恢复，直到 ClickHouse 回来。明细行数齐，PV 恒等式差 2，不把它叫成完全一致。

重启 Kafka 时作业没有进入 RESTARTING。生产端 `retries=20`、`delivery.timeout.ms=120000`，`max_behind_s` 0.185 秒，回放没有失败。恒等式差额 0。这里没有“恢复耗时”，因为作业没有重启。

## 瓶颈

`pace-before` 中途的 Flink backpressure REST 没有返回 subtask 的 ratio（`subtasks` 是空的），所以不能用背压等级点名算子。

`docker stats` 在 `pace-before` 只采到 3 个点，峰值是：TaskManager CPUPerc 88.46（内存 554.5MiB / 1.5GiB），Kafka 13.66，ClickHouse 13.10（当时容器还是 1GiB），JobManager 5.35，Grafana 0.13。按“100 大约是 1 个核”来读，TaskManager 大约 0.88 核，配额是 1.50 核，没有顶到 cgroup。更快的 `fast-before` 同样只有 3 个采样点，TaskManager 峰值 CPUPerc 23.21，ClickHouse 27.50（1023MiB / 3GiB）。采样太稀，不能用来证明某一秒没有尖峰。

能确定的是：从 4600 条/秒到大约 9900 条/秒，`max_behind_s` 都不到 0.2 秒，ODS 每秒平均摄入和生产速率同级。瓶颈是回放时钟，不是 TaskManager、Kafka 或 ClickHouse 的配额。把 sink 批次加大没有提高吞吐，只把 ODS p99 从 268 ms 抬到 584 ms。ClickHouse 的 763 MiB 进程上限会在两轮 70 万行的 `FINAL` 上先失败，那是查询和累计数据的限制，不是单轮摄入的限制。

`pace-before` 结束时 checkpoint 17：357 ms，状态 31846069 字节。

## 这一轮的测量范围

上面这一轮停在 2019-11-01 中午，背压接口没有 subtask ratio，`docker stats` 每轮大约 3 个点，测完就把 `run_id` 删了。写出改造前后的故障重跑、去掉回放时钟之后的瓶颈，以及完整的一天，用的是同一套恒等式。修之前的结果保留在上面。

## 写出改成幂等加两阶段提交之后

根因是旧 `ClickHouseSink`：`invoke`、后台计时、`snapshotState` 和 `close` 都会刷 ClickHouse，`version` 用墙钟，存在算子状态里，恢复时和当前时间取最大。窗口在上次成功的 checkpoint 之后已经把完整聚合刷出去，任务死掉时“已经发出”这个标记还没进 checkpoint。恢复后第二阶段再吐一个更小的数，墙钟 version 更大，`ReplacingMergeTree` 的 `FINAL` 留下小的那行。明细的 `event_id` 内容不变，所以 ODS 的 `count()` 和 `FINAL` 仍然能对上。重启 ClickHouse 的 +2 是同一类非原子提交，不是四舍五入。

现在的写出分成两条：

- 明细和侧输出（`ods_events`、`dwd_late_events`、`dwd_invalid_events`、`dwd_duplicate_events`）不进 checkpoint 状态。`version` 固定为 1。批次满、`snapshotState`、`close` 时插入。内容不变，重试在 `FINAL` 里收成一行。把每条 ODS 放进状态曾经让 checkpoint 涨到大约 75 MB，Flink 邮箱被 HTTP 堵住，作业失败。这条做法没有留用：`exact-tm-1` 差额 -17101；`exact-tm-2` 在 30 次重启后失败，差额 -618747，是 ODS 停在半截上的数，不当成修好之后的结果。
- 聚合表在 `invoke` 里只缓冲。`snapshotState` 把行编成 `checkpointId` 加 JSON 放进 ListState，不插入。`notifyCheckpointComplete` 才插入，`version` 就是这次 checkpoint id。`notifyCheckpointAborted` 把行放回缓冲区。`close` 不刷未提交的聚合。PV、漏斗、Top-N 的第二阶段在第一次 `onTimer` 发出之后置 `emitted`，恢复回来的部分和不再写一行更大的 version。Sink 全部 `disableChaining()`。checkpoint 超时改成 180 秒，可容忍失败 10 次。

重跑仍是 70 万行、speedup 287.6667、种子 11、punctuated、batch 1000、并行度 2、每次新建 topic。四次 `produced` 都是 701354，重复侧输出都是 1811 / PV 1400，`settle` 稳定，窗口 72。查询前没有 `OPTIMIZE`。Kafka 故障没有重跑。

| | 修前杀 TM | exact3-tm-1 | exact3-tm-2 | 修前重启 CH | exact3-ch-1 | exact3-ch-2 |
| --- | --- | --- | --- | --- | --- | --- |
| 作业 | `3c9d3915…` | `6a95558d63a08e0a8df9d8805a59fbaf` | `4f2b1b6036f52478d611c5721d4f64d8` | `80ec21fb…` | `f02ec5b7e45f5b7b6b325a2133e93ae9` | `aa996fee6f81bdd7199d3df1bb1c3860` |
| RESTARTING | 1 | 1 | 1 | 3 | 3 | 2 |
| 作业回到 RUNNING | 5.001 秒 | 5.002 秒 | 5.001 秒 | 16.180 秒 | 15.874 秒 | 10.436 秒 |
| 最后任务 RUNNING | 6.652 秒 | 7.145 秒 | 6.723 秒 | 16.703 秒 | 16.579 秒 | 11.116 秒 |
| 恢复的 checkpoint | chk-3 | chk-3 | chk-2 | 三次 chk-3 | 三次 chk-3 | chk-3 |
| ODS count() / FINAL | 701354 / 701354 | 759354 / 701354 | 701354 / 701354 | 701354 / 701354 | 701354 / 701354 | 701354 / 701354 |
| late 事件 / late_pv | 5884 / 5748 | 5749 / 5595 | 7913 / 7726 | — / 4017 | 7021 / 6798 | 4623 / 4516 |
| ads_pv | 666587 | 670742 | 668661 | 672298 | 669585 | 671846 |
| 恒等式差额 | -3978 | +24 | +74 | +2 | +70 | +49 |

相对修前干净的 `pace-before`（ads_pv 674838，late_pv 1475，差额 0）：这四次都是 late_pv 的增加略大于 ads_pv 的减少，所以差额为正。重复 PV 仍是 1400，不是把 topic 又读了一整遍。ODS `FINAL` 始终等于 701354。`exact3-tm-1` 的物理行 759354 是 version=1 的重试，`FINAL` 折成 701354。和离线按窗口比，PV 绝对差合计分别是 5570、7385、6297、3690，最大单窗 1326、1414、1896、1516。

差额没有到 0。剩下的几十个 PV 同时出现在已提交的窗口聚合和迟到侧输出里。能确定的边界是：聚合作出不再被更小的行盖掉（-3978 那种缺口没有再现），明细在 `FINAL` 下不丢；源、去重和“这条事件算不算迟到”仍可能在同一次恢复里错开。四次作业日志里都有 checkpoint 被拒，异常文本是某个明细 sink 的 `Could not perform checkpoint`。端到端精确一次不成立。

## 去掉回放时钟之后的瓶颈

4 个生产者各发 70 万行文件里互不相交的 175000 行，不限速，只跑一轮，同一 topic、同一 `run_id`。`docker stats` 和 Kafka lag 大约 8 秒一个点，每轮只有 3 个点，不是长时间稳态。脚本把第一个 `current=0` 的点也算进平均，得到的 16442.73 和 21004.79 条/秒偏低，下面不用它们做标题。标题用两个非零点之间的消费增量。

| | 优化前 | 优化后 |
| --- | --- | --- |
| 并行度 / 分区 / sink 批次 | 2 / 3 / 1000 | 3 / 3 / 1000 |
| TaskManager slot | 当时要能调度并行度 2；当前 compose 的 slot 是 4 | 4（CPU 配额仍是 1.50） |
| 四路生产速率 | 13210.37、13717.28、13072.65、13380.51，合计 53380.81 | 12622.38、12747.26、13354.46、13323.63，合计 52047.73 |
| 非零采样间隔 | 8.389 秒 | 8.076 秒 |
| 这段时间的消费 | 66535 → 272845，约 24593.83 条/秒 | 43494 → 335034，约 36100.05 条/秒 |
| lag 两个非零点 | 404323 → 427155，最大 427155 | 403192 → 364966，最大 403192 |
| 之后 drain 看到的 lag | 0（追平耗时没有单独计） | 0（同样没有单独计） |
| ODS quantileExact min / p50 / p99 / max ms | 63 / 3614 / 6273 / 8984 | 1346 / 7462 / 11508 / 19295 |
| ODS count() / FINAL | 700000 / 700000 | 700000 / 700000 |
| TaskManager CPUPerc | 124.8（655.2MiB / 1.5GiB） | 146.54（671.1MiB / 1.5GiB） |
| ClickHouse CPUPerc | 45.28（594.6MiB / 3GiB） | 52.29（663MiB / 3GiB） |
| Kafka CPUPerc | 10.05 | 4.67 |
| 结束时 checkpoint | id 5，399 ms，状态 32927065 字节 | id 6，583 ms，状态 32941149 字节 |

优化前 source 背压等级 `low`，两个 subtask 的 ratio 0.151 和 0.128，busy 大约 0.44，idle 大约 0.41。去重 busy 大约 0.47。`sink-ods` busy 大约 0.30，idle 大约 0.70。Kafka 和 ClickHouse 都没顶到配额。最热的是 TaskManager。

这一次优化只加并行度 2 → 3。分区仍是 3。slot 从 2 改成 4，否则并行度 3 排不下去。消费大约从 24594 条/秒升到 36100 条/秒。P99 从 6273 ms 变到 11508 ms，没有变好。优化后 TaskManager CPUPerc 146.54，已经贴着 1.50 核的配额（`docker stats` 的 100 大约是 1 个核）。同一轮采样里 source、去重、`sink-ods` 的 busy 都是 1.0，背压等级是 `ok`。再加并行度而不同时加 CPU，这条机器上没有空间。采样只有大约 8 秒，lag 后来又回到 0，所以这是突发追赶时的吞吐，不是多分钟稳态。

## 完整的 2019-11-01

文件 `data/realistic-day/events.csv`，1445360 行，sha256 `56d3655556e3a754776b79ece51f1f9b0cf6cb5d9a74afeb5991b4d5de59ee6e`。事件时间 1572566400000 到 1572652799000（00:00:00 到 23:59:59 UTC），跨度 86399000 ms。行为：pv 1403991，cart 18911，buy 22458。文件内自然键重复 752，其中 PV 189。离线 Python 6.184 秒，重复事件 752。维表 `data/dim/dim_item_day.csv`，作业参数 `/opt/data/dim_item_day.csv`。speedup = 86399000 / 200000 = 431.995，墙钟 201.155 秒，生产速率 7199.62 条/秒，`max_behind_s` 0.371。注入：data 1365913，jitter 72117，late 7330，duplicate 2832，malformed 50。`produced` 1448192。作业 `93599ba471e8fb411b2a64139fd6138e`。`settle` 稳定：ODS 1448192，窗口 144，late 11221。

文件按 UTC 小时的事件数，16 点是峰：

| 小时 | 00 | 01 | 02 | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 | 11 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 事件 | 10887 | 14000 | 32498 | 49348 | 61480 | 73450 | 76155 | 79426 | 79818 | 76812 | 75667 | 71407 |

| 小时 | 12 | 13 | 14 | 15 | 16 | 17 | 18 | 19 | 20 | 21 | 22 | 23 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 事件 | 72744 | 81756 | 91563 | 100115 | 104740 | 96141 | 76217 | 53333 | 31107 | 18059 | 10725 | 7912 |

70 万行那份在 11:59:10 截断，所以它的 11 点是 70459，全天文件的 11 点是 71407。

ODS `FINAL` 按 `event_time` 的小时计数，峰同样在 16 点，104949 条。相邻小时：15 点 100314，17 点 96337，23 点 7927，00 点 10908。和文件差在注入的重复、以及抖动只改发送时刻不改事件时间，所以形状跟文件一致，条数略高。`ads_pv_uv` 按窗口起点的小时，16 点 PV 102845。小时 UV 如果把 10 分钟窗口的 UV 相加，不是该小时的去重用户，这里不把那个和当成日活。

这一轮没有人手杀进程，但 JobManager 日志里作业自己 RESTARTING 了 5 次，分别从 chk-3、chk-7、chk-9、chk-12、chk-15 恢复（09:55:53、09:56:25、09:56:59、09:57:31、09:58:16 UTC）。checkpoint 统计：completed 16，failed 6，restored 5。TaskManager 日志里 barrier 被拒的原因是 `HTTP/1.1 header parser received no bytes`，以及 Broken pipe、EOF。明细 sink 仍在 `snapshotState` 里同步插入，ClickHouse 把连接掐断时这次 checkpoint 就失败，作业重启。查询时 ClickHouse `MemoryTracking` 812132426 字节。行留在库里，`run_id` 是 `day-20191101`。指标写入之后取消了作业，避免再重启把聚合盖掉。取消后再查，ODS 物理行 / `FINAL` 仍是 1479012 / 1448192，`ads_pv` 仍是 1396045。

| 项 | 数 |
| --- | --- |
| ODS count() / FINAL | 1479012 / 1448192（`FINAL` = `produced`） |
| 重复侧输出 | 3584，等于文件 752 + 注入 2832；其中 PV 2943 |
| late 事件 / late_pv | 11221 / 10899（注入的迟到只有 7330，多出来的是重启之后被当成迟到） |
| ads_pv | 1396045 |
| 离线第一次出现的 PV | 1403991 - 189 = 1403802 |
| 恒等式差额 | +199 |
| 按窗口 PV 绝对差 | 144 窗都在，137 窗有差，合计 10504，最大 3112 |
| ODS quantileExact p50 / p99 / max ms | 4673 / 28764 / 34988 |

差额 +199 不能当成无故障基线。这 201 秒里作业自己重启了 5 次，迟到侧输出比注入的迟到更多，和上面故障实验里“窗口里有、迟到里也有”是同一类缺口。p99 28.8 秒也含这几次重启，不能和修之前干净回放的 292 ms 直接比。
