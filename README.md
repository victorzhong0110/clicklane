# PulseBoard

仓库：[github.com/victorzhong0110/pulseboard](https://github.com/victorzhong0110/pulseboard)

电商大促实时运营大盘：把公开电商行为按事件时间回放到 Kafka，用 Flink 按 10 分钟窗口统计 PV、加购和下单转化。聚合结果等 checkpoint 完成才写入 ClickHouse，节点故障恢复后不会用更小的数盖掉已经提交的窗口。Grafana 读这些表，离线任务用同一口径对账。

[English](#english)

## 数据

样本来自 Michael Kechinov 发布、REES46 / Open CDP 采集的 [eCommerce behavior data from multi category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)。数据集说明：可免费用于研究、书籍和教学，需要注明 REES46 与该 Kaggle 页面。本仓库不提交原始行。

下载镜像（Hugging Face 上的同一份 CSV）写在 `replayer/prepare.py`。默认只取开头 20 万行。也支持天池 UserBehavior 的无表头 CSV：`user,item,category,behavior,timestamp秒`。

## 本地运行

需要 Docker、JDK 17、Maven、Python 3.12。

```bash
python3 -m venv .venv
.venv/bin/pip install -r replayer/requirements.txt
.venv/bin/python -m replayer.prepare --rows 200000
mvn -B -f flink-jobs/pom.xml test package
sh scripts/fetch_grafana_plugin.sh
docker compose up -d
```

如果容器之间互相连不上（JobManager 在、TaskManager 的 slot 却是 0），看一下 `iptables-legacy` 的 FORWARD 是不是 DROP。只放行 `docker0` 时，Compose 的网桥会被丢掉：

```bash
sudo iptables-legacy -P FORWARD ACCEPT
```

端口：

| 服务 | 地址 |
| --- | --- |
| Grafana | http://127.0.0.1:43123 （匿名只读，管理员 admin / admin） |
| Flink UI | http://127.0.0.1:18081 |
| ClickHouse HTTP | http://127.0.0.1:18123 |
| Kafka（宿主机） | 127.0.0.1:19092 |

提交作业并回放一次：

```bash
docker exec pulseboard-jobmanager /opt/flink/bin/flink run -d \
  -c io.pulseboard.pipeline.ClickstreamJob \
  /opt/flink/jobs/pulseboard-pipeline.jar \
  --kafka.bootstrap kafka:9092 \
  --kafka.topic events-baseline \
  --kafka.group pulseboard-baseline \
  --run.id baseline \
  --dim.path /opt/data/dim_item.csv

.venv/bin/python -m replayer.replay \
  --bootstrap 127.0.0.1:19092 \
  --topic events-baseline \
  --run-id baseline \
  --rate 0 \
  --disorder-ratio 0 \
  --kick
```

`--rate 0` 表示尽快发送。`--disorder-ratio` 和 `--disorder-span-ms` 控制乱序比例和事件时间回退幅度。

整套测量（含乱序、对账、杀掉 TaskManager 后的恢复）:

```bash
.venv/bin/python scripts/benchmark.py
```

结果写到 `docs/benchmark.md`。没有测到的项会写成「未测到」和原因。

按真实事件时间加速、并注入抖动、迟到、重复和畸形记录的测量：

```bash
.venv/bin/python -m replayer.prepare --skip-download --input data/raw/day_head.csv --rows 700000 \
  --events-out data/realistic/events.csv \
  --dim-out data/dim/dim_item_realistic.csv \
  --meta-out data/realistic/meta.json
.venv/bin/python scripts/benchmark_realistic.py
```

结果写到 `docs/benchmark-realistic.md`。`--speedup 0` 时回放器仍走原来的限速路径。

测试：

```bash
.venv/bin/python -m pytest replayer/tests batch/test_metrics.py
mvn -B -f flink-jobs/pom.xml test
```

## 文档

- [docs/design.md](docs/design.md)：架构、数据模型、水位、幂等写出、取舍
- [docs/benchmark.md](docs/benchmark.md)：尽快回放的实测数字
- [docs/benchmark-realistic.md](docs/benchmark-realistic.md)：按真实事件时间加速、带故障注入的实测数字

## 参考

实现是原创的，没有拷贝这些仓库的代码或示例数据。读它们是为了对照业界常见的演示形态：

- [wuchong/flink-sql-demo](https://github.com/wuchong/flink-sql-demo)（仓库未声明许可证）：端到端用 Docker Compose 串起回放、Flink 和看板，以及用淘宝风格的用户行为讲指标。只借鉴这个形状。
- [apache/flink-playgrounds](https://github.com/apache/flink-playgrounds)（Apache-2.0）：故障后从 checkpoint 恢复的演示方式。
- [ververica/flink-sql-cookbook](https://github.com/ververica/flink-sql-cookbook)（Apache-2.0）：窗口、Top-N、维表关联这几类 SQL 模式。`sql/flink_reference.sql` 是按本项目口径自己写的对照，没有执行，也没有作为基准数字。
- [geekyouth/SZT-bigdata](https://github.com/geekyouth/SZT-bigdata)（许可证标记为 NOASSERTION）：公开数据集、业务指标、以及 Flink / Spark / ClickHouse 放在一个项目里的组织方式。只借鉴这个范围，不使用其中的代码。

## English

Repository: [github.com/victorzhong0110/pulseboard](https://github.com/victorzhong0110/pulseboard)

PulseBoard is a live operations board for an e-commerce promotion. A public clickstream is replayed into Kafka on event time. Flink aggregates PV, add-to-cart, and purchase conversion in 10-minute event-time windows, and writes the aggregates to ClickHouse only after the checkpoint completes, so a node failure does not replace a committed window total with a smaller number. Grafana reads ClickHouse. An offline job (Python, ClickHouse SQL, and Spark) uses the same definitions and writes a reconciliation table.

The sample is the first 200,000 rows of Michael Kechinov's "eCommerce behavior data from multi category store" (collected by REES46 / Open CDP). The dataset card allows research, books, and teaching with attribution. Rows are not committed; `replayer/prepare.py` downloads them.

```bash
python3 -m venv .venv
.venv/bin/pip install -r replayer/requirements.txt
.venv/bin/python -m replayer.prepare --rows 200000
mvn -B -f flink-jobs/pom.xml test package
sh scripts/fetch_grafana_plugin.sh
docker compose up -d
.venv/bin/python scripts/benchmark.py
```

`scripts/benchmark.py` downloads the Grafana plugin if it is missing and sets `iptables-legacy` FORWARD to ACCEPT so Compose containers can reach each other when that chain drops forwarded traffic. The ClickHouse image's `config.d` is replaced by the mounted directory, so `clickhouse/config.d/limits.xml` sets `listen_host` to `0.0.0.0`.

Grafana: http://127.0.0.1:43123 (anonymous Viewer, admin/admin). Flink UI: http://127.0.0.1:18081. ClickHouse HTTP: http://127.0.0.1:18123. Kafka on the host: 127.0.0.1:19092.

Design notes and the measured benchmarks are in `docs/`, in Chinese. `docs/benchmark.md` is the max-rate replay. `docs/benchmark-realistic.md` is the event-time speedup with jitter, late events, duplicates, malformed records, CPU limits, and chaos. Both files only contain numbers produced on the machine that ran them.

The Flink job is an original DataStream implementation (`io.pulseboard.pipeline`). The repositories listed in the Chinese section were read for the shape of a demo (compose, checkpoint restart, window/top-N patterns, a public-dataset project layout). Their code and sample files were not copied. `wuchong/flink-sql-demo` has no license and `geekyouth/SZT-bigdata` is marked NOASSERTION; both were used as ideas only. `apache/flink-playgrounds` and `ververica/flink-sql-cookbook` are Apache-2.0.
