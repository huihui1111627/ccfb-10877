# LogScope — 日志模式分析平台

面向大规模微服务日志的**无正则**自动模板归纳与异常检测平台。日志进入后自动抽取
模板与变量、按分钟统计频次、识别突增/骤降/新模板，并在故障时间点前后跨服务关联
模板变化，给出根因线索。全部用 Rust 实现，单机吞吐 **百万行/秒量级**，附带零构建
依赖的网页控制台。

## 能力对照

| # | 需求 | 实现方式 |
|---|------|----------|
| 1 | 无正则自动抽模板/变量 | Drain 固定深度树 + 字符扫描归一化（数字/IP/UUID/十六进制/带单位耗时→`<*>`），混合 token 按数字边界切分 |
| 2 | 版本升级模板合并/拆分 | 跨版本用 token Jaccard 重叠建血缘边（长度不敏感），同版本相似度≥0.85 周期合并；UI 展示 `MergedFrom`/`SplitFrom` |
| 3 | 多行堆栈合成一条 | 时间戳前缀识别条目边界；JVM/Python 续行折叠成单个 `<stacktrace>` 固定 token，堆栈深度不影响归簇 |
| 4 | 突增/骤降/新模板告警 | 每模板每分钟计数，滚动 20 桶中位数+MAD；突增≥4×且过 3.5σ，骤降需连续 3 桶近乎静默；ERROR 新模板首现即告警 |
| 5 | 跨服务根因关联 | 事件前后 2/8 分钟窗口，按错误级别、时间领先度、事件类型加权排序，输出可疑模板与推理链 |
| 6 | 数值变量独立趋势 | 耗时（归一到 ms）、大小（归一到字节）、百分比单独成序列，P² 流式 p95，模板详情页看均值/峰值曲线 |
| 7 | 持久化增量学习 | 30s JSON 快照（整数/元组 key 用数组编码），固定 FNV-1a 路由保证重启后服务落同一分片 |
| 8 | 单机 ≥10 万行/秒 | 读线程组装多行 → 解析线程池 → 按服务哈希分片，每分片独占 Drain+统计零锁竞争；实测 **120 万+ 行/秒** |

## 目录结构

```
src/
  main.rs        CLI 入口（serve / bench / inspect）
  preprocess.rs  行解析、变量归一化、tokenize、多行堆栈折叠
  interner.rs    分片内字符串 intern（u32 引用）
  drain.rs       Drain 聚类、模板更新、版本血缘、周期合并
  stats.rs       分钟桶时间序列、MAD 检测器、P² 分位数
  shard.rs       分片状态机、告警生成、快照读写
  pipeline.rs    读/解析/分片三级线程流水线、TCP 采集
  rootcause.rs   跨服务事件关联与根因打分
  web.rs         axum HTTP API
  static/index.html  单页控制台（五个标签页，原生 JS/Canvas）
tools/
  gen_logs.py    仿 5 个服务真实风格的日志生成器，注入 5 次已知故障，带真值
  evaluate.py    聚类 P/R/F1、故障召回/延迟、误报率量化
  run_all.sh     一键复现
```

## 快速开始

```bash
# 一键：生成日志 → 跑引擎 → 量化评测 → 大流量压测
bash tools/run_all.sh

# 或分步
cargo build --release
python3 -I tools/gen_logs.py data/logs
./target/release/logscope --state-dir data/state bench data/logs/all.log --fresh \
    --report data/bench.json --assignments data/assignments.jsonl \
    --events-out data/events.jsonl
python3 -I tools/evaluate.py data/logs/truth.jsonl data/logs/faults.json \
    data/assignments.jsonl data/events.jsonl data/bench.json data/eval-report.json

# 网页控制台（自动加载已持久化的模板库）
./target/release/logscope --state-dir data/state serve
#   UI:  http://localhost:8080
#   采集: nc localhost 5050 < app.log
```

## 网页五个标签页

1. **日志接入** — TCP 采集地址、状态目录、已接入服务、每分钟总吞吐曲线
2. **模板库** — 按服务/关键字过滤，级别、版本、血缘、样例，点击看详情与数值趋势
3. **时间分布** — 总体与 Top 模板的每分钟频次曲线
4. **异常事件** — 突增/骤降/新模板，含观测值/基线，一键跳转根因
5. **根因线索** — 可疑模板打分排序、各服务窗口内变化统计、推理过程

## 量化结果（默认数据集：33.7 万行 / 120 分钟 / 5 服务 / 5 故障）

| 指标 | 结果 |
|------|------|
| 模板抽取 pairwise F1 | **0.98**（4 个服务 1.00，order-gateway 因 v8/v9 刻意格式变化为 0.90） |
| 真值 38 模板 → 聚出 | 37 簇（v1/v2 API 路径正确合并为 1） |
| 多行堆栈聚类一致性 | **1.00**（3900+ 条堆栈） |
| 故障召回 | **100%（5/5）** |
| 平均发现延迟 | **60 秒**（一个检测周期内） |
| 真实误报 | **0**（冷启动 ERROR 首现 5 条为设计内即时告警，单独统计） |
| 吞吐 | 标准集 119 万行/秒；338 万行大流量集 128 万行/秒 |
| 峰值内存 | 338 万行仅 ~20 MB |

## 关键设计取舍

- **告警口径**：骤降要求模板中位数≥8/分钟且连续 3 桶近乎静默（<10%），避免稀疏模板
  Poisson 自然波动刷屏；版本切换导致的老模板消失由血缘识别并抑制，不报错。
- **冷启动**：模板需积累 10 分钟历史才参与 spike/drop 检测，基线至少 8 个有效桶。
- **路由稳定**：用固定种子 FNV-1a 而非 ahash 默认哈希，保证进程重启后服务→分片映射不变，
  否则 interner 不连续会让模板被重复学习。
- **时间桶**：插入用全量查找+排序，容忍跨线程乱序/重复，杜绝重复桶污染 MAD 基线。
- **持久化格式**：选 JSON 快照而非 SQLite，零 C 依赖、可直接检视；环形缓冲只留 4 小时分钟级数据。
