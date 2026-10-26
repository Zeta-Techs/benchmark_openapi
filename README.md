# benchmark_openapi

OpenAI Chat Completions 协议的压测客户端。**压测目标是真实网关**（`--gateway`），[`mock-stream`](../mock-stream) 仅作为网关背后的上游，需自行启动。

## 构建

```bash
cargo build --release
```

## 用法

最小用法（假设网关已在 `http://127.0.0.1:9000` 就绪）：

```bash
./target/release/benchmark_openapi --gateway http://127.0.0.1:9000
```

带鉴权、限速与结果导出：

```bash
./target/release/benchmark_openapi \
  --gateway https://api.example.com \
  --api-key sk-xxx \
  --rps 200 --concurrency 512 \
  --duration-secs 60 --warmup-secs 5 \
  --out results/run.json --chart results/run.png --progress
```

## 关键参数

| 参数 | 默认 | 说明 |
|---|---|---|
| `--gateway` | — | 被压测的网关地址（必填） |
| `--api-key` | — | Bearer Key，留空不携带 |
| `--duration-secs` | 15 | 计量窗口时长 |
| `--warmup-secs` | 2 | 预热时长，不计入统计 |
| `--concurrency` | 16 | 闭环：worker 数；开环：最大在途请求数 |
| `--rps` | 0 | 开环目标请求速率；0 = 闭环打满 |
| `--stream-percent` | 100 | 流式请求占比（0-100）；0 表示全非流式 |
| `--timeout-secs` | 300 | 单请求超时，含读完整个响应体（连接超时另为 min(10s, 该值)） |
| `--prompt-chars` | 256 | 单条 user 消息长度 |
| `--vary-payload` | false | 每次请求体带随机 nonce，绕过网关/上游缓存 |
| `--model` | mock-stream-1 | 模型名 |
| `--temperature` / `--top-p` / `--n` / `--frequency-penalty` / `--presence-penalty` | — | OpenAI 可选字段，仅显式传入时携带 |
| `--gateway-metrics-url` | — | 网关自身指标端点（JSON），结束后抓取并入报告 |
| `--out` | — | 结果 JSON 输出路径 |
| `--chart` | — | 延迟分位数柱状图 PNG 输出路径 |
| `--progress` | false | oha 风格实时统计面板（每秒刷新） |

## 计量口径

- **闭环**（`--rps 0`）：每个 worker 上一个请求结束才发下一个，吞吐 ≈ 并发数 / 延迟。
- **开环**（`--rps N`）：按固定间隔的计划时刻发请求，不等前一个返回。延迟**从计划时刻起算**，因并发上限排队、发送滞后的时间都计入（避免 coordinated omission 低估尾延迟）。
  - `queued_for_concurrency`：计划时刻在途已满、需要排队的请求数。
  - `dropped`：到窗口结束仍拿不到并发许可而未发出的计划请求数。出现时说明 `--concurrency` 不足以支撑目标 rps（流式场景需要 ≥ rps × 单请求时长）。
- **统计范围**：计划发送时刻落在 `[warmup, warmup + duration)` 内的请求，无论何时返回都完整计入延迟与成败；各项速率 = 这批请求的累计量 / `window_secs`。窗口结束后等待它们返回的时间记为 `drain_secs`。
- **成功判定**：2xx 且响应完整。以下记为错误并分类：
  - `http`：非 2xx 状态码；
  - `transport`：连接失败、超时、读取中断；
  - `protocol`：JSON 无法解析、响应带 `error` 对象、非流式缺 `choices`、流式未以 `data: [DONE]` 结束。
- 成功但没有 `usage` 的响应计入 `missing_usage`，token 均值只按带 usage 的请求计算。
- 客户端只走 HTTP/1.1（reqwest 未开 http2），每个在途请求占一条连接。

## 输出

控制台摘要 + 完整 JSON（`--out`）+ 延迟图表 PNG（`--chart`），包含：

- **吞吐**：发送/完成/错误数（含分类）、req/s、响应体 MB/s、流式与非流式各占多少、排队与 dropped 数
- **延迟**（微秒，HDR 直方图，超过 1 小时按上限记录）：
  - `e2e`：端到端；
  - `ttfb`：响应头到达；
  - `ttft`：首个模型输出 delta（`content` / `reasoning_content` / `tool_calls`）到达，仅流式；
  - 每项含 min/p50/p90/p95/p99/max/mean 与 10 段线性直方图 `buckets`。
- **tokens**：usage 中的 prompt/completion 总量、每请求均值、每秒速率
- **drain_secs**：计量结束后等待窗口内请求全部返回的时间
- **gateway_metrics**：若提供 `--gateway-metrics-url`，附网关自身指标便于互证瓶颈

## 实时统计面板（oha 风格）

加 `--progress` 后，计量期间每秒清屏刷新：

```
=== 实时统计 (6.0s) ===
请求: 6000 已发送 / 5936 完成 / 0 错误 / 64 在途
吞吐: 989.3 req/s, 2.96 MB/s
模式: 6000 流式 / 0 非流式
端到端延迟 (µs): p50=107 p95=300 p99=1106 max=15983
首 token延迟 (µs): p50=101 p95=285 p99=1041 max=15975
直方图 (e2e): █▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁
```

## 示例结果

以下是**客户端直连 mock-stream 上游**（无网关，`token_delay_ms=8`，Apple Silicon 单机）的基线，用于确认客户端自身不是瓶颈，不代表任何网关的性能：

| 场景 | 参数 | req/s | e2e p99 | TTFT 均值 |
|---|---|---|---|---|
| `unary_closed` | 非流式，64 并发 | 68582 | 2.8 ms | — |
| `unary_open_5k` | 非流式，开环 5000 rps，64 并发 | 5000 | 2.2 ms | — |
| `stream_closed` | 流式，64 并发 | 17.1 | 3.95 s | 9.8 ms |
| `stream_open_20` | 流式，开环 20 rps，128 并发 | 20.0 | 3.89 s | 11.3 ms |
