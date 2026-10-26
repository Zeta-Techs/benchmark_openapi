//! OpenAI Chat Completions 压测客户端：打真实网关，mock 上游（../mock-stream）仅作为网关的后端。
//!
//! 采集性能数据：发送/完成速率、响应体吞吐、端到端 / 首字节 / 首 token 延迟直方图、每秒 token 速率、
//! 以及 usage 中的 prompt/completion tokens（用于核算吞吐与验证响应完整性）。
//!
//! 计量口径：只统计「计划发送时刻」落在计量窗口 [warmup, warmup + duration) 内的请求（无论何时返回），
//! 速率 = 这批请求的累计量 / 窗口长度；窗口结束后等待它们全部返回的时间单独记为 drain。

use std::fmt;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::Parser;
use futures_util::StreamExt;
use hdrhistogram::Histogram;
use plotters::prelude::*;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

/// 延迟直方图上限（微秒），1 小时；超出的样本按上限记录，不会被丢弃。
const HIST_MAX_US: u64 = 3_600_000_000;
/// 报告中线性直方图的分桶数。
const HIST_BUCKETS: usize = 10;

#[derive(Parser)]
#[command(name = "benchmark_openapi", about = "OpenAI Chat Completions 压测")]
struct Cli {
    /// 被压测的网关地址，例如 http://127.0.0.1:9000
    #[arg(long)]
    gateway: String,
    /// 网关要求的 API Key（Bearer），留空则不携带
    #[arg(long)]
    api_key: Option<String>,
    /// 压测时长（秒）
    #[arg(long, default_value_t = 15, value_parser = clap::value_parser!(u64).range(1..))]
    duration_secs: u64,
    /// 预热时长（秒），预热数据不计入统计
    #[arg(long, default_value_t = 2)]
    warmup_secs: u64,
    /// 并发上限：闭环为 worker 数，开环为最大在途请求数
    #[arg(long, default_value_t = 16, value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..))]
    concurrency: usize,
    /// 目标每秒请求数（开环，按计划时刻发送），0 表示闭环不限速
    #[arg(long, default_value_t = 0.0)]
    rps: f64,
    /// 流式请求占比（0-100）
    #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u8).range(0..=100))]
    stream_percent: u8,
    /// 单请求超时（秒），包含读完整个响应体
    #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..))]
    timeout_secs: u64,
    /// prompt 字符数（单条 user 消息长度）
    #[arg(long, default_value_t = 256)]
    prompt_chars: usize,
    /// 每次请求体随机化（绕过网关/上游的响应缓存）
    #[arg(long, default_value_t = false)]
    vary_payload: bool,
    /// 模型名
    #[arg(long, default_value = "mock-stream-1")]
    model: String,
    /// 采样温度（temperature）
    #[arg(long)]
    temperature: Option<f64>,
    /// 核采样概率（top_p）
    #[arg(long)]
    top_p: Option<f64>,
    /// 生成多条候选回复（n）
    #[arg(long)]
    n: Option<u32>,
    /// 频率惩罚（frequency_penalty）
    #[arg(long)]
    frequency_penalty: Option<f64>,
    /// 存在惩罚（presence_penalty）
    #[arg(long)]
    presence_penalty: Option<f64>,
    /// 结果 JSON 输出路径
    #[arg(long)]
    out: Option<String>,
    /// 延迟分位数柱状图 PNG 输出路径
    #[arg(long)]
    chart: Option<String>,
    /// 可选：网关进程的 /metrics（或自定义指标端点）URL，结束后抓取并入报告
    #[arg(long)]
    gateway_metrics_url: Option<String>,
    /// oha 风格实时统计面板（每秒刷新）
    #[arg(long, default_value_t = false)]
    progress: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Unary,
    Stream,
}

/// 计量窗口内请求的累计计数（多任务共享，原子操作）。
#[derive(Default)]
struct Counters {
    sent: AtomicU64,
    done: AtomicU64,
    stream_reqs: AtomicU64,
    unary_reqs: AtomicU64,
    bytes: AtomicU64,
    prompt_tokens: AtomicU64,
    completion_tokens: AtomicU64,
    err_http: AtomicU64,
    err_transport: AtomicU64,
    err_protocol: AtomicU64,
    /// 成功但响应里没有 usage 的请求数，此时 token 统计不完整。
    missing_usage: AtomicU64,
    /// 开环：计划时刻已达并发上限、需排队等待的请求数（排队时间计入延迟）。
    queued: AtomicU64,
    /// 开环：计划在窗口内发送、但到窗口结束仍拿不到并发许可而未发出的请求数。
    dropped: AtomicU64,
}

impl Counters {
    fn errors(&self) -> u64 {
        self.err_http.load(Ordering::Relaxed)
            + self.err_transport.load(Ordering::Relaxed)
            + self.err_protocol.load(Ordering::Relaxed)
    }
}

/// 延迟直方图：写入需要互斥，单次持锁只做一次 record，开销可忽略。
struct Hists {
    e2e: Mutex<Histogram<u64>>,
    ttfb: Mutex<Histogram<u64>>,
    ttft: Mutex<Histogram<u64>>,
}

impl Hists {
    fn new() -> Self {
        let h = || Mutex::new(Histogram::new_with_max(HIST_MAX_US, 3).unwrap());
        Self { e2e: h(), ttfb: h(), ttft: h() }
    }
}

fn record(h: &Mutex<Histogram<u64>>, d: Duration) {
    h.lock().unwrap().saturating_record(d.as_micros() as u64);
}

struct Shared {
    cli: Cli,
    url: String,
    counters: Counters,
    hists: Hists,
    client: reqwest::Client,
    seq: AtomicU64,
    /// 所有在途请求数（含预热与排空阶段），供实时面板展示。
    in_flight: AtomicI64,
    measure_start: Instant,
    measure_end: Instant,
}

impl Shared {
    fn in_window(&self, t: Instant) -> bool {
        t >= self.measure_start && t < self.measure_end
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if !(cli.rps.is_finite() && cli.rps >= 0.0) {
        bail!("--rps 必须是非负有限数");
    }

    let timeout = Duration::from_secs(cli.timeout_secs);
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(cli.concurrency)
        .tcp_nodelay(true)
        .connect_timeout(timeout.min(Duration::from_secs(10)))
        .timeout(timeout)
        .build()
        .context("build http client")?;

    let measure_start = Instant::now() + Duration::from_secs(cli.warmup_secs);
    let measure_end = measure_start + Duration::from_secs(cli.duration_secs);
    let shared = Arc::new(Shared {
        url: format!("{}/v1/chat/completions", cli.gateway.trim_end_matches('/')),
        counters: Counters::default(),
        hists: Hists::new(),
        client,
        cli,
        seq: AtomicU64::new(0),
        in_flight: AtomicI64::new(0),
        measure_start,
        measure_end,
    });

    let mut tasks = JoinSet::new();
    if shared.cli.rps > 0.0 {
        tasks.spawn(open_loop(shared.clone()));
    } else {
        for _ in 0..shared.cli.concurrency {
            tasks.spawn(closed_worker(shared.clone()));
        }
    }

    tokio::time::sleep_until(measure_start.into()).await;
    let progress = shared.cli.progress.then(|| spawn_progress(shared.clone()));
    tokio::time::sleep_until(measure_end.into()).await;
    if let Some(h) = progress {
        h.abort();
    }
    while let Some(r) = tasks.join_next().await {
        r.context("worker panicked")?;
    }
    let drain = measure_end.elapsed();

    // 结束后抓取网关自身的指标端点（如果提供），与客户端数据互证瓶颈位置。
    let gateway_metrics = match &shared.cli.gateway_metrics_url {
        Some(url) => fetch_json(&shared.client, url).await,
        None => None,
    };

    let report = build_report(&shared, drain, gateway_metrics);
    print_report(&report);
    if let Some(path) = &shared.cli.out {
        std::fs::write(path, serde_json::to_string_pretty(&report)?)
            .with_context(|| format!("write report to {path}"))?;
        eprintln!("report written to {path}");
    }
    if let Some(path) = &shared.cli.chart {
        draw_latency_chart(&report, path).with_context(|| format!("draw chart to {path}"))?;
        eprintln!("chart written to {path}");
    }

    Ok(())
}

/// 闭环 worker：上一个请求结束才发下一个，吞吐由 并发数 / 延迟 决定。
async fn closed_worker(sh: Arc<Shared>) {
    // SmallRng 是 Send 的（ThreadRng 不是），可安全跨 await 持有。
    let mut rng = SmallRng::from_rng(&mut rand::rng());
    loop {
        let mode = pick_mode(&sh.cli, &mut rng);
        let body = build_body(&sh.cli, &mut rng, &sh.seq, mode);
        let now = Instant::now();
        if now >= sh.measure_end {
            break;
        }
        issue(&sh, mode, body, now, sh.in_window(now)).await;
    }
}

/// 开环发牌器：按固定间隔的计划时刻发请求，不等前一个返回；在途数达到并发上限时排队。
/// 延迟从计划时刻起算，排队与发送滞后都计入，避免 coordinated omission 低估尾延迟。
/// 计量结束时仍在排队的计划请求不再发出，记为 dropped。
async fn open_loop(sh: Arc<Shared>) {
    let sem = Arc::new(Semaphore::new(sh.cli.concurrency));
    let mut rng = SmallRng::from_rng(&mut rand::rng());
    let mut in_flight = JoinSet::new();
    let start = Instant::now();
    let tick = |i: u64| start + Duration::from_secs_f64(i as f64 / sh.cli.rps);
    for i in 0u64.. {
        let scheduled = tick(i);
        if scheduled >= sh.measure_end {
            break;
        }
        tokio::time::sleep_until(scheduled.into()).await;
        let counted = sh.in_window(scheduled);
        let permit = match sem.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                if counted {
                    sh.counters.queued.fetch_add(1, Ordering::Relaxed);
                }
                let wait = sem.clone().acquire_owned();
                match tokio::time::timeout_at(sh.measure_end.into(), wait).await {
                    Ok(p) => p.expect("semaphore closed"),
                    Err(_) => {
                        let dropped = (i..)
                            .map(tick)
                            .take_while(|t| *t < sh.measure_end)
                            .filter(|t| sh.in_window(*t))
                            .count();
                        sh.counters.dropped.fetch_add(dropped as u64, Ordering::Relaxed);
                        break;
                    }
                }
            }
        };
        let mode = pick_mode(&sh.cli, &mut rng);
        let body = build_body(&sh.cli, &mut rng, &sh.seq, mode);
        let sh = sh.clone();
        in_flight.spawn(async move {
            issue(&sh, mode, body, scheduled, counted).await;
            drop(permit);
        });
        // 及时回收已结束的任务，避免 JoinSet 无限增长。
        while in_flight.try_join_next().is_some() {}
    }
    while in_flight.join_next().await.is_some() {}
}

/// 发出一个请求并（若在计量窗口内）记录结果；所有延迟都从 `scheduled` 起算。
async fn issue(sh: &Shared, mode: Mode, body: Value, scheduled: Instant, counted: bool) {
    let c = &sh.counters;
    if counted {
        c.sent.fetch_add(1, Ordering::Relaxed);
        match mode {
            Mode::Stream => c.stream_reqs.fetch_add(1, Ordering::Relaxed),
            Mode::Unary => c.unary_reqs.fetch_add(1, Ordering::Relaxed),
        };
    }
    sh.in_flight.fetch_add(1, Ordering::Relaxed);
    let outcome = run_request(sh, mode, &body, scheduled).await;
    sh.in_flight.fetch_sub(1, Ordering::Relaxed);
    if !counted {
        return;
    }
    match outcome {
        Ok(r) => {
            c.done.fetch_add(1, Ordering::Relaxed);
            c.bytes.fetch_add(r.bytes, Ordering::Relaxed);
            match r.usage {
                Some((p, comp)) => {
                    c.prompt_tokens.fetch_add(p, Ordering::Relaxed);
                    c.completion_tokens.fetch_add(comp, Ordering::Relaxed);
                }
                None => {
                    c.missing_usage.fetch_add(1, Ordering::Relaxed);
                }
            }
            record(&sh.hists.e2e, r.e2e);
            record(&sh.hists.ttfb, r.ttfb);
            if let Some(ttft) = r.ttft {
                record(&sh.hists.ttft, ttft);
            }
        }
        Err(e) => {
            let counter = match e {
                ReqError::Http(..) => &c.err_http,
                ReqError::Transport(_) => &c.err_transport,
                ReqError::Protocol(_) => &c.err_protocol,
            };
            counter.fetch_add(1, Ordering::Relaxed);
            // 仅打印前几个错误样本，避免刷屏。
            if c.errors() <= 5 {
                eprintln!("[error] {e}");
            }
        }
    }
}

fn pick_mode(cli: &Cli, rng: &mut impl Rng) -> Mode {
    if cli.stream_percent >= 100 {
        Mode::Stream
    } else if cli.stream_percent == 0 {
        Mode::Unary
    } else if rng.random_range(0..100u8) < cli.stream_percent {
        Mode::Stream
    } else {
        Mode::Unary
    }
}

fn build_body(cli: &Cli, rng: &mut impl Rng, seq: &AtomicU64, mode: Mode) -> Value {
    let mut content = String::with_capacity(cli.prompt_chars + 64);
    if cli.vary_payload {
        // 随机 nonce 使每次请求体不同，绕过网关/上游的响应缓存；放在开头且不参与截断。
        let n = seq.fetch_add(1, Ordering::Relaxed);
        content.push_str(&format!("nonce-{n}-{:016x} ", rng.random::<u64>()));
    } else {
        content.push_str("benchmark prompt ");
    }
    let prefix_len = content.len();
    while content.len() < cli.prompt_chars {
        content.push_str("lorem ipsum dolor sit amet ");
    }
    content.truncate(cli.prompt_chars.max(prefix_len));

    let mut body = json!({
        "model": cli.model,
        "messages": [{"role": "user", "content": content}],
        "stream": mode == Mode::Stream,
    });

    // OpenAI Chat Completions 可选字段，仅在显式传入时携带。
    if let Some(t) = cli.temperature {
        body["temperature"] = json!(t);
    }
    if let Some(t) = cli.top_p {
        body["top_p"] = json!(t);
    }
    if let Some(n) = cli.n {
        body["n"] = json!(n);
    }
    if let Some(f) = cli.frequency_penalty {
        body["frequency_penalty"] = json!(f);
    }
    if let Some(p) = cli.presence_penalty {
        body["presence_penalty"] = json!(p);
    }
    if mode == Mode::Stream {
        body["stream_options"] = json!({"include_usage": true});
    }

    body
}

#[derive(Debug)]
enum ReqError {
    /// 非 2xx 状态码，附响应体片段。
    Http(reqwest::StatusCode, String),
    /// 连接、超时或读取失败。
    Transport(reqwest::Error),
    /// 响应不完整或不合协议：JSON 无法解析、携带 error 对象、流未以 [DONE] 结束等。
    Protocol(String),
}

impl fmt::Display for ReqError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http(status, body) => write!(f, "gateway status {status}: {body}"),
            Self::Transport(e) => write!(f, "transport: {e}"),
            Self::Protocol(msg) => write!(f, "protocol: {msg}"),
        }
    }
}

impl From<reqwest::Error> for ReqError {
    fn from(e: reqwest::Error) -> Self {
        Self::Transport(e)
    }
}

#[derive(Debug)]
struct ReqOutcome {
    e2e: Duration,
    /// 响应头到达。
    ttfb: Duration,
    /// 首个模型输出 delta 到达（仅流式）。
    ttft: Option<Duration>,
    bytes: u64,
    /// (prompt_tokens, completion_tokens)，响应中没有 usage 时为 None。
    usage: Option<(u64, u64)>,
}

async fn run_request(
    sh: &Shared,
    mode: Mode,
    body: &Value,
    scheduled: Instant,
) -> Result<ReqOutcome, ReqError> {
    let mut req = sh.client.post(&sh.url).json(body);
    if let Some(key) = &sh.cli.api_key {
        req = req.bearer_auth(key);
    }
    let resp = req.send().await?;
    let ttfb = scheduled.elapsed();
    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(ReqError::Http(status, text.chars().take(200).collect()));
    }
    match mode {
        Mode::Unary => read_unary(resp, scheduled, ttfb).await,
        Mode::Stream => read_stream(resp, scheduled, ttfb).await,
    }
}

async fn read_unary(
    resp: reqwest::Response,
    scheduled: Instant,
    ttfb: Duration,
) -> Result<ReqOutcome, ReqError> {
    let raw = resp.bytes().await?;
    let v: Value = serde_json::from_slice(&raw)
        .map_err(|e| ReqError::Protocol(format!("invalid JSON body: {e}")))?;
    if let Some(err) = v.get("error") {
        return Err(ReqError::Protocol(format!("error object: {err}")));
    }
    if !v.get("choices").is_some_and(Value::is_array) {
        return Err(ReqError::Protocol("response without choices".into()));
    }
    Ok(ReqOutcome {
        e2e: scheduled.elapsed(),
        ttfb,
        ttft: None,
        bytes: raw.len() as u64,
        usage: extract_usage(&v),
    })
}

async fn read_stream(
    resp: reqwest::Response,
    scheduled: Instant,
    ttfb: Duration,
) -> Result<ReqOutcome, ReqError> {
    let mut ttft = None;
    let mut bytes = 0u64;
    let mut usage = None;
    let mut stream = resp.bytes_stream();
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        bytes += chunk.len() as u64;
        // SSE 事件可能跨 TCP 分片：只处理完整的行，残余留在 buf 等下一个分片。
        buf.extend_from_slice(&chunk);
        let mut start = 0;
        while let Some(nl) = buf[start..].iter().position(|&b| b == b'\n') {
            let line = buf[start..start + nl].trim_ascii();
            start += nl + 1;
            // 空行、注释（": keep-alive"）、event:/id: 等字段不携带数据。
            let Some(data) = line.strip_prefix(b"data:") else {
                continue;
            };
            let data = data.trim_ascii();
            if data == b"[DONE]" {
                return Ok(ReqOutcome {
                    e2e: scheduled.elapsed(),
                    ttfb,
                    ttft,
                    bytes,
                    usage,
                });
            }
            let v: Value = serde_json::from_slice(data)
                .map_err(|e| ReqError::Protocol(format!("invalid SSE data: {e}")))?;
            if let Some(err) = v.get("error") {
                return Err(ReqError::Protocol(format!("error event: {err}")));
            }
            if ttft.is_none() && has_output_delta(&v) {
                ttft = Some(scheduled.elapsed());
            }
            if let Some(u) = extract_usage(&v) {
                usage = Some(u);
            }
        }
        buf.drain(..start);
    }
    Err(ReqError::Protocol(format!(
        "stream ended without [DONE] after {bytes} bytes"
    )))
}

/// chunk 是否携带模型输出（content / reasoning_content / tool_calls），用于判定首 token。
fn has_output_delta(v: &Value) -> bool {
    let Some(choices) = v.get("choices").and_then(Value::as_array) else {
        return false;
    };
    choices.iter().filter_map(|c| c.get("delta")).any(|d| {
        ["content", "reasoning_content"]
            .iter()
            .any(|k| d.get(*k).and_then(Value::as_str).is_some_and(|s| !s.is_empty()))
            || d.get("tool_calls")
                .and_then(Value::as_array)
                .is_some_and(|t| !t.is_empty())
    })
}

fn extract_usage(v: &Value) -> Option<(u64, u64)> {
    // 开启 include_usage 时，非末尾 chunk 的 usage 为 null。
    let u = v.get("usage").filter(|u| u.is_object())?;
    Some((
        u.get("prompt_tokens").and_then(Value::as_u64).unwrap_or(0),
        u.get("completion_tokens").and_then(Value::as_u64).unwrap_or(0),
    ))
}

/// oha 风格实时统计：清屏打印完整面板，包含直方图分位数、错误数、吞吐等。
fn spawn_progress(sh: Arc<Shared>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.tick().await;
        loop {
            tick.tick().await;
            let c = &sh.counters;
            let elapsed = sh.measure_start.elapsed().as_secs_f64();
            let sent = c.sent.load(Ordering::Relaxed);
            let done = c.done.load(Ordering::Relaxed);
            let errors = c.errors();
            let bytes = c.bytes.load(Ordering::Relaxed) as f64;
            let stream = c.stream_reqs.load(Ordering::Relaxed);
            let unary = c.unary_reqs.load(Ordering::Relaxed);
            let in_flight = sh.in_flight.load(Ordering::Relaxed);

            // 先拷贝直方图再释放锁，避免打印期间阻塞请求记录延迟。
            let e2e = sh.hists.e2e.lock().unwrap().clone();
            let (first_label, first) = if stream > 0 {
                ("首 token", sh.hists.ttft.lock().unwrap().clone())
            } else {
                ("首字节", sh.hists.ttfb.lock().unwrap().clone())
            };

            // 清屏 + 光标归位
            eprint!("\x1b[2J\x1b[H");
            eprintln!("=== 实时统计 ({elapsed:.1}s) ===");
            eprintln!("请求: {sent} 已发送 / {done} 完成 / {errors} 错误 / {in_flight} 在途");
            eprintln!(
                "吞吐: {:.1} req/s, {:.2} MB/s",
                done as f64 / elapsed,
                bytes / elapsed / 1e6
            );
            eprintln!("模式: {stream} 流式 / {unary} 非流式");
            eprintln!("端到端延迟 (µs): {}", quantiles_line(&e2e));
            eprintln!("{first_label}延迟 (µs): {}", quantiles_line(&first));
            eprintln!("直方图 (e2e): {}", histogram_sparkline(&e2e));
        }
    })
}

fn quantiles_line(h: &Histogram<u64>) -> String {
    format!(
        "p50={} p95={} p99={} max={}",
        h.value_at_quantile(0.5),
        h.value_at_quantile(0.95),
        h.value_at_quantile(0.99),
        h.max()
    )
}

/// 把 [min, max] 等分成 n 个线性桶，返回 (桶上界, 计数)。
fn linear_buckets(h: &Histogram<u64>, n: usize) -> Vec<(u64, u64)> {
    if h.is_empty() {
        return Vec::new();
    }
    let (lo, hi) = (h.min(), h.max());
    let span = (hi - lo).max(1) as f64;
    let mut out: Vec<(u64, u64)> = (1..=n)
        .map(|i| (lo + ((hi - lo) as f64 * i as f64 / n as f64) as u64, 0))
        .collect();
    for v in h.iter_recorded() {
        let x = v.value_iterated_to().saturating_sub(lo) as f64;
        let idx = ((x / span) * n as f64) as usize;
        out[idx.min(n - 1)].1 += v.count_at_value();
    }
    out
}

/// 生成 ASCII 直方图，直观展示延迟分布（oha 风格）。
fn histogram_sparkline(h: &Histogram<u64>) -> String {
    let bars = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
    let counts = linear_buckets(h, 20);
    let Some(max_count) = counts.iter().map(|&(_, c)| c).max() else {
        return "无数据".to_string();
    };
    counts
        .iter()
        .map(|&(_, c)| bars[((c as f64 / max_count as f64) * (bars.len() - 1) as f64) as usize])
        .collect()
}

async fn fetch_json(client: &reqwest::Client, url: &str) -> Option<Value> {
    let resp = client.get(url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    resp.json::<Value>().await.ok()
}

#[derive(Serialize)]
struct Report {
    config: Value,
    /// 计量窗口长度；各项速率 = 窗口内计划发送的请求的累计量 / 窗口长度。
    window_secs: f64,
    /// 窗口结束后等待窗口内请求全部返回的时间。
    drain_secs: f64,
    throughput: Value,
    latency_us: Value,
    tokens: Value,
    gateway_metrics: Option<Value>,
}

fn hist_json(h: &Histogram<u64>) -> Value {
    let buckets: Vec<Value> = linear_buckets(h, HIST_BUCKETS)
        .into_iter()
        .map(|(le, count)| json!({"le": le, "count": count}))
        .collect();
    json!({
        "count": h.len(),
        "min": h.min(),
        "p50": h.value_at_quantile(0.50),
        "p90": h.value_at_quantile(0.90),
        "p95": h.value_at_quantile(0.95),
        "p99": h.value_at_quantile(0.99),
        "max": h.max(),
        "mean": h.mean(),
        "buckets": buckets,
    })
}

fn build_report(sh: &Shared, drain: Duration, gateway_metrics: Option<Value>) -> Report {
    let c = &sh.counters;
    let cli = &sh.cli;
    let secs = cli.duration_secs as f64;
    let load = |a: &AtomicU64| a.load(Ordering::Relaxed);
    let done = load(&c.done) as f64;
    let errors = c.errors() as f64;
    let prompt = load(&c.prompt_tokens) as f64;
    let completion = load(&c.completion_tokens) as f64;
    let total = prompt + completion;
    let bytes = load(&c.bytes) as f64;
    // 只有带 usage 的请求才贡献 token，均值按它们计算。
    let with_usage = (done - load(&c.missing_usage) as f64).max(1.0);

    Report {
        config: json!({
            "gateway": cli.gateway,
            "duration_secs": cli.duration_secs,
            "warmup_secs": cli.warmup_secs,
            "concurrency": cli.concurrency,
            "rps": cli.rps,
            "stream_percent": cli.stream_percent,
            "timeout_secs": cli.timeout_secs,
            "prompt_chars": cli.prompt_chars,
            "vary_payload": cli.vary_payload,
            "model": cli.model,
            "temperature": cli.temperature,
            "top_p": cli.top_p,
            "n": cli.n,
            "frequency_penalty": cli.frequency_penalty,
            "presence_penalty": cli.presence_penalty,
        }),
        window_secs: secs,
        drain_secs: drain.as_secs_f64(),
        throughput: json!({
            "requests_sent": load(&c.sent),
            "requests_done": done,
            "requests_error": errors,
            "errors": {
                "http": load(&c.err_http),
                "transport": load(&c.err_transport),
                "protocol": load(&c.err_protocol),
            },
            "stream_requests": load(&c.stream_reqs),
            "unary_requests": load(&c.unary_reqs),
            "missing_usage": load(&c.missing_usage),
            "queued_for_concurrency": load(&c.queued),
            "dropped": load(&c.dropped),
            "req_per_sec": done / secs,
            "err_per_sec": errors / secs,
            "response_mb_per_sec": bytes / secs / 1e6,
        }),
        latency_us: json!({
            "e2e": hist_json(&sh.hists.e2e.lock().unwrap()),
            "ttfb": hist_json(&sh.hists.ttfb.lock().unwrap()),
            "ttft": hist_json(&sh.hists.ttft.lock().unwrap()),
        }),
        tokens: json!({
            "prompt_total": prompt,
            "completion_total": completion,
            "total": total,
            "prompt_per_req": prompt / with_usage,
            "completion_per_req": completion / with_usage,
            "completion_per_sec": completion / secs,
            "total_per_sec": total / secs,
        }),
        gateway_metrics,
    }
}

fn print_report(r: &Report) {
    let t = &r.throughput;
    let l = &r.latency_us;
    let tok = &r.tokens;
    let f = |v: &Value| v.as_f64().unwrap_or(0.0);
    let secs = r.window_secs;
    let done = f(&t["requests_done"]);
    let errors = f(&t["requests_error"]);
    let bytes = f(&t["response_mb_per_sec"]) * secs * 1e6;

    // oha 风格最终报告
    println!("\nSummary:");
    if done + errors > 0.0 {
        println!("  Success rate:  {:.2}%", 100.0 * done / (done + errors));
    } else {
        println!("  Success rate:  N/A (no completed requests)");
    }
    println!("  Window:        {secs:.2} secs (+{:.2} secs drain)", r.drain_secs);
    println!("  Slowest:       {:.4} secs", f(&l["e2e"]["max"]) / 1e6);
    println!("  Fastest:       {:.4} secs", f(&l["e2e"]["min"]) / 1e6);
    println!("  Average:       {:.4} secs", f(&l["e2e"]["mean"]) / 1e6);
    println!("  Requests/sec:  {:.1}", f(&t["req_per_sec"]));
    println!("  Total data:    {:.2} MiB", bytes / 1024.0 / 1024.0);
    println!("  Size/request:  {:.0} B", bytes / done.max(1.0));
    println!(
        "  Tokens/sec:    {:.0} completion, {:.0} total",
        f(&tok["completion_per_sec"]),
        f(&tok["total_per_sec"])
    );

    if errors > 0.0 {
        let e = &t["errors"];
        println!(
            "\nErrors: {} http / {} transport / {} protocol",
            e["http"], e["transport"], e["protocol"]
        );
    }
    if f(&t["missing_usage"]) > 0.0 {
        println!(
            "\n[warn] {} 个成功响应缺少 usage，token 统计不完整",
            t["missing_usage"]
        );
    }
    if f(&t["dropped"]) > 0.0 {
        println!(
            "\n[warn] {} 个计划请求到窗口结束仍因并发上限未能发出（dropped），实际负载低于目标 rps",
            t["dropped"]
        );
    }
    if f(&t["queued_for_concurrency"]) > 0.0 {
        println!(
            "\n[warn] {} 个请求在计划时刻因并发上限排队，目标 rps 未能按时发出（排队时间已计入延迟）",
            t["queued_for_concurrency"]
        );
    }

    println!("\nResponse time histogram (e2e):");
    if let Some(buckets) = l["e2e"]["buckets"].as_array() {
        let max_count = buckets.iter().map(|b| f(&b["count"])).fold(1.0, f64::max);
        for b in buckets {
            let count = f(&b["count"]);
            let bar = "■".repeat((count / max_count * 32.0).round() as usize);
            println!("  {:.4} secs [{count}] |{bar}", f(&b["le"]) / 1e6);
        }
    }

    println!("\nLatency distribution (e2e):");
    for key in ["p50", "p90", "p95", "p99"] {
        println!("  {key} in {:.4} secs", f(&l["e2e"][key]) / 1e6);
    }

    println!("\nDetails (average):");
    println!("  TTFB:          {:.4} secs", f(&l["ttfb"]["mean"]) / 1e6);
    if f(&l["ttft"]["count"]) > 0.0 {
        println!("  TTFT:          {:.4} secs", f(&l["ttft"]["mean"]) / 1e6);
    }
    println!("  E2E:           {:.4} secs", f(&l["e2e"]["mean"]) / 1e6);

    if let Some(g) = &r.gateway_metrics {
        println!("\n网关 /metrics:");
        println!("{}", serde_json::to_string_pretty(g).unwrap_or_default());
    }
}

/// 画 e2e / ttfb / ttft 的 p50/p95/p99 柱状图，空的直方图（如非流式的 ttft）跳过。
fn draw_latency_chart(report: &Report, path: &str) -> Result<()> {
    let bars: Vec<(String, u64, RGBColor)> = [("e2e", BLUE), ("ttfb", GREEN), ("ttft", RED)]
        .into_iter()
        .filter(|(k, _)| report.latency_us[*k]["count"].as_u64().unwrap_or(0) > 0)
        .flat_map(|(k, color)| {
            let h = &report.latency_us[k];
            ["p50", "p95", "p99"].map(|q| (format!("{k} {q}"), h[q].as_u64().unwrap_or(0), color))
        })
        .collect();
    if bars.is_empty() {
        bail!("no latency samples to chart");
    }
    let max_y = bars.iter().map(|(_, v, _)| *v).max().unwrap_or(0).max(1);
    // 顶部留白给柱顶标签。
    let top = max_y + max_y / 6 + 1;

    let root = BitMapBackend::new(path, (1024, 480)).into_drawing_area();
    root.fill(&WHITE)?;
    // 文本只用 ASCII：ttf 后端在系统缺 CJK 字体时会把中文渲染成空白。
    let mut chart = ChartBuilder::on(&root)
        .caption("Latency percentiles (us)", ("sans-serif", 28))
        .margin(20)
        .x_label_area_size(10)
        .y_label_area_size(70)
        .build_cartesian_2d(0f64..bars.len() as f64, 0u64..top)?;
    chart
        .configure_mesh()
        .disable_x_mesh()
        .x_labels(0)
        .y_desc("microseconds")
        .draw()?;

    for (i, (label, v, color)) in bars.iter().enumerate() {
        let x = i as f64;
        chart.draw_series(std::iter::once(Rectangle::new(
            [(x + 0.1, 0), (x + 0.9, *v)],
            color.filled(),
        )))?;
        chart.draw_series(std::iter::once(Text::new(
            format!("{label}: {v}"),
            (x + 0.1, *v + top / 20),
            ("sans-serif", 12).into_font(),
        )))?;
    }

    root.present()?;
    Ok(())
}
