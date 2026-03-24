//! Feldera benchmark runner — zero-dependency lifecycle manager.
//!
//! Runs as PID 1 inside the Feldera container:
//!   1. Starts pipeline-manager as a child process
//!   2. Creates & compiles a benchmark pipeline via REST API
//!   3. Starts the pipeline, writes /tmp/feldera-healthy
//!   4. Waits for SIGTERM, then gracefully stops everything

use std::env;
use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Command, Child, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

// ── Signal handling (zero deps) ─────────────────────────────────────

static SHUTDOWN: AtomicBool = AtomicBool::new(false);

extern "C" fn on_signal(_sig: i32) {
    SHUTDOWN.store(true, Ordering::SeqCst);
}

extern "C" {
    fn signal(sig: i32, handler: extern "C" fn(i32)) -> usize;
}

fn setup_signals() {
    unsafe {
        signal(15, on_signal); // SIGTERM
        signal(2, on_signal);  // SIGINT
    }
}

// ── Minimal HTTP client (HTTP/1.0, no chunked encoding) ─────────────

fn http(method: &str, path: &str, body: Option<&str>) -> Result<(u16, String), String> {
    let mut stream = TcpStream::connect("127.0.0.1:8080")
        .map_err(|e| format!("connect: {e}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .ok();

    let payload = body.unwrap_or("");
    let mut req = format!("{method} {path} HTTP/1.0\r\nHost: localhost\r\n");
    if body.is_some() {
        req += &format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            payload.len()
        );
    }
    req += "\r\n";
    req += payload;

    stream.write_all(req.as_bytes()).map_err(|e| format!("write: {e}"))?;
    stream.flush().ok();

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).ok();
    let raw = String::from_utf8_lossy(&buf).to_string();

    let status = raw
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0u16);

    let body_str = raw
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();

    Ok((status, body_str))
}

// ── JSON helpers (no serde — values are simple strings/ints) ────────

/// Produce a JSON-encoded string value: `"hello \"world\""`.
fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Extract the first `"key": "value"` from a JSON blob (simple, no nesting).
fn json_field<'a>(json: &'a str, key: &str) -> &'a str {
    let needle = format!("\"{}\"", key);
    let Some(pos) = json.find(&needle) else {
        return "";
    };
    let rest = &json[pos + needle.len()..];
    // skip optional whitespace and colon
    let rest = rest.trim_start();
    let rest = rest.strip_prefix(':').unwrap_or(rest).trim_start();
    if let Some(rest) = rest.strip_prefix('"') {
        let end = rest.find('"').unwrap_or(rest.len());
        &rest[..end]
    } else {
        let end = rest
            .find(|c: char| c == ',' || c == '}' || c == ' ' || c == '\n')
            .unwrap_or(rest.len());
        &rest[..end]
    }
}

// ── Pipeline SQL builder ────────────────────────────────────────────

fn build_sql(
    eh_conn: &str,
    eh_name: &str,
    bootstrap: &str,
    delta_uri: &str,
    adls_account: &str,
    adls_key: &str,
) -> String {
    // Build Kafka connector JSON array
    let kafka_json = format!(
        concat!(
            r#"[{{"transport":{{"name":"kafka_input","config":{{"#,
            r#""topic":{topic},"#,
            r#""start_from":"latest","#,
            r#""bootstrap.servers":{bootstrap},"#,
            r#""security.protocol":"SASL_SSL","#,
            r#""sasl.mechanism":"PLAIN","#,
            r#""sasl.username":"$ConnectionString","#,
            r#""sasl.password":{password}"#,
            r#"}}}},"format":{{"name":"json","config":{{"update_format":"raw"}}}}}}]"#
        ),
        topic = json_str(eh_name),
        bootstrap = json_str(bootstrap),
        password = json_str(eh_conn),
    );

    // Build Delta Lake connector JSON array
    let delta_json = format!(
        concat!(
            r#"[{{"transport":{{"name":"delta_table_output","config":{{"#,
            r#""uri":{uri},"#,
            r#""mode":"truncate","#,
            r#""azure_storage_account_name":{account},"#,
            r#""azure_storage_account_key":{key}"#,
            r#"}}}},"enable_output_buffer":true,"max_output_buffer_time_millis":1000}}]"#
        ),
        uri = json_str(delta_uri),
        account = json_str(adls_account),
        key = json_str(adls_key),
    );

    // Assemble SQL — inner JSON uses double quotes, embedded in SQL single quotes
    format!(
        concat!(
            "CREATE TABLE events (\n",
            "    ts TIMESTAMP NOT NULL,\n",
            "    producer_id INT,\n",
            "    seq BIGINT\n",
            ") WITH (\n",
            "    'connectors' = '{kafka}'\n",
            ");\n",
            "\n",
            "CREATE VIEW benchmark_output\n",
            "WITH (\n",
            "    'connectors' = '{delta}'\n",
            ") AS SELECT ts FROM events;"
        ),
        kafka = kafka_json,
        delta = delta_json,
    )
}

// ── Pipeline manager lifecycle ──────────────────────────────────────

const PM_BIN: &str = "/home/ubuntu/feldera/build/pipeline-manager";
const PM_ARGS: &[&str] = &[
    "--bind-address=0.0.0.0",
    "--sql-compiler-path=/home/ubuntu/feldera/build/sql2dbsp-jar-with-dependencies.jar",
    "--compilation-cargo-lock-path=/home/ubuntu/feldera/Cargo.lock",
    "--dbsp-override-path=/home/ubuntu/feldera",
    "--demos-dir",
    "/home/ubuntu/feldera/demos/sql",
];

fn start_pipeline_manager() -> Child {
    Command::new(PM_BIN)
        .args(PM_ARGS)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("failed to start pipeline-manager")
}

fn wait_pm_healthy() {
    eprintln!("[runner] Waiting for pipeline-manager...");
    loop {
        if http("GET", "/healthz", None).is_ok_and(|(s, _)| s == 200) {
            break;
        }
        thread::sleep(Duration::from_secs(1));
    }
    eprintln!("[runner] Pipeline-manager healthy.");
}

// ── Main ────────────────────────────────────────────────────────────

fn main() {
    setup_signals();

    // Read config from environment
    let eh_conn = env::var("EVENTHUB_CONNECTION_STRING")
        .expect("EVENTHUB_CONNECTION_STRING must be set");
    let eh_name = env::var("EVENTHUB_NAME").unwrap_or_else(|_| "benchmark".into());
    let adls_account =
        env::var("ADLSG2_ACCOUNT_NAME").expect("ADLSG2_ACCOUNT_NAME must be set");
    let adls_key =
        env::var("ADLSG2_ACCOUNT_KEY").expect("ADLSG2_ACCOUNT_KEY must be set");
    let adls_container =
        env::var("ADLSG2_CONTAINER").unwrap_or_else(|_| "benchmark".into());
    let workers: u32 = env::var("FELDERA_WORKERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16);

    // Derive connection parameters
    let namespace = eh_conn
        .split("sb://")
        .nth(1)
        .and_then(|s| s.split('.').next())
        .expect("Cannot extract namespace from EVENTHUB_CONNECTION_STRING");
    let bootstrap = format!("{namespace}.servicebus.windows.net:9093");
    let delta_uri = format!(
        "abfss://{adls_container}@{adls_account}.dfs.core.windows.net/feldera/benchmark"
    );

    // 1. Start pipeline-manager
    let mut pm = start_pipeline_manager();
    wait_pm_healthy();

    // 2. Create pipeline
    let sql = build_sql(&eh_conn, &eh_name, &bootstrap, &delta_uri, &adls_account, &adls_key);
    let body = format!(
        concat!(
            r#"{{"name":"benchmark","description":"Stream processing benchmark - Feldera","#,
            r#""program_code":{sql},"#,
            r#""program_config":{{"profile":"optimized"}},"#,
            r#""runtime_config":{{"workers":{workers}}}}}"#
        ),
        sql = json_str(&sql),
        workers = workers,
    );

    let (status, resp) = http("PUT", "/v0/pipelines/benchmark", Some(&body))
        .expect("Failed to create pipeline");
    eprintln!("[runner] Create pipeline: HTTP {status}");
    if status >= 400 {
        eprintln!("[runner] Error: {resp}");
        pm.kill().ok();
        std::process::exit(1);
    }

    // 3. Wait for compilation
    eprintln!("[runner] Compiling pipeline (this takes a few minutes)...");
    let t0 = Instant::now();
    loop {
        let (_, resp) = http("GET", "/v0/pipelines/benchmark", None)
            .expect("Failed to poll pipeline");
        let ps = json_field(&resp, "program_status");
        match ps {
            "Success" => {
                eprintln!("[runner] Compiled in {:.0}s.", t0.elapsed().as_secs_f64());
                break;
            }
            "SqlError" | "RustError" | "SystemError" => {
                eprintln!("[runner] Compilation FAILED ({ps}):");
                eprintln!("{resp}");
                pm.kill().ok();
                std::process::exit(1);
            }
            _ => thread::sleep(Duration::from_secs(5)),
        }
    }

    // 4. Start pipeline
    let (status, _) = http("POST", "/v0/pipelines/benchmark/start?initial=running", Some(""))
        .expect("Failed to start pipeline");
    eprintln!("[runner] Start request: HTTP {status}");

    loop {
        let (_, resp) = http("GET", "/v0/pipelines/benchmark", None)
            .expect("Failed to poll pipeline");
        if json_field(&resp, "deployment_status") == "Running" {
            break;
        }
        thread::sleep(Duration::from_secs(2));
    }
    eprintln!("[runner] Pipeline running.");

    // 5. Signal healthy
    fs::write("/tmp/feldera-healthy", "ok").expect("Cannot write health file");
    eprintln!("[runner] Health file written. Processing data...");

    // 6. Wait for SIGTERM
    while !SHUTDOWN.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(200));
    }

    // 7. Graceful shutdown
    fs::remove_file("/tmp/feldera-healthy").ok();
    eprintln!("[runner] SIGTERM received — stopping pipeline...");

    http("POST", "/v0/pipelines/benchmark/stop?force=true", Some("")).ok();

    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if Instant::now() > deadline {
            eprintln!("[runner] Timeout waiting for pipeline to stop.");
            break;
        }
        if let Ok((_, resp)) = http("GET", "/v0/pipelines/benchmark", None) {
            if json_field(&resp, "deployment_status") == "Stopped" {
                eprintln!("[runner] Pipeline stopped.");
                break;
            }
        }
        thread::sleep(Duration::from_secs(2));
    }

    eprintln!("[runner] Shutting down pipeline-manager...");
    pm.kill().ok();
    pm.wait().ok();
    eprintln!("[runner] Done.");
}
