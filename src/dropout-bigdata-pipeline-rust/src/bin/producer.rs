use anyhow::{Context, Result};
use clap::Parser;
use dropout_bigdata_pipeline_rust::StudentRecord;
use hdrhistogram::Histogram;
use polars::prelude::*;
use rdkafka::{
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord, Producer},
    util::Timeout,
};
use std::{
    env,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::Semaphore, time::sleep};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

// ── Constants ─────────────────────────────────────────────────────────────────

const MICROS_PER_SEC: u64 = 1_000_000;
const FLUSH_TIMEOUT: Duration = Duration::from_secs(15);
const SEND_TIMEOUT: Duration = Duration::from_secs(10);
const LOG_DIR: &str = "../../logs";

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "producer", version)]
struct Cli {
    #[arg(short, long, default_value_t = 100)]
    rate: u64,
    #[arg(long, default_value_t = 1)]
    iterations: usize,
    #[arg(long, default_value = "../../data/dataset_clean.csv")]
    dataset: PathBuf,
    #[arg(
        long,
        env = "KAFKA_BOOTSTRAP_SERVERS",
        default_value = "localhost:9092"
    )]
    brokers: String,
    #[arg(long, default_value = "student-data-rust")]
    topic: String,
    #[arg(long, default_value_t = 256)]
    inflight: usize,
}

// ── Shared counters (type alias keeps call-sites readable) ────────────────────

type Counter = Arc<AtomicU64>;

fn new_counter() -> Counter {
    Arc::new(AtomicU64::new(0))
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let cli = Cli::parse();
    let measure_latency = env::var("LATENCY_MEASURE").as_deref() == Ok("1");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &cli.brokers)
        .set("message.timeout.ms", "10000")
        .set("queue.buffering.max.messages", "200000")
        .set("batch.num.messages", "5000")
        .set("compression.type", "snappy")
        .set("acks", "1")
        .create()
        .context("Failed to create Kafka producer")?;

    info!("Loading dataset from: {:?}", cli.dataset);
    let records = load_dataset(&cli.dataset)?;
    info!(total = records.len(), "Dataset loaded into memory");

    let sent = new_counter();
    let errors = new_counter();
    let semaphore = Arc::new(Semaphore::new(cli.inflight));

    let (tx_lat, mut rx_lat) = tokio::sync::mpsc::channel::<u64>(100_000);

    let interval = (cli.rate > 0).then(|| Duration::from_micros(MICROS_PER_SEC / cli.rate));
    let start = Instant::now();
    let mut next_send = Instant::now();

    for iter in 0..cli.iterations {
        for (idx, rec) in records.iter().enumerate() {
            let rec = prepare_record(rec, iter, idx);
            let payload = serde_json::to_string(&rec)?;
            let key = sent.load(Ordering::Relaxed).to_string();

            if let Some(interval) = interval {
                let now = Instant::now();
                if now < next_send {
                    sleep(next_send - now).await;
                }
                next_send = Instant::now() + interval;
            }

            spawn_send(
                &producer,
                &cli.topic,
                payload,
                key,
                Arc::clone(&semaphore),
                Arc::clone(&sent),
                Arc::clone(&errors),
                tx_lat.clone(),
                measure_latency,
            );
        }
    }

    producer.flush(Timeout::After(FLUSH_TIMEOUT)).ok();
    drop(tx_lat);

    let mut lat_hist = Histogram::<u64>::new(3).unwrap();
    while let Some(us) = rx_lat.recv().await {
        lat_hist.record(us).unwrap_or(());
    }

    let elapsed = start.elapsed().as_secs_f64();
    let total = sent.load(Ordering::Relaxed);

    println!("\n=== RUST PRODUCER FINISHED ===");
    println!(
        "Throughput: {:.2} rec/s | Sent: {} | Iterations: {}",
        total as f64 / elapsed,
        total,
        cli.iterations,
    );
    if measure_latency {
        println!("Note: true network RTT was measured — expect lower throughput.");
    }

    save_result(
        &cli,
        total,
        errors.load(Ordering::Relaxed),
        elapsed,
        &lat_hist,
    )
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Stamp an event ID and timestamp onto a cloned record.
fn prepare_record(rec: &StudentRecord, iter: usize, idx: usize) -> StudentRecord {
    let mut out = rec.clone();
    out.event_id = format!("{iter}_{idx}");
    out.event_ts_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    out
}

/// Spawn a Tokio task that sends one message and records latency.
#[allow(clippy::too_many_arguments)]
fn spawn_send(
    producer: &FutureProducer,
    topic: &str,
    payload: String,
    key: String,
    semaphore: Arc<Semaphore>,
    sent: Counter,
    errors: Counter,
    tx_lat: tokio::sync::mpsc::Sender<u64>,
    measure_latency: bool,
) {
    let producer = producer.clone();
    let topic = topic.to_owned();
    let t_dispatch = Instant::now();

    tokio::spawn(async move {
        let _permit = semaphore.acquire_owned().await;
        let t_network = Instant::now();

        match producer
            .send(
                FutureRecord::to(&topic)
                    .key(key.as_bytes())
                    .payload(payload.as_bytes()),
                Timeout::After(SEND_TIMEOUT),
            )
            .await
        {
            Ok(_) => {
                sent.fetch_add(1, Ordering::Relaxed);
                if measure_latency {
                    let _ = tx_lat.send(t_network.elapsed().as_micros() as u64).await;
                }
            }
            Err(e) => {
                errors.fetch_add(1, Ordering::Relaxed);
                warn!("Send failed: {e:?}");
            }
        }

        if !measure_latency {
            let _ = tx_lat.send(t_dispatch.elapsed().as_micros() as u64).await;
        }
    });
}

// ── Dataset loading ───────────────────────────────────────────────────────────

fn load_dataset(path: &PathBuf) -> Result<Vec<StudentRecord>> {
    let mut df = CsvReadOptions::default()
        .with_has_header(true)
        .with_ignore_errors(true)
        .try_into_reader_with_file_path(Some(path.clone()))
        .context("Failed to set up CSV reader")?
        .finish()
        .context("Failed to read CSV")?;

    // Trim whitespace from column names in-place.
    let col_names: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();
    for name in &col_names {
        let trimmed: PlSmallStr = name.trim().into();
        let _ = df.rename(name.as_str(), trimmed);
    }

    (0..df.height()).map(|i| extract_record(&df, i)).collect()
}

/// Extract a single `StudentRecord` from a row of the `DataFrame`.
fn extract_record(df: &DataFrame, i: usize) -> Result<StudentRecord> {
    let get_i32 = |col: &str| -> i32 {
        df.column(col)
            .ok()
            .and_then(|c| c.get(i).ok())
            .and_then(|v| v.try_extract::<i32>().ok())
            .unwrap_or(0)
    };
    let get_f64 = |col: &str| -> f64 {
        df.column(col)
            .ok()
            .and_then(|c| c.get(i).ok())
            .and_then(|v| v.try_extract::<f64>().ok())
            .unwrap_or(0.0)
    };
    let get_str = |col: &str| -> String {
        df.column(col)
            .ok()
            .and_then(|c| c.get(i).ok())
            .map(|v| match v {
                AnyValue::String(s) => s.to_owned(),
                other => other.to_string().trim_matches('"').to_owned(),
            })
            .unwrap_or_default()
    };

    Ok(StudentRecord {
        marital_status: get_i32("Marital status"),
        gender: get_i32("Gender"),
        age_at_enrollment: get_i32("Age at enrollment"),
        international: get_i32("International"),
        displaced: get_i32("Displaced"),
        educational_special_needs: get_i32("Educational special needs"),
        nacionality: get_i32("Nacionality"),
        mothers_qualification: get_i32("Mother's qualification"),
        fathers_qualification: get_i32("Father's qualification"),
        mothers_occupation: get_i32("Mother's occupation"),
        fathers_occupation: get_i32("Father's occupation"),
        scholarship_holder: get_i32("Scholarship holder"),
        debtor: get_i32("Debtor"),
        tuition_fees_up_to_date: get_i32("Tuition fees up to date"),
        application_mode: get_i32("Application mode"),
        application_order: get_i32("Application order"),
        course: get_i32("Course"),
        daytime_evening_attendance: get_i32("Daytime/evening attendance"),
        previous_qualification: get_i32("Previous qualification"),
        previous_qualification_grade: get_f64("Previous qualification (grade)"),
        admission_grade: get_f64("Admission grade"),
        curricular_units_1st_sem_credited: get_i32("Curricular units 1st sem (credited)"),
        curricular_units_1st_sem_enrolled: get_i32("Curricular units 1st sem (enrolled)"),
        curricular_units_1st_sem_evaluations: get_i32("Curricular units 1st sem (evaluations)"),
        curricular_units_1st_sem_approved: get_i32("Curricular units 1st sem (approved)"),
        curricular_units_1st_sem_grade: get_f64("Curricular units 1st sem (grade)"),
        curricular_units_1st_sem_without_evaluations: get_i32(
            "Curricular units 1st sem (without evaluations)",
        ),
        curricular_units_2nd_sem_credited: get_i32("Curricular units 2nd sem (credited)"),
        curricular_units_2nd_sem_enrolled: get_i32("Curricular units 2nd sem (enrolled)"),
        curricular_units_2nd_sem_evaluations: get_i32("Curricular units 2nd sem (evaluations)"),
        curricular_units_2nd_sem_approved: get_i32("Curricular units 2nd sem (approved)"),
        curricular_units_2nd_sem_grade: get_f64("Curricular units 2nd sem (grade)"),
        curricular_units_2nd_sem_without_evaluations: get_i32(
            "Curricular units 2nd sem (without evaluations)",
        ),
        unemployment_rate: get_f64("Unemployment rate"),
        inflation_rate: get_f64("Inflation rate"),
        gdp: get_f64("GDP"),
        target: get_str("Target"),
        event_id: String::new(),
        event_ts_ms: 0,
    })
}

// ── Persistence ───────────────────────────────────────────────────────────────

fn save_result(
    cli: &Cli,
    total: u64,
    errors: u64,
    elapsed: f64,
    hist: &Histogram<u64>,
) -> Result<()> {
    use std::io::Write;

    std::fs::create_dir_all(LOG_DIR)?;

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let path = format!(
        "{LOG_DIR}/producer_rust_rate{}_scale{}_{ts}.json",
        cli.rate, cli.iterations,
    );

    let json = serde_json::json!({
        "impl":          "rust",
        "target_rate":   cli.rate,
        "scale_factor":  cli.iterations,
        "total_sent":    total,
        "total_errors":  errors,
        "total_time_s":  elapsed,
        "avg_rate":      total as f64 / elapsed,
        "latency_us": {
            "p50": hist.value_at_quantile(0.50),
            "p99": hist.value_at_quantile(0.99),
        },
    });

    let mut f = std::fs::File::create(&path)?;
    write!(f, "{}", serde_json::to_string_pretty(&json)?)?;
    Ok(())
}
