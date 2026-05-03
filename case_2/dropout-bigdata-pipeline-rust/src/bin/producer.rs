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
    time::{Duration, Instant},
};
use tokio::{
    sync::{Semaphore, mpsc},
    time::sleep,
};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();
    let cli = Cli::parse();

    let measure_latency = env::var("LATENCY_MEASURE").unwrap_or_else(|_| "0".to_string()) == "1";

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
    info!(
        total = records.len(),
        "Dataset successfully parsed into memory"
    );

    let sent_total = Arc::new(AtomicU64::new(0));
    let error_total = Arc::new(AtomicU64::new(0));
    let semaphore = Arc::new(Semaphore::new(cli.inflight));

    let (tx_lat, mut rx_lat) = mpsc::channel::<u64>(100_000);

    let interval_us = if cli.rate > 0 {
        Some(1_000_000 / cli.rate)
    } else {
        None
    };

    let start = Instant::now();
    let mut next_ns = Instant::now();

    for iter in 0..cli.iterations {
        for (idx, rec) in records.iter().enumerate() {
            let mut rec = rec.clone();
            rec.event_id = format!("{}_{}", iter, idx);
            rec.event_ts_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            let payload = serde_json::to_string(&rec)?;
            let key = format!("{}", sent_total.load(Ordering::Relaxed));

            if let Some(ius) = interval_us {
                let now = Instant::now();
                if now < next_ns {
                    sleep(next_ns - now).await;
                }
                next_ns = Instant::now() + Duration::from_micros(ius);
            }

            let permit = semaphore.clone().acquire_owned().await?;
            let prod = producer.clone();
            let topic = cli.topic.clone();
            let sent_c = sent_total.clone();
            let err_c = error_total.clone();

            let t_dispatch = Instant::now();
            let tx_lat_clone = tx_lat.clone();

            tokio::spawn(async move {
                let _permit = permit;
                let t_network = Instant::now();

                match prod
                    .send(
                        FutureRecord::to(&topic)
                            .key(key.as_bytes())
                            .payload(payload.as_bytes()),
                        Timeout::After(Duration::from_secs(10)),
                    )
                    .await
                {
                    Ok(_) => {
                        sent_c.fetch_add(1, Ordering::Relaxed);
                        if measure_latency {
                            let _ = tx_lat_clone
                                .send(t_network.elapsed().as_micros() as u64)
                                .await;
                        }
                    }
                    Err(e) => {
                        err_c.fetch_add(1, Ordering::Relaxed);
                        warn!("Failed to send: {:?}", e);
                    }
                }
            });

            if !measure_latency {
                let _ = tx_lat.send(t_dispatch.elapsed().as_micros() as u64).await;
            }
        }
    }

    let _ = producer.flush(Timeout::After(Duration::from_secs(15)));
    drop(tx_lat);

    let mut lat_hist = Histogram::<u64>::new(3).unwrap();

    while let Some(latency_micros) = rx_lat.recv().await {
        lat_hist.record(latency_micros).unwrap_or(());
    }

    let elapsed = start.elapsed().as_secs_f64();
    let total = sent_total.load(Ordering::Relaxed);

    println!("\n=== RUST PRODUCER FINISHED ===");
    println!(
        "Throughput: {:.2} rec/s | Sent: {} | Iterations: {}",
        total as f64 / elapsed,
        total,
        cli.iterations
    );
    if measure_latency {
        println!("Note: True network RTT latency was measured. Expect lower throughput.");
    }

    save_result(
        &cli,
        total,
        error_total.load(Ordering::Relaxed),
        elapsed,
        &lat_hist,
    )?;
    Ok(())
}

fn load_dataset(path: &PathBuf) -> Result<Vec<StudentRecord>> {
    let mut df = CsvReadOptions::default()
        .with_has_header(true)
        .with_ignore_errors(true)
        .try_into_reader_with_file_path(Some(path.clone()))
        .context("Failed to setup reader")?
        .finish()
        .context("Failed to read CSV")?;

    let col_names: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();
    for name in col_names {
        let new_name: PlSmallStr = name.trim().into();
        let _ = df.rename(name.as_str(), new_name);
    }

    let mut records = Vec::with_capacity(df.height());

    for i in 0..df.height() {
        let get_i32 = |col_name: &str| -> i32 {
            if let Ok(col) = df.column(col_name) {
                if let Ok(val) = col.get(i) {
                    if let Ok(v) = val.try_extract::<i32>() {
                        return v;
                    }
                }
            }
            0
        };
        let get_f64 = |col_name: &str| -> f64 {
            if let Ok(col) = df.column(col_name) {
                if let Ok(val) = col.get(i) {
                    if let Ok(v) = val.try_extract::<f64>() {
                        return v;
                    }
                }
            }
            0.0
        };
        let get_str = |col_name: &str| -> String {
            if let Ok(col) = df.column(col_name) {
                if let Ok(val) = col.get(i) {
                    if let AnyValue::String(s) = val {
                        return s.to_string();
                    } else {
                        return val.to_string().trim_matches('"').to_owned();
                    }
                }
            }
            String::new()
        };

        records.push(StudentRecord {
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
        });
    }
    Ok(records)
}

fn save_result(
    cli: &Cli,
    total: u64,
    errors: u64,
    elapsed: f64,
    hist: &Histogram<u64>,
) -> Result<()> {
    use std::io::Write;
    std::fs::create_dir_all("../../logs")?;
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let path = format!(
        "../../logs/producer_rust_rate{}_scale{}_{}.json",
        cli.rate, cli.iterations, ts
    );
    let json = serde_json::json!({
        "impl": "rust", "target_rate": cli.rate, "scale_factor": cli.iterations, "total_sent": total, "total_errors": errors,
        "total_time_s": elapsed, "avg_rate": total as f64 / elapsed,
        "latency_us": { "p50": hist.value_at_quantile(0.50), "p99": hist.value_at_quantile(0.99) }
    });
    let mut f = std::fs::File::create(&path)?;
    write!(f, "{}", serde_json::to_string_pretty(&json)?)?;
    Ok(())
}
