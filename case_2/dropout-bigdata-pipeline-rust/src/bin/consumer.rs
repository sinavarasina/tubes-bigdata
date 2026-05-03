use anyhow::{Context, Result};
use clap::Parser;
use dropout_bigdata_pipeline_rust::StudentRecord;
use polars::prelude::*;
use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, StreamConsumer},
    message::Message,
};
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};
use tokio::time::timeout;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "consumer")]
struct Cli {
    #[arg(
        long,
        env = "KAFKA_BOOTSTRAP_SERVERS",
        default_value = "localhost:9092"
    )]
    brokers: String,
    #[arg(long, env = "KAFKA_TOPIC", default_value = "student-data-rust")]
    topic: String,
    #[arg(long, env = "KAFKA_GROUP_ID", default_value = "dropout-consumer-rust")]
    group_id: String,
    #[arg(
        long,
        env = "PARQUET_OUT_DIR",
        default_value = "../../data/preprocessed"
    )]
    output: PathBuf,
    #[arg(long, default_value_t = 5_000)]
    batch_size: usize,
    #[arg(long, default_value_t = 10)]
    idle_timeout: u64,
}

// ── Loop control ──────────────────────────────────────────────────────────────

enum LoopAction {
    Continue,
    FlushAndStop,
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let cli = Cli::parse();
    std::fs::create_dir_all(&cli.output)?;

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &cli.brokers)
        .set("group.id", &cli.group_id)
        .set("auto.offset.reset", "earliest")
        .create()
        .context("Failed to create Kafka consumer")?;

    consumer
        .subscribe(&[cli.topic.as_str()])
        .context("Failed to subscribe to topic")?;

    let mut batch: Vec<StudentRecord> = Vec::with_capacity(cli.batch_size);
    let mut batch_idx: usize = 0;
    let mut total_recv: u64 = 0;
    let start = Instant::now();
    let idle = Duration::from_secs(cli.idle_timeout);

    info!("Waiting for data on topic: {}", cli.topic);

    loop {
        let action = match timeout(idle, consumer.recv()).await {
            Err(_elapsed) => LoopAction::FlushAndStop,
            Ok(Err(e)) => {
                warn!("Kafka error: {e}");
                LoopAction::Continue
            }
            Ok(Ok(msg)) => {
                if let Some(payload) = msg.payload() {
                    if let Ok(rec) = serde_json::from_slice::<StudentRecord>(payload) {
                        total_recv += 1;
                        batch.push(rec);
                    }
                }
                LoopAction::Continue
            }
        };

        // Flush when the batch is full.
        if batch.len() >= cli.batch_size {
            flush_batch(&batch, &cli.output, batch_idx).await?;
            batch.clear();
            batch_idx += 1;
        }

        match action {
            LoopAction::Continue => {}
            LoopAction::FlushAndStop => {
                if !batch.is_empty() {
                    flush_batch(&batch, &cli.output, batch_idx).await?;
                }
                info!("Idle timeout reached — stopping.");
                break;
            }
        }
    }

    println!("\n=== RUST CONSUMER FINISHED ===");
    println!(
        "Total received: {} | Time: {:.2} s",
        total_recv,
        start.elapsed().as_secs_f64(),
    );
    Ok(())
}

// ── Batch flushing ────────────────────────────────────────────────────────────

async fn flush_batch(batch: &[StudentRecord], output: &PathBuf, idx: usize) -> Result<()> {
    let mut df = records_to_dataframe(batch)?;
    let path = output.join(format!("batch_{idx:04}.parquet"));
    let mut file = std::fs::File::create(&path)?;

    ParquetWriter::new(&mut file)
        .with_compression(ParquetCompression::Snappy)
        .finish(&mut df)?;

    info!("Saved: {} ({} rows)", path.display(), batch.len());
    Ok(())
}

// ── DataFrame builder ─────────────────────────────────────────────────────────

fn records_to_dataframe(records: &[StudentRecord]) -> Result<DataFrame> {
    // Collect each column once to avoid repeated iteration.
    macro_rules! col_i32 {
        ($field:ident) => {
            records.iter().map(|r| r.$field).collect::<Vec<i32>>()
        };
    }
    macro_rules! col_f64 {
        ($field:ident) => {
            records.iter().map(|r| r.$field).collect::<Vec<f64>>()
        };
    }
    macro_rules! col_computed {
        ($method:ident, $ty:ty) => {
            records.iter().map(|r| r.$method()).collect::<Vec<$ty>>()
        };
    }

    let df = df!(
        "marital_status"                              => col_i32!(marital_status),
        "gender"                                      => col_i32!(gender),
        "age_at_enrollment"                           => col_i32!(age_at_enrollment),
        "international"                               => col_i32!(international),
        "displaced"                                   => col_i32!(displaced),
        "educational_special_needs"                   => col_i32!(educational_special_needs),
        "nacionality"                                 => col_i32!(nacionality),
        "mothers_qualification"                       => col_i32!(mothers_qualification),
        "fathers_qualification"                       => col_i32!(fathers_qualification),
        "mothers_occupation"                          => col_i32!(mothers_occupation),
        "fathers_occupation"                          => col_i32!(fathers_occupation),
        "scholarship_holder"                          => col_i32!(scholarship_holder),
        "debtor"                                      => col_i32!(debtor),
        "tuition_fees_up_to_date"                     => col_i32!(tuition_fees_up_to_date),
        "application_mode"                            => col_i32!(application_mode),
        "application_order"                           => col_i32!(application_order),
        "course"                                      => col_i32!(course),
        "daytime_evening_attendance"                  => col_i32!(daytime_evening_attendance),
        "previous_qualification"                      => col_i32!(previous_qualification),
        "previous_qualification_grade"                => col_f64!(previous_qualification_grade),
        "admission_grade"                             => col_f64!(admission_grade),
        "curricular_units_1st_sem_credited"           => col_i32!(curricular_units_1st_sem_credited),
        "curricular_units_1st_sem_enrolled"           => col_i32!(curricular_units_1st_sem_enrolled),
        "curricular_units_1st_sem_evaluations"        => col_i32!(curricular_units_1st_sem_evaluations),
        "curricular_units_1st_sem_approved"           => col_i32!(curricular_units_1st_sem_approved),
        "curricular_units_1st_sem_grade"              => col_f64!(curricular_units_1st_sem_grade),
        "curricular_units_1st_sem_without_evaluations"=> col_i32!(curricular_units_1st_sem_without_evaluations),
        "curricular_units_2nd_sem_credited"           => col_i32!(curricular_units_2nd_sem_credited),
        "curricular_units_2nd_sem_enrolled"           => col_i32!(curricular_units_2nd_sem_enrolled),
        "curricular_units_2nd_sem_evaluations"        => col_i32!(curricular_units_2nd_sem_evaluations),
        "curricular_units_2nd_sem_approved"           => col_i32!(curricular_units_2nd_sem_approved),
        "curricular_units_2nd_sem_grade"              => col_f64!(curricular_units_2nd_sem_grade),
        "curricular_units_2nd_sem_without_evaluations"=> col_i32!(curricular_units_2nd_sem_without_evaluations),
        "unemployment_rate"                           => col_f64!(unemployment_rate),
        "inflation_rate"                              => col_f64!(inflation_rate),
        "gdp"                                         => col_f64!(gdp),
        "pass_rate_1st_sem"                           => col_computed!(pass_rate_1st, f64),
        "pass_rate_2nd_sem"                           => col_computed!(pass_rate_2nd, f64),
        "grade_delta"                                 => col_computed!(grade_delta, f64),
        "financial_stability_index"                   => col_computed!(financial_stability_index, f64),
        "target"                                      => records.iter().map(|r| r.target.clone()).collect::<Vec<String>>(),
        "label"                                       => records.iter().map(|r| r.label_i32()).collect::<Vec<i32>>(),
        "event_ts_ms"                                 => records.iter().map(|r| r.event_ts_ms).collect::<Vec<u64>>(),
    )
    .context("Failed to build Parquet DataFrame")?;

    Ok(df)
}
