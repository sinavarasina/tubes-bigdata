use anyhow::{Context, Result};
use clap::Parser;
use dropout_bigdata_pipeline_rust::StudentRecord;
use hdrhistogram::Histogram;
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
    #[arg(long, default_value_t = 5000)]
    batch_size: usize,
    #[arg(long, default_value_t = 10)]
    idle_timeout: u64,
}

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
        .subscribe(&[&cli.topic])
        .context("Failed to subscribe to topic")?;

    let mut batch = Vec::with_capacity(cli.batch_size);
    let mut batch_idx = 0usize;
    let mut total_recv = 0u64;
    let mut lat_hist = Histogram::<u64>::new(3).unwrap();
    let start = Instant::now();

    info!("Waiting for data from topic: {}", cli.topic);

    loop {
        let msg_result = timeout(Duration::from_secs(cli.idle_timeout), consumer.recv()).await;
        match msg_result {
            Err(_) => {
                if !batch.is_empty() {
                    flush_batch(&batch, &cli.output, batch_idx).await?;
                    batch.clear();
                }
                info!("Idle timeout reached. Finished pulling data.");
                break;
            }
            Ok(Ok(msg)) => {
                let t_recv = Instant::now();
                total_recv += 1;

                if let Some(payload) = msg.payload() {
                    if let Ok(rec) = serde_json::from_slice::<StudentRecord>(payload) {
                        batch.push(rec);
                        lat_hist
                            .record(t_recv.elapsed().as_micros() as u64)
                            .unwrap_or(());
                    }
                }

                if batch.len() >= cli.batch_size {
                    flush_batch(&batch, &cli.output, batch_idx).await?;
                    batch.clear();
                    batch_idx += 1;
                }
            }
            Ok(Err(e)) => warn!("Kafka error: {}", e),
        }
    }

    println!("\n=== RUST CONSUMER FINISHED ===");
    println!(
        "Total Received: {} | Time: {:.2} seconds",
        total_recv,
        start.elapsed().as_secs_f64()
    );
    Ok(())
}

async fn flush_batch(batch: &[StudentRecord], output: &PathBuf, batch_idx: usize) -> Result<()> {
    let mut df = records_to_dataframe(batch)?;
    let path = output.join(format!("batch_{:04}.parquet", batch_idx));
    let mut file = std::fs::File::create(&path)?;

    ParquetWriter::new(&mut file)
        .with_compression(ParquetCompression::Snappy)
        .finish(&mut df)?;

    info!("Saved: {} ({} rows)", path.display(), batch.len());
    Ok(())
}

fn records_to_dataframe(records: &[StudentRecord]) -> Result<DataFrame> {
    let df = df!(
        "marital_status" => records.iter().map(|r| r.marital_status).collect::<Vec<i32>>(),
        "gender" => records.iter().map(|r| r.gender).collect::<Vec<i32>>(),
        "age_at_enrollment" => records.iter().map(|r| r.age_at_enrollment).collect::<Vec<i32>>(),
        "international" => records.iter().map(|r| r.international).collect::<Vec<i32>>(),
        "displaced" => records.iter().map(|r| r.displaced).collect::<Vec<i32>>(),
        "educational_special_needs" => records.iter().map(|r| r.educational_special_needs).collect::<Vec<i32>>(),
        "nacionality" => records.iter().map(|r| r.nacionality).collect::<Vec<i32>>(),
        "mothers_qualification" => records.iter().map(|r| r.mothers_qualification).collect::<Vec<i32>>(),
        "fathers_qualification" => records.iter().map(|r| r.fathers_qualification).collect::<Vec<i32>>(),
        "mothers_occupation" => records.iter().map(|r| r.mothers_occupation).collect::<Vec<i32>>(),
        "fathers_occupation" => records.iter().map(|r| r.fathers_occupation).collect::<Vec<i32>>(),
        "scholarship_holder" => records.iter().map(|r| r.scholarship_holder).collect::<Vec<i32>>(),
        "debtor" => records.iter().map(|r| r.debtor).collect::<Vec<i32>>(),
        "tuition_fees_up_to_date" => records.iter().map(|r| r.tuition_fees_up_to_date).collect::<Vec<i32>>(),
        "application_mode" => records.iter().map(|r| r.application_mode).collect::<Vec<i32>>(),
        "application_order" => records.iter().map(|r| r.application_order).collect::<Vec<i32>>(),
        "course" => records.iter().map(|r| r.course).collect::<Vec<i32>>(),
        "daytime_evening_attendance" => records.iter().map(|r| r.daytime_evening_attendance).collect::<Vec<i32>>(),
        "previous_qualification" => records.iter().map(|r| r.previous_qualification).collect::<Vec<i32>>(),
        "previous_qualification_grade" => records.iter().map(|r| r.previous_qualification_grade).collect::<Vec<f64>>(),
        "admission_grade" => records.iter().map(|r| r.admission_grade).collect::<Vec<f64>>(),
        "curricular_units_1st_sem_credited" => records.iter().map(|r| r.curricular_units_1st_sem_credited).collect::<Vec<i32>>(),
        "curricular_units_1st_sem_enrolled" => records.iter().map(|r| r.curricular_units_1st_sem_enrolled).collect::<Vec<i32>>(),
        "curricular_units_1st_sem_evaluations" => records.iter().map(|r| r.curricular_units_1st_sem_evaluations).collect::<Vec<i32>>(),
        "curricular_units_1st_sem_approved" => records.iter().map(|r| r.curricular_units_1st_sem_approved).collect::<Vec<i32>>(),
        "curricular_units_1st_sem_grade" => records.iter().map(|r| r.curricular_units_1st_sem_grade).collect::<Vec<f64>>(),
        "curricular_units_1st_sem_without_evaluations" => records.iter().map(|r| r.curricular_units_1st_sem_without_evaluations).collect::<Vec<i32>>(),
        "curricular_units_2nd_sem_credited" => records.iter().map(|r| r.curricular_units_2nd_sem_credited).collect::<Vec<i32>>(),
        "curricular_units_2nd_sem_enrolled" => records.iter().map(|r| r.curricular_units_2nd_sem_enrolled).collect::<Vec<i32>>(),
        "curricular_units_2nd_sem_evaluations" => records.iter().map(|r| r.curricular_units_2nd_sem_evaluations).collect::<Vec<i32>>(),
        "curricular_units_2nd_sem_approved" => records.iter().map(|r| r.curricular_units_2nd_sem_approved).collect::<Vec<i32>>(),
        "curricular_units_2nd_sem_grade" => records.iter().map(|r| r.curricular_units_2nd_sem_grade).collect::<Vec<f64>>(),
        "curricular_units_2nd_sem_without_evaluations" => records.iter().map(|r| r.curricular_units_2nd_sem_without_evaluations).collect::<Vec<i32>>(),
        "unemployment_rate" => records.iter().map(|r| r.unemployment_rate).collect::<Vec<f64>>(),
        "inflation_rate" => records.iter().map(|r| r.inflation_rate).collect::<Vec<f64>>(),
        "gdp" => records.iter().map(|r| r.gdp).collect::<Vec<f64>>(),
        "pass_rate_1st_sem" => records.iter().map(|r| r.pass_rate_1st()).collect::<Vec<f64>>(),
        "pass_rate_2nd_sem" => records.iter().map(|r| r.pass_rate_2nd()).collect::<Vec<f64>>(),
        "grade_delta" => records.iter().map(|r| r.grade_delta()).collect::<Vec<f64>>(),
        "financial_stability_index" => records.iter().map(|r| r.financial_stability_index()).collect::<Vec<f64>>(),
        "target" => records.iter().map(|r| r.target.clone()).collect::<Vec<String>>(),
        "label" => records.iter().map(|r| r.label().unwrap_or(-1)).collect::<Vec<i32>>(),
    ).context("Failed to build Parquet DataFrame")?;

    Ok(df)
}
