use plotters::prelude::*;
use serde_json::Value;
use std::{collections::HashMap, env, error::Error, fs, path::Path};

// ── Types ─────────────────────────────────────────────────────────────────────

/// One data point to plot, derived from the latest JSON log for each target rate.
#[derive(Debug)]
struct DataPoint {
    label: String,
    actual_rate: f64,
}

impl DataPoint {
    fn from_json(json: &Value) -> Option<Self> {
        let target_rate = json["target_rate"].as_u64()?;
        let actual_rate = json["avg_rate"].as_f64()?;
        let label = if target_rate == 0 {
            "Burst Mode".to_owned()
        } else {
            format!("{target_rate} rec/s")
        };
        Some(DataPoint { label, actual_rate })
    }
}

// ── Entry point ───────────────────────────────────────────────────────────────

fn main() -> Result<(), Box<dyn Error>> {
    let log_dir = env::var("LOG_DIR").unwrap_or_else(|_| "../../logs".to_owned());
    let out_path = format!("{log_dir}/throughput_benchmark.svg");

    let mut data = load_data(&log_dir);

    if data.is_empty() {
        eprintln!("No valid JSON log files found in {log_dir}. Visualizer will use fallback data.");
        data.push(DataPoint {
            label: "No Data".to_owned(),
            actual_rate: 0.0,
        });
    } else {
        data.sort_by(|a, b| a.actual_rate.partial_cmp(&b.actual_rate).unwrap());
    }

    render_chart(&data, &out_path)?;
    println!("SVG chart written to {out_path}");
    Ok(())
}

// ── Data loading ──────────────────────────────────────────────────────────────

/// Reads all `producer_rust*.json` files and returns the **latest** entry per
/// target rate, deduplicated by timestamp.
fn load_data(log_dir: &str) -> Vec<DataPoint> {
    // key = target_rate, value = (DataPoint, timestamp)
    let mut latest: HashMap<u64, (DataPoint, u64)> = HashMap::new();

    let entries = match fs::read_dir(log_dir) {
        Ok(e) => e,
        Err(_) => return Vec::new(),
    };

    for path in entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| is_rust_producer_json(p))
    {
        let Some(ts) = parse_timestamp(&path) else {
            continue;
        };
        let Ok(content) = fs::read_to_string(&path) else {
            continue;
        };
        let Ok(json) = serde_json::from_str::<Value>(&content) else {
            continue;
        };
        let Some(point) = DataPoint::from_json(&json) else {
            continue;
        };

        let target_rate = json["target_rate"].as_u64().unwrap_or(0);
        let entry = latest.entry(target_rate).or_insert_with(|| (point, ts));
        if ts > entry.1 {
            *entry = (DataPoint::from_json(&json).expect("already validated"), ts);
        }
    }

    latest.into_values().map(|(dp, _)| dp).collect()
}

// ── Chart rendering ───────────────────────────────────────────────────────────

fn render_chart(data: &[DataPoint], out_path: &str) -> Result<(), Box<dyn Error>> {
    let labels: Vec<&str> = data.iter().map(|d| d.label.as_str()).collect();
    let values: Vec<i32> = data.iter().map(|d| d.actual_rate as i32).collect();
    let max_val = values.iter().copied().max().unwrap_or(1000) + 500;
    let max_value = values.iter().copied().max().unwrap_or(0);

    let root = SVGBackend::new(out_path, (800, 600)).into_drawing_area();
    root.fill(&WHITE)?;

    let mut chart = ChartBuilder::on(&root)
        .caption(
            "Throughput Ingestion (Latest Actual Data)",
            ("sans-serif", 40).into_font(),
        )
        .margin(30)
        .x_label_area_size(40)
        .y_label_area_size(50)
        .build_cartesian_2d((0usize..values.len()).into_segmented(), 0i32..max_val)?;

    chart
        .configure_mesh()
        .disable_x_mesh()
        .bold_line_style(BLACK.mix(0.3))
        .y_desc("Actual Throughput (Records / Second)")
        .x_labels(labels.len())
        .x_label_formatter(&|x| match x {
            SegmentValue::Exact(i) | SegmentValue::CenterOf(i) => {
                labels.get(*i).copied().unwrap_or("").to_owned()
            }
            _ => String::new(),
        })
        .draw()?;

    chart.draw_series(values.iter().enumerate().map(|(i, &val)| {
        let color = if val == max_value {
            RED.filled()
        } else {
            BLUE.filled()
        };
        let mut bar = Rectangle::new(
            [
                (SegmentValue::Exact(i), 0),
                (SegmentValue::Exact(i + 1), val),
            ],
            color,
        );
        bar.set_margin(0, 0, 20, 20);
        bar
    }))?;

    root.present()?;
    Ok(())
}

// ── Path helpers ──────────────────────────────────────────────────────────────

fn is_rust_producer_json(path: &Path) -> bool {
    path.is_file()
        && path.extension().and_then(|e| e.to_str()) == Some("json")
        && path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("producer_rust"))
}

/// Extracts the trailing `_<timestamp>` from a file stem like
/// `producer_rust_rate100_scale1_1700000000`.
fn parse_timestamp(path: &Path) -> Option<u64> {
    path.file_stem()?.to_str()?.rsplit('_').next()?.parse().ok()
}
