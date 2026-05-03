use plotters::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs;

fn main() -> Result<(), Box<dyn Error>> {
    let log_dir = env::var("LOG_DIR").unwrap_or_else(|_| "../../logs".to_string());
    let out_file_name = format!("{}/throughput_benchmark.svg", log_dir);

    let mut latest_data: HashMap<u64, (String, f64, u64)> = HashMap::new();

    if let Ok(entries) = fs::read_dir(&log_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("json") {
                let file_name = path.file_name().unwrap().to_string_lossy();

                if file_name.starts_with("producer_rust") {
                    let ts_str = file_name
                        .trim_end_matches(".json")
                        .split('_')
                        .last()
                        .unwrap_or("0");
                    let timestamp: u64 = ts_str.parse().unwrap_or(0);

                    let content = fs::read_to_string(&path)?;
                    if let Ok(json) = serde_json::from_str::<Value>(&content) {
                        let target_rate = json["target_rate"].as_u64().unwrap_or(0);
                        let avg_rate = json["avg_rate"].as_f64().unwrap_or(0.0);

                        let label = if target_rate == 0 {
                            "Burst Mode".to_string()
                        } else {
                            format!("{} rec/s", target_rate)
                        };

                        let entry = latest_data.entry(target_rate).or_insert((
                            label.clone(),
                            avg_rate,
                            timestamp,
                        ));
                        if timestamp > entry.2 {
                            *entry = (label, avg_rate, timestamp);
                        }
                    }
                }
            }
        }
    }

    let mut data_points: Vec<(String, f64)> = latest_data
        .values()
        .map(|(l, v, _)| (l.clone(), *v))
        .collect();

    if data_points.is_empty() {
        println!(
            "No valid JSON log files found in {}. Visualizer will use fallback data.",
            log_dir
        );
        data_points = vec![("No Data".to_string(), 0.0)];
    } else {
        data_points.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    }

    let labels: Vec<String> = data_points.iter().map(|(l, _)| l.clone()).collect();
    let values: Vec<i32> = data_points.iter().map(|(_, v)| *v as i32).collect();
    let max_val = values.iter().max().unwrap_or(&1000) + 500;

    let root = SVGBackend::new(&out_file_name, (800, 600)).into_drawing_area();
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
                if *i < labels.len() {
                    labels[*i].to_string()
                } else {
                    String::new()
                }
            }
            _ => String::new(),
        })
        .draw()?;

    chart.draw_series(values.iter().enumerate().map(|(i, &val)| {
        let x0 = SegmentValue::Exact(i);
        let x1 = SegmentValue::Exact(i + 1);

        let color = if val == *values.iter().max().unwrap_or(&0) {
            RED.filled()
        } else {
            BLUE.filled()
        };

        let mut bar = Rectangle::new([(x0, 0), (x1, val)], color);
        bar.set_margin(0, 0, 20, 20);
        bar
    }))?;

    root.present()?;
    println!("SVG chart generated at {}", out_file_name);

    Ok(())
}
