use plotters::prelude::*;
use std::env;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let log_dir = env::var("LOG_DIR").unwrap_or_else(|_| "../../logs".to_string());
    let out_file_name = format!("{}/throughput_benchmark.svg", log_dir);

    let root = SVGBackend::new(&out_file_name, (800, 600)).into_drawing_area();
    root.fill(&WHITE)?;

    let labels = ["100 rec/s", "500 rec/s", "1000 rec/s", "Burst (4200)"];
    let values = [100i32, 500i32, 1000i32, 4200i32];

    let mut chart = ChartBuilder::on(&root)
        .caption(
            "Throughput Ingestion (Case 2)",
            ("sans-serif", 40).into_font(),
        )
        .margin(30)
        .x_label_area_size(40)
        .y_label_area_size(50)
        .build_cartesian_2d((0usize..3usize).into_segmented(), 0i32..4500i32)?;

    chart
        .configure_mesh()
        .disable_x_mesh()
        .bold_line_style(BLACK.mix(0.3))
        .y_desc("Throughput (Records / Second)")
        .x_labels(4)
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

        let color = if i == 3 { RED.filled() } else { BLUE.filled() };

        let mut bar = Rectangle::new([(x0, 0), (x1, val)], color);

        bar.set_margin(0, 0, 20, 20);
        bar
    }))?;

    root.present()?;
    println!("SVG chart successfully generated at {}", out_file_name);

    Ok(())
}
