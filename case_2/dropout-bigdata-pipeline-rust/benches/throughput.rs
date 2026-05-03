use criterion::{Criterion, criterion_group, criterion_main};
use dropout_bigdata_pipeline_rust::StudentRecord;
use serde_json::to_string;

fn serialize_benchmark(c: &mut Criterion) {
    let dummy_record = StudentRecord {
        marital_status: 1,
        gender: 1,
        age_at_enrollment: 20,
        international: 0,
        displaced: 1,
        educational_special_needs: 0,
        nacionality: 1,
        mothers_qualification: 1,
        fathers_qualification: 1,
        mothers_occupation: 1,
        fathers_occupation: 1,
        scholarship_holder: 1,
        debtor: 0,
        tuition_fees_up_to_date: 1,
        application_mode: 1,
        application_order: 1,
        course: 1,
        daytime_evening_attendance: 1,
        previous_qualification: 1,
        previous_qualification_grade: 120.0,
        admission_grade: 130.0,
        curricular_units_1st_sem_credited: 0,
        curricular_units_1st_sem_enrolled: 6,
        curricular_units_1st_sem_evaluations: 6,
        curricular_units_1st_sem_approved: 6,
        curricular_units_1st_sem_grade: 14.0,
        curricular_units_1st_sem_without_evaluations: 0,
        curricular_units_2nd_sem_credited: 0,
        curricular_units_2nd_sem_enrolled: 6,
        curricular_units_2nd_sem_evaluations: 6,
        curricular_units_2nd_sem_approved: 6,
        curricular_units_2nd_sem_grade: 13.5,
        curricular_units_2nd_sem_without_evaluations: 0,
        unemployment_rate: 10.8,
        inflation_rate: 1.4,
        gdp: 1.74,
        target: "Graduate".to_string(),
        event_id: "bench_1".to_string(),
        event_ts_ms: 1670000000000,
    };

    c.bench_function("serialize_student_record_json", |b| {
        b.iter(|| to_string(&dummy_record).unwrap())
    });
}

criterion_group!(benches, serialize_benchmark);
criterion_main!(benches);
