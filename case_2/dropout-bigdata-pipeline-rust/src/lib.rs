use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StudentRecord {
    pub marital_status: i32,
    pub gender: i32,
    pub age_at_enrollment: i32,
    pub international: i32,
    pub displaced: i32,
    pub educational_special_needs: i32,
    pub nacionality: i32,
    pub mothers_qualification: i32,
    pub fathers_qualification: i32,
    pub mothers_occupation: i32,
    pub fathers_occupation: i32,
    pub scholarship_holder: i32,
    pub debtor: i32,
    pub tuition_fees_up_to_date: i32,
    pub application_mode: i32,
    pub application_order: i32,
    pub course: i32,
    pub daytime_evening_attendance: i32,
    pub previous_qualification: i32,
    pub previous_qualification_grade: f64,
    pub admission_grade: f64,
    pub curricular_units_1st_sem_credited: i32,
    pub curricular_units_1st_sem_enrolled: i32,
    pub curricular_units_1st_sem_evaluations: i32,
    pub curricular_units_1st_sem_approved: i32,
    pub curricular_units_1st_sem_grade: f64,
    pub curricular_units_1st_sem_without_evaluations: i32,
    pub curricular_units_2nd_sem_credited: i32,
    pub curricular_units_2nd_sem_enrolled: i32,
    pub curricular_units_2nd_sem_evaluations: i32,
    pub curricular_units_2nd_sem_approved: i32,
    pub curricular_units_2nd_sem_grade: f64,
    pub curricular_units_2nd_sem_without_evaluations: i32,
    pub unemployment_rate: f64,
    pub inflation_rate: f64,
    pub gdp: f64,
    pub target: String,
    #[serde(default)]
    pub event_id: String,
    #[serde(default)]
    pub event_ts_ms: u64,
}

impl StudentRecord {
    pub fn pass_rate_1st(&self) -> f64 {
        if self.curricular_units_1st_sem_enrolled > 0 {
            self.curricular_units_1st_sem_approved as f64
                / self.curricular_units_1st_sem_enrolled as f64
        } else {
            0.0
        }
    }

    pub fn pass_rate_2nd(&self) -> f64 {
        if self.curricular_units_2nd_sem_enrolled > 0 {
            self.curricular_units_2nd_sem_approved as f64
                / self.curricular_units_2nd_sem_enrolled as f64
        } else {
            0.0
        }
    }

    pub fn grade_delta(&self) -> f64 {
        self.curricular_units_2nd_sem_grade - self.curricular_units_1st_sem_grade
    }

    pub fn financial_stability_index(&self) -> f64 {
        self.tuition_fees_up_to_date as f64 + self.scholarship_holder as f64 - self.debtor as f64
    }

    pub fn label(&self) -> Option<i32> {
        match self.target.as_str() {
            "Dropout" => Some(0),
            "Enrolled" => Some(1),
            "Graduate" => Some(2),
            _ => None,
        }
    }

    pub fn is_valid(&self) -> bool {
        (17..=70).contains(&self.age_at_enrollment)
            && self.curricular_units_1st_sem_grade >= 0.0
            && self.curricular_units_2nd_sem_grade >= 0.0
    }
}

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub brokers: String,
    pub topic: String,
    pub group_id: String,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            brokers: std::env::var("KAFKA_BOOTSTRAP_SERVERS")
                .unwrap_or_else(|_| "localhost:9092".into()),
            topic: "student-data-rust".into(),
            group_id: "dropout-consumer-rust".into(),
        }
    }
}
