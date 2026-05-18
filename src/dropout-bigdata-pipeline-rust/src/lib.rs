use serde::{Deserialize, Serialize};
use std::fmt;

// ── Error type ───────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    #[error("unknown target label: {0:?}")]
    UnknownTarget(String),
}

// ── Domain model ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(i32)]
pub enum Label {
    Dropout = 0,
    Enrolled = 1,
    Graduate = 2,
}

impl fmt::Display for Label {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Label::Dropout => write!(f, "Dropout"),
            Label::Enrolled => write!(f, "Enrolled"),
            Label::Graduate => write!(f, "Graduate"),
        }
    }
}

impl TryFrom<&str> for Label {
    type Error = RecordError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "Dropout" => Ok(Label::Dropout),
            "Enrolled" => Ok(Label::Enrolled),
            "Graduate" => Ok(Label::Graduate),
            other => Err(RecordError::UnknownTarget(other.to_owned())),
        }
    }
}

// ── StudentRecord ─────────────────────────────────────────────────────────────

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
    // ── Computed features ───────────────────────────────────────────────────

    pub fn pass_rate_1st(&self) -> f64 {
        safe_ratio(
            self.curricular_units_1st_sem_approved,
            self.curricular_units_1st_sem_enrolled,
        )
    }

    pub fn pass_rate_2nd(&self) -> f64 {
        safe_ratio(
            self.curricular_units_2nd_sem_approved,
            self.curricular_units_2nd_sem_enrolled,
        )
    }

    /// Grade improvement (positive = improving, negative = declining).
    pub fn grade_delta(&self) -> f64 {
        self.curricular_units_2nd_sem_grade - self.curricular_units_1st_sem_grade
    }

    /// Simple proxy: 1 point per paid tuition / scholarship, −1 per debt.
    pub fn financial_stability_index(&self) -> f64 {
        f64::from(self.tuition_fees_up_to_date) + f64::from(self.scholarship_holder)
            - f64::from(self.debtor)
    }

    // ── Label helpers ───────────────────────────────────────────────────────

    /// Infallible label; returns `None` when the target string is unknown.
    pub fn label(&self) -> Option<Label> {
        Label::try_from(self.target.as_str()).ok()
    }

    /// Returns the numeric (`i32`) representation expected by the Parquet schema.
    pub fn label_i32(&self) -> i32 {
        self.label().map(|l| l as i32).unwrap_or(-1)
    }

    // ── Validation ──────────────────────────────────────────────────────────

    pub fn is_valid(&self) -> bool {
        const AGE_RANGE: std::ops::RangeInclusive<i32> = 17..=70;
        AGE_RANGE.contains(&self.age_at_enrollment)
            && self.curricular_units_1st_sem_grade >= 0.0
            && self.curricular_units_2nd_sem_grade >= 0.0
    }
}

// ── Kafka config ──────────────────────────────────────────────────────────────

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

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Returns `approved / enrolled`, or 0 when `enrolled == 0`.
#[inline]
fn safe_ratio(approved: i32, enrolled: i32) -> f64 {
    if enrolled > 0 {
        f64::from(approved) / f64::from(enrolled)
    } else {
        0.0
    }
}
