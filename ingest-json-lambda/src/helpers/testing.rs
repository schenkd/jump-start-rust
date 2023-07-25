use aws_lambda_events::encodings::{Base64Data, SecondTimestamp};
use serde_json;
use std::env;
use std::fs;
use std::io::BufReader;

use aws_lambda_events::event::kinesis::KinesisEvent;
use aws_lambda_events::kinesis::{KinesisEventRecord, KinesisRecord};
use chrono::Utc;

/// Helper function to load testdata from data directory
///
/// # Arguments
///
/// * `file_name` - Filename you want to load from data directory.
fn load_data(file_name: &str) -> Vec<serde_json::Value> {
    let current_path = env::current_dir().unwrap();
    let formatted_path = format!("{}/data/{file_name}", current_path.display()).to_string();
    let file = fs::File::open(formatted_path).unwrap();
    let raw_events: Vec<serde_json::Value> = serde_json::from_reader(BufReader::new(file)).unwrap();

    return raw_events;
}

/// Helper function to load json raw data and put them in a KinesisEvent for the lambda function
pub fn load_kinesis_event() -> KinesisEvent {
    let raw_data = load_data("kinesis_events.json");

    let mut kinesis_event_records = vec![];

    for (sequence_number, event) in raw_data.iter().enumerate() {
        kinesis_event_records.push(KinesisEventRecord {
            aws_region: Some("eu-central-1".to_string()),
            event_id: Some(
                "shardId-000000000006:49590338271490256608559692538361571095921575989136588898"
                    .to_string(),
            ),
            event_name: Some("aws:kinesis:record".to_string()),
            event_source: Some("aws:kinesis".to_string()),
            event_source_arn: Some(
                "arn:aws:kinesis:eu-central-1:123456789012:stream/lambda-stream".to_string(),
            ),
            event_version: Some("1.0".to_string()),
            invoke_identity_arn: Some("arn:aws:iam::123456789012:role/lambda-role".to_string()),
            kinesis: KinesisRecord {
                approximate_arrival_timestamp: SecondTimestamp(Utc::now()),
                data: Base64Data(serde_json::to_vec(event).unwrap()),
                encryption_type: None,
                partition_key: Some("1".to_string()),
                sequence_number: Some(sequence_number.to_string()),
                kinesis_schema_version: Some("1.0".to_string()),
            },
        })
    }

    KinesisEvent {
        records: kinesis_event_records,
    }
}
