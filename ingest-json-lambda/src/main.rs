mod helpers;

use aws_lambda_events::event::kinesis::KinesisEvent;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use serde::{Deserialize, Serialize};
use serde_json::{json};
use chrono::{DateTime, Utc};


#[derive(Serialize, Deserialize, Debug)]
struct EventMetadata {
    event_id: String,
    event_ts: DateTime<Utc>,
    event_type: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Event {
    metadata: EventMetadata,
    username: String,
    user_agent: String,
    email: String,
    ip_address: String,
}

async fn function_handler(event: LambdaEvent<KinesisEvent>) -> Result<serde_json::Value, Error> {
    let (event, _context) = event.into_parts();

    let events: Vec<Event> = event.records.into_iter().map(|kinesis_event_record| {
        serde_json::from_slice(&kinesis_event_record.kinesis.data.as_slice()).unwrap()
    }).collect();

    print!("{:?}", events);

    Ok(json!({"statusCode": 200}))
}

#[tokio::test]
async fn test_function_handler() {
    // arrange
    let event = helpers::testing::load_kinesis_event();
    let context = lambda_runtime::Context::default();
    let lambda_event = LambdaEvent::new(event, context);

    // action
    let response = function_handler(lambda_event).await;

    // assert
    assert!(response.is_ok());
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    run(service_fn(function_handler)).await
}

