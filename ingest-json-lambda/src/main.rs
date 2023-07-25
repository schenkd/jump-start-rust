mod helpers;

use aws_lambda_events::event::kinesis::KinesisEvent;
use chrono::{DateTime, Utc};
use deltalake::writer::DeltaWriter;
use deltalake::{action, schema, writer};
use helpers::delta as delta_helper;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::env;

#[derive(Deserialize, Serialize)]
struct EventMetadata {
    event_id: String,
    event_ts: DateTime<Utc>,
    event_type: String,
}

#[derive(Deserialize, Serialize)]
struct LoginEvent {
    metadata: EventMetadata,
    username: String,
    user_agent: String,
    email: String,
    ip_address: String,
}

trait DeltaSchema {
    fn get_schema_type_struct() -> schema::SchemaTypeStruct;
}

impl DeltaSchema for LoginEvent {
    fn get_schema_type_struct() -> schema::SchemaTypeStruct {
        return schema::SchemaTypeStruct::new(vec![
            schema::SchemaField::new(
                "metadata".to_string(),
                schema::SchemaDataType::r#struct(schema::SchemaTypeStruct::new(vec![
                    schema::SchemaField::new(
                        "event_id".to_string(),
                        schema::SchemaDataType::primitive("string".to_string()),
                        false,
                        HashMap::new(),
                    ),
                    schema::SchemaField::new(
                        "event_ts".to_string(),
                        schema::SchemaDataType::primitive("string".to_string()),
                        false,
                        HashMap::new(),
                    ),
                    schema::SchemaField::new(
                        "event_type".to_string(),
                        schema::SchemaDataType::primitive("string".to_string()),
                        false,
                        HashMap::new(),
                    ),
                ])),
                false,
                HashMap::new(),
            ),
            schema::SchemaField::new(
                "username".to_string(),
                schema::SchemaDataType::primitive("string".to_string()),
                false,
                HashMap::new(),
            ),
            schema::SchemaField::new(
                "user_agent".to_string(),
                schema::SchemaDataType::primitive("string".to_string()),
                false,
                HashMap::new(),
            ),
            schema::SchemaField::new(
                "email".to_string(),
                schema::SchemaDataType::primitive("string".to_string()),
                false,
                HashMap::new(),
            ),
            schema::SchemaField::new(
                "ip_address".to_string(),
                schema::SchemaDataType::primitive("string".to_string()),
                false,
                HashMap::new(),
            ),
        ]);
    }
}

/// This is the main body for the function.
/// Write your code inside it.
/// There are some code example in the following URLs:
/// - https://github.com/awslabs/aws-lambda-rust-runtime/tree/main/examples
/// - https://github.com/aws-samples/serverless-rust-demo/
async fn function_handler(event: LambdaEvent<KinesisEvent>) -> Result<serde_json::Value, Error> {
    let (event, _context) = event.into_parts();

    let events: Vec<LoginEvent> = event
        .records
        .into_iter()
        .map(|kinesis_event_record| {
            serde_json::from_slice(&kinesis_event_record.kinesis.data.as_slice()).unwrap()
        })
        .collect();

    let table_uri = env::var("TABLE_URI")?;
    let table_name = env::var("TABLE_NAME")?;
    let partition_columns = vec![];
    let schema_type_struct = LoginEvent::get_schema_type_struct();

    let mut delta_table = delta_helper::get_or_create_delta_table(
        table_uri,
        table_name,
        partition_columns,
        schema_type_struct,
    )
    .await;

    let mut json_writer = writer::JsonWriter::for_table(&delta_table)?;
    json_writer
        .write(serde_json::from_str(&serde_json::to_string(&events)?)?)
        .await?;

    match json_writer.flush_and_commit(&mut delta_table).await {
        Ok(table_version) => {
            if table_version % 10 == 0 {
                action::checkpoints::create_checkpoint(&delta_table)
                    .await
                    .unwrap();
                action::checkpoints::cleanup_metadata(&delta_table)
                    .await
                    .unwrap();
            };

            println!("Delta table version {:?}", table_version)
        }
        Err(_error) => panic!("Unable to write table"),
    }

    Ok(json!({"statusCode": 200}))
}

#[tokio::test]
async fn test_function_handler() {
    use helpers::testing;
    use std::fs;

    let table_name = "logins";
    let current_path = env::current_dir().unwrap();
    let table_uri = &format!("file://{}/data/{table_name}", current_path.display());
    fs::create_dir_all(format!("data/{table_name}")).expect("Failed to create login directory!");

    env::set_var("TABLE_URI", table_uri);
    env::set_var("TABLE_NAME", table_name);

    let test_events = testing::load_kinesis_event();
    let response = function_handler(LambdaEvent {
        payload: test_events,
        context: Default::default(),
    })
    .await;

    // assert
    assert!(response.is_ok());

    let body = response.unwrap();
    assert!(body.is_object());

    let status_code = body.get("statusCode").unwrap();
    assert_eq!(status_code.as_i64(), Some(i64::from(200)));

    let delta_table_result = deltalake::open_table(&*table_uri).await;
    assert!(delta_table_result.is_ok());
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
