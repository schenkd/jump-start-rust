mod helpers;

use aws_lambda_events::event::kinesis::KinesisEvent;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};

async fn function_handler(event: LambdaEvent<KinesisEvent>) -> Result<(), Error> {
    // Extract some useful information from the request

    Ok(())
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

