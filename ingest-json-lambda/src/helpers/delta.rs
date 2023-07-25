use std::collections::HashMap;
use std::sync::Arc;

use deltalake::{storage, DeltaOps, DeltaTable, Schema};

/// Get delta table or create if absent
///
/// # Arguments
///
/// * `table_uri` - URI of the table.
/// * `partition_columns` - Columns that are used as partitons to create the delta table if not exist.
/// * `schema` - Schema that will be used to create the delta table if not exist.
pub async fn get_or_create_delta_table(
    table_uri: String,
    table_name: String,
    partition_columns: Vec<&str>,
    schema: Schema,
) -> DeltaTable {
    let storage_options = HashMap::new();

    match deltalake::open_table(&table_uri).await {
        Ok(table) => table,
        Err(_error) => {
            let object_store = storage::DeltaObjectStore::try_new(
                table_uri.parse().unwrap(),
                storage::config::StorageOptions::from(storage_options.clone()),
            )
            .unwrap();
            let ops = DeltaOps::try_from_uri(&table_uri).await.unwrap();

            ops.create()
                .with_object_store(Arc::new(object_store))
                .with_table_name(table_name)
                .with_partition_columns(partition_columns)
                .with_columns(schema.get_fields().clone())
                .await
                .unwrap()
        }
    }
}
