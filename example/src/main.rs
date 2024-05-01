use std::time::Duration;

use pg_db_idle_agent_async::PgDbIdleAgent;
use sqlx::prelude::FromRow;
use sqlx::PgPool;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

#[derive(FromRow, Debug, PartialEq)]
pub struct Example {
    pub id: i32,
    pub data: String,
    pub is_sent: bool,
    pub version: i32,
}

async fn drop_examples(pool: &Pool<Postgres>) {
    // IMPORTANT:

    // in example TABLE we have:
    // SERIAL PRIMARY id
    // this internally creates example_id_seq SEQUENCE to generate unique values for id
    // for that reason we have to DROP sequence too when we drop example TABLE.
    // CASCADE removes all dependent objects are dropped alog with the table.

    sqlx::query("DROP TABLE IF EXISTS example CASCADE")
        .execute(pool)
        .await
        .unwrap();

    // This is sort of redundant but let it be here.

    sqlx::query("DROP SEQUENCE IF EXISTS example_id_seq CASCADE")
        .execute(pool)
        .await
        .unwrap();
}
async fn create_example_table(pool: &Pool<Postgres>) {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS example (
                id SERIAL PRIMARY KEY,
                data TEXT NOT NULL,
                is_sent BOOLEAN NOT NULL,
                version INT NOT NULL
            )",
    )
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_example_data(pool: &PgPool) {
    let data_list = vec![
        ("Some random text".to_string(), false, 0),
        ("Another text".to_string(), true, 1),
        ("third text".to_string(), true, 0),
    ];

    for (data, is_sent, version) in data_list {
        sqlx::query("INSERT INTO example (data, is_sent, version) VALUES ($1, $2, $3)")
            .bind(&data)
            .bind(is_sent)
            .bind(version)
            .execute(pool)
            .await
            .unwrap();
    }
}

async fn setup_db() -> Pool<Postgres> {
    let pool = PgPoolOptions::new()
        .connect("postgres://test:test@localhost:5439/test")
        .await
        .unwrap();

    let tx = pool.begin().await.unwrap();

    drop_examples(&pool).await;

    create_example_table(&pool).await;

    insert_example_data(&pool).await;

    tx.commit().await.unwrap();

    pool
}

#[tokio::main]
async fn main() {
    let log = |ex: &Example| {
        dbg!(ex.id);
    };

    let log_error = |err: sqlx::Error| {
        dbg!("Error happend...");
        dbg!(err);
    };
    let pool = setup_db().await;
    let idle_agent = PgDbIdleAgent::new(
        Duration::from_secs(1),
        pool,
        "SELECT id, data, is_sent, version FROM example".to_string(),
        log,
        log_error,
    );
    let handle = idle_agent.start().await;
    
    tokio::time::sleep(Duration::from_secs(5)).await;

    handle.abort();
}
