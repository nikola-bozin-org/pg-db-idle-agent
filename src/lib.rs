use std::time::Duration;

use sqlx::{pool, PgPool};
use tokio::{task::JoinHandle, time};

pub struct PgDbIdleAgent {
    interval_secs: Duration,
    pool: PgPool,
    query: String,
}

impl PgDbIdleAgent {
    pub fn new(interval_secs: Duration, pool: PgPool, query: String) -> Self {
        Self {
            interval_secs,
            pool,
            query,
        }
    }

    pub async fn start(self) -> JoinHandle<()> {
        let mut ticker = time::interval(self.interval_secs);
        tokio::task::spawn(async move {
            loop {
                ticker.tick().await;
                self.check_data().await;
            }
        })
    }

    async fn check_data(&self) {
        println!("Checking data");
        // let rows = sqlx::query_as(self.query).fetch_all(self.pool).await.unwrap();
    }
}

#[cfg(test)]
mod tests {
    #[derive(FromRow,Debug,PartialEq)]
    struct Example {
        id: i32,
        data: String,
        is_sent: bool,
        version: i32,
    }
    use super::*;
    use sqlx::{postgres::PgPoolOptions, prelude::FromRow, Pool, Postgres};

    async fn drop_examples(pool:&Pool<Postgres>){
        sqlx::query(
            "DROP TABLE IF EXISTS example"
        )
        .execute(pool)
        .await
        .unwrap();

    }

    async fn create_example_table(pool:&Pool<Postgres>){
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

    async fn insert_example_data(pool: &PgPool){
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


        drop_examples(&pool).await;

        create_example_table(&pool).await;

        insert_example_data(&pool).await;

        pool
    }

    async fn get_all_examples(pool: &sqlx::PgPool)->Vec<Example> {
        sqlx::query_as::<_,Example>(
            "SELECT id, data, is_sent, version FROM example"
        )
        .fetch_all(pool)
        .await
        .unwrap()
    }
    

    #[tokio::test]
    async fn test_db_setup() {
        let expected_data = [
            Example { id: 1, data: "Some random text".to_string(), is_sent: false, version: 0 },
            Example { id: 2, data: "Another text".to_string(), is_sent: true, version: 1 },
            Example { id: 3, data: "third text".to_string(), is_sent: true, version: 0 },
        ];
        let pool = setup_db().await;
        let examples = get_all_examples(&pool).await;
        examples.into_iter().enumerate().for_each(|(index,e)|{
            assert_eq!(e, expected_data[index], "The fetched data does not match the expected data.");
        })
    }
}
