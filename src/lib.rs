use std::{fmt::Debug, marker::PhantomData, time::Duration};

use sqlx::{FromRow, PgPool};
use tokio::{task::JoinHandle, time};

pub struct PgDbIdleAgent<T, F>
where
    T: for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> + Send + Sync + Unpin + Debug + 'static,
    F: Fn(&T) + Send + Sync + 'static,
{
    interval_secs: Duration,
    pool: PgPool,
    query: String,
    action: F,
    _marker: PhantomData<T>,
}

impl<T, F> PgDbIdleAgent<T, F>
where
    T: for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> + Send + Sync + Unpin + Debug + 'static,
    F: Fn(&T) + Send + Sync + 'static,
{
    pub fn new(interval_secs: Duration, pool: PgPool, query: String, action: F) -> Self {
        Self {
            interval_secs,
            pool,
            query,
            action,
            _marker: PhantomData,
        }
    }

    pub async fn start(self) -> JoinHandle<()>
    where
        T: for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> + Send + Sync + Unpin + Debug,
    {
        let mut ticker = time::interval(self.interval_secs);
        tokio::task::spawn(async move {
            loop {
                ticker.tick().await;
                self.check_data().await;
            }
        })
    }

    async fn check_data(&self)
    where
        T: for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> + Send + Sync + Unpin + Debug,
    {
        println!("Checking data");
        let rows: Vec<T> = sqlx::query_as::<_, T>(self.query.as_str())
            .fetch_all(&self.pool)
            .await
            .unwrap();
        (self.action)(&rows[0]);
    }
}

#[derive(FromRow, Debug, PartialEq)]
pub struct Example {
    pub id: i32,
    pub data: String,
    pub is_sent: bool,
    pub version: i32,
}

#[cfg(test)]
mod tests {

    use super::*;
    use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

    async fn drop_examples(pool: &Pool<Postgres>) {
        sqlx::query("DROP TABLE IF EXISTS example")
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

        drop_examples(&pool).await;

        create_example_table(&pool).await;

        insert_example_data(&pool).await;

        pool
    }

    async fn get_all_examples(pool: &sqlx::PgPool) -> Vec<Example> {
        sqlx::query_as::<_, Example>("SELECT id, data, is_sent, version FROM example")
            .fetch_all(pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_db_setup() {
        let expected_data = [
            Example {
                id: 1,
                data: "Some random text".to_string(),
                is_sent: false,
                version: 0,
            },
            Example {
                id: 2,
                data: "Another text".to_string(),
                is_sent: true,
                version: 1,
            },
            Example {
                id: 3,
                data: "third text".to_string(),
                is_sent: true,
                version: 0,
            },
        ];
        let pool = setup_db().await;
        let examples = get_all_examples(&pool).await;
        examples.into_iter().enumerate().for_each(|(index, e)| {
            assert_eq!(
                e, expected_data[index],
                "The fetched data does not match the expected data."
            );
        })
    }
}
