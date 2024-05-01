use std::{fmt::Debug, marker::PhantomData, time::Duration};

use sqlx::{postgres::PgRow, PgPool};
use tokio::{task::JoinHandle, time};

/// Quick reminders:
/// Send    - Needed for types that are moved between threads. This trait ensures that ownership can be transferable safely. Required by: (Tokio)
/// Sync    - This trait ensures safe reference sharing across threads.
/// Unpin   - Types that are used with async tasks, ensuring they can be safely pinned in memory.
/// 'static - It should live for an entire duration of an program

pub struct PgDbIdleAgent<T, F, E>
where
    T: for<'r> sqlx::FromRow<'r, PgRow> + Send + Sync + Unpin + 'static,
    F: Fn(&T) + Send + Sync + 'static,
    E: Fn(sqlx::Error) + Send + Sync + 'static, // Error handling callback

{
    interval_secs: Duration,
    pool: PgPool,
    query: String,
    action: F,
    error_handler: E,
    _marker: PhantomData<T>, // Add this so compile does not complain about unused parameter T.
}

impl<T, F, E> PgDbIdleAgent<T, F, E>
where
    T: for<'r> sqlx::FromRow<'r, PgRow> + Send + Sync + Unpin + 'static,

    F: Fn(&T) + Send + Sync + 'static,
    E: Fn(sqlx::Error) + Send + Sync + 'static, // Error handling callback
    

    {
    pub fn new(interval_secs: Duration, pool: PgPool, query: String, action: F,error_handler:E) -> Self {
        Self {
            interval_secs,
            pool,
            query,
            action,
            error_handler,
            _marker: PhantomData, // This is hwo to initialize phantom data.
        }
    }

    pub async fn start(self) -> JoinHandle<()> {
        let mut ticker = time::interval(self.interval_secs);
        tokio::task::spawn(async move {
            loop {
                ticker.tick().await;
                if let Err(e) = self.check_data().await {
                    (self.error_handler)(e);
                }
            }
        })
    }

    async fn check_data(&self) -> Result<(),sqlx::Error>
    where
        T: for<'r> sqlx::FromRow<'r, PgRow> + Send + Sync + Unpin ,
    {
        let rows: Vec<T> = sqlx::query_as::<_, T>(self.query.as_str())
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().for_each(|element| {
            (self.action)(&element); // This is how to invoke an action thats a property.
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::{postgres::PgPoolOptions, Pool, Postgres, FromRow};

    #[derive(FromRow, Debug, PartialEq)]
    pub struct Example {
        pub id: i32,
        pub data: String,
        pub is_sent: bool,
        pub version: i32,
    }

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

        let tx = pool.begin().await.unwrap();

        drop_examples(&pool).await;

        create_example_table(&pool).await;

        insert_example_data(&pool).await;

        tx.commit().await.unwrap();

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
