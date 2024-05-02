mod pg_db_agent_params;

pub use pg_db_agent_params::*;
use sqlx::postgres::PgRow;
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
    params: PgDbAgentParams<T,F,E>
}

impl<T, F, E> PgDbIdleAgent<T, F, E>
where
    T: for<'r> sqlx::FromRow<'r, PgRow> + Send + Sync + Unpin + 'static,

    F: Fn(&T) + Send + Sync + 'static,
    E: Fn(sqlx::Error) + Send + Sync + 'static, // Error handling callback
{
    pub fn new(
        params: PgDbAgentParams<T, F, E>,
    ) -> Self {
        Self {
            params,
        }
    }

    pub async fn start(self) -> JoinHandle<()> {
        let mut ticker = time::interval(self.params.interval_secs);
        tokio::task::spawn(async move {
            loop {
                ticker.tick().await;
                if let Err(e) = self.check_data().await {
                    (self.params.error_handler)(e);
                }
            }
        })
    }

    async fn check_data(&self) -> Result<(), sqlx::Error>
    where
        T: for<'r> sqlx::FromRow<'r, PgRow> + Send + Sync + Unpin,
    {
        for param in &self.params.query_actions {
            dbg!(format!("Processing: {}",param.query));
            let rows: Vec<T> = sqlx::query_as::<_, T>(param.query.as_str())
                .fetch_all(&param.pool)
                .await?;
            for element in rows {
                (param.action)(&element); // This is how to invoke an action that's a property.
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use serial_test::serial;
    use sqlx::{postgres::PgPoolOptions, FromRow, PgPool, Pool, Postgres};

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

    async fn get_all_examples(pool: &sqlx::PgPool) -> Vec<Example> {
        sqlx::query_as::<_, Example>("SELECT id, data, is_sent, version FROM example")
            .fetch_all(pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    #[serial]
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

    #[tokio::test]
    #[serial]
    async fn test_pg_db_idle_agent() {
        let pool = setup_db().await;

        let action = |example: &Example| {
            println!("Processing example {:?}", example);
        };

        let error_handler = |err: sqlx::Error| {
            eprintln!("Error while processing examples: {:?}", err);
        };

        let interval_secs = Duration::from_secs(1);
        let query = "SELECT id, data, is_sent, version FROM example".to_string();


        let params = PgDbAgentParams::new(
            vec![PgDbAgentQueryActionParams::new(pool, query, action)],
            interval_secs,
            error_handler,
        );
    
        let agent = PgDbIdleAgent::new(params);

        let handle = agent.start().await;

        tokio::time::sleep(Duration::from_secs(4)).await;

        handle.abort();
    }

    #[tokio::test]
    #[serial]
    async fn test_pg_db_idle_agent_error() {
        let pool = setup_db().await;

        let action = |example: &Example| {
            println!("Processing example {:?}", example);
        };

        let error_handler = |err: sqlx::Error| {
            eprintln!("Error while processing examples: {:?}", err);
        };

        let interval_secs = Duration::from_secs(1);
        let query = "INVALID SQL".to_string();

        let params = PgDbAgentParams::new(
            vec![PgDbAgentQueryActionParams::new(pool, query, action)],
            interval_secs,
            error_handler,
        );
    
        let agent = PgDbIdleAgent::new(params);

        let handle = agent.start().await;

        tokio::time::sleep(Duration::from_secs(4)).await;

        handle.abort();
    }
}
