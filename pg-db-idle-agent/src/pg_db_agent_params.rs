use std::{marker::PhantomData, time::Duration};

use sqlx::{postgres::PgRow, PgPool};




pub struct PgDbAgentQueryActionParams<T, F>
where
    T: for<'r> sqlx::FromRow<'r, PgRow> + Send + Sync + Unpin + 'static,
    F: Fn(&T) + Send + Sync + 'static,
{
    pub pool: PgPool,
    pub query: String,
    pub action: F,
    pub _marker: PhantomData<T>, // Add this so compile does not complain about unused parameter T.
}

impl<T, F> PgDbAgentQueryActionParams<T, F>
where
    T: for<'r> sqlx::FromRow<'r, PgRow> + Send + Sync + Unpin + 'static,
    F: Fn(&T) + Send + Sync + 'static,
{
    pub fn new(pool: PgPool, query: String, action: F) -> Self {
        Self {
            pool,
            query,
            action,
            _marker: PhantomData,
        }
    }
}



pub struct PgDbAgentParams<T, F, E>
where
    T: for<'r> sqlx::FromRow<'r, PgRow> + Send + Sync + Unpin + 'static,
    F: Fn(&T) + Send + Sync + 'static,
{
    pub query_actions: Vec<PgDbAgentQueryActionParams<T, F>>,
    pub interval_secs: Duration,
    pub error_handler: E,
}

impl<T, F, E> PgDbAgentParams<T, F, E>
where
    T: for<'r> sqlx::FromRow<'r, PgRow> + Send + Sync + Unpin + 'static,
    F: Fn(&T) + Send + Sync + 'static,
{
    pub fn new(query_actions: Vec<PgDbAgentQueryActionParams<T, F>>, interval_secs: Duration, error_handler: E) -> Self {
        Self {
            query_actions,
            interval_secs,
            error_handler,
        }
    }
}