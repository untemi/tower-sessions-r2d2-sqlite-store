use async_trait::async_trait;
use time::OffsetDateTime;

use r2d2_sqlite::{
    SqliteConnectionManager,
    rusqlite::{Error as SqlError, OptionalExtension, params},
};

use tower_sessions_core::{
    SessionStore,
    session::{Id, Record},
    session_store,
};

#[derive(thiserror::Error, Debug)]
pub enum SqliteStoreError {
    #[error(transparent)]
    Rusqlite(#[from] SqlError),

    #[error(transparent)]
    R2d2(#[from] r2d2::Error),

    #[error(transparent)]
    Encode(#[from] rmp_serde::encode::Error),

    #[error(transparent)]
    Decode(#[from] rmp_serde::decode::Error),
}

impl From<SqliteStoreError> for session_store::Error {
    fn from(err: SqliteStoreError) -> Self {
        match err {
            SqliteStoreError::Rusqlite(inner) => session_store::Error::Backend(inner.to_string()),
            SqliteStoreError::R2d2(inner) => session_store::Error::Backend(inner.to_string()),
            SqliteStoreError::Decode(inner) => session_store::Error::Decode(inner.to_string()),
            SqliteStoreError::Encode(inner) => session_store::Error::Encode(inner.to_string()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SqliteStore {
    pool: r2d2::Pool<SqliteConnectionManager>,
}

impl SqliteStore {
    pub fn new(r2d2_conn_pool: r2d2::Pool<SqliteConnectionManager>) -> Self {
        Self {
            pool: r2d2_conn_pool,
        }
    }

    pub fn migrate(&self) -> session_store::Result<()> {
        let query = r#"
            create table if not exists tower_sessions (
                id text primary key not null,
                data blob not null,
                expiry_date integer not null
            )"#;

        let conn = self.pool.get().map_err(SqliteStoreError::R2d2)?;

        conn.execute(query, [])
            .map_err(SqliteStoreError::Rusqlite)?;

        Ok(())
    }

    fn try_create_with_conn(&self, record: &Record) -> session_store::Result<bool> {
        let query = r#"select exists(select 1 from tower_sessions where id = ?1)"#;

        let conn = self.pool.get().map_err(SqliteStoreError::R2d2)?;

        let res = conn
            .query_row(query, [record.id.to_string()], |row| row.get(0))
            .map_err(SqliteStoreError::Rusqlite)?;

        Ok(res)
    }

    fn save_with_conn(&self, record: &Record) -> session_store::Result<()> {
        let query = r#"
            insert into tower_sessions
                (id, data, expiry_date)
                values (?1, ?2, ?3)
            on conflict(id) do update set
            data = excluded.data,
            expiry_date = excluded.expiry_date
        "#;

        let conn = self.pool.get().map_err(SqliteStoreError::R2d2)?;

        conn.execute(
            query,
            params![
                record.id.to_string(),
                rmp_serde::to_vec(record).map_err(SqliteStoreError::Encode)?,
                record.expiry_date.unix_timestamp(),
            ],
        )
        .map_err(SqliteStoreError::Rusqlite)?;

        Ok(())
    }
}

#[async_trait]
impl SessionStore for SqliteStore {
    async fn create(&self, record: &mut Record) -> session_store::Result<()> {
        while self.try_create_with_conn(record)? {
            record.id = Id::default();
        }

        self.save_with_conn(&record)?;

        Ok(())
    }

    async fn save(&self, record: &Record) -> session_store::Result<()> {
        self.save_with_conn(record)?;
        Ok(())
    }

    async fn load(&self, session_id: &Id) -> session_store::Result<Option<Record>> {
        let query = r#"
            select data from tower_sessions
            where id = ? and expiry_date > ?
        "#;

        let conn = self.pool.get().map_err(SqliteStoreError::R2d2)?;

        let data: Option<Vec<u8>> = conn
            .query_row(
                query,
                params![
                    session_id.to_string(),
                    OffsetDateTime::now_utc().unix_timestamp()
                ],
                |row| {
                    let data: Vec<u8> = row.get(0)?;
                    Ok(data)
                },
            )
            .optional()
            .map_err(SqliteStoreError::Rusqlite)?;

        match data {
            Some(data) => {
                let record: Record =
                    rmp_serde::from_slice(&data).map_err(SqliteStoreError::Decode)?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    async fn delete(&self, session_id: &Id) -> session_store::Result<()> {
        let query = "delete from tower_sessions where id = ?";
        let conn = self.pool.get().map_err(SqliteStoreError::R2d2)?;

        conn.execute(query, params![session_id.to_string()])
            .map_err(SqliteStoreError::Rusqlite)?;

        Ok(())
    }
}
