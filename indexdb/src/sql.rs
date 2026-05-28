use crate::ffi::{ColumnValue, Connection as RawConnection, OpenMode, StepResult};
use crate::StorageError;
use std::cell::RefCell;
use std::fmt::{Display, Formatter};
use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

const SQLITE_BUSY: i32 = 5;
const SQLITE_LOCKED: i32 = 6;

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<&String> for Value {
    fn from(value: &String) -> Self {
        Self::Text(value.clone())
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::Text(value.to_string())
    }
}

impl From<&&str> for Value {
    fn from(value: &&str) -> Self {
        Self::Text((*value).to_string())
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<&i64> for Value {
    fn from(value: &i64) -> Self {
        Self::Integer(*value)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Self::Integer(i64::from(value))
    }
}

impl From<&i32> for Value {
    fn from(value: &i32) -> Self {
        Self::Integer(i64::from(*value))
    }
}

impl From<usize> for Value {
    fn from(value: usize) -> Self {
        Self::Integer(i64::try_from(value).unwrap_or(i64::MAX))
    }
}

impl From<&usize> for Value {
    fn from(value: &usize) -> Self {
        Self::Integer(i64::try_from(*value).unwrap_or(i64::MAX))
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Integer(i64::from(value))
    }
}

impl From<&bool> for Value {
    fn from(value: &bool) -> Self {
        Self::Integer(i64::from(*value))
    }
}

impl From<Option<String>> for Value {
    fn from(value: Option<String>) -> Self {
        value.map_or(Self::Null, Self::Text)
    }
}

impl From<&Option<String>> for Value {
    fn from(value: &Option<String>) -> Self {
        value.as_ref().map_or(Self::Null, |value| Self::Text(value.clone()))
    }
}

impl From<Option<&str>> for Value {
    fn from(value: Option<&str>) -> Self {
        value.map_or(Self::Null, |value| Self::Text(value.to_string()))
    }
}

impl From<&Option<&str>> for Value {
    fn from(value: &Option<&str>) -> Self {
        value.map_or(Self::Null, |value| Self::Text(value.to_string()))
    }
}

impl From<Option<i64>> for Value {
    fn from(value: Option<i64>) -> Self {
        value.map_or(Self::Null, Self::Integer)
    }
}

impl From<&Option<i64>> for Value {
    fn from(value: &Option<i64>) -> Self {
        value.map_or(Self::Null, Self::Integer)
    }
}

#[macro_export]
macro_rules! params {
    () => {
        Vec::<$crate::sql::Value>::new()
    };
    ($($value:expr),+ $(,)?) => {
        vec![$($crate::sql::Value::from(&$value)),+]
    };
}

pub fn params_from_iter<I>(values: I) -> Vec<Value>
where
    I: IntoIterator<Item = Value>,
{
    values.into_iter().collect()
}

#[derive(Debug, Clone)]
pub enum Error {
    Sqlite { code: i32, message: String },
    QueryReturnedNoRows,
    InvalidColumnIndex(usize),
    InvalidColumnType {
        index: usize,
        expected: &'static str,
        actual: &'static str,
    },
    InvalidData(String),
    LockPoisoned,
}

impl Error {
    pub fn is_busy_or_locked(&self) -> bool {
        match self {
            Self::Sqlite { code, message } => {
                let primary = code & 0xff;
                primary == SQLITE_BUSY
                    || primary == SQLITE_LOCKED
                    || message.to_lowercase().contains("database is locked")
                    || message.to_lowercase().contains("database is busy")
            }
            _ => false,
        }
    }

    pub fn sqlite_message(&self) -> Option<&str> {
        match self {
            Self::Sqlite { message, .. } => Some(message),
            Self::InvalidData(message) => Some(message),
            _ => None,
        }
    }
}

impl From<StorageError> for Error {
    fn from(value: StorageError) -> Self {
        match value {
            StorageError::Sqlite { code, message } => Self::Sqlite { code, message },
            StorageError::InvalidInput(message) => Self::InvalidData(message),
            other => Self::InvalidData(other.to_string()),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlite { code, message } => write!(f, "sqlite engine error {code}: {message}"),
            Self::QueryReturnedNoRows => write!(f, "query returned no rows"),
            Self::InvalidColumnIndex(index) => write!(f, "invalid column index {index}"),
            Self::InvalidColumnType {
                index,
                expected,
                actual,
            } => write!(
                f,
                "invalid column type at {index}: expected {expected}, got {actual}"
            ),
            Self::InvalidData(message) => write!(f, "invalid data: {message}"),
            Self::LockPoisoned => write!(f, "sqlite engine lock poisoned"),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

pub trait OptionalExtension<T> {
    fn optional(self) -> Result<Option<T>>;
}

impl<T> OptionalExtension<T> for Result<T> {
    fn optional(self) -> Result<Option<T>> {
        match self {
            Ok(value) => Ok(Some(value)),
            Err(Error::QueryReturnedNoRows) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

pub struct Connection {
    inner: Mutex<RawConnection>,
}

pub trait SqlExecutor {
    fn execute<P>(&self, sql: &str, params: P) -> Result<usize>
    where
        P: Into<Vec<Value>>;

    fn prepare(&self, sql: &str) -> Result<Statement>;

    fn query_row<P, F, T>(&self, sql: &str, params: P, f: F) -> Result<T>
    where
        P: Into<Vec<Value>>,
        F: FnOnce(&Row) -> Result<T>;
}

impl Connection {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let raw =
            RawConnection::open_plain_sql(path.as_ref(), OpenMode::ReadWriteCreate, 2 * 1024 * 1024)
                .map_err(Error::from)?;
        Ok(Self {
            inner: Mutex::new(raw),
        })
    }

    pub fn busy_timeout(&self, duration: Duration) -> Result<()> {
        let millis = duration.as_millis().min(i32::MAX as u128) as i32;
        self.with_raw(|raw| raw.busy_timeout(millis).map_err(Error::from))
    }

    pub fn pragma_update<T>(&self, _database: Option<&str>, pragma: &str, value: T) -> Result<()>
    where
        T: ToPragmaValue,
    {
        let sql = format!("PRAGMA {pragma} = {}", value.to_pragma_value());
        self.execute_batch(&sql)
    }

    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        self.with_raw(|raw| raw.exec(sql).map_err(Error::from))
    }

    pub fn transaction(&mut self) -> Result<Transaction<'_>> {
        let mut guard = self.inner.lock().map_err(|_| Error::LockPoisoned)?;
        guard.exec("BEGIN IMMEDIATE").map_err(Error::from)?;
        Ok(Transaction {
            guard: RefCell::new(guard),
            complete: false,
        })
    }

    fn with_raw<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut RawConnection) -> Result<T>,
    {
        let mut raw = self.inner.lock().map_err(|_| Error::LockPoisoned)?;
        f(&mut raw)
    }
}

impl SqlExecutor for Connection {
    fn execute<P>(&self, sql: &str, params: P) -> Result<usize>
    where
        P: Into<Vec<Value>>,
    {
        self.with_raw(|raw| execute_raw(raw, sql, params.into()))
    }

    fn prepare(&self, sql: &str) -> Result<Statement> {
        self.with_raw(|raw| {
            raw.prepare(sql)
                .map(|inner| Statement { inner })
                .map_err(Error::from)
        })
    }

    fn query_row<P, F, T>(&self, sql: &str, params: P, f: F) -> Result<T>
    where
        P: Into<Vec<Value>>,
        F: FnOnce(&Row) -> Result<T>,
    {
        self.prepare(sql)?.query_row(params, f)
    }
}

pub struct Transaction<'a> {
    guard: RefCell<MutexGuard<'a, RawConnection>>,
    complete: bool,
}

impl SqlExecutor for Transaction<'_> {
    fn execute<P>(&self, sql: &str, params: P) -> Result<usize>
    where
        P: Into<Vec<Value>>,
    {
        let mut guard = self.guard.borrow_mut();
        execute_raw(&mut guard, sql, params.into())
    }

    fn prepare(&self, sql: &str) -> Result<Statement> {
        let mut guard = self.guard.borrow_mut();
        guard
            .prepare(sql)
            .map(|inner| Statement { inner })
            .map_err(Error::from)
    }

    fn query_row<P, F, T>(&self, sql: &str, params: P, f: F) -> Result<T>
    where
        P: Into<Vec<Value>>,
        F: FnOnce(&Row) -> Result<T>,
    {
        self.prepare(sql)?.query_row(params, f)
    }
}

impl Transaction<'_> {
    pub fn commit(mut self) -> Result<()> {
        self.guard.get_mut().exec("COMMIT").map_err(Error::from)?;
        self.complete = true;
        Ok(())
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.complete {
            let _ = self.guard.get_mut().exec("ROLLBACK");
        }
    }
}

pub struct Statement {
    inner: crate::ffi::Statement,
}

impl Statement {
    pub fn query_row<P, F, T>(&mut self, params: P, f: F) -> Result<T>
    where
        P: Into<Vec<Value>>,
        F: FnOnce(&Row) -> Result<T>,
    {
        bind_values(&mut self.inner, params.into())?;
        match self.inner.step().map_err(Error::from)? {
            StepResult::Row => {
                let row = Row::from_statement(&self.inner)?;
                f(&row)
            }
            StepResult::Done => Err(Error::QueryReturnedNoRows),
        }
    }

    pub fn query_map<P, F, T>(&mut self, params: P, mut f: F) -> Result<MappedRows<T>>
    where
        P: Into<Vec<Value>>,
        F: FnMut(&Row) -> Result<T>,
    {
        bind_values(&mut self.inner, params.into())?;
        let mut rows = Vec::new();
        loop {
            match self.inner.step().map_err(Error::from)? {
                StepResult::Row => {
                    let row = Row::from_statement(&self.inner)?;
                    rows.push(f(&row));
                }
                StepResult::Done => break,
            }
        }
        Ok(MappedRows {
            inner: rows.into_iter(),
        })
    }
}

pub struct MappedRows<T> {
    inner: std::vec::IntoIter<Result<T>>,
}

impl<T> Iterator for MappedRows<T> {
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub struct Row {
    values: Vec<Value>,
}

impl Row {
    fn from_statement(stmt: &crate::ffi::Statement) -> Result<Self> {
        let count = stmt.column_count();
        let mut values = Vec::with_capacity(usize::try_from(count.max(0)).unwrap_or_default());
        for index in 0..count {
            values.push(match stmt.column_value(index) {
                ColumnValue::Null => Value::Null,
                ColumnValue::Integer(value) => Value::Integer(value),
                ColumnValue::Real(value) => Value::Real(value),
                ColumnValue::Text(value) => Value::Text(value),
                ColumnValue::Blob(value) => Value::Blob(value),
            });
        }
        Ok(Self { values })
    }

    pub fn get<I, T>(&self, index: I) -> Result<T>
    where
        I: RowIndex,
        T: FromSqlValue,
    {
        let index = index.to_index();
        let value = self
            .values
            .get(index)
            .ok_or(Error::InvalidColumnIndex(index))?;
        T::from_sql_value(index, value)
    }
}

pub trait RowIndex {
    fn to_index(self) -> usize;
}

impl RowIndex for usize {
    fn to_index(self) -> usize {
        self
    }
}

impl RowIndex for i32 {
    fn to_index(self) -> usize {
        self.max(0) as usize
    }
}

pub trait FromSqlValue: Sized {
    fn from_sql_value(index: usize, value: &Value) -> Result<Self>;
}

impl FromSqlValue for String {
    fn from_sql_value(index: usize, value: &Value) -> Result<Self> {
        match value {
            Value::Text(value) => Ok(value.clone()),
            Value::Integer(value) => Ok(value.to_string()),
            Value::Real(value) => Ok(value.to_string()),
            Value::Null => Err(Error::InvalidColumnType {
                index,
                expected: "TEXT",
                actual: "NULL",
            }),
            Value::Blob(_) => Err(Error::InvalidColumnType {
                index,
                expected: "TEXT",
                actual: "BLOB",
            }),
        }
    }
}

impl FromSqlValue for Option<String> {
    fn from_sql_value(index: usize, value: &Value) -> Result<Self> {
        match value {
            Value::Null => Ok(None),
            _ => String::from_sql_value(index, value).map(Some),
        }
    }
}

impl FromSqlValue for i64 {
    fn from_sql_value(index: usize, value: &Value) -> Result<Self> {
        match value {
            Value::Integer(value) => Ok(*value),
            Value::Text(value) => value.parse::<i64>().map_err(|err| Error::InvalidData(format!(
                "column {index} is not an integer: {err}"
            ))),
            Value::Real(value) => Ok(*value as i64),
            Value::Null => Err(Error::InvalidColumnType {
                index,
                expected: "INTEGER",
                actual: "NULL",
            }),
            Value::Blob(_) => Err(Error::InvalidColumnType {
                index,
                expected: "INTEGER",
                actual: "BLOB",
            }),
        }
    }
}

impl FromSqlValue for Option<i64> {
    fn from_sql_value(index: usize, value: &Value) -> Result<Self> {
        match value {
            Value::Null => Ok(None),
            _ => i64::from_sql_value(index, value).map(Some),
        }
    }
}

fn execute_raw(raw: &mut RawConnection, sql: &str, params: Vec<Value>) -> Result<usize> {
    let mut stmt = raw
        .prepare(sql)
        .map(|inner| Statement { inner })
        .map_err(Error::from)?;
    bind_values(&mut stmt.inner, params)?;
    stmt.inner.step_done().map_err(Error::from)?;
    Ok(usize::try_from(raw.changes()).unwrap_or_default())
}

fn bind_values(stmt: &mut crate::ffi::Statement, params: Vec<Value>) -> Result<()> {
    for (offset, value) in params.into_iter().enumerate() {
        let index = i32::try_from(offset + 1)
            .map_err(|_| Error::InvalidData("too many SQL parameters".to_string()))?;
        match value {
            Value::Null => stmt.bind_null(index).map_err(Error::from)?,
            Value::Integer(value) => stmt.bind_i64(index, value).map_err(Error::from)?,
            Value::Real(value) => stmt
                .bind_text(index, &value.to_string())
                .map_err(Error::from)?,
            Value::Text(value) => stmt.bind_text(index, &value).map_err(Error::from)?,
            Value::Blob(value) => stmt.bind_blob(index, &value).map_err(Error::from)?,
        }
    }
    Ok(())
}

pub trait ToPragmaValue {
    fn to_pragma_value(self) -> String;
}

impl ToPragmaValue for &str {
    fn to_pragma_value(self) -> String {
        self.to_string()
    }
}

impl ToPragmaValue for String {
    fn to_pragma_value(self) -> String {
        self
    }
}

impl ToPragmaValue for i64 {
    fn to_pragma_value(self) -> String {
        self.to_string()
    }
}

impl ToPragmaValue for usize {
    fn to_pragma_value(self) -> String {
        self.to_string()
    }
}
