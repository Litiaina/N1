use crate::TypeName;
use std::fmt::{Display, Formatter};
use std::io;
use std::panic;
use std::sync::PoisonError;

#[derive(Debug)]
pub enum StorageError {
    Io(io::Error),
    Sqlite {
        code: i32,
        message: String,
    },
    InvalidInput(String),
    ValueTooLarge(usize),
    TableDoesNotExist(String),
    TableExists(String),
    TableTypeMismatch {
        table: String,
        key: TypeName,
        value: TypeName,
    },
    TableAlreadyOpen(String, &'static panic::Location<'static>),
    LockPoisoned(&'static panic::Location<'static>),
    DatabaseClosed,
}

impl<T> From<PoisonError<T>> for StorageError {
    fn from(_: PoisonError<T>) -> Self {
        Self::LockPoisoned(panic::Location::caller())
    }
}

impl From<io::Error> for StorageError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

impl Display for StorageError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "I/O error: {err}"),
            Self::Sqlite { code, message } => write!(f, "indexdb engine error {code}: {message}"),
            Self::InvalidInput(message) => write!(f, "Invalid input: {message}"),
            Self::ValueTooLarge(len) => write!(f, "Value too large: {len} bytes"),
            Self::TableDoesNotExist(table) => write!(f, "Table '{table}' does not exist"),
            Self::TableExists(table) => write!(f, "Table '{table}' already exists"),
            Self::TableTypeMismatch { table, key, value } => write!(
                f,
                "{table} is not compatible with Table<{}, {}>",
                key.name(),
                value.name()
            ),
            Self::TableAlreadyOpen(name, location) => {
                write!(f, "Table '{name}' already opened at: {location}")
            }
            Self::LockPoisoned(location) => write!(f, "Lock poisoned at: {location}"),
            Self::DatabaseClosed => write!(f, "Database has been closed"),
        }
    }
}

impl std::error::Error for StorageError {}

#[derive(Debug)]
pub enum TableError {
    Storage(StorageError),
    TableDoesNotExist(String),
    TableExists(String),
    TableTypeMismatch {
        table: String,
        key: TypeName,
        value: TypeName,
    },
    TableAlreadyOpen(String, &'static panic::Location<'static>),
}

impl From<StorageError> for TableError {
    fn from(err: StorageError) -> Self {
        match err {
            StorageError::TableDoesNotExist(table) => Self::TableDoesNotExist(table),
            StorageError::TableExists(table) => Self::TableExists(table),
            StorageError::TableTypeMismatch { table, key, value } => {
                Self::TableTypeMismatch { table, key, value }
            }
            StorageError::TableAlreadyOpen(table, location) => {
                Self::TableAlreadyOpen(table, location)
            }
            other => Self::Storage(other),
        }
    }
}

impl<T> From<PoisonError<T>> for TableError {
    fn from(_: PoisonError<T>) -> Self {
        Self::Storage(StorageError::LockPoisoned(panic::Location::caller()))
    }
}

impl Display for TableError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(err) => err.fmt(f),
            Self::TableDoesNotExist(table) => write!(f, "Table '{table}' does not exist"),
            Self::TableExists(table) => write!(f, "Table '{table}' already exists"),
            Self::TableTypeMismatch { table, key, value } => write!(
                f,
                "{table} is not compatible with Table<{}, {}>",
                key.name(),
                value.name()
            ),
            Self::TableAlreadyOpen(table, location) => {
                write!(f, "Table '{table}' already opened at: {location}")
            }
        }
    }
}

impl std::error::Error for TableError {}

#[derive(Debug)]
pub enum DatabaseError {
    DatabaseAlreadyOpen,
    RepairAborted,
    UpgradeRequired(u8),
    Storage(StorageError),
}

impl From<StorageError> for DatabaseError {
    fn from(err: StorageError) -> Self {
        Self::Storage(err)
    }
}

impl From<io::Error> for DatabaseError {
    fn from(err: io::Error) -> Self {
        Self::Storage(StorageError::Io(err))
    }
}

impl<T> From<PoisonError<T>> for DatabaseError {
    fn from(_: PoisonError<T>) -> Self {
        Self::Storage(StorageError::LockPoisoned(panic::Location::caller()))
    }
}

impl Display for DatabaseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DatabaseAlreadyOpen => write!(f, "Database already open"),
            Self::RepairAborted => write!(f, "Database repair aborted"),
            Self::UpgradeRequired(version) => {
                write!(f, "Database file requires upgrade from format {version}")
            }
            Self::Storage(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for DatabaseError {}

#[derive(Debug)]
pub enum TransactionError {
    Storage(StorageError),
}

impl From<StorageError> for TransactionError {
    fn from(err: StorageError) -> Self {
        Self::Storage(err)
    }
}

impl<T> From<PoisonError<T>> for TransactionError {
    fn from(_: PoisonError<T>) -> Self {
        Self::Storage(StorageError::LockPoisoned(panic::Location::caller()))
    }
}

impl Display for TransactionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for TransactionError {}

#[derive(Debug)]
pub enum CommitError {
    Storage(StorageError),
}

impl From<StorageError> for CommitError {
    fn from(err: StorageError) -> Self {
        Self::Storage(err)
    }
}

impl<T> From<PoisonError<T>> for CommitError {
    fn from(_: PoisonError<T>) -> Self {
        Self::Storage(StorageError::LockPoisoned(panic::Location::caller()))
    }
}

impl Display for CommitError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for CommitError {}

#[derive(Debug)]
pub enum CompactionError {
    Storage(StorageError),
}

impl From<StorageError> for CompactionError {
    fn from(err: StorageError) -> Self {
        Self::Storage(err)
    }
}

impl<T> From<PoisonError<T>> for CompactionError {
    fn from(_: PoisonError<T>) -> Self {
        Self::Storage(StorageError::LockPoisoned(panic::Location::caller()))
    }
}

impl Display for CompactionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for CompactionError {}

#[derive(Debug)]
pub enum Error {
    DatabaseAlreadyOpen,
    RepairAborted,
    Corrupted(String),
    UpgradeRequired(u8),
    Storage(StorageError),
    Table(TableError),
    Transaction(TransactionError),
    Commit(CommitError),
}

impl From<StorageError> for Error {
    fn from(err: StorageError) -> Self {
        Self::Storage(err)
    }
}

impl From<TableError> for Error {
    fn from(err: TableError) -> Self {
        Self::Table(err)
    }
}

impl From<DatabaseError> for Error {
    fn from(err: DatabaseError) -> Self {
        match err {
            DatabaseError::DatabaseAlreadyOpen => Self::DatabaseAlreadyOpen,
            DatabaseError::RepairAborted => Self::RepairAborted,
            DatabaseError::UpgradeRequired(version) => Self::UpgradeRequired(version),
            DatabaseError::Storage(err) => Self::Storage(err),
        }
    }
}

impl From<TransactionError> for Error {
    fn from(err: TransactionError) -> Self {
        Self::Transaction(err)
    }
}

impl From<CommitError> for Error {
    fn from(err: CommitError) -> Self {
        Self::Commit(err)
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::Storage(StorageError::Io(err))
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_: PoisonError<T>) -> Self {
        Self::Storage(StorageError::LockPoisoned(panic::Location::caller()))
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DatabaseAlreadyOpen => write!(f, "Database already open"),
            Self::RepairAborted => write!(f, "Repair aborted"),
            Self::Corrupted(message) => write!(f, "DB corrupted: {message}"),
            Self::UpgradeRequired(version) => {
                write!(f, "Database file requires upgrade from format {version}")
            }
            Self::Storage(err) => err.fmt(f),
            Self::Table(err) => err.fmt(f),
            Self::Transaction(err) => err.fmt(f),
            Self::Commit(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for Error {}
