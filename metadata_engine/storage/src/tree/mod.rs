//! Immutable ordered tree. Only touched paths are rewritten by a transaction.
use crate::{
    Error, MAX_VALUE_BYTES, Result,
    format::{self, Appender, Generation, Input},
};
use std::{
    collections::{HashMap, VecDeque},
    ops::Bound,
    sync::{Arc, Mutex, atomic::Ordering},
};

const TARGET_BYTES: usize = 16 * 1024;
// Small mutable metadata should not rewrite a 16 KiB leaf for one changed
// binding. Branches retain their wider fanout for large catalogues.
const LEAF_TARGET_BYTES: usize = 4 * 1024;
const MAX_ITEMS: usize = 256;
const MAX_DEPTH: usize = 64;

#[derive(Clone, Debug)]
pub(crate) struct Entry {
    pub key: Vec<u8>,
    pub sequence: u64,
    pub offset: u64,
    pub length: u32,
}

#[derive(Clone)]
pub(crate) struct Child {
    pub key: Vec<u8>,
    pub offset: u64,
}

pub(crate) enum Node {
    Leaf(Vec<Entry>),
    Branch(Vec<Child>),
}

impl Node {
    fn bytes(&self) -> usize {
        match self {
            Self::Leaf(entries) => entries.iter().map(|e| e.key.len() + 64).sum::<usize>() + 64,
            Self::Branch(children) => children.iter().map(|e| e.key.len() + 40).sum::<usize>() + 64,
        }
    }
}

mod bulk;
mod cache;
mod cursor;
mod edit;
mod integrity;
mod node;
pub(crate) use bulk::BulkBuilder;
pub(crate) use cache::NodeCache;
pub(crate) use cursor::{Cursor, get};
pub(crate) use edit::apply;
pub use integrity::Integrity;
pub(crate) use integrity::verify;
pub(crate) use node::load;
use node::write_node;
