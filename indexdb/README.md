# indexdb

`indexdb` is the internal embedded database owned and used by Litiaina N2.

This crate is not published and is not maintained as a general-purpose public dependency. In this
workspace it exists to provide the ordered, transactional local storage layer that N1 uses for its
authoritative per-partition object/session index.

## Scope

- internal library only
- single-process embedded storage
- ordered key/value tables with ACID transactions
- foundation for N1 partition metadata authority

## Example

```rust
use indexdb::{Database, Error, ReadableDatabase, TableDefinition};

const TABLE: TableDefinition<&str, u64> = TableDefinition::new("my_data");

fn main() -> Result<(), Error> {
    let db = Database::create("partition.ndxdb")?;
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(TABLE)?;
        table.insert("generation", &1)?;
    }
    write_txn.commit()?;

    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(TABLE)?;
    assert_eq!(table.get("generation")?.unwrap().value(), 1);

    Ok(())
}
```

## Notes

- Public packaging, docker publishing, python bindings, and benchmark-container workflows are out of
  scope.
- The active tree is intentionally limited to the internal Rust library surface that N1 uses inside
  this workspace.
