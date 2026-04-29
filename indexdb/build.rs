fn main() {
    cc::Build::new()
        .file("sqlite/sqlite3.c")
        .define("SQLITE_THREADSAFE", "1")
        .define("SQLITE_OMIT_LOAD_EXTENSION", None)
        .define("SQLITE_DEFAULT_MEMSTATUS", "0")
        .define("SQLITE_USE_URI", "1")
        .define("SQLITE_ENABLE_DBSTAT_VTAB", None)
        .warnings(false)
        .compile("n1_indexdb");

    println!("cargo:rerun-if-changed=sqlite/sqlite3.c");
    println!("cargo:rerun-if-changed=sqlite/sqlite3.h");
    println!("cargo:rerun-if-changed=sqlite/sqlite3ext.h");
    println!("cargo:rerun-if-changed=sqlite/VERSION.txt");
}
