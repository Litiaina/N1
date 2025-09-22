use std::{env, sync::LazyLock};

pub static DATABASE_URL: LazyLock<String> =
    LazyLock::new(|| env::var("DATABASE_URL").expect("DATABASE_URL must be set"));
