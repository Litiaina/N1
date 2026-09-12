fn main() -> std::process::ExitCode {
    match n1::core::backup::cli::run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("N1 backup failed: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
