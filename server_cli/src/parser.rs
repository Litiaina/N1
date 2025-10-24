use std::io::Write;

pub enum CliCommand {
    Exit,
    Status,
    Help,
    Unknown(String),
}

impl CliCommand {
    pub fn parse(input: &str) -> Self {
        match input.trim() {
            "/exit" => CliCommand::Exit,
            "/status" => CliCommand::Status,
            "/help" => CliCommand::Help,
            other => CliCommand::Unknown(other.to_string()),
        }
    }
}

pub fn clear_screen() {
    if cfg!(windows) {
        std::process::Command::new("cmd")
            .args(&["/C", "cls"])
            .status()
            .ok();
    } else {
        print!("\x1B[2J\x1B[H");
        std::io::stdout().flush().ok();
    }
}
