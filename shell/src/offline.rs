//! Shell entrypoints share the engine's offline implementation, rather than
//! duplicating storage algorithms or requiring a separately installed helper.
use crate::args::OfflineArgs;
use std::io;
use std::process::Command;

pub fn run(operation: &str, args: &OfflineArgs) -> cli_core::Result<()> {
    let engine = match &args.engine_binary {
        Some(path) => path.clone(),
        None => std::env::current_exe()?
            .parent()
            .ok_or_else(|| io::Error::other("shell has no parent directory"))?
            .join("litiaina-n1"),
    };
    let status = Command::new(engine)
        .arg(operation)
        .args(&args.arguments)
        .status()?;
    if !status.success() {
        return Err(io::Error::other(format!("N1 {operation} task exited with {status}")).into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::args::{Action, BackupAction, Cli};
    use clap::Parser;
    #[test]
    fn offline_commands_preserve_flags_without_network_authentication() {
        let cli = Cli::try_parse_from([
            "shell",
            "pool",
            "replace",
            "--config",
            "/etc/n1.json",
            "--pool",
            "A",
            "--member",
            "0",
            "--authorize",
            "--resume",
        ])
        .unwrap();
        let Action::Pool(args) = cli.command else {
            panic!("wrong command");
        };
        assert_eq!(args.arguments.last().unwrap(), "--resume");
        let cli = Cli::try_parse_from([
            "shell",
            "backup",
            "offline",
            "restore",
            "--snapshot",
            "id",
            "--target-config",
            "/etc/n1.json",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Action::Backup {
                command: BackupAction::Offline(_)
            }
        ));
    }
}
