use super::*;

#[test]
fn profile_precheck_does_not_discard_normal_commands() {
    let mut cli = Cli::try_parse_from([
        "litiaina-n2-shell",
        "--native-url",
        "n2://127.0.0.1:50002",
        "--fragment",
        "frag",
        "--secret",
        "secret",
        "upload",
        "--object-key",
        "bench/test.bin",
        "--file",
        "/tmp/test.bin",
    ])
    .expect("upload command parses");

    assert!(take_profile_command(&mut cli).is_none());
    assert!(matches!(cli.command, Some(Command::Upload(_))));
}

#[test]
fn serve_precheck_does_not_discard_native_commands() {
    let mut cli = Cli::try_parse_from([
        "litiaina-n2-shell",
        "--native-url",
        "n2://127.0.0.1:50002",
        "ping",
    ])
    .expect("ping command parses");

    assert!(take_serve_command(&mut cli).is_none());
    assert!(matches!(cli.command, Some(Command::Ping)));
}

#[test]
fn profile_precheck_takes_profile_commands_only() {
    let mut cli = Cli::try_parse_from(["litiaina-n2-shell", "profile", "path"])
        .expect("profile command parses");

    assert!(matches!(
        take_profile_command(&mut cli),
        Some(ProfileCommand::Path)
    ));
    assert!(cli.command.is_none());
}

#[test]
fn fragment_create_check_and_hash_are_public_commands() {
    assert!(is_public_fragment_command(&FragmentCommand::Create {
        name: "frag".to_string(),
        key: "key".to_string(),
        master_key: "master".to_string(),
    }));
    assert!(is_public_fragment_command(&FragmentCommand::Check {
        fragment: "frag".to_string(),
    }));
    assert!(is_public_fragment_command(&FragmentCommand::Hash {
        fragment: "frag".to_string(),
    }));
    assert!(!is_public_fragment_command(&FragmentCommand::List));
}

#[test]
fn flat_namespace_shell_has_no_manual_purge_or_empty_directory_commands() {
    for command in ["mkdir", "purge", "empty-trash"] {
        assert!(
            Cli::try_parse_from(["litiaina-n2-shell", command]).is_err(),
            "retired command '{command}' unexpectedly parsed"
        );
    }
}
