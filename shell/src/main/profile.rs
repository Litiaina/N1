use super::*;

pub(super) fn run_profile_command(command: ProfileCommand, cli: &Cli) -> NativeClientResult<()> {
    match command {
        ProfileCommand::Path => {
            println!("{}", profile_store_path()?.display());
            Ok(())
        }
        ProfileCommand::List => {
            let profiles = load_profiles()?;
            if profiles.is_empty() {
                println!("No N2 shell profiles saved.");
                return Ok(());
            }
            for (name, profile) in profiles {
                let fragment = profile.fragment.unwrap_or_else(|| "-".to_string());
                let secret = if profile.secret.is_some() {
                    "stored"
                } else {
                    "not stored"
                };
                println!(
                    "{name}\n  native_url: {}\n  fragment:   {fragment}\n  secret:     {secret}",
                    profile.native_url
                );
            }
            Ok(())
        }
        ProfileCommand::Save { name, store_secret } => {
            let mut profiles = load_profiles()?;
            let profile = ShellProfile {
                native_url: cli.native_url.clone(),
                fragment: cli.fragment.clone(),
                secret: if store_secret {
                    cli.secret.clone()
                } else {
                    None
                },
            };
            profiles.insert(name.clone(), profile);
            save_profiles(&profiles)?;
            println!("saved N2 shell profile '{name}'");
            Ok(())
        }
        ProfileCommand::Remove { name } => {
            let mut profiles = load_profiles()?;
            if profiles.remove(&name).is_some() {
                save_profiles(&profiles)?;
                println!("removed N2 shell profile '{name}'");
            } else {
                println!("profile '{name}' was not found");
            }
            Ok(())
        }
    }
}

pub(super) fn apply_profile(cli: &mut Cli) -> NativeClientResult<()> {
    let Some(name) = cli.profile.clone() else {
        return Ok(());
    };
    let profiles = load_profiles()?;
    let profile = profiles.get(&name).ok_or_else(|| {
        NativeClientError::protocol(format!("N2 shell profile '{name}' not found"))
    })?;
    if cli.native_url == DEFAULT_NATIVE_URL {
        cli.native_url = profile.native_url.clone();
    }
    if cli.fragment.is_none() {
        cli.fragment = profile.fragment.clone();
    }
    if cli.secret.is_none() {
        cli.secret = profile.secret.clone();
    }
    Ok(())
}

pub(super) fn load_profiles() -> NativeClientResult<BTreeMap<String, ShellProfile>> {
    let path = profile_store_path()?;
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let bytes = fs::read(&path)?;
    if bytes.is_empty() {
        return Ok(BTreeMap::new());
    }
    Ok(serde_json::from_slice(&bytes)?)
}

pub(super) fn save_profiles(profiles: &BTreeMap<String, ShellProfile>) -> NativeClientResult<()> {
    let path = profile_store_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let body = serde_json::to_vec_pretty(profiles)?;
    fs::write(path, body)?;
    Ok(())
}

pub(super) fn profile_store_path() -> NativeClientResult<PathBuf> {
    if let Ok(config_home) = env::var("XDG_CONFIG_HOME") {
        return Ok(PathBuf::from(config_home).join("litiaina/n2/shell-profiles.json"));
    }
    let home = env::var("HOME").map_err(|_| {
        NativeClientError::protocol("HOME is not set; cannot resolve N2 shell profile path")
    })?;
    Ok(PathBuf::from(home).join(".config/litiaina/n2/shell-profiles.json"))
}

pub(super) fn monitor_payload(args: MonitorArgs, auth_fragment: &Option<String>) -> Value {
    json!({
        "fragment": args.target_fragment.or_else(|| auth_fragment.clone()),
        "prefix": args.prefix,
        "object_limit": args.object_limit,
        "probe_objects": args.probe_objects,
        "refresh": args.refresh,
        "deep_scan": args.deep_scan,
    })
}
