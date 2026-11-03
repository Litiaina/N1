//! Original operator INI backed by durable immutable pool enrollment state.
//! This is node configuration, never an object catalogue or metadata service.
mod bootstrap;
mod mapping;
mod registry;
use super::config::Config;
use crate::{
    core::{Error, Result, instance_lease::InstanceLease},
    protocol::ops::Service,
};
use cli_core::settings::N1Settings;
use std::{path::Path, sync::Arc};

pub fn is_ini(path: &Path) -> bool {
    path.extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("ini"))
}

fn settings(path: &Path) -> Result<N1Settings> {
    N1Settings::load(path).map_err(|error| Error::Invalid(error.to_string()))
}

pub fn load(path: &Path) -> Result<Config> {
    let settings = settings(path)?;
    let environment = settings
        .read_env()
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let registry = registry::Registry::load(&settings.control_dir)?;
    registry.validate(&settings)?;
    mapping::config(&settings, &environment, &registry)
}

/// Adopt an existing trusted LINK service configuration without enrolling,
/// moving or rewriting any member. Both old/new control and pool leases apply.
pub fn import(path: &Path, source: &Path) -> Result<()> {
    if is_ini(source) {
        return Err(Error::Invalid(
            "--from-config requires an existing private LINK JSON configuration".into(),
        ));
    }
    bootstrap::ensure(path)?;
    let settings = settings(path)?;
    let environment = settings
        .read_env()
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let original = Config::load(source)?;
    mapping::validate(&settings)?;
    registry::control_directory(&settings.control_dir, true)?;
    let old_control = original.ensure_state_root()?;
    let _lease = InstanceLease::engine(&[old_control, settings.control_dir.clone()])?;
    let _pools = original.open_pools()?;
    registry::import(&settings, &environment, &original)
}

pub fn open(path: &Path, enroll: bool) -> Result<Arc<Service>> {
    if enroll {
        bootstrap::ensure(path)?;
    }
    let settings = settings(path)?;
    let environment = settings
        .read_env()
        .map_err(|error| Error::Invalid(error.to_string()))?;
    // Validate operational settings before writing enrollment state or members.
    mapping::validate(&settings)?;
    registry::control_directory(&settings.control_dir, enroll)?;
    let lease = Arc::new(InstanceLease::engine(std::slice::from_ref(
        &settings.control_dir,
    ))?);
    let registry = if enroll {
        registry::prepare(&settings, &environment)?
    } else {
        registry::Registry::load(&settings.control_dir)?
    };
    registry.validate(&settings)?;
    let config = mapping::config(&settings, &environment, &registry)?;
    Service::open_leased(config, lease)
}
