//! Administrative authority is a signed capability, not namespace state.
use crate::{
    core::fragment_metadata::resolve_stable_fragment_id, enums::app_error_enum::AppError,
    structs::auth_structs::Claims,
};
use std::collections::HashMap;

pub async fn target(
    claims: &Claims,
    params: &HashMap<String, String>,
    disruptive: bool,
) -> Result<Option<String>, AppError> {
    claims.require_maintenance()?;
    if disruptive || params.get("all_fragments").is_some_and(|v| v == "true") {
        claims.require_operator()?;
    }
    match params
        .get("fragment")
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
    {
        Some(requested) => {
            let stable = resolve_stable_fragment_id(requested).await?;
            if !claims.operator && stable != claims.fragment {
                return Err(AppError::Forbidden(
                    "Maintenance is restricted to the token's fragment".into(),
                ));
            }
            Ok(Some(stable))
        }
        None if claims.operator => Ok(None),
        None => Ok(Some(claims.fragment.clone())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn claims(operator: bool, delete: bool) -> Claims {
        Claims {
            operator,
            fragment: "own".into(),
            fragment_alias: None,
            read: true,
            write: true,
            modify: true,
            delete,
            exp: usize::MAX,
        }
    }
    #[tokio::test]
    async fn fragment_authority_cannot_request_global_or_disruptive_work() {
        assert_eq!(
            target(&claims(false, true), &HashMap::new(), false)
                .await
                .unwrap(),
            Some("own".into())
        );
        assert!(
            target(&claims(false, false), &HashMap::new(), false)
                .await
                .is_err()
        );
        assert!(
            target(&claims(false, true), &HashMap::new(), true)
                .await
                .is_err()
        );
        let all = HashMap::from([("all_fragments".into(), "true".into())]);
        assert!(target(&claims(false, true), &all, false).await.is_err());
        assert_eq!(
            target(&claims(true, true), &all, false).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn explicit_cross_fragment_work_requires_master_operator() {
        let _guard = crate::test_support::global_test_lock().lock().await;
        crate::middleware::auth_middleware::tests::ensure_test_env();
        let other = format!("maintenance-other-{}", ulid::Ulid::new());
        crate::middleware::auth_middleware::tests::ensure_auth_test_fragment(&other).await;
        let params = HashMap::from([("fragment".into(), other.clone())]);
        assert!(matches!(
            target(&claims(false, true), &params, false).await,
            Err(AppError::Forbidden(_))
        ));
        let selected = target(&claims(true, true), &params, false)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(selected, "own");
    }
}
