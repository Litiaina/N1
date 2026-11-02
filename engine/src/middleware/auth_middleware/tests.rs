use super::*;
use argon2::{PasswordHasher, password_hash::SaltString};
struct Fixture {
    root: std::path::PathBuf,
    fragments: Vec<Fragment>,
}
impl Fixture {
    fn new() -> Self {
        let hash = Argon2::default()
            .hash_password(b"test-secret", &SaltString::encode_b64(&[9; 16]).unwrap())
            .unwrap()
            .to_string();
        Self {
            root: std::env::temp_dir().join(format!("n1-refresh-{}", ulid::Ulid::new())),
            fragments: vec![Fragment {
                id: PoolId([5; 16]),
                alias: "test".into(),
                secret_hash: hash,
                permissions: Permissions::all(),
                operator: true,
            }],
        }
    }
    fn auth(&self) -> Auth {
        Auth::with_session_root(self.fragments.clone(), &[33; 32], self.root.clone())
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_login_keeps_control_lease_until_its_owned_session_write_finishes() {
    use crate::core::instance_lease::InstanceLease;
    use std::os::unix::fs::DirBuilderExt;
    let fixture = Fixture::new();
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&fixture.root)
        .unwrap();
    let auth = Arc::new(Auth::with_session_root(
        fixture.fragments.clone(),
        &[33; 32],
        fixture.root.join("sessions"),
    ));
    auth.activity
        .bind_lease(Arc::new(
            InstanceLease::acquire(std::slice::from_ref(&fixture.root), false).unwrap(),
        ))
        .unwrap();
    let weak = Arc::downgrade(&auth.activity);
    let blocked = crate::middleware::refresh_store::hold_session_writes().await;
    let caller = {
        let auth = auth.clone();
        tokio::spawn(async move {
            auth.login("test".into(), "test-secret".into(), None, None)
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while auth.activity.status().unwrap().active_operations != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    caller.abort();
    assert!(matches!(caller.await,Err(error) if error.is_cancelled()));
    drop(auth);
    assert!(InstanceLease::acquire(std::slice::from_ref(&fixture.root), true).is_err());
    drop(blocked);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while weak.upgrade().is_some() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let records = std::fs::read_dir(fixture.root.join("sessions"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .count();
    assert_eq!(records, 1);
    InstanceLease::acquire(std::slice::from_ref(&fixture.root), true).unwrap();
}

#[tokio::test]
async fn refresh_rotates_token_and_logout_revokes_session() {
    let fixture = Fixture::new();
    let auth = fixture.auth();
    let initial = auth
        .login("test".into(), "test-secret".into(), None, Some(120))
        .await
        .unwrap();
    let rotated = auth.refresh(&initial.refresh_token).await.unwrap();
    assert_ne!(initial.refresh_token, rotated.refresh_token);
    assert_eq!(initial.session.session_id, rotated.session.session_id);
    auth.logout(&rotated.session).await.unwrap();
    assert!(matches!(
        fixture.auth().refresh(&rotated.refresh_token).await,
        Err(Error::Unauthorized)
    ));
}
#[tokio::test]
async fn refresh_reuse_detection_revokes_rotated_session_after_restart() {
    let fixture = Fixture::new();
    let initial = fixture
        .auth()
        .login("test".into(), "test-secret".into(), None, None)
        .await
        .unwrap();
    let rotated = fixture
        .auth()
        .refresh(&initial.refresh_token)
        .await
        .unwrap();
    assert!(matches!(
        fixture.auth().refresh(&initial.refresh_token).await,
        Err(Error::Unauthorized)
    ));
    assert!(matches!(
        fixture.auth().refresh(&rotated.refresh_token).await,
        Err(Error::Unauthorized)
    ));
}
#[tokio::test]
async fn refresh_cannot_expand_permissions_or_survive_credential_rotation() {
    let mut fixture = Fixture::new();
    let restricted = Permissions {
        read: true,
        ..Default::default()
    };
    let initial = fixture
        .auth()
        .login("test".into(), "test-secret".into(), Some(restricted), None)
        .await
        .unwrap();
    let rotated = fixture
        .auth()
        .refresh(&initial.refresh_token)
        .await
        .unwrap();
    assert_eq!(rotated.session.permissions, restricted);
    assert!(!rotated.session.operator);
    fixture.fragments[0].secret_hash = Argon2::default()
        .hash_password(
            b"changed-secret",
            &SaltString::encode_b64(&[8; 16]).unwrap(),
        )
        .unwrap()
        .to_string();
    assert!(matches!(
        fixture.auth().refresh(&rotated.refresh_token).await,
        Err(Error::Unauthorized)
    ));
}
#[tokio::test]
async fn refresh_rejects_traversal_malformed_tokens_and_expired_sessions() {
    use crate::middleware::refresh_store::*;
    let fixture = Fixture::new();
    for token in [
        "../../escape.secret",
        "/tmp/secret.value",
        "invalid",
        "00000000-0000-0000-0000-000000000000.invalid",
    ] {
        assert!(matches!(
            fixture.auth().refresh(token).await,
            Err(Error::Unauthorized)
        ));
    }
    let expired = create_refresh_session(
        &fixture.root,
        fixture.fragments[0].id,
        SessionPermissions {
            operator: false,
            read: true,
            write: false,
            delete: false,
            repair: false,
        },
        60,
        0,
        credential_fingerprint(&fixture.fragments[0].secret_hash),
    )
    .await
    .unwrap();
    assert!(matches!(
        fixture.auth().refresh(&expired.refresh_token).await,
        Err(Error::Unauthorized)
    ));
}
