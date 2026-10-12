use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSystemClaims {
    pub path: String,
    pub read: bool,
    pub write: bool,
    pub modify: bool,
    pub delete: bool,
    pub exp: usize,
}

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub path: String,
    pub secret: String,
    #[serde(default)]
    pub read: bool,
    #[serde(default)]
    pub write: bool,
    #[serde(default)]
    pub modify: bool,
    #[serde(default)]
    pub delete: bool,
    pub expires_in: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub fragment: String,
    /// Granted only by master-key authentication, never by a fragment secret.
    #[serde(default)]
    pub operator: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fragment_alias: Option<String>,
    pub read: bool,
    pub write: bool,
    pub modify: bool,
    pub delete: bool,
    pub exp: usize,
}

impl Claims {
    pub fn require_modify(&self) -> Result<(), crate::enums::app_error_enum::AppError> {
        if self.modify {
            Ok(())
        } else {
            Err(crate::enums::app_error_enum::AppError::Forbidden(
                "Modify permission required".into(),
            ))
        }
    }
    pub fn require_delete(&self) -> Result<(), crate::enums::app_error_enum::AppError> {
        if self.delete {
            Ok(())
        } else {
            Err(crate::enums::app_error_enum::AppError::Forbidden(
                "Delete permission required".into(),
            ))
        }
    }
    pub fn require_operator(&self) -> Result<(), crate::enums::app_error_enum::AppError> {
        if self.operator {
            Ok(())
        } else {
            Err(crate::enums::app_error_enum::AppError::Forbidden(
                "Master-key operator authentication required for server-wide maintenance".into(),
            ))
        }
    }

    pub fn require_maintenance(&self) -> Result<(), crate::enums::app_error_enum::AppError> {
        if self.write && self.modify && self.delete {
            Ok(())
        } else {
            Err(crate::enums::app_error_enum::AppError::Forbidden(
                "Write, Modify and Delete permissions required".into(),
            ))
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct AuthorizationRequest {
    pub fragment: String,
    pub secret: String,
    #[serde(default)]
    pub read: bool,
    #[serde(default)]
    pub write: bool,
    #[serde(default)]
    pub modify: bool,
    #[serde(default)]
    pub delete: bool,
    pub expires_in: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct RefreshRequest {
    pub refresh_token: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum AuthSessionScope {
    Fragment { fragment: String },
    FileSystem { path: String },
}

#[derive(Debug, Serialize)]
pub struct AuthBody {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: u64,
    pub refresh_token: Option<String>,
    pub refresh_expires_in: Option<u64>,
}
