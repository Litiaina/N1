use crate::enums::app_error_enum::AppError;
use crate::middleware::auth_middleware::ADMIN_KEY;
use crate::structs::auth_structs::Claims;

fn is_admin_claim(claims: &Claims) -> bool {
    claims.fragment == *ADMIN_KEY
}

pub(super) fn ensure_bucket_access(
    claims: &Claims,
    bucket: &str,
    read: bool,
    write: bool,
    delete: bool,
) -> Result<(), AppError> {
    if read && !claims.read {
        return Err(AppError::Forbidden(
            "S3 read permission required".to_string(),
        ));
    }
    if write && !claims.write {
        return Err(AppError::Forbidden(
            "S3 write permission required".to_string(),
        ));
    }
    if delete && !claims.delete {
        return Err(AppError::Forbidden(
            "S3 delete permission required".to_string(),
        ));
    }
    if !is_admin_claim(claims) && claims.fragment != bucket {
        return Err(AppError::Forbidden("S3 bucket access denied".to_string()));
    }
    Ok(())
}
