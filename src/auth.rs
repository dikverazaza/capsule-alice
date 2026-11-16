use crate::state::AppState;
use axum::{body::Body, extract::State, http::{header, Request, StatusCode}, middleware::Next, response::{IntoResponse, Response}};
use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use pbkdf2::pbkdf2_hmac;
use rand::{rngs::OsRng, RngCore};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use subtle::ConstantTimeEq;
use std::{fs, path::Path, sync::RwLock};

static VERIFIED_HEADER_CACHE: RwLock<Option<String>> = RwLock::new(None);

const ITERATIONS: u32 = 220_000;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuthData {
    #[serde(default)]
    pub salt: String,
    #[serde(default)]
    pub hash: String,
    #[serde(default)]
    pub disabled: bool,
}

pub fn ensure_auth(path: &Path, force_reset: bool) -> Result<(AuthData, Option<String>)> {
    if path.exists() && !force_reset {
        let data: AuthData = serde_json::from_slice(&fs::read(path).context("Не удалось прочитать web-auth.json")?)
            .context("Некорректный формат web-auth.json")?;
        #[cfg(unix)] {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        }
        return Ok((data, None));
    }

    let mut salt = [0u8; 16];
    OsRng.fill_bytes(&mut salt);
    let mut password_bytes = [0u8; 24];
    OsRng.fill_bytes(&mut password_bytes);
    let password = STANDARD.encode(password_bytes);
    let mut digest = [0u8; 32];
    pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, ITERATIONS, &mut digest);
    let data = AuthData { salt: STANDARD.encode(salt), hash: STANDARD.encode(digest), disabled: false };

    if let Some(parent) = path.parent() { fs::create_dir_all(parent)?; }
    let tmp = path.with_extension("json.new");
    fs::write(&tmp, serde_json::to_vec(&data)?)?;
    #[cfg(unix)] {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&tmp, fs::Permissions::from_mode(0o600))?;
    }
    fs::rename(&tmp, path)?;
    Ok((data, Some(password)))
}

pub fn verify_basic(header_value: Option<&header::HeaderValue>, data: &AuthData) -> bool {
    let Some(raw) = header_value.and_then(|v| v.to_str().ok()) else { return false; };
    if let Ok(cache) = VERIFIED_HEADER_CACHE.read() {
        if let Some(ref cached) = *cache {
            if cached.as_str() == raw { return true; }
        }
    }
    let Some(encoded) = raw.strip_prefix("Basic ") else { return false; };
    let Ok(decoded) = STANDARD.decode(encoded) else { return false; };
    let Ok(credentials) = String::from_utf8(decoded) else { return false; };
    let Some((user, password)) = credentials.split_once(':') else { return false; };
    if user != "admin" { return false; }

    let Ok(salt) = STANDARD.decode(&data.salt) else { return false; };
    let Ok(expected) = STANDARD.decode(&data.hash) else { return false; };
    if salt.len() != 16 || expected.len() != 32 { return false; }
    let mut actual = vec![0u8; expected.len()];
    pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, ITERATIONS, &mut actual);
    if actual.as_slice().ct_eq(expected.as_slice()).into() {
        if let Ok(mut cache) = VERIFIED_HEADER_CACHE.write() {
            *cache = Some(raw.to_string());
        }
        true
    } else {
        false
    }
}

pub async fn require_auth(State(state): State<AppState>, request: Request<Body>, next: Next) -> Response {
    if state.auth.disabled {
        return next.run(request).await;
    }
    let header_opt = request.headers().get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).map(str::to_owned);
    let is_authed = if let Some(ref raw) = header_opt {
        if let Ok(cache) = VERIFIED_HEADER_CACHE.read() {
            if let Some(ref cached) = *cache {
                cached == raw
            } else {
                false
            }
        } else {
            false
        }
    } else {
        false
    };

    if is_authed {
        return next.run(request).await;
    }

    let auth_data = state.auth.clone();
    let verified = tokio::task::spawn_blocking(move || {
        let Some(raw) = header_opt else { return false; };
        let Some(encoded) = raw.strip_prefix("Basic ") else { return false; };
        let Ok(decoded) = STANDARD.decode(encoded) else { return false; };
        let Ok(credentials) = String::from_utf8(decoded) else { return false; };
        let Some((user, password)) = credentials.split_once(':') else { return false; };
        if user != "admin" { return false; }

        let Ok(salt) = STANDARD.decode(&auth_data.salt) else { return false; };
        let Ok(expected) = STANDARD.decode(&auth_data.hash) else { return false; };
        if salt.len() != 16 || expected.len() != 32 { return false; }
        let mut actual = vec![0u8; expected.len()];
        pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, ITERATIONS, &mut actual);
        if actual.as_slice().ct_eq(expected.as_slice()).into() {
            if let Ok(mut cache) = VERIFIED_HEADER_CACHE.write() {
                *cache = Some(raw);
            }
            true
        } else {
            false
        }
    }).await.unwrap_or(false);

    if verified {
        return next.run(request).await;
    }

    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Basic realm=\"Capsula Mini\"")],
        "Требуется авторизация",
    ).into_response()
}
