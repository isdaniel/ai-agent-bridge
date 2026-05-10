//! HMAC-SHA256 signature verification for LINE webhook bodies.

use base64::Engine as _;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

/// Returns true iff `signature` (base64) matches `HMAC-SHA256(secret, body)`.
/// Constant-time comparison.
pub fn verify_signature(secret: &str, body: &[u8], signature: &str) -> bool {
    let mut mac = match Hmac::<Sha256>::new_from_slice(secret.as_bytes()) {
        Ok(m) => m,
        Err(_) => return false,
    };
    mac.update(body);
    let computed = mac.finalize().into_bytes();
    let provided = match base64::engine::general_purpose::STANDARD.decode(signature) {
        Ok(b) => b,
        Err(_) => return false,
    };
    if provided.len() != computed.len() {
        return false;
    }
    computed.as_slice().ct_eq(&provided).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn good_signature_passes() {
        let secret = "topsecret";
        let body = br#"{"events":[]}"#;
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        let sig = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
        assert!(verify_signature(secret, body, &sig));
    }

    #[test]
    fn tampered_body_rejected() {
        let secret = "topsecret";
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(b"original");
        let sig = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
        assert!(!verify_signature(secret, b"tampered", &sig));
    }

    #[test]
    fn bad_b64_rejected() {
        assert!(!verify_signature("k", b"x", "%%%not-base64%%%"));
    }
}
