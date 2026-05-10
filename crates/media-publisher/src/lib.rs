//! Publish a local file under a public HTTPS URL so chat platforms (e.g. LINE,
//! whose Messaging API only accepts URLs, not binary uploads) can fetch it.
//!
//! Implementations:
//! * [`local_http::LocalHttpPublisher`] — runs an in-process axum server that
//!   serves files registered via [`MediaPublisher::publish`]. For development
//!   and self-hosted deployments behind a reverse proxy / tunnel.
//! * `R2Publisher` (planned) — uploads to Cloudflare R2 / S3-compatible storage
//!   and returns a presigned GET URL. Not implemented in this crate to keep
//!   the dependency footprint small; downstream code can implement
//!   [`MediaPublisher`] with `aws-sdk-s3` or similar.

use std::path::Path;

use async_trait::async_trait;
use url::Url;

#[cfg(feature = "local-http")]
pub mod local_http;

#[async_trait]
pub trait MediaPublisher: Send + Sync {
    /// Make `path` available at a public HTTPS URL. The URL should remain
    /// accessible long enough for the recipient platform to fetch it
    /// (LINE follows redirects synchronously when the message is delivered).
    async fn publish(&self, path: &Path, mime: &str) -> anyhow::Result<Url>;
}

/// A no-op publisher that returns an error on every call. Useful when the
/// platform is configured without a publisher and we want failures to be loud.
pub struct DisabledPublisher;

#[async_trait]
impl MediaPublisher for DisabledPublisher {
    async fn publish(&self, _path: &Path, _mime: &str) -> anyhow::Result<Url> {
        anyhow::bail!("media publisher is not configured")
    }
}
