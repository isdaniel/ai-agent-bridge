use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use azure_storage::prelude::BlobSasPermissions;
use azure_storage::{CloudLocation, ConnectionString};
use azure_storage_blobs::prelude::{BlobContentType, ClientBuilder, ContainerClient};
use time::OffsetDateTime;
use tracing::info;
use url::Url;
use uuid::Uuid;

use crate::MediaPublisher;

pub struct AzureBlobPublisher {
    container_client: ContainerClient,
    sas_expiry: Duration,
}

impl AzureBlobPublisher {
    pub fn new(
        connection_string: &str,
        container: &str,
        sas_expiry: Duration,
    ) -> anyhow::Result<Self> {
        let cs = ConnectionString::new(connection_string)
            .map_err(|e| anyhow::anyhow!("invalid connection string: {e}"))?;

        let credentials = cs
            .storage_credentials()
            .map_err(|e| anyhow::anyhow!("invalid storage credentials: {e}"))?;

        let account = cs
            .account_name
            .ok_or_else(|| anyhow::anyhow!("connection string missing AccountName"))?;

        let cloud_location = if let Some(blob_endpoint) = cs.blob_endpoint {
            CloudLocation::Custom {
                account: account.to_string(),
                uri: blob_endpoint.to_string(),
            }
        } else {
            CloudLocation::Public {
                account: account.to_string(),
            }
        };

        let container_client =
            ClientBuilder::with_location(cloud_location, credentials).container_client(container);

        info!(container = %container, "Azure Blob publisher initialized");
        Ok(Self {
            container_client,
            sas_expiry,
        })
    }
}

#[async_trait]
impl MediaPublisher for AzureBlobPublisher {
    async fn publish(&self, path: &Path, mime: &str) -> anyhow::Result<Url> {
        let bytes = tokio::fs::read(path).await?;
        let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("file");
        let blob_name = format!("media/{}/{}", Uuid::new_v4(), filename);

        let blob_client = self.container_client.blob_client(&blob_name);
        blob_client
            .put_block_blob(bytes)
            .content_type(BlobContentType::from(mime.to_string()))
            .await
            .map_err(|e| anyhow::anyhow!("blob upload failed: {e}"))?;

        let expiry =
            OffsetDateTime::now_utc() + time::Duration::seconds(self.sas_expiry.as_secs() as i64);
        let permissions = BlobSasPermissions {
            read: true,
            ..Default::default()
        };
        let sas = blob_client
            .shared_access_signature(permissions, expiry)
            .await
            .map_err(|e| anyhow::anyhow!("SAS generation failed: {e}"))?;
        let url = blob_client
            .generate_signed_blob_url(&sas)
            .map_err(|e| anyhow::anyhow!("signed URL generation failed: {e}"))?;

        info!(blob = %blob_name, "published to Azure Blob Storage");
        Ok(Url::parse(url.as_str())?)
    }
}
