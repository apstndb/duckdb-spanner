use thiserror::Error;

#[derive(Error, Debug)]
pub enum SpannerError {
    #[error("Google Cloud error: {0}")]
    GoogleCloud(#[from] google_cloud_gax::error::Error),

    #[error("Google Cloud client builder error: {0}")]
    GoogleCloudClientBuilder(#[from] google_cloud_gax::client_builder::Error),

    #[error("Type conversion error: {0}")]
    Conversion(String),

    #[error("{0}")]
    Other(String),
}
