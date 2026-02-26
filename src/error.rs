use thiserror::Error;

#[derive(Error, Debug)]
pub enum SpannerError {
    #[error("Spanner client error: {0}")]
    Client(#[from] google_cloud_spanner::client::Error),

    #[error("gRPC error: {0}")]
    Grpc(#[from] google_cloud_gax::grpc::Status),

    #[error("Row extraction error: {0}")]
    Row(#[from] google_cloud_spanner::row::Error),

    #[error("Conversion error: {0}")]
    Conversion(String),

    #[error("{0}")]
    Other(String),
}
