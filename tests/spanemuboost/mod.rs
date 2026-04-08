//! Rust port of apstndb/spanemuboost.
//!
//! Design follows the Go original: emulator (container + instance) is separated
//! from database creation, allowing multiple databases on the same instance for
//! parallel test isolation.
//!
//! - [`SpanEmuBoost`]: container + instance (like Go's `Emulator`)
//! - [`SpanEmuDatabase`]: a database on the instance (like Go's `Clients`)
//! - [`SpanEmuBoost::create_database`]: create additional databases (like Go's `OpenClients`)

use std::error::Error;

use google_cloud_gax::conn::Environment;
use google_cloud_spanner::admin::client::Client as AdminClient;
use google_cloud_spanner::admin::AdminClientConfig;
use google_cloud_spanner::client::{Client, ClientConfig, Error as SpannerClientError};
use google_cloud_spanner::session::SessionConfig;
use google_cloud_spanner::statement::Statement;
use google_cloud_googleapis::spanner::admin::database::v1::{
    CreateDatabaseRequest, DatabaseDialect,
};
use google_cloud_googleapis::spanner::admin::instance::v1::{CreateInstanceRequest, Instance};
use testcontainers::core::IntoContainerPort;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};

const DEFAULT_IMAGE: &str = "gcr.io/cloud-spanner-emulator/emulator";
const DEFAULT_TAG: &str = "1.5.51";
const DEFAULT_PROJECT_ID: &str = "emulator-project";
const DEFAULT_INSTANCE_ID: &str = "emulator-instance";
const GRPC_PORT: u16 = 9010;

/// Emulator container + Spanner instance.
///
/// Create databases with [`SpanEmuBoost::create_database`]. Multiple databases
/// (including different dialects) can coexist on the same instance, enabling
/// parallel test execution without schema change conflicts.
#[allow(dead_code)]
pub struct SpanEmuBoost {
    _container: ContainerAsync<GenericImage>,
    emulator_host: String,
    project_id: String,
    instance_id: String,
}

#[allow(dead_code)]
impl SpanEmuBoost {
    pub fn builder() -> SpanEmuBoostBuilder {
        SpanEmuBoostBuilder::default()
    }

    pub fn emulator_host(&self) -> &str {
        &self.emulator_host
    }

    pub fn instance_path(&self) -> String {
        format!(
            "projects/{}/instances/{}",
            self.project_id, self.instance_id
        )
    }

    pub fn project_path(&self) -> String {
        format!("projects/{}", self.project_id)
    }

    /// Create a database on this emulator instance.
    ///
    /// Each database is fully isolated — different databases can use different
    /// dialects and run DDL concurrently without conflicting.
    pub async fn create_database(
        &self,
        database_id: &str,
        dialect: DatabaseDialect,
        ddls: Vec<String>,
        dmls: Vec<String>,
    ) -> Result<SpanEmuDatabase, Box<dyn Error>> {
        let admin_config = AdminClientConfig {
            environment: Environment::Emulator(self.emulator_host.clone()),
            ..Default::default()
        };
        let admin_client = AdminClient::new(admin_config).await?;

        let create_statement = match dialect {
            DatabaseDialect::Postgresql => format!("CREATE DATABASE \"{}\"", database_id),
            _ => format!("CREATE DATABASE `{}`", database_id),
        };
        let create_db_req = CreateDatabaseRequest {
            parent: self.instance_path(),
            create_statement,
            extra_statements: ddls,
            encryption_config: None,
            database_dialect: dialect.into(),
            proto_descriptors: vec![],
        };
        let mut db_op = admin_client
            .database()
            .create_database(create_db_req, None)
            .await?;
        db_op.wait(None).await?;

        let database_path = format!(
            "projects/{}/instances/{}/databases/{}",
            self.project_id, self.instance_id, database_id
        );

        if !dmls.is_empty() {
            let mut session_config = SessionConfig::default();
            session_config.min_opened = 1;
            session_config.max_opened = 1;
            let data_client = Client::new(
                &database_path,
                ClientConfig {
                    session_config,
                    environment: Environment::Emulator(self.emulator_host.clone()),
                    ..Default::default()
                },
            )
            .await?;

            let stmts: Vec<Statement> = dmls
                .iter()
                .map(|dml| Statement::new(dml.as_str()))
                .collect();

            let result: Result<(_, Vec<i64>), SpannerClientError> = data_client
                .read_write_transaction(|tx| {
                    let stmts = stmts.clone();
                    Box::pin(async move {
                        let counts = tx.batch_update(stmts).await?;
                        Ok(counts)
                    })
                })
                .await;
            result?;

            data_client.close().await;
        }

        Ok(SpanEmuDatabase {
            emulator_host: self.emulator_host.clone(),
            database_path,
            database_id: database_id.to_string(),
        })
    }
}

/// A database on an emulator instance, created via [`SpanEmuBoost::create_database`].
#[allow(dead_code)]
pub struct SpanEmuDatabase {
    emulator_host: String,
    database_path: String,
    database_id: String,
}

#[allow(dead_code)]
impl SpanEmuDatabase {
    pub fn emulator_host(&self) -> &str {
        &self.emulator_host
    }

    pub fn database_path(&self) -> &str {
        &self.database_path
    }

    pub fn database_id(&self) -> &str {
        &self.database_id
    }
}

/// Builder for [`SpanEmuBoost`].
///
/// Creates the emulator container and Spanner instance.
/// Use [`SpanEmuBoost::create_database`] afterwards to create databases.
pub struct SpanEmuBoostBuilder {
    image: String,
    tag: String,
    project_id: String,
    instance_id: String,
}

impl Default for SpanEmuBoostBuilder {
    fn default() -> Self {
        Self {
            image: DEFAULT_IMAGE.to_string(),
            tag: DEFAULT_TAG.to_string(),
            project_id: DEFAULT_PROJECT_ID.to_string(),
            instance_id: DEFAULT_INSTANCE_ID.to_string(),
        }
    }
}

#[allow(dead_code)]
impl SpanEmuBoostBuilder {
    pub fn image(mut self, image: &str) -> Self {
        self.image = image.to_string();
        self
    }

    pub fn tag(mut self, tag: &str) -> Self {
        self.tag = tag.to_string();
        self
    }

    pub fn project_id(mut self, id: &str) -> Self {
        self.project_id = id.to_string();
        self
    }

    pub fn instance_id(mut self, id: &str) -> Self {
        self.instance_id = id.to_string();
        self
    }

    /// Start the emulator container and create the Spanner instance.
    ///
    /// No database is created — use [`SpanEmuBoost::create_database`] to create
    /// one or more databases with the desired dialect, DDLs, and seed data.
    pub async fn start(self) -> Result<SpanEmuBoost, Box<dyn Error>> {
        // 1. Start the emulator container
        let container = GenericImage::new(&self.image, &self.tag)
            .with_exposed_port(GRPC_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stderr("Cloud Spanner emulator running"))
            .start()
            .await?;

        let host_port = container.get_host_port_ipv4(GRPC_PORT).await?;
        let emulator_host = format!("localhost:{host_port}");

        let project_path = format!("projects/{}", self.project_id);
        let instance_path = format!("{}/instances/{}", project_path, self.instance_id);

        // 2. Create admin client
        let admin_config = AdminClientConfig {
            environment: Environment::Emulator(emulator_host.clone()),
            ..Default::default()
        };
        let admin_client = AdminClient::new(admin_config).await?;

        // 3. Create instance
        let create_instance_req = CreateInstanceRequest {
            parent: project_path,
            instance_id: self.instance_id.clone(),
            instance: Some(Instance {
                name: instance_path,
                config: String::new(),
                display_name: self.instance_id.clone(),
                node_count: 1,
                processing_units: 0,
                replica_compute_capacity: vec![],
                autoscaling_config: None,
                state: 0,
                labels: Default::default(),
                instance_type: 0,
                endpoint_uris: vec![],
                create_time: None,
                update_time: None,
                free_instance_metadata: None,
                edition: 0,
                default_backup_schedule_type: 0,
            }),
        };
        let mut instance_op = admin_client
            .instance()
            .create_instance(create_instance_req, None)
            .await?;
        instance_op.wait(None).await?;

        Ok(SpanEmuBoost {
            _container: container,
            emulator_host,
            project_id: self.project_id,
            instance_id: self.instance_id,
        })
    }
}
