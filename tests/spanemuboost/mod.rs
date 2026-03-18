//! Rust port of apstndb/spanemuboost.
//!
//! Provides a builder that starts a Spanner emulator via testcontainers,
//! creates an instance/database through the gRPC Admin API,
//! and optionally seeds data via DML in a read-write transaction.

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
const DEFAULT_DATABASE_ID: &str = "emulator-database";
const GRPC_PORT: u16 = 9010;

#[allow(dead_code)]
pub struct SpanEmuBoost {
    _container: ContainerAsync<GenericImage>,
    emulator_host: String,
    project_id: String,
    instance_id: String,
    database_id: String,
}

#[allow(dead_code)]
impl SpanEmuBoost {
    pub fn builder() -> SpanEmuBoostBuilder {
        SpanEmuBoostBuilder::default()
    }

    pub fn emulator_host(&self) -> &str {
        &self.emulator_host
    }

    pub fn database_path(&self) -> String {
        format!(
            "projects/{}/instances/{}/databases/{}",
            self.project_id, self.instance_id, self.database_id
        )
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
}

pub struct SpanEmuBoostBuilder {
    image: String,
    tag: String,
    project_id: String,
    instance_id: String,
    database_id: String,
    database_dialect: DatabaseDialect,
    setup_ddls: Vec<String>,
    setup_dmls: Vec<String>,
}

impl Default for SpanEmuBoostBuilder {
    fn default() -> Self {
        Self {
            image: DEFAULT_IMAGE.to_string(),
            tag: DEFAULT_TAG.to_string(),
            project_id: DEFAULT_PROJECT_ID.to_string(),
            instance_id: DEFAULT_INSTANCE_ID.to_string(),
            database_id: DEFAULT_DATABASE_ID.to_string(),
            database_dialect: DatabaseDialect::GoogleStandardSql,
            setup_ddls: Vec::new(),
            setup_dmls: Vec::new(),
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

    pub fn database_id(mut self, id: &str) -> Self {
        self.database_id = id.to_string();
        self
    }

    pub fn setup_ddls(mut self, ddls: Vec<String>) -> Self {
        self.setup_ddls = ddls;
        self
    }

    pub fn database_dialect(mut self, dialect: DatabaseDialect) -> Self {
        self.database_dialect = dialect;
        self
    }

    pub fn setup_dmls(mut self, dmls: Vec<String>) -> Self {
        self.setup_dmls = dmls;
        self
    }

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
        };
        let admin_client = AdminClient::new(admin_config).await?;

        // 3. Create instance
        let create_instance_req = CreateInstanceRequest {
            parent: project_path,
            instance_id: self.instance_id.clone(),
            instance: Some(Instance {
                name: instance_path.clone(),
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

        // 4. Create database with DDLs
        // GoogleSQL uses backtick-quoted identifiers; PostgreSQL uses double-quoted.
        let create_statement = match self.database_dialect {
            DatabaseDialect::Postgresql => format!("CREATE DATABASE \"{}\"", self.database_id),
            _ => format!("CREATE DATABASE `{}`", self.database_id),
        };
        let create_db_req = CreateDatabaseRequest {
            parent: instance_path,
            create_statement,
            extra_statements: self.setup_ddls,
            encryption_config: None,
            database_dialect: self.database_dialect.into(),
            proto_descriptors: vec![],
        };
        let mut db_op = admin_client
            .database()
            .create_database(create_db_req, None)
            .await?;
        db_op.wait(None).await?;

        // 5. Execute DMLs if any
        if !self.setup_dmls.is_empty() {
            let database_path = format!(
                "projects/{}/instances/{}/databases/{}",
                self.project_id, self.instance_id, self.database_id
            );
            let mut session_config = SessionConfig::default();
            session_config.min_opened = 1;
            session_config.max_opened = 1;
            let data_client = Client::new(
                &database_path,
                ClientConfig {
                    session_config,
                    environment: Environment::Emulator(emulator_host.clone()),
                    ..Default::default()
                },
            )
            .await?;

            let stmts: Vec<Statement> = self
                .setup_dmls
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

        Ok(SpanEmuBoost {
            _container: container,
            emulator_host,
            project_id: self.project_id,
            instance_id: self.instance_id,
            database_id: self.database_id,
        })
    }
}
