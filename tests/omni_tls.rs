use super::tls_support::Certificates;
use spanner_grpc_mock::{MockSpanner, google::spanner::v1 as proto};
use tonic::Response;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

fn query_mock() -> MockSpanner {
    let mut mock = MockSpanner::new();
    mock.expect_create_session().returning(|_| {
        Ok(Response::new(proto::Session {
            name: "projects/test/instances/test/databases/test/sessions/test".into(),
            multiplexed: true,
            ..Default::default()
        }))
    });
    mock.expect_delete_session()
        .returning(|_| Ok(Response::new(())));
    mock.expect_execute_streaming_sql().returning(|_| {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tx.try_send(Ok(proto::PartialResultSet {
            metadata: Some(proto::ResultSetMetadata {
                row_type: Some(proto::StructType {
                    fields: vec![proto::struct_type::Field {
                        name: "Value".into(),
                        r#type: Some(proto::Type {
                            code: proto::TypeCode::Int64 as i32,
                            ..Default::default()
                        }),
                    }],
                }),
                ..Default::default()
            }),
            values: vec![prost_types::Value {
                kind: Some(prost_types::value::Kind::StringValue("42".into())),
            }],
            ..Default::default()
        }))
        .unwrap();
        Ok(Response::new(rx))
    });
    mock
}

fn configure(conn: &duckdb::Connection, certs: &Certificates, endpoint: &str, mtls: bool) {
    conn.execute_batch(&format!(
        "SET spanner_database_path='projects/test/instances/test/databases/test'; \
         SET spanner_endpoint_mode='omni'; SET spanner_endpoint='{endpoint}'; \
         SET spanner_tls_root_ca_file='{}'; SET spanner_tls_server_name='spanner.test'",
        certs
            .setting("spanner_tls_root_ca_file")
            .unwrap()
            .replace('\'', "''")
    ))
    .unwrap();
    if mtls {
        for name in [
            "spanner_tls_client_cert_file",
            "spanner_tls_client_key_file",
        ] {
            conn.execute_batch(&format!(
                "SET {name}='{}'",
                certs.setting(name).unwrap().replace('\'', "''")
            ))
            .unwrap();
        }
    }
}

#[test]
fn test_omni_tls_and_mtls_query_via_public_extension() {
    let rt = super::test_runtime();
    for mtls in [false, true] {
        let certs = Certificates::new();
        let listener = rt
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let endpoint = format!(
            "https://127.0.0.1:{}",
            listener.local_addr().unwrap().port()
        );
        let mut tls = ServerTlsConfig::new()
            .identity(Identity::from_pem(&certs.server_cert, &certs.server_key));
        if mtls {
            tls = tls.client_ca_root(Certificate::from_pem(&certs.ca));
        }
        let server = rt.spawn(async move {
            Server::builder()
                .tls_config(tls)
                .unwrap()
                .add_service(proto::spanner_server::SpannerServer::new(query_mock()))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let conn = super::open_extension_connection();
        configure(&conn, &certs, &endpoint, mtls);
        let mut statement = conn
            .prepare(
                "SELECT Value FROM spanner_query('SELECT 42 AS Value', parallelism_mode := 'off')",
            )
            .unwrap();
        let value: i64 = statement.query_row([], |row| row.get(0)).unwrap();
        assert_eq!(value, 42);
        if mtls {
            let other = Certificates::new();
            std::fs::write(certs.directory.join("client.key"), &other.client_key).unwrap();
            let old_snapshot: i64 = statement.query_row([], |row| row.get(0)).unwrap();
            assert_eq!(old_snapshot, 42);
            for sql in [
                "SELECT * FROM spanner_query('SELECT 42 AS Value')",
                "SELECT * FROM spanner_scan('T', dialect := 'googlesql')",
                "SELECT * FROM spanner_tables()",
                "SELECT * FROM \"spanner:T\"",
                "COPY (SELECT 1::BIGINT AS Id) TO 'T' (FORMAT spanner, dialect 'googlesql')",
            ] {
                let error = conn.prepare(sql).unwrap_err().to_string();
                assert!(
                    error.contains("do not match"),
                    "unexpected error for {sql}: {error}"
                );
            }
        }
        server.abort();
    }
}

#[test]
fn test_omni_tls_rejects_wrong_trust_name_and_missing_client_identity() {
    let rt = super::test_runtime();
    let certs = Certificates::new();
    let other = Certificates::new();
    let listener = rt
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let endpoint = format!(
        "https://127.0.0.1:{}",
        listener.local_addr().unwrap().port()
    );
    let tls = ServerTlsConfig::new()
        .identity(Identity::from_pem(&certs.server_cert, &certs.server_key))
        .client_ca_root(Certificate::from_pem(&certs.ca));
    let server = rt.spawn(async move {
        Server::builder()
            .tls_config(tls)
            .unwrap()
            .add_service(proto::spanner_server::SpannerServer::new(query_mock()))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let sql = "SELECT Value FROM spanner_query('SELECT 42 AS Value', parallelism_mode := 'off')";
    let control = super::open_extension_connection();
    configure(&control, &certs, &endpoint, true);
    let value: i64 = control.query_row(sql, [], |row| row.get(0)).unwrap();
    assert_eq!(value, 42);
    for case in ["trust", "name", "identity"] {
        let conn = super::open_extension_connection();
        configure(&conn, &certs, &endpoint, case != "identity");
        match case {
            "trust" => conn
                .execute_batch(&format!(
                    "SET spanner_tls_root_ca_file='{}'",
                    other
                        .setting("spanner_tls_root_ca_file")
                        .unwrap()
                        .replace('\'', "''")
                ))
                .unwrap(),
            "name" => conn
                .execute_batch("SET spanner_tls_server_name='wrong.test'")
                .unwrap(),
            "identity" => {}
            _ => unreachable!(),
        }
        // This is SQL/transport rejection, not a Rust result-decoding mismatch.
        assert!(
            conn.prepare(sql).is_err(),
            "{case} unexpectedly reached SQL"
        );
    }
    server.abort();
}

#[test]
fn test_omni_tls_admin_rejection_is_a_sql_error_before_connecting() {
    let conn = super::open_extension_connection();
    let certs = Certificates::new();
    configure(&conn, &certs, "https://127.0.0.1:1", false);
    for sql in [
        "SELECT * FROM spanner_ddl('CREATE TABLE T (Id INT64) PRIMARY KEY (Id)')",
        "SELECT * FROM spanner_ddl_async('CREATE TABLE T (Id INT64) PRIMARY KEY (Id)')",
        "SELECT * FROM spanner_operations()",
        "SELECT * FROM spanner_operations(admin_endpoint := 'http://127.0.0.1:1')",
    ] {
        let error = conn.prepare(sql).unwrap_err().to_string();
        assert!(
            error.contains("DatabaseAdmin REST transport"),
            "unexpected error: {error}"
        );
    }
}
