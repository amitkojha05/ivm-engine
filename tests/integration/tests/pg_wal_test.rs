use ivm_connectors::ConnectorState;
use ivm_core::{Row, Value, ZSet};
use ivm_runtime::checkpoint::CheckpointManager;
use std::collections::HashMap;

#[tokio::test]
async fn test_two_phase_checkpoint_no_orphan_tmp_on_clean_run() {
    let dir = std::env::temp_dir().join("ivm_two_phase_it_test");
    let _ = std::fs::remove_dir_all(&dir);
    let mut mgr = CheckpointManager::new(&dir);

    let mut zset = ZSet::default();
    zset.insert(Row(HashMap::from([("id".into(), Value::Int(1))])), 1);

    let state = ConnectorState {
        checkpoint_epoch: 1,
        ..Default::default()
    };
    let tmp = mgr.save_tmp(&zset, 1, &state).unwrap();
    assert!(tmp.to_string_lossy().ends_with(".tmp"));
    mgr.confirm(tmp, 1, state).unwrap();

    let entries: Vec<_> = std::fs::read_dir(&dir).unwrap().collect();
    assert!(entries.iter().all(|e| {
        !e.as_ref()
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".tmp")
    }));

    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(not(target_os = "windows"))]
mod docker_tests {
    use super::*;
    use ivm_connectors::InputConnector;
    use ivm_pg_wal::WalStreamConnector;
    use ivm_snapshot::SnapshotConnector;
    use testcontainers::clients::Cli;
    use testcontainers_modules::postgres::Postgres;
    use tokio_postgres::NoTls;

    #[tokio::test]
    async fn test_snapshot_bootstrap_then_wal_no_gap() {
        let docker = Cli::default();
        let pg = docker.run(Postgres::default());
        let conn_str = format!(
            "postgres://postgres:postgres@127.0.0.1:{}/postgres",
            pg.get_host_port_ipv4(5432)
        );

        let (client, conn) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
        tokio::spawn(async move {
            conn.await.ok();
        });
        client
            .batch_execute("CREATE TABLE orders (id BIGINT PRIMARY KEY, amount BIGINT)")
            .await
            .unwrap();
        client
            .execute("INSERT INTO orders VALUES (1,100),(2,200)", &[])
            .await
            .unwrap();

        // Task 9: bootstrap should snapshot the 2 existing rows.
        let bootstrap = SnapshotConnector::new(&conn_str, "orders", "test_slot")
            .bootstrap()
            .await
            .unwrap();
        assert_eq!(
            bootstrap.snapshot.len(),
            2,
            "Snapshot must capture pre-existing rows"
        );

        client
            .execute("INSERT INTO orders VALUES (3,300)", &[])
            .await
            .unwrap();

        let wal = WalStreamConnector::new(&conn_str, "test_slot", "test_pub")
            .await
            .unwrap();
        let batch = wal.poll_batch(100).await.unwrap();
        assert_eq!(
            batch.delta.len(),
            1,
            "Only the post-bootstrap insert should appear"
        );

        // Task 2 regression: acknowledge must not be a no-op.
        wal.acknowledge_lsn(batch.epoch).await.unwrap();
    }
}
