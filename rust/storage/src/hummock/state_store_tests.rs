use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use hyper::body::HttpBody;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Client, Request, Response, Server};
use prometheus::{Encoder, Registry, TextEncoder};

use super::iterator::UserIterator;
use super::{HummockOptions, HummockStorage};
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::mock::{MockHummockMetaClient, MockHummockMetaService};
use crate::hummock::version_manager::VersionManager;
use crate::object::InMemObjectStore;

async fn prometheus_service(
    _req: Request<Body>,
    registry: &Registry,
) -> Result<Response<Body>, hyper::Error> {
    let encoder = TextEncoder::new();
    let mut buffer = vec![];
    let mf = registry.gather();
    encoder.encode(&mf, &mut buffer).unwrap();
    let response = Response::builder()
        .header(hyper::header::CONTENT_TYPE, encoder.format_type())
        .body(Body::from(buffer))
        .unwrap();

    Ok(response)
}

#[tokio::test]
async fn test_prometheus_endpoint_hummock() {
    let hummock_options = HummockOptions::default_for_test();
    let host_addr = "127.0.0.1:1222";
    let object_client = Arc::new(InMemObjectStore::new());
    let local_version_manager = Arc::new(LocalVersionManager::new(
        object_client.clone(),
        &hummock_options.remote_dir,
        None,
    ));
    let hummock_storage = HummockStorage::new(
        object_client,
        hummock_options,
        Arc::new(VersionManager::new()),
        local_version_manager,
        Arc::new(MockHummockMetaClient::new(Arc::new(
            MockHummockMetaService::new(),
        ))),
    )
    .await
    .unwrap();
    let anchor = Bytes::from("aa");
    let mut batch1 = vec![
        (anchor.clone(), Some(Bytes::from("111"))),
        (Bytes::from("bb"), Some(Bytes::from("222"))),
    ];
    let epoch: u64 = 0;
    batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
    hummock_storage
        .write_batch(
            batch1
                .into_iter()
                .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec()).into())),
            epoch,
        )
        .await
        .unwrap();

    assert_eq!(
        hummock_storage.get(&anchor).await.unwrap().unwrap(),
        Bytes::from("111")
    );
    let notifier = Arc::new(tokio::sync::Notify::new());
    let notifiee = notifier.clone();

    let make_svc = make_service_fn(move |_| {
        let registry = prometheus::default_registry();
        async move {
            Ok::<_, Infallible>(service_fn(move |req: Request<Body>| async move {
                prometheus_service(req, registry).await
            }))
        }
    });

    let server = Server::bind(&host_addr.parse().unwrap()).serve(make_svc);

    tokio::spawn(async move {
        notifier.notify_one();
        if let Err(err) = server.await {
            eprintln!("server error: {}", err);
        }
    });

    notifiee.notified().await;
    let client = Client::new();
    let uri = "http://127.0.0.1:1222/metrics".parse().unwrap();
    let mut response = client.get(uri).await.unwrap();

    let mut web_page: Vec<u8> = Vec::new();
    while let Some(next) = response.data().await {
        let chunk = next.unwrap();
        web_page.append(&mut chunk.to_vec());
    }

    let s = String::from_utf8_lossy(&web_page);
    println!("\n---{}---\n", s);
    assert!(s.contains("state_store_put_bytes"));
    assert!(!s.contains("state_store_pu_bytes"));

    assert!(s.contains("state_store_get_bytes"));
}

#[tokio::test]
/// Fix this when we finished epoch management.
#[ignore]
async fn test_basic() {
    let object_client = Arc::new(InMemObjectStore::new());
    let hummock_options = HummockOptions::default_for_test();
    let local_version_manager = Arc::new(LocalVersionManager::new(
        object_client.clone(),
        &hummock_options.remote_dir,
        None,
    ));
    let hummock_storage = HummockStorage::new(
        object_client,
        hummock_options,
        Arc::new(VersionManager::new()),
        local_version_manager,
        Arc::new(MockHummockMetaClient::new(Arc::new(
            MockHummockMetaService::new(),
        ))),
    )
    .await
    .unwrap();
    let anchor = Bytes::from("aa");

    // First batch inserts the anchor and others.
    let mut batch1 = vec![
        (anchor.clone(), Some(Bytes::from("111"))),
        (Bytes::from("bb"), Some(Bytes::from("222"))),
    ];

    // Make sure the batch is sorted.
    batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // Second batch modifies the anchor.
    let mut batch2 = vec![
        (Bytes::from("cc"), Some(Bytes::from("333"))),
        (anchor.clone(), Some(Bytes::from("111111"))),
    ];

    // Make sure the batch is sorted.
    batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // Third batch deletes the anchor
    let mut batch3 = vec![
        (Bytes::from("dd"), Some(Bytes::from("444"))),
        (Bytes::from("ee"), Some(Bytes::from("555"))),
        (anchor.clone(), None),
    ];

    // Make sure the batch is sorted.
    batch3.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // epoch 0 is reserved by storage service
    let mut epoch: u64 = 1;

    // Write first batch.
    hummock_storage
        .write_batch(
            batch1
                .into_iter()
                .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec()).into())),
            epoch,
        )
        .await
        .unwrap();

    let snapshot1 = hummock_storage.get_snapshot().await.unwrap();

    // Get the value after flushing to remote.
    let value = snapshot1.get(&anchor).await.unwrap().unwrap();
    assert_eq!(Bytes::from(value), Bytes::from("111"));

    // Test looking for a nonexistent key. `next()` would return the next key.
    let value = snapshot1.get(&Bytes::from("ab")).await.unwrap();
    assert_eq!(value, None);

    // Write second batch.
    epoch += 1;
    hummock_storage
        .write_batch(
            batch2
                .into_iter()
                .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec()).into())),
            epoch,
        )
        .await
        .unwrap();

    let snapshot2 = hummock_storage.get_snapshot().await.unwrap();

    // Get the value after flushing to remote.
    let value = snapshot2.get(&anchor).await.unwrap().unwrap();
    assert_eq!(Bytes::from(value), Bytes::from("111111"));

    // Write third batch.
    epoch += 1;
    hummock_storage
        .write_batch(
            batch3
                .into_iter()
                .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec()).into())),
            epoch,
        )
        .await
        .unwrap();

    let snapshot3 = hummock_storage.get_snapshot().await.unwrap();

    // Get the value after flushing to remote.
    let value = snapshot3.get(&anchor).await.unwrap();
    assert_eq!(value, None);

    // Get non-existent maximum key.
    let value = snapshot3.get(&Bytes::from("ff")).await.unwrap();
    assert_eq!(value, None);

    // write aa bb
    let mut iter = snapshot1.range_scan(..=b"ee".to_vec()).await.unwrap();
    iter.rewind().await.unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 2);

    // Get the anchor value at the first snapshot
    let value = snapshot1.get(&anchor).await.unwrap().unwrap();
    assert_eq!(Bytes::from(value), Bytes::from("111"));

    // drop snapshot 1
    drop(snapshot1);

    // Get the anchor value at the second snapshot
    let value = snapshot2.get(&anchor).await.unwrap().unwrap();
    assert_eq!(Bytes::from(value), Bytes::from("111111"));
    // update aa, write cc
    let mut iter = snapshot2.range_scan(..=b"ee".to_vec()).await.unwrap();
    iter.rewind().await.unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 3);

    // drop snapshot 2
    drop(snapshot2);

    // delete aa, write dd,ee
    let mut iter = snapshot3.range_scan(..=b"ee".to_vec()).await.unwrap();
    iter.rewind().await.unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 4);
}

async fn count_iter(iter: &mut UserIterator) -> usize {
    let mut c: usize = 0;
    while iter.is_valid() {
        c += 1;
        iter.next().await.unwrap();
    }
    c
}

#[tokio::test]
/// Fix this when we finished epoch management.
#[ignore]
async fn test_reload_storage() {
    let mem_objstore = Arc::new(InMemObjectStore::new());
    let hummock_options = HummockOptions::default_for_test();
    let version_manager = Arc::new(VersionManager::new());
    let local_version_manager = Arc::new(LocalVersionManager::new(
        mem_objstore.clone(),
        &hummock_options.remote_dir,
        None,
    ));
    let hummock_meta_client = Arc::new(MockHummockMetaClient::new(Arc::new(
        MockHummockMetaService::new(),
    )));

    let hummock_storage = HummockStorage::new(
        mem_objstore.clone(),
        hummock_options,
        version_manager.clone(),
        local_version_manager.clone(),
        hummock_meta_client.clone(),
    )
    .await
    .unwrap();
    let anchor = Bytes::from("aa");

    // First batch inserts the anchor and others.
    let mut batch1 = vec![
        (anchor.clone(), Some(Bytes::from("111"))),
        (Bytes::from("bb"), Some(Bytes::from("222"))),
    ];

    // Make sure the batch is sorted.
    batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // Second batch modifies the anchor.
    let mut batch2 = vec![
        (Bytes::from("cc"), Some(Bytes::from("333"))),
        (anchor.clone(), Some(Bytes::from("111111"))),
    ];

    // Make sure the batch is sorted.
    batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // epoch 0 is reserved by storage service
    let mut epoch: u64 = 1;

    // Write first batch.
    hummock_storage
        .write_batch(
            batch1
                .into_iter()
                .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec()).into())),
            epoch,
        )
        .await
        .unwrap();

    let snapshot1 = hummock_storage.get_snapshot().await.unwrap();

    // Mock somthing happened to storage internal, and storage is reloaded.
    drop(hummock_storage);
    let hummock_storage = HummockStorage::new(
        mem_objstore,
        HummockOptions::default_for_test(),
        version_manager,
        local_version_manager,
        hummock_meta_client,
    )
    .await
    .unwrap();

    // Get the value after flushing to remote.
    let value = snapshot1.get(&anchor).await.unwrap().unwrap();
    assert_eq!(Bytes::from(value), Bytes::from("111"));

    // Test looking for a nonexistent key. `next()` would return the next key.
    let value = snapshot1.get(&Bytes::from("ab")).await.unwrap();
    assert_eq!(value, None);

    // Write second batch.
    epoch += 1;
    hummock_storage
        .write_batch(
            batch2
                .into_iter()
                .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec()).into())),
            epoch,
        )
        .await
        .unwrap();

    let snapshot2 = hummock_storage.get_snapshot().await.unwrap();

    // Get the value after flushing to remote.
    let value = snapshot2.get(&anchor).await.unwrap().unwrap();
    assert_eq!(Bytes::from(value), Bytes::from("111111"));

    // write aa bb
    let mut iter = snapshot1.range_scan(..=b"ee".to_vec()).await.unwrap();
    iter.rewind().await.unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 2);

    // Get the anchor value at the first snapshot
    let value = snapshot1.get(&anchor).await.unwrap().unwrap();
    assert_eq!(Bytes::from(value), Bytes::from("111"));

    // drop snapshot 1
    drop(snapshot1);

    // Get the anchor value at the second snapshot
    let value = snapshot2.get(&anchor).await.unwrap().unwrap();
    assert_eq!(Bytes::from(value), Bytes::from("111111"));
    // update aa, write cc
    let mut iter = snapshot2.range_scan(..=b"ee".to_vec()).await.unwrap();
    iter.rewind().await.unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 3);

    // drop snapshot 2
    drop(snapshot2);
}
