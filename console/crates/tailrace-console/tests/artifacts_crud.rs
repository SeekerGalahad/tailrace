//! Artifact lifecycle (§4.4): multipart upload (server-side sha256) → list
//! with refCount → duplicate 409 → guarded delete; storage lands under the
//! per-test artifact dir.

mod common;

use std::collections::HashMap;

use axum::http::StatusCode;
use common::*;

/// Hand-rolled multipart body (no test-only deps): file + name + version.
fn multipart_body(
    boundary: &str,
    filename: &str,
    payload: &[u8],
    name: &str,
    version: &str,
) -> Vec<u8> {
    let mut body: Vec<u8> = Vec::new();
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        format!("Content-Disposition: form-data; name=\"name\"\r\n\r\n{name}\r\n").as_bytes(),
    );
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        format!("Content-Disposition: form-data; name=\"version\"\r\n\r\n{version}\r\n").as_bytes(),
    );
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        format!(
            "Content-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\n\
                 Content-Type: application/gzip\r\n\r\n"
        )
        .as_bytes(),
    );
    body.extend_from_slice(payload);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    body
}

fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(data);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

#[tokio::test]
async fn artifact_upload_list_delete() {
    let app = app().await;
    let payload: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
    let boundary = "tailrace-test-boundary";
    let body = multipart_body(boundary, "node.tar.gz", &payload, "tailrace-node", "1.0.0");
    let mut headers = HashMap::new();
    headers.insert(
        "content-type",
        format!("multipart/form-data; boundary={boundary}"),
    );

    let (s, b, _ct) = raw_request(
        &app,
        "POST",
        "/api/artifacts",
        headers.clone(),
        body.clone(),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let id = b["id"].as_i64().unwrap();
    assert_eq!(
        b["sha256"],
        sha256_hex(&payload),
        "server-side sha256 anchor"
    );

    // duplicate name+version -> 409
    let (s, b, _ct) = raw_request(&app, "POST", "/api/artifacts", headers, body).await;
    assert_eq!(s, StatusCode::CONFLICT, "{b}");

    // wrong extension -> 400
    let bad = multipart_body(boundary, "node.zip", &payload, "tailrace-node", "1.0.1");
    let mut h2 = HashMap::new();
    h2.insert(
        "content-type",
        format!("multipart/form-data; boundary={boundary}"),
    );
    let (s, b, _ct) = raw_request(&app, "POST", "/api/artifacts", h2, bad).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");

    // --- list: refCount 0, source upload, uploadedBy admin
    let (s, b) = req(&app, "GET", "/api/artifacts", None).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["artifacts"].as_array().unwrap().len(), 1);
    let a = &b["artifacts"][0];
    assert_eq!(a["id"], id);
    assert_eq!(a["name"], "tailrace-node");
    assert_eq!(a["version"], "1.0.0");
    assert_eq!(a["sizeBytes"], payload.len() as i64);
    assert_eq!(a["sha256"], sha256_hex(&payload));
    assert_eq!(a["refCount"], 0);
    assert_eq!(a["source"], "upload");
    assert_eq!(a["uploadedBy"], "admin");

    // --- delete: no node references it -> 204
    let (s, _b) = req(&app, "DELETE", &format!("/api/artifacts/{id}"), None).await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    let (_s, b) = req(&app, "GET", "/api/artifacts", None).await;
    assert_eq!(b["artifacts"].as_array().unwrap().len(), 0);
    let (s, _) = req(&app, "DELETE", &format!("/api/artifacts/{id}"), None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}
