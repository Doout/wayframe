use axum::{routing::get, Json, Router};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rsa::{
    pkcs8::{EncodePrivateKey, LineEnding},
    traits::PublicKeyParts,
    RsaPrivateKey,
};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use wayframe_auth::{Config, JwksAuth, ProtectedResource};
use wayframe_core::Decision;
use wayframe_http::RequestContext;
fn key(id: &str) -> (EncodingKey, Value) {
    let key = RsaPrivateKey::new(&mut rand08::rngs::OsRng, 2048).unwrap();
    let jwk = json!({"kty":"RSA","kid":id,"alg":"RS256","use":"sig","key_ops":["verify"],"n":URL_SAFE_NO_PAD.encode(key.n().to_bytes_be()),"e":URL_SAFE_NO_PAD.encode(key.e().to_bytes_be())});
    (
        EncodingKey::from_rsa_pem(key.to_pkcs8_pem(LineEnding::LF).unwrap().as_bytes()).unwrap(),
        jwk,
    )
}
fn context(token: &str) -> RequestContext {
    let mut ctx = RequestContext::new(
        axum::http::Method::POST,
        "/mcp".into(),
        "127.0.0.1".parse().unwrap(),
        tokio::time::Instant::now() + Duration::from_secs(5),
    );
    ctx.headers
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    ctx
}
fn signed(key: &EncodingKey, kid: &str, claims: &Value) -> String {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(kid.into());
    jsonwebtoken::encode(&header, claims, key).unwrap()
}
#[tokio::test]
async fn verifies_claims_signature_and_rotated_keys_without_trusting_scopes() {
    let (key_a, jwk_a) = key("a");
    let (key_b, jwk_b) = key("b");
    let keys = Arc::new(Mutex::new(json!({"keys":[jwk_a]})));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let data = keys.clone();
    let router = Router::new().route(
        "/keys",
        get(move || {
            let data = data.clone();
            async move { Json(data.lock().unwrap().clone()) }
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    struct Stop(tokio::task::JoinHandle<()>);
    impl Drop for Stop {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let _server = Stop(server);
    let config = Config {
        issuer: origin.clone(),
        audience: format!("{origin}/mcp"),
        jwks_uri: format!("{origin}/keys"),
        tenant_claim: "tenant".into(),
        permissions_claim: "scope".into(),
        permission_map: HashMap::from([("reader".into(), "tools:read".into())]),
        refresh_seconds: 1,
        ca: None,
    };
    let auth = JwksAuth::prepare("auth", config).await.unwrap();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let claims = json!({"iss":origin,"aud":format!("{origin}/mcp"),"sub":"alice","tenant":"one","scope":"reader tools:admin","exp":now+300});
    let token = signed(&key_a, "a", &claims);
    let mut ctx = context(&token);
    assert_eq!(auth.authenticate(&mut ctx).await, Decision::Continue);
    assert_eq!(
        ctx.identity.unwrap().permissions,
        ["tools:read".into()].into()
    );
    for (field, value) in [
        ("aud", json!("wrong")),
        ("iss", json!("https://wrong")),
        ("exp", json!(0)),
        ("nbf", json!(now + 300)),
        ("tenant", json!(null)),
        ("scope", json!([17])),
    ] {
        let mut claims = claims.clone();
        claims[field] = value;
        let mut ctx = context(&signed(&key_a, "a", &claims));
        assert_ne!(
            auth.authenticate(&mut ctx).await,
            Decision::Continue,
            "{field}"
        );
        assert!(ctx.identity.is_none());
    }
    assert_ne!(
        auth.authenticate(&mut context(&signed(&key_b, "a", &claims)))
            .await,
        Decision::Continue
    );
    let mut duplicate = context(&token);
    duplicate
        .headers
        .append("authorization", format!("Bearer {token}").parse().unwrap());
    assert_ne!(auth.authenticate(&mut duplicate).await, Decision::Continue);
    *keys.lock().unwrap() = json!({"keys":[jwk_b]});
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        auth.authenticate(&mut context(&signed(&key_b, "b", &claims)))
            .await,
        Decision::Continue
    );
    assert_ne!(
        auth.authenticate(&mut context(&token)).await,
        Decision::Continue
    );
}
#[test]
fn metadata_uses_rfc_well_known_path() {
    let metadata = ProtectedResource::new(
        "https://gateway.example/mcp".into(),
        vec!["https://issuer.example".into()],
        vec![],
    )
    .unwrap();
    assert_eq!(metadata.challenge(None).unwrap(),"Bearer resource_metadata=\"https://gateway.example/.well-known/oauth-protected-resource/mcp\"");
}
