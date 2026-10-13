use std::sync::{Arc, Mutex};
use wayframe_http::RequestContext;
use wayframe_plugins::*;
struct F {
    events: Arc<Mutex<Vec<String>>>,
    fail: bool,
}
struct P {
    id: String,
    events: Arc<Mutex<Vec<String>>>,
    fail: bool,
}
impl Plugin<RequestContext> for P {
    fn id(&self) -> &str {
        &self.id
    }
    fn start(&self) -> AsyncResult<'_> {
        Box::pin(async {
            self.events
                .lock()
                .unwrap()
                .push(format!("start:{}", self.id));
            anyhow::ensure!(!self.fail, "failed start");
            Ok(())
        })
    }
    fn shutdown(&self) -> AsyncResult<'_> {
        Box::pin(async {
            self.events
                .lock()
                .unwrap()
                .push(format!("stop:{}", self.id));
            Ok(())
        })
    }
    fn drain(&self) -> AsyncResult<'_> {
        Box::pin(async {
            self.events
                .lock()
                .unwrap()
                .push(format!("drain:{}", self.id));
            Ok(())
        })
    }
}
impl Factory<RequestContext> for F {
    fn descriptor(&self) -> Descriptor {
        Descriptor {
            kind: "service".into(),
            version: "1".into(),
            phase: None,
            requires: Default::default(),
            provides: ["service.ready".into()].into(),
            settings_schema: serde_json::json!({"type":"object","additionalProperties":false}),
        }
    }
    fn prepare<'a>(&'a self, c: &'a Config) -> AsyncResult<'a, Arc<dyn Plugin<RequestContext>>> {
        Box::pin(async {
            Ok(Arc::new(P {
                id: c.id.clone(),
                events: self.events.clone(),
                fail: self.fail && c.id == "b",
            }) as Arc<dyn Plugin<RequestContext>>)
        })
    }
}
fn config(id: &str, after: Vec<String>) -> Config {
    Config {
        id: id.into(),
        kind: "service".into(),
        version: "1".into(),
        settings: serde_json::json!({}),
        after,
    }
}
#[tokio::test]
async fn successful_bundle_drains_dependents_before_dependencies() {
    let events = Arc::new(Mutex::new(vec![]));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(F {
            events: events.clone(),
            fail: false,
        }))
        .unwrap();
    let bundle = registry
        .prepare(
            &[config("b", vec!["a".into()]), config("a", vec![])],
            Default::default(),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap();
    bundle.shutdown().await.unwrap();
    assert_eq!(
        *events.lock().unwrap(),
        ["start:a", "start:b", "drain:b", "stop:b", "drain:a", "stop:a"]
    );
}
#[tokio::test]
async fn dependencies_and_failed_start_roll_back_in_reverse_order() {
    let events = Arc::new(Mutex::new(vec![]));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(F {
            events: events.clone(),
            fail: true,
        }))
        .unwrap();
    let configs = [config("b", vec!["a".into()]), config("a", vec![])];
    assert!(registry
        .prepare(
            &configs,
            Default::default(),
            std::time::Duration::from_secs(1)
        )
        .await
        .is_err());
    assert_eq!(
        *events.lock().unwrap(),
        ["start:a", "start:b", "stop:b", "stop:a"]
    );
    events.lock().unwrap().clear();
    let mut invalid = config("a", vec![]);
    invalid.settings = serde_json::json!({"unexpected":true});
    assert!(registry
        .prepare(
            &[invalid],
            Default::default(),
            std::time::Duration::from_secs(1)
        )
        .await
        .is_err());
    assert!(events.lock().unwrap().is_empty());
    let cycle = [config("a", vec!["b".into()]), config("b", vec!["a".into()])];
    assert!(registry
        .prepare(
            &cycle,
            Default::default(),
            std::time::Duration::from_secs(1)
        )
        .await
        .is_err());
    assert!(events.lock().unwrap().is_empty());
}
