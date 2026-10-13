//! No listener, HTTP context or plugin engine: demonstrate the generic kernel.
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use wayframe::prelude::*;
struct Message {
    deadline: Instant,
    text: String,
}
impl Context for Message {
    fn deadline(&self) -> Instant {
        self.deadline
    }
}
struct Normalize;
impl Block<Message> for Normalize {
    fn id(&self) -> &str {
        "normalize"
    }
    fn phase(&self) -> Phase {
        Phase::Inspect
    }
    fn execute<'a>(&'a self, message: &'a mut Message) -> Execution<'a> {
        message.text = message.text.trim().to_ascii_lowercase();
        Execution::Immediate(Decision::Continue)
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pipeline = Pipeline::compile(vec![Arc::new(Normalize) as Arc<dyn Block<Message>>])?;
    let mut message = Message {
        deadline: Instant::now() + Duration::from_secs(1),
        text: "  HELLO WAYFRAME  ".into(),
    };
    assert_eq!(pipeline.evaluate(&mut message).await, Decision::Continue);
    assert_eq!(message.text, "hello wayframe");
    println!("{}", message.text);
    Ok(())
}
