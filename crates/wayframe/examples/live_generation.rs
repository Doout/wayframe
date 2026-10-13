//! A deployment remains alive while readers hold its snapshot.
use wayframe::Live;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let live = Live::new("policy-v1");
    let in_flight = live.snapshot();
    // Real applications compile/validate/prewarm the complete candidate first.
    let prepared = "policy-v2";
    let generation = live.publish(in_flight.id, prepared)?;
    assert_eq!(in_flight.value, "policy-v1");
    assert_eq!(live.snapshot().value, "policy-v2");
    assert!(live.publish(in_flight.id, "stale-policy").is_err());
    println!(
        "new generation={generation}; existing reader still holds {}",
        in_flight.value
    );
    Ok(())
}
