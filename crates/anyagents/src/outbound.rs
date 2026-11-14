//! `anyagents outbound`: sent messages whose delivery could not be confirmed.
//!
//! The server holds such a send instead of resending it (a resend could
//! deliver it twice), tells its sender once, and lists it here, in `agents
//! inspect` and in `status`. The operator decides: `retry` sends it again
//! through the ordinary send path, `discard` settles it without sending.

use std::path::Path;

use anyagents_core::{Error, Store};

use crate::api::{SendResponse, UnsettledOutboundResponse, UnsettledOutboundView};
use crate::{client, server, ui};

fn print_one(held: &UnsettledOutboundView) {
    println!(
        "{}\t{}\t{}\t{}\t{}\t{}",
        held.key,
        held.agent_id,
        held.destination,
        held.since_ms
            .map_or_else(|| "-".to_string(), ui::format_timestamp_ms),
        held.error.as_deref().unwrap_or("-"),
        held.excerpt
    );
}

fn print_json<T: serde::Serialize>(value: &T) -> Result<(), Error> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).map_err(|err| Error::Message(err.to_string()))?
    );
    Ok(())
}

fn operator_only() -> Result<(), Error> {
    if client::agent_identity().is_some() {
        return Err(Error::Message(
            "unconfirmed sends are the operator's to retry or discard".into(),
        ));
    }
    Ok(())
}

/// Every unconfirmed send, oldest first.
pub fn list(data: &Path, json: bool) -> Result<(), Error> {
    operator_only()?;
    let listed = match client::get::<UnsettledOutboundResponse>(data, "/v1/outbound")
        .map_err(Error::Message)?
    {
        Some(listed) => listed,
        None => {
            let store = Store::open(data.join("state.db"))?;
            UnsettledOutboundResponse {
                unsettled: server::outbound_history::unsettled(&store)
                    .iter()
                    .map(server::unsettled_view)
                    .collect(),
            }
        }
    };
    if json {
        return print_json(&listed);
    }
    for held in &listed.unsettled {
        print_one(held);
    }
    Ok(())
}

/// Send one again through the running server.
pub fn retry(data: &Path, key: &str, json: bool) -> Result<(), Error> {
    operator_only()?;
    if !client::can_call_server(data) {
        return Err(Error::Message(
            "a retry sends through the running server; start it first".into(),
        ));
    }
    let response = client::call::<SendResponse>(
        data,
        "POST",
        &format!(
            "/v1/outbound/{}/retry",
            crate::channel::urlencoding_simple(key)
        ),
        None,
        &client::authority(data),
    )
    .map_err(Error::Message)?
    .ok_or_else(|| Error::Message("the server answered nothing".into()))?;
    if json {
        return print_json(&response);
    }
    for delivery in &response.deliveries {
        println!("retried\t{key}\t{}\t{}", delivery.to, delivery.id);
    }
    for skipped in &response.skipped {
        println!("skipped\t{key}\t{}\t{}", skipped.to, skipped.reason);
    }
    Ok(())
}

/// Settle one without sending it.
pub fn discard(data: &Path, key: &str, json: bool) -> Result<(), Error> {
    operator_only()?;
    let held = if client::can_call_server(data) {
        client::call::<UnsettledOutboundView>(
            data,
            "POST",
            &format!(
                "/v1/outbound/{}/discard",
                crate::channel::urlencoding_simple(key)
            ),
            None,
            &client::authority(data),
        )
        .map_err(Error::Message)?
        .ok_or_else(|| Error::Message("the server answered nothing".into()))?
    } else {
        let store = Store::open(data.join("state.db"))?;
        server::unsettled_view(&server::outbound_history::discard(&store, key)?)
    };
    if json {
        return print_json(&held);
    }
    print!("discarded\t");
    print_one(&held);
    Ok(())
}
