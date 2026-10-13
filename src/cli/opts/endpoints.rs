// SPDX-License-Identifier: Apache-2.0
//! Validate endpoint syntax and expand data-parallel rank addresses.

use super::EngineBase;
use crate::cli::error::CliError;
use infertap::record::InstanceLabel;
use infertap::source::{Endpoint, SourceKey};
use std::collections::HashMap;

/// `--engine-endpoint` entries: `label=tcp://host:port` or a bare address, which takes the
/// label `engine`. One tap usually watches one engine (the sidecar shape), and the label is
/// the instance identity every record carries, so a deployment passes it explicitly.
pub(crate) fn parse_engine_endpoints(spec: &str) -> Result<Vec<EngineBase>, CliError> {
    spec.split(',')
        .map(|entry| {
            let (label, address) = entry.split_once('=').unwrap_or(("engine", entry));
            let label = label.trim();
            let address = address.trim();
            validate_label(label)?;
            split_tcp_addr(address)?;
            Ok(EngineBase {
                label: InstanceLabel::from(label),
                address: address.into(),
            })
        })
        .collect()
}

/// The expansion's product: one subscription per rank, and the map from each source key
/// to the one instance label its records carry.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Expanded {
    pub(crate) endpoints: Vec<Endpoint>,
    pub(crate) labels: HashMap<SourceKey, InstanceLabel>,
}

/// Expand each engine's TCP base port over its configured data-parallel rank range.
/// Rank N uses `base + N`, where N is global. `max` bounds the number of local ranks;
/// the default range contains the current local ranks and can include future ranks.
///
/// Each rank has a distinct source key for sequence and content tracking. All ranks of
/// one engine share its instance label on emitted records.
pub(crate) fn expand_engine_endpoints(
    engines: &[EngineBase],
    start: u32,
    local: u32,
    max: u32,
) -> Result<Expanded, CliError> {
    if local < 1 {
        return Err(CliError::Usage(
            "--dp-local: at least one local rank".to_string(),
        ));
    }
    if max < local {
        return Err(CliError::Usage(
            "--dp-max must not be below --dp-local".into(),
        ));
    }
    let mut out = Expanded::default();
    // The highest rank this pod may hold, checked before any port is derived from it: the
    // rank space is u32 and a start near its top must be refused, not wrapped.
    let Some(top_rank) = start.checked_add(max - 1) else {
        return Err(CliError::Usage(
            "--dp-start-rank plus --dp-max exceeds the rank space".into(),
        ));
    };
    for EngineBase { label, address } in engines {
        let (host, base) = split_tcp_addr(address)?;
        let top = u64::from(base) + u64::from(top_rank);
        if top > 65_535 {
            return Err(CliError::Usage(
                "--engine-endpoint: expanded rank ports are past 65535".into(),
            ));
        }
        for rank in start..=top_rank {
            // The single-rank default uses the operator's label without a rank suffix.
            let source = if max == 1 && start == 0 {
                SourceKey::from(label.as_str())
            } else {
                SourceKey::from(format!("{label}#{rank}"))
            };
            #[allow(clippy::cast_possible_truncation)] // bounded by the 65535 check above
            let port = base + rank as u16;
            out.endpoints.push(Endpoint {
                source: source.clone(),
                address: format!("tcp://{host}:{port}"),
            });
            out.labels.insert(source, label.clone());
        }
    }
    Ok(out)
}

/// Validate a TCP peer, optionally preceded by a local source address.
/// DNS resolution and platform transport availability are checked by libzmq at startup.
pub(crate) fn split_tcp_addr(addr: &str) -> Result<(&str, u16), CliError> {
    let rest = addr
        .strip_prefix("tcp://")
        .ok_or_else(|| usage("expected tcp://host:port"))?;
    let peer = if let Some((source, peer)) = rest.split_once(';') {
        tcp_pair(source, true)?;
        peer
    } else {
        rest
    };
    tcp_pair(peer, false)?;
    let (host, port) = rest
        .rsplit_once(':')
        .ok_or_else(|| usage("expected host:port"))?;
    let port = port
        .parse()
        .map_err(|_| usage("port must be an integer from 1 to 65535"))?;
    Ok((host, port))
}

fn usage(message: &str) -> CliError {
    CliError::Usage(format!("endpoint: {message}"))
}

fn validate_label(label: &str) -> Result<(), CliError> {
    if label.is_empty() || label.chars().any(char::is_control) {
        return Err(usage(
            "source label must be nonempty and contain no control characters",
        ));
    }
    Ok(())
}

fn tcp_pair(address: &str, local: bool) -> Result<(), CliError> {
    let (host, port) = address
        .rsplit_once(':')
        .ok_or_else(|| usage("expected host:port"))?;
    if host.is_empty()
        || host.chars().any(|c| {
            c.is_whitespace() || c.is_control() || matches!(c, '/' | ';' | '@' | '?' | '#')
        })
    {
        return Err(usage("invalid TCP host"));
    }
    if host.contains('*') && !(local && host == "*") {
        return Err(usage("a wildcard is a bind shape; specify a peer host"));
    }
    if host.starts_with('[') {
        let literal = host
            .strip_prefix('[')
            .and_then(|h| h.strip_suffix(']'))
            .ok_or_else(|| usage("invalid IPv6 host"))?;
        let ip = literal.split_once('%').map_or(literal, |(ip, _)| ip);
        ip.parse::<core::net::Ipv6Addr>()
            .map_err(|_| usage("invalid IPv6 host"))?;
    } else if host.contains([':', '[', ']']) {
        return Err(usage("IPv6 hosts must be bracketed"));
    }
    if local && port == "*" {
        return Ok(());
    }
    let port: u16 = port
        .parse()
        .map_err(|_| usage("port must be an integer from 1 to 65535"))?;
    if port == 0 && !local {
        return Err(usage("peer port must be nonzero"));
    }
    Ok(())
}

/// Validate transport names and address structure without opening sockets.
/// Non-TCP transport details remain subject to libzmq and platform support.
fn validate_address(address: &str) -> Result<(), CliError> {
    let (transport, peer) = address
        .split_once("://")
        .ok_or_else(|| usage("expected transport://address"))?;
    if peer.is_empty() || peer.chars().any(char::is_control) {
        return Err(usage(
            "address must be nonempty and contain no control characters",
        ));
    }
    match transport {
        "tcp" => {
            split_tcp_addr(address)?;
        }
        "ipc" | "inproc" | "pgm" | "epgm" | "vmci" | "vsock" | "udp" | "ws" | "wss" => {}
        _ => return Err(usage("unrecognized transport")),
    }
    Ok(())
}

/// Parse manual `source=address` pairs. Source names are checked for uniqueness later.
pub(crate) fn parse_endpoints(spec: &str) -> Result<Vec<Endpoint>, CliError> {
    spec.split(',')
        .map(|pair| {
            let (label, address) = pair
                .split_once('=')
                .ok_or_else(|| usage("expected source=address"))?;
            let label = label.trim();
            let address = address.trim();
            validate_label(label)?;
            validate_address(address)?;
            Ok(Endpoint {
                source: SourceKey::from(label),
                address: address.into(),
            })
        })
        .collect()
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn invalid_endpoints_are_rejected_without_echoing_values() {
        for spec in [
            "=not-an-endpoint",
            "source=",
            "source=private-value",
            "source=tcp://:5557",
            "source=tcp://*:5557",
            "source=tcp://host:0",
            "source=tcp://host:65536",
            "source=tcp://host:private-value",
            "source=unknown://private-value",
            "source=tcp://[invalid]:5557",
        ] {
            let error = parse_endpoints(spec).expect_err("invalid").to_string();
            assert!(!error.contains("private-value"));
        }
    }

    #[test]
    fn manual_endpoints_preserve_tcp_and_non_tcp_transports() {
        for spec in [
            "s=tcp://localhost:5557",
            "s=tcp://[::1]:5557",
            "s=tcp://eth0:0;host:5557",
            "s=ipc:///tmp/events",
            "s=inproc://events",
            "s=epgm://eth0;239.0.0.1:5557",
        ] {
            assert!(parse_endpoints(spec).is_ok(), "{spec}");
        }
        assert!(parse_engine_endpoints("s=ipc:///tmp/events").is_err());
    }
}
