//! `brenn-bridge` — the pod's attachment to the brenn message bus.
//!
//! A pod is a native daemon on someone's kitchen counter; brenn is a server
//! somewhere else on the LAN. This crate is the link between them: it dials the
//! server's remote attach route over `wss://`, authenticates with a bearer
//! token, and holds the attachment up across deploys, reboots and netsplits so
//! that a pod application sees a channel it subscribes and publishes on rather
//! than a socket it has to nurse.
//!
//! Everything about the wire — frames, versions, cursors, retention, resume —
//! belongs to the upstream attachment stack (`brenn-attach-client`,
//! `brenn-attach-proto`, `brenn-envelope`), which this crate embeds rather than
//! reimplements. What is here is the part an embedder cannot borrow: a
//! configuration file, a credential with a posture, a supervision loop with an
//! opinion about when to stop trying, and an API shaped for a pod application
//! instead of for a test.
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use std::path::Path;
//! use brenn_bridge::{Bridge, BridgeEvent, Config, ResumePolicy, SubscriptionDepths};
//!
//! let config = Config::load(Path::new("/etc/brenn/bridge.toml"))?;
//! let (bridge, handle, mut events) = Bridge::new(&config)?;
//! let task = tokio::spawn(bridge.run());
//!
//! handle
//!     .subscribe(
//!         "brenn:chat.app.home.roster",
//!         SubscriptionDepths { push_depth: 1, retain_depth: 1 },
//!         ResumePolicy::Cursorless,
//!     )
//!     .await?;
//!
//! while let Some(event) = events.recv().await {
//!     if let BridgeEvent::Delivered(delivery) = event {
//!         println!("{}: {}", delivery.channel, delivery.envelope.body);
//!     }
//! }
//!
//! let outcome = task.await?;
//! std::process::exit(i32::from(outcome.exit_code()));
//! # }
//! ```
//!
//! Nothing here logs. A library that logs decides an application's
//! observability for it, and this one is embedded in a daemon that already has
//! a sink of its own: every fact worth a line leaves as a
//! [`BridgeEvent`](bridge::BridgeEvent) or as the
//! [`BridgeOutcome`](bridge::BridgeOutcome) the run loop ends with, and the
//! embedder renders it in whatever shape its own event stream takes.

pub mod bridge;
pub mod config;
pub mod exit;

pub use bridge::{
    Bridge, BridgeEvent, BridgeGone, BridgeHandle, BridgeOutcome, Delivery, PublishError,
};
pub use config::{Config, ConfigError, ReconnectConfig, Token};

// The upstream vocabulary this crate's own API is stated in, re-exported so an
// embedder names it through the crate it is calling rather than reaching past
// it into a pinned git dependency.
pub use brenn_attach_client::conn::{AttachmentFacts, ConnConfig, DetachReason};
pub use brenn_attach_client::publish::PublishRequest;
pub use brenn_attach_client::subs::{ResumePolicy, SubscriptionDepths};
// What building a [`Bridge`] over something other than the native connector
// takes: the parameters it is configured with, and the traits a transport
// implements.
pub use brenn_attach_client::transport::{
    TransportConnection, TransportConnector, TransportError, TransportEvent,
};
pub use brenn_attach_proto::{
    AlertSeverity, GapInfo, GapReason, PublishOutcome, Urgency, VersionRange,
};
pub use brenn_envelope::MessageEnvelope;
