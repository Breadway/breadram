//! Versioned local IPC contract.
//!
//! The daemon and every client (the CLI, the desktop UI, and optional
//! integrations) speak one local socket protocol. That protocol is versioned so
//! that a mismatch fails loudly with an actionable error instead of silently
//! misbehaving.
//!
//! Compatibility rule: the major version must match exactly, and a peer may
//! speak a minor version lower than or equal to this build's. A peer that
//! speaks a newer minor version is rejected, because this build cannot know
//! about fields or messages it does not ship.

/// Major protocol version. Breaking changes bump this.
pub const PROTOCOL_MAJOR: u16 = 1;

/// Minor protocol version. Backward compatible additions bump this.
pub const PROTOCOL_MINOR: u16 = 0;

/// A rejected protocol version negotiation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ProtocolError {
    /// The peer speaks a different major version.
    #[error(
        "incompatible IPC major version: peer speaks {peer_major}, this build speaks {local_major}"
    )]
    MajorMismatch {
        /// Major version reported by the peer.
        peer_major: u16,
        /// Major version this build speaks.
        local_major: u16,
    },
    /// The peer speaks a newer minor version than this build understands.
    #[error("peer IPC minor version {peer_minor} is newer than this build's {local_minor}")]
    PeerTooNew {
        /// Minor version reported by the peer.
        peer_minor: u16,
        /// Minor version this build speaks.
        local_minor: u16,
    },
}

/// Check whether a peer speaking `peer_major`/`peer_minor` may talk to this
/// build.
///
/// Returns `Ok(())` when the connection is allowed, or a [`ProtocolError`]
/// describing which side is out of date.
pub fn check_protocol(peer_major: u16, peer_minor: u16) -> Result<(), ProtocolError> {
    if peer_major != PROTOCOL_MAJOR {
        return Err(ProtocolError::MajorMismatch {
            peer_major,
            local_major: PROTOCOL_MAJOR,
        });
    }
    if peer_minor > PROTOCOL_MINOR {
        return Err(ProtocolError::PeerTooNew {
            peer_minor,
            local_minor: PROTOCOL_MINOR,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_version_is_accepted() {
        assert_eq!(check_protocol(PROTOCOL_MAJOR, PROTOCOL_MINOR), Ok(()));
    }

    #[test]
    fn older_minor_is_accepted() {
        // A client built before a backward compatible addition must still work.
        assert_eq!(check_protocol(PROTOCOL_MAJOR, 0), Ok(()));
    }

    #[test]
    fn different_major_is_rejected() {
        let err = check_protocol(PROTOCOL_MAJOR + 1, PROTOCOL_MINOR).unwrap_err();
        assert!(matches!(err, ProtocolError::MajorMismatch { .. }));
    }

    #[test]
    fn newer_minor_is_rejected() {
        let err = check_protocol(PROTOCOL_MAJOR, PROTOCOL_MINOR + 1).unwrap_err();
        assert!(matches!(err, ProtocolError::PeerTooNew { .. }));
    }

    #[test]
    fn a_wrong_major_beats_a_wrong_minor() {
        // Major compatibility is decided first, so the reported error names the
        // more fundamental problem.
        let err = check_protocol(PROTOCOL_MAJOR + 1, PROTOCOL_MINOR + 5).unwrap_err();
        assert!(matches!(err, ProtocolError::MajorMismatch { .. }));
    }
}
