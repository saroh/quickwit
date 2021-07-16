//  Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::io;

use thiserror::Error;

/// Cluster error kinds.
#[derive(Debug, Error)]
pub enum ClusterError {
    /// Create cluster error.
    #[error("Failed to create cluster. Cause `{cause}`")]
    CreateClusterError {
        /// Root cause.
        #[source]
        cause: anyhow::Error,
    },

    /// Port binding error.
    #[error("Failed to bind UDP port :{port} for the gossip membership algorithm. Cause: {cause}")]
    UDPPortBindingError {
        /// Port number.
        port: u16,

        /// Root cause.
        #[source]
        cause: io::Error,
    },

    /// Read host key error.
    #[error("Failed to read host key. Cause: `{cause}`")]
    ReadHostKeyError {
        /// Root cause.
        #[source]
        cause: anyhow::Error,
    },

    /// Write host key error.
    #[error("Failed to write host key. Cause: `{cause}`")]
    WriteHostKeyError {
        /// Root cause.
        #[source]
        cause: anyhow::Error,
    },
}

/// Generic Result type for cluster operations.
pub type ClusterResult<T> = Result<T, ClusterError>;