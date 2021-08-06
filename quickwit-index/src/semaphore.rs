use std::sync::Arc;

// Quickwit
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

/// This is a trick to get semaphore for which the SemaphoreGuard has a 'static lifetime.
pub(crate) struct Semaphore {
    inner: Arc<tokio::sync::Semaphore>,
}

impl Semaphore {
    pub fn new(num_permits: usize) -> Semaphore {
        let inner_semaphore = tokio::sync::Semaphore::new(num_permits);
        Semaphore {
            inner: Arc::new(inner_semaphore),
        }
    }

    pub async fn acquire(&self) -> SemaphoreGuard {
        self.inner
            .acquire()
            .await
            .expect("This should never fail. The semaphore does not allow close().")
            .forget();
        SemaphoreGuard {
            permits: self.inner.clone(),
        }
    }
}

pub struct SemaphoreGuard {
    permits: Arc<tokio::sync::Semaphore>,
}

impl Drop for SemaphoreGuard {
    fn drop(&mut self) {
        self.permits.add_permits(1);
    }
}
