use std::io;
use std::path::Path;

use quickwit_actors::Actor;
use quickwit_actors::AsyncActor;
use quickwit_actors::Context;
use quickwit_actors::MessageProcessError;
use tokio::fs::File;
use async_trait::async_trait;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use anyhow::Context as AnyhowContext;

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

pub struct FileSource {
    file_position: FilePosition,
    file: BufReader<File>
}

impl FileSource {
    pub async fn new(path: &Path) -> io::Result<FileSource> {
        let file= File::open(path).await?;
        Ok(FileSource {
            file_position: FilePosition::default(),
            file: BufReader::new(file),
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct FilePosition {
    num_bytes: u64,
    line_num: u64,
}

impl Actor for FileSource {
    type Message = ();

    type ObservableState = FilePosition;

    fn observable_state(&self) -> Self::ObservableState {
        self.file_position
    }

    fn default_message(&self) -> Option<Self::Message> {
        Some(())
    }
}

#[async_trait]
impl AsyncActor for FileSource {
    async fn process_message(
        &mut self,
        message: Self::Message,
        context: Context<'_, Self::Message>,
    ) -> Result<(), MessageProcessError> {
        let mut line = String::new();
        let num_bytes = self.file
            .read_line(&mut line).await
            .map_err(|io_err: io::Error| {
                match io_err.kind() {
                    io::ErrorKind::ConnectionAborted | io::ErrorKind::BrokenPipe | io::ErrorKind::UnexpectedEof => {
                        MessageProcessError::Terminated
                    },
                    _ => {
                        MessageProcessError::Error(anyhow::anyhow!(io_err))
                    }
                }
            })?;
        self.file_position.num_bytes += num_bytes as u64;
        self.file_position.line_num += 1u64;
        Ok(())
    }
}
