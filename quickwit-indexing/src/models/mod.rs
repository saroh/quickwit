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

mod batch;
mod indexed_split;
mod manifest;
mod packaged_split;
mod split_label;
mod uploaded_split;
mod raw_batch;

pub use self::batch::Batch;
pub use self::raw_batch::RawBatch;
pub use self::indexed_split::IndexedSplit;
pub use self::manifest::Manifest;
pub use self::packaged_split::PackagedSplit;
pub use self::split_label::SplitLabel;
pub use self::uploaded_split::UploadedSplit;

pub type SourceId = String;
pub type IndexId = String;
pub type Offset = Vec<u8>;
