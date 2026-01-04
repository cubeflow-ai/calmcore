//! Memory-mapped full-text index implementation
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;

use memmap2::Mmap;

use crate::utils::error::{CoreError, CoreResult};

pub const TERMS_FILE: &str = "terms.dict";
pub const POSTINGS_FILE: &str = "postings.dat";
pub const FIELD_STATS_FILE: &str = "field_stats.json";

/// On-disk representation of a term in the dictionary
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct TermDictEntry {
    pub term_offset: u64,
    pub term_len: u32,
    pub postings_offset: u64,
    pub postings_len: u32,
}

pub struct MmapIndex {
    terms_mmap: Mmap,
    postings_mmap: Mmap,
    num_terms: usize,
}

impl MmapIndex {
    pub fn open(path: &str) -> CoreResult<Self> {
        let terms_path = Path::new(path).join(TERMS_FILE);
        let postings_path = Path::new(path).join(POSTINGS_FILE);

        let terms_file = File::open(&terms_path).map_err(|e| {
            CoreError::IOError(format!(
                "Failed to open terms file {}: {}",
                terms_path.display(),
                e
            ))
        })?;
        let postings_file = File::open(&postings_path).map_err(|e| {
            CoreError::IOError(format!(
                "Failed to open postings file {}: {}",
                postings_path.display(),
                e
            ))
        })?;

        let terms_mmap = unsafe { Mmap::map(&terms_file)? };
        let postings_mmap = unsafe { Mmap::map(&postings_file)? };

        let num_terms = terms_mmap.len() / std::mem::size_of::<TermDictEntry>();

        Ok(Self {
            terms_mmap,
            postings_mmap,
            num_terms,
        })
    }

    pub fn get_term_entry(&self, index: usize) -> Option<&TermDictEntry> {
        if index >= self.num_terms {
            return None;
        }
        let offset = index * std::mem::size_of::<TermDictEntry>();
        let entry_bytes = &self.terms_mmap[offset..offset + std::mem::size_of::<TermDictEntry>()];
        let entry = unsafe { &*(entry_bytes.as_ptr() as *const TermDictEntry) };
        Some(entry)
    }

    pub fn get_term(&self, entry: &TermDictEntry) -> &str {
        let term_bytes = &self.postings_mmap
            [entry.term_offset as usize..(entry.term_offset + entry.term_len as u64) as usize];
        std::str::from_utf8(term_bytes).unwrap_or("")
    }

    pub fn get_postings_data(&self, entry: &TermDictEntry) -> &[u8] {
        &self.postings_mmap[entry.postings_offset as usize
            ..(entry.postings_offset + entry.postings_len as u64) as usize]
    }

    pub fn num_terms(&self) -> usize {
        self.num_terms
    }
}

pub struct MmapIndexWriter {
    terms_writer: BufWriter<File>,
    postings_writer: BufWriter<File>,
    current_postings_offset: u64,
}

impl MmapIndexWriter {
    pub fn new(path: &str) -> CoreResult<Self> {
        let terms_path = Path::new(path).join(TERMS_FILE);
        let postings_path = Path::new(path).join(POSTINGS_FILE);

        let terms_file = File::create(&terms_path).map_err(|e| {
            CoreError::IOError(format!(
                "Failed to create terms file {}: {}",
                terms_path.display(),
                e
            ))
        })?;
        let postings_file = File::create(&postings_path).map_err(|e| {
            CoreError::IOError(format!(
                "Failed to create postings file {}: {}",
                postings_path.display(),
                e
            ))
        })?;

        Ok(Self {
            terms_writer: BufWriter::new(terms_file),
            postings_writer: BufWriter::new(postings_file),
            current_postings_offset: 0,
        })
    }

    pub fn write_term(&mut self, term: &str, postings_data: &[u8]) -> io::Result<()> {
        let term_offset = self.current_postings_offset;
        let term_len = term.len() as u32;

        self.postings_writer.write_all(term.as_bytes())?;
        self.current_postings_offset += term_len as u64;

        let postings_offset = self.current_postings_offset;
        let postings_len = postings_data.len() as u32;

        self.postings_writer.write_all(postings_data)?;
        self.current_postings_offset += postings_len as u64;

        let entry = TermDictEntry {
            term_offset,
            term_len,
            postings_offset,
            postings_len,
        };

        let entry_bytes = unsafe {
            std::slice::from_raw_parts(
                &entry as *const _ as *const u8,
                std::mem::size_of::<TermDictEntry>(),
            )
        };

        self.terms_writer.write_all(entry_bytes)?;

        Ok(())
    }

    pub fn finish(self) -> CoreResult<()> {
        self.terms_writer
            .into_inner()
            .map_err(|e| CoreError::IOError(format!("Failed to flush terms writer: {}", e.to_string())))?
            .sync_all()?;
        self.postings_writer
            .into_inner()
            .map_err(|e| {
                CoreError::IOError(format!("Failed to flush postings writer: {}", e.to_string()))
            })?
            .sync_all()?;
        Ok(())
    }
}
