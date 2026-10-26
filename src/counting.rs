//! A `Storage` that counts every byte the engine appends, by file kind, over the OS backend.
//!
//! Ordering: the counters are statistics read after the writers are done; nothing is published
//! through them, so every access is Relaxed. Each counter has its own 128-byte line, since the
//! log writer and the compaction threads append concurrently.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tessarikv::{OsStorage, ReadableFile, Result, Storage, StorageLock, WritableFile};

/// The kinds of file the engine writes, told apart by name.
#[derive(Debug, Clone, Copy)]
enum Kind {
    Log,
    Tables,
    Manifest,
    Other,
}

impl Kind {
    fn of(path: &Path) -> Self {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        if name.ends_with(".log") {
            Self::Log
        } else if name.ends_with(".sst") {
            Self::Tables
        } else if name.starts_with("MANIFEST-") {
            Self::Manifest
        } else {
            Self::Other
        }
    }
}

#[derive(Debug, Default)]
#[repr(align(128))]
struct Padded(AtomicU64);

/// Bytes appended so far, per kind of file.
#[derive(Debug, Default)]
pub struct Counters([Padded; 4]);

/// One reading of the counters.
#[derive(Debug, Clone, Copy, Default)]
pub struct Bytes {
    pub log: u64,
    pub tables: u64,
    pub manifest: u64,
    pub other: u64,
}

impl Counters {
    fn slot(&self, kind: Kind) -> &AtomicU64 {
        let [log, tables, manifest, other] = &self.0;
        match kind {
            Kind::Log => &log.0,
            Kind::Tables => &tables.0,
            Kind::Manifest => &manifest.0,
            Kind::Other => &other.0,
        }
    }

    fn add(&self, kind: Kind, bytes: u64) {
        // Relaxed: see the module documentation.
        self.slot(kind).fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn read(&self) -> Bytes {
        // Relaxed: see the module documentation.
        let load = |kind: Kind| self.slot(kind).load(Ordering::Relaxed);
        Bytes {
            log: load(Kind::Log),
            tables: load(Kind::Tables),
            manifest: load(Kind::Manifest),
            other: load(Kind::Other),
        }
    }
}

impl Bytes {
    /// The bytes written between `earlier` and this reading.
    pub fn since(self, earlier: Bytes) -> Bytes {
        Bytes {
            log: self.log.saturating_sub(earlier.log),
            tables: self.tables.saturating_sub(earlier.tables),
            manifest: self.manifest.saturating_sub(earlier.manifest),
            other: self.other.saturating_sub(earlier.other),
        }
    }

    pub fn all(self) -> u64 {
        self.log
            .saturating_add(self.tables)
            .saturating_add(self.manifest)
            .saturating_add(self.other)
    }
}

/// The OS backend with every append counted into `counters`.
#[derive(Debug, Default)]
pub struct CountingStorage {
    inner: OsStorage,
    pub counters: Arc<Counters>,
    /// Every table file closed so far, in order: (name, bytes). Touched once per file.
    pub tables: TableSizes,
    /// Positional reads of opened files: calls, then bytes.
    pub reads: Arc<Counters>,
}

struct CountingReader {
    inner: Arc<dyn ReadableFile>,
    reads: Arc<Counters>,
}

impl ReadableFile for CountingReader {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        self.reads.add(Kind::Log, 1);
        self.reads
            .add(Kind::Tables, u64::try_from(buf.len()).unwrap_or(u64::MAX));
        self.inner.read_at(offset, buf)
    }
    fn try_read_at(&self, offset: u64, buf: &mut [u8]) -> Result<bool> {
        let made = self.inner.try_read_at(offset, buf)?;
        if made {
            self.reads.add(Kind::Log, 1);
            self.reads
                .add(Kind::Tables, u64::try_from(buf.len()).unwrap_or(u64::MAX));
        }
        Ok(made)
    }
    fn len(&self) -> Result<u64> {
        self.inner.len()
    }
}

/// Each table file's name and size in bytes, recorded when it closes.
type TableSizes = Arc<Mutex<Vec<(String, u64)>>>;

struct CountingFile {
    inner: Box<dyn WritableFile>,
    counters: Arc<Counters>,
    kind: Kind,
    /// For a table file: its name and where to record it when it closes.
    table: Option<(String, TableSizes)>,
}

impl Drop for CountingFile {
    fn drop(&mut self) {
        if let Some((name, tables)) = self.table.take() {
            let bytes = self.inner.len();
            tables
                .lock()
                .expect("an unpoisoned table list")
                .push((name, bytes));
        }
    }
}

impl WritableFile for CountingFile {
    fn append(&mut self, data: &[u8]) -> Result<()> {
        self.inner.append(data)?;
        self.counters
            .add(self.kind, u64::try_from(data.len()).unwrap_or(u64::MAX));
        Ok(())
    }
    fn sync(&mut self) -> Result<()> {
        self.inner.sync()
    }
    fn len(&self) -> u64 {
        self.inner.len()
    }
}

impl Storage for CountingStorage {
    fn create(&self, path: &Path) -> Result<Box<dyn WritableFile>> {
        let inner = self.inner.create(path)?;
        let kind = Kind::of(path);
        let table = matches!(kind, Kind::Tables).then(|| {
            let name = path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned());
            (name.unwrap_or_default(), Arc::clone(&self.tables))
        });
        Ok(Box::new(CountingFile {
            inner,
            counters: Arc::clone(&self.counters),
            kind,
            table,
        }))
    }
    fn open(&self, path: &Path) -> Result<Arc<dyn ReadableFile>> {
        Ok(Arc::new(CountingReader {
            inner: self.inner.open(path)?,
            reads: Arc::clone(&self.reads),
        }))
    }
    fn exists(&self, path: &Path) -> Result<bool> {
        self.inner.exists(path)
    }
    fn remove(&self, path: &Path) -> Result<()> {
        self.inner.remove(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.rename(from, to)
    }
    fn list(&self, dir: &Path) -> Result<Vec<String>> {
        self.inner.list(dir)
    }
    fn create_dir_all(&self, dir: &Path) -> Result<()> {
        self.inner.create_dir_all(dir)
    }
    fn sync_dir(&self, dir: &Path) -> Result<()> {
        self.inner.sync_dir(dir)
    }
    fn lock(&self, path: &Path) -> Result<Box<dyn StorageLock>> {
        self.inner.lock(path)
    }
}
