//! Original Dispatcharr upload selection with bounded streaming decompression.
//! Filename suffixes are deliberately case-sensitive, as in fetch_m3u_lines.
use std::{
    fs::File,
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom},
    path::Path,
};

#[derive(Clone, Copy, Debug)]
pub struct UploadLimits {
    pub source_bytes: u64,
    pub decoded_bytes: u64,
    pub xz_memory_kib: u32,
    pub zip_entries: u64,
    pub zip_directory_bytes: u64,
}
impl Default for UploadLimits {
    fn default() -> Self {
        Self {
            source_bytes: 512 * 1024 * 1024,
            decoded_bytes: 2 * 1024 * 1024 * 1024,
            xz_memory_kib: 64 * 1024,
            zip_entries: 4096,
            zip_directory_bytes: 8 * 1024 * 1024,
        }
    }
}
#[derive(Debug)]
pub enum UploadError {
    Io(io::Error),
    Archive(zip::result::ZipError),
    Capacity(&'static str),
    MissingPlaylist,
}
impl From<io::Error> for UploadError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
impl From<zip::result::ZipError> for UploadError {
    fn from(value: zip::result::ZipError) -> Self {
        Self::Archive(value)
    }
}
impl std::fmt::Display for UploadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => error.fmt(f),
            Self::Archive(error) => error.fmt(f),
            Self::Capacity(name) => write!(f, "Uploaded M3U exceeds configured {name} limit"),
            Self::MissingPlaylist => f.write_str("No .m3u file found in ZIP archive"),
        }
    }
}
impl std::error::Error for UploadError {}

struct Limited<R> {
    inner: R,
    remaining: u64,
}
// Python LZMAFile starts an AUTO decoder for each member. It ignores an
// invalid trailing member after a complete stream, but still rejects truncated
// potential members (including all-zero XZ padding). liblzma preserves that
// distinction and also accepts legacy .lzma content in an .xz-named upload.
struct SourceLzma<R: BufRead> {
    reader: R,
    stream: xz2::stream::Stream,
    limit: u64,
    trailing: bool,
    finished: bool,
    member_finished: bool,
}
impl<R: BufRead> SourceLzma<R> {
    fn new(reader: R, limit: u64) -> io::Result<Self> {
        let stream = xz2::stream::Stream::new_auto_decoder(limit, 0)?;
        Ok(Self {
            reader,
            stream,
            limit,
            trailing: false,
            finished: false,
            member_finished: false,
        })
    }
}
impl<R: BufRead> Read for SourceLzma<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() || self.finished {
            return Ok(0);
        }
        loop {
            if self.member_finished {
                if self.reader.fill_buf()?.is_empty() {
                    self.finished = true;
                    return Ok(0);
                }
                self.stream = xz2::stream::Stream::new_auto_decoder(self.limit, 0)?;
                self.member_finished = false;
                self.trailing = true;
            }
            let input = self.reader.fill_buf()?;
            let eof = input.is_empty();
            let before_in = self.stream.total_in();
            let before_out = self.stream.total_out();
            let result = self.stream.process(
                input,
                output,
                if eof {
                    xz2::stream::Action::Finish
                } else {
                    xz2::stream::Action::Run
                },
            );
            let consumed = (self.stream.total_in() - before_in) as usize;
            let produced = (self.stream.total_out() - before_out) as usize;
            self.reader.consume(consumed);
            match result {
                Ok(status) => {
                    self.member_finished = status == xz2::stream::Status::StreamEnd;
                    if produced > 0 {
                        self.trailing = false;
                        return Ok(produced);
                    }
                    if self.member_finished {
                        continue;
                    }
                    if eof {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "Compressed file ended before the end-of-stream marker was reached",
                        ));
                    }
                    if consumed == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "LZMA decoder made no progress",
                        ));
                    }
                }
                Err(error) => {
                    let invalid = matches!(
                        error,
                        xz2::stream::Error::Data
                            | xz2::stream::Error::Format
                            | xz2::stream::Error::Options
                    );
                    if self.trailing && invalid {
                        self.finished = true;
                        return Ok(0);
                    }
                    return Err(error.into());
                }
            }
        }
    }
}
impl<R: Read> Read for Limited<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if self.remaining == 0 {
            let mut byte = [0];
            return if self.inner.read(&mut byte)? == 0 {
                Ok(0)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Uploaded M3U exceeds configured decoded-byte limit",
                ))
            };
        }
        let count = output
            .len()
            .min(self.remaining.min(usize::MAX as u64) as usize);
        let count = self.inner.read(&mut output[..count])?;
        self.remaining -= count as u64;
        Ok(count)
    }
}
fn invoke<R: Read, T>(
    inner: R,
    maximum: u64,
    read: impl FnOnce(&mut dyn BufRead) -> Result<T, UploadError>,
) -> Result<T, UploadError> {
    let mut reader = BufReader::with_capacity(
        8192,
        Limited {
            inner,
            remaining: maximum,
        },
    );
    read(&mut reader)
}

/// The callback must consume the input to EOF before committing imported rows.
/// A trailing CRC/error or expanded-byte overflow must fail the whole refresh.
/// No archive member is extracted to a provider-controlled filesystem path.
pub fn with_uploaded_playlist<T>(
    path: &Path,
    limits: UploadLimits,
    read: impl FnOnce(&mut dyn BufRead) -> Result<T, UploadError>,
) -> Result<T, UploadError> {
    if limits.source_bytes == 0 || limits.decoded_bytes == 0 || limits.xz_memory_kib == 0 {
        return Err(UploadError::Capacity("nonzero capacity"));
    }
    let mut file = File::open(path)?;
    if file.metadata()?.len() > limits.source_bytes {
        return Err(UploadError::Capacity("source-byte"));
    }
    let name = path.as_os_str().to_string_lossy();
    if name.ends_with(".gz") {
        if file.metadata()?.len() == 0 {
            return invoke(file, limits.decoded_bytes, read);
        }
        invoke(
            flate2::read::MultiGzDecoder::new(file),
            limits.decoded_bytes,
            read,
        )
    } else if name.ends_with(".xz") {
        invoke(
            SourceLzma::new(
                BufReader::with_capacity(8192, file),
                u64::from(limits.xz_memory_kib) * 1024,
            )?,
            limits.decoded_bytes,
            read,
        )
    } else if name.ends_with(".zip") {
        check_zip_directory(&mut file, limits)?;
        let mut archive = zip::ZipArchive::new(file)?;
        let first = archive
            .file_names()
            .find_map(|name| match name {
                Ok(name) if name.ends_with(".m3u") => Some(Ok(name.into_owned())),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .transpose()?
            .ok_or(UploadError::MissingPlaylist)?;
        // Python namelist() chooses the first matching name, then open(name)
        // resolves duplicate names to their final central-directory entry.
        let member = archive.by_name(&first)?;
        if member.size() > limits.decoded_bytes {
            return Err(UploadError::Capacity("decoded-byte"));
        }
        invoke(member, limits.decoded_bytes, read)
    } else {
        invoke(file, limits.decoded_bytes, read)
    }
}

fn invalid_zip() -> UploadError {
    io::Error::new(io::ErrorKind::InvalidData, "Invalid ZIP central directory").into()
}
fn le16(bytes: &[u8]) -> u64 {
    u16::from_le_bytes(bytes[..2].try_into().unwrap()) as u64
}
fn le32(bytes: &[u8]) -> u64 {
    u32::from_le_bytes(bytes[..4].try_into().unwrap()) as u64
}
fn le64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes[..8].try_into().unwrap())
}
/// Inspect the bounded footer before the ZIP library allocates entry metadata.
fn check_zip_directory(file: &mut File, limits: UploadLimits) -> Result<(), UploadError> {
    let length = file.metadata()?.len();
    let tail_size = length.min(65557) as usize;
    file.seek(SeekFrom::End(-(tail_size as i64)))?;
    let mut tail = vec![0; tail_size];
    file.read_exact(&mut tail)?;
    let offset = tail
        .windows(4)
        .rposition(|bytes| bytes == b"PK\x05\x06")
        .ok_or_else(invalid_zip)?;
    if offset + 22 > tail.len() {
        return Err(invalid_zip());
    }
    let end = &tail[offset..];
    if 22 + le16(&end[20..]) as usize > end.len() {
        return Err(invalid_zip());
    }
    let mut entries = le16(&end[10..]);
    let mut directory = le32(&end[12..]);
    let mut directory_end = length - tail_size as u64 + offset as u64;
    if entries == 65535 || directory == u32::MAX as u64 {
        let eocd = length - tail_size as u64 + offset as u64;
        if eocd < 20 {
            return Err(invalid_zip());
        }
        file.seek(SeekFrom::Start(eocd - 20))?;
        let mut locator = [0; 20];
        file.read_exact(&mut locator)?;
        if &locator[..4] != b"PK\x06\x07" {
            return Err(invalid_zip());
        }
        directory_end = le64(&locator[8..]);
        file.seek(SeekFrom::Start(directory_end))?;
        let mut record = [0; 56];
        file.read_exact(&mut record)?;
        if &record[..4] != b"PK\x06\x06" {
            return Err(invalid_zip());
        }
        entries = le64(&record[32..]);
        directory = le64(&record[40..]);
    }
    if entries > limits.zip_entries {
        return Err(UploadError::Capacity("ZIP entry-count"));
    }
    if directory > limits.zip_directory_bytes {
        return Err(UploadError::Capacity("ZIP directory-byte"));
    }
    // Do not trust a forged EOCD entry count to bound the library's allocations.
    // Count the variable-length directory records before constructing ZipArchive.
    let start = directory_end
        .checked_sub(directory)
        .ok_or_else(invalid_zip)?;
    file.seek(SeekFrom::Start(start))?;
    let mut position = start;
    let mut actual_entries = 0_u64;
    while position < directory_end {
        if directory_end - position < 46 {
            return Err(invalid_zip());
        }
        let mut record = [0; 46];
        file.read_exact(&mut record)?;
        if &record[..4] != b"PK\x01\x02" {
            return Err(invalid_zip());
        }
        actual_entries += 1;
        if actual_entries > limits.zip_entries {
            return Err(UploadError::Capacity("ZIP entry-count"));
        }
        let variable = le16(&record[28..]) + le16(&record[30..]) + le16(&record[32..]);
        position = position
            .checked_add(46 + variable)
            .filter(|position| *position <= directory_end)
            .ok_or_else(invalid_zip)?;
        file.seek(SeekFrom::Start(position))?;
    }
    file.rewind()?;
    Ok(())
}
