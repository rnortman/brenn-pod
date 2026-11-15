//! WAV files of integer PCM: the header [`export_wav`](crate::export_wav)
//! writes, and the reader `write-capture` takes its samples from.

use std::fmt;
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use crate::capture_stream::SampleFormat;

/// Bytes of the WAV header this writes: `RIFF`, a 40-byte
/// `WAVE_FORMAT_EXTENSIBLE` `fmt ` chunk, and the `data` chunk's header.
pub const WAV_HEADER_BYTES: u64 = 68;

const WAVE_FORMAT_PCM: u16 = 0x0001;
const WAVE_FORMAT_EXTENSIBLE: u16 = 0xfffe;

/// The PCM sub-format GUID, `00000001-0000-0010-8000-00aa00389b71`.
const KSDATAFORMAT_SUBTYPE_PCM: [u8; 16] = [
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x80, 0x00, 0x00, 0xaa, 0x00, 0x38, 0x9b, 0x71,
];

/// The header of a `WAVE_FORMAT_EXTENSIBLE` PCM file holding `data_bytes` of
/// samples. No speaker positions are claimed.
pub(crate) fn header(
    channels: u16,
    rate_hz: u32,
    bits: u16,
    byte_rate: u32,
    data_bytes: u32,
) -> Vec<u8> {
    let block_align = channels * (bits / 8);
    let mut h = Vec::with_capacity(WAV_HEADER_BYTES as usize);
    h.extend_from_slice(b"RIFF");
    h.extend_from_slice(&(data_bytes + (WAV_HEADER_BYTES as u32 - 8)).to_le_bytes());
    h.extend_from_slice(b"WAVE");
    h.extend_from_slice(b"fmt ");
    h.extend_from_slice(&40u32.to_le_bytes());
    h.extend_from_slice(&WAVE_FORMAT_EXTENSIBLE.to_le_bytes());
    h.extend_from_slice(&channels.to_le_bytes());
    h.extend_from_slice(&rate_hz.to_le_bytes());
    h.extend_from_slice(&byte_rate.to_le_bytes());
    h.extend_from_slice(&block_align.to_le_bytes());
    h.extend_from_slice(&bits.to_le_bytes());
    h.extend_from_slice(&22u16.to_le_bytes());
    h.extend_from_slice(&bits.to_le_bytes());
    h.extend_from_slice(&0u32.to_le_bytes());
    h.extend_from_slice(&KSDATAFORMAT_SUBTYPE_PCM);
    h.extend_from_slice(b"data");
    h.extend_from_slice(&data_bytes.to_le_bytes());
    debug_assert_eq!(h.len() as u64, WAV_HEADER_BYTES);
    h
}

/// What a WAV file's `fmt ` chunk states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WavFormat {
    pub rate_hz: u32,
    pub channels: u16,
    pub sample_format: SampleFormat,
}

impl WavFormat {
    /// Bytes of one frame: one sample per channel.
    pub fn frame_bytes(&self) -> usize {
        usize::from(self.channels) * self.sample_format.bytes_per_sample()
    }
}

/// Why a WAV file was not read.
#[derive(Debug)]
pub enum WavError {
    /// The file is not a WAV file this reads; the message says what was found.
    Refused(String),
    Io(io::Error),
}

impl fmt::Display for WavError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WavError::Refused(why) => write!(f, "WAV file refused: {why}"),
            WavError::Io(e) => write!(f, "reading the WAV file: {e}"),
        }
    }
}

impl std::error::Error for WavError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WavError::Io(e) => Some(e),
            WavError::Refused(_) => None,
        }
    }
}

impl From<io::Error> for WavError {
    fn from(e: io::Error) -> Self {
        WavError::Io(e)
    }
}

fn refused<T>(why: String) -> Result<T, WavError> {
    Err(WavError::Refused(why))
}

/// A WAV file of 16- or 32-bit little-endian integer PCM, as
/// `WAVE_FORMAT_PCM` or as `WAVE_FORMAT_EXTENSIBLE` with the PCM sub-format,
/// read a block of frames at a time. Chunks other than `fmt ` and `data` are
/// skipped.
pub struct WavReader<R> {
    inner: R,
    format: WavFormat,
    frames: u64,
    /// Bytes of the `data` chunk not yet read.
    remaining: u64,
}

impl WavReader<BufReader<File>> {
    pub fn open(path: &Path) -> Result<Self, WavError> {
        WavReader::new(BufReader::new(File::open(path)?))
    }
}

impl<R: Read + Seek> WavReader<R> {
    /// Read the header up to the start of the samples. A `data` chunk that is
    /// not whole frames or runs past the end of the input is refused here.
    pub fn new(mut inner: R) -> Result<Self, WavError> {
        let len = inner.seek(SeekFrom::End(0))?;
        inner.seek(SeekFrom::Start(0))?;
        let mut riff = [0u8; 12];
        read_header_bytes(&mut inner, &mut riff, "the RIFF header")?;
        if &riff[0..4] != b"RIFF" || &riff[8..12] != b"WAVE" {
            return refused("not a RIFF/WAVE file".to_owned());
        }
        let mut pos = 12u64;
        let mut format = None;
        loop {
            let mut chunk = [0u8; 8];
            if pos + 8 > len {
                return refused("no data chunk".to_owned());
            }
            read_header_bytes(&mut inner, &mut chunk, "a chunk header")?;
            pos += 8;
            let id = [chunk[0], chunk[1], chunk[2], chunk[3]];
            let size = u64::from(u32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]));
            match &id {
                b"fmt " => {
                    if size > len - pos {
                        return refused(format!(
                            "the fmt chunk states {size} bytes and the file holds {}",
                            len - pos
                        ));
                    }
                    let mut body = vec![0u8; size as usize];
                    read_header_bytes(&mut inner, &mut body, "the fmt chunk")?;
                    format = Some(parse_fmt(&body)?);
                }
                b"data" => {
                    let Some(format) = format else {
                        return refused("the data chunk comes before any fmt chunk".to_owned());
                    };
                    if size > len - pos {
                        return refused(format!(
                            "the data chunk states {size} bytes and the file holds {}",
                            len - pos
                        ));
                    }
                    let frame_bytes = format.frame_bytes() as u64;
                    if !size.is_multiple_of(frame_bytes) {
                        return refused(format!(
                            "the data chunk's {size} bytes are not whole {frame_bytes}-byte frames"
                        ));
                    }
                    return Ok(WavReader {
                        inner,
                        format,
                        frames: size / frame_bytes,
                        remaining: size,
                    });
                }
                _ => {
                    let skip = size + (size & 1);
                    if skip > len - pos {
                        return refused(format!(
                            "chunk {:?} states {size} bytes and the file holds {}",
                            String::from_utf8_lossy(&id),
                            len - pos
                        ));
                    }
                    inner.seek(SeekFrom::Current(skip as i64))?;
                    pos += skip;
                    continue;
                }
            }
            pos += size;
            if size & 1 == 1 {
                inner.seek(SeekFrom::Current(1))?;
                pos += 1;
            }
        }
    }

    pub fn format(&self) -> WavFormat {
        self.format
    }

    /// Frames in the `data` chunk.
    pub fn frames(&self) -> u64 {
        self.frames
    }

    /// The next `frames` frames, or as many as remain.
    pub fn read_frames(&mut self, frames: u64) -> io::Result<Vec<u8>> {
        let bytes = (frames * self.format.frame_bytes() as u64).min(self.remaining);
        let mut buf = vec![0u8; bytes as usize];
        self.inner.read_exact(&mut buf)?;
        self.remaining -= bytes;
        Ok(buf)
    }

    /// Pass over the next `frames` frames without reading them. More frames
    /// than remain is `InvalidInput`, and nothing is passed over.
    pub fn skip_frames(&mut self, frames: u64) -> io::Result<()> {
        let bytes = frames
            .checked_mul(self.format.frame_bytes() as u64)
            .filter(|b| *b <= self.remaining)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "{frames} frames to skip and {} remain",
                        self.remaining / self.format.frame_bytes() as u64
                    ),
                )
            })?;
        self.inner.seek(SeekFrom::Current(bytes as i64))?;
        self.remaining -= bytes;
        Ok(())
    }
}

fn read_header_bytes(r: &mut impl Read, buf: &mut [u8], what: &str) -> Result<(), WavError> {
    match r.read_exact(buf) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
            refused(format!("the file ends inside {what}"))
        }
        Err(e) => Err(WavError::Io(e)),
    }
}

fn u16_at(b: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([b[at], b[at + 1]])
}

fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}

fn parse_fmt(b: &[u8]) -> Result<WavFormat, WavError> {
    if b.len() < 16 {
        return refused(format!("a fmt chunk of {} bytes", b.len()));
    }
    let tag = u16_at(b, 0);
    let channels = u16_at(b, 2);
    let rate_hz = u32_at(b, 4);
    let byte_rate = u32_at(b, 8);
    let block_align = u16_at(b, 12);
    let bits = u16_at(b, 14);
    match tag {
        WAVE_FORMAT_PCM => {}
        WAVE_FORMAT_EXTENSIBLE => {
            if b.len() < 40 || u16_at(b, 16) < 22 {
                return refused(format!(
                    "a WAVE_FORMAT_EXTENSIBLE fmt chunk of {} bytes",
                    b.len()
                ));
            }
            if b[24..40] != KSDATAFORMAT_SUBTYPE_PCM {
                return refused("a sub-format that is not integer PCM".to_owned());
            }
            let valid = u16_at(b, 18);
            if valid != bits {
                return refused(format!("{valid} valid bits in {bits}-bit samples"));
            }
        }
        other => return refused(format!("format tag {other:#06x}, not integer PCM")),
    }
    let sample_format = match bits {
        16 => SampleFormat::S16Le,
        32 => SampleFormat::S32Le,
        other => return refused(format!("{other}-bit samples; 16 or 32 are read")),
    };
    if channels == 0 || rate_hz == 0 {
        return refused(format!("{channels} channels at {rate_hz} Hz"));
    }
    let format = WavFormat {
        rate_hz,
        channels,
        sample_format,
    };
    let frame_bytes = format.frame_bytes();
    if usize::from(block_align) != frame_bytes {
        return refused(format!(
            "a block align of {block_align} for {channels} channels of {bits} bits"
        ));
    }
    if u64::from(byte_rate) != u64::from(rate_hz) * frame_bytes as u64 {
        return refused(format!(
            "a byte rate of {byte_rate} for {rate_hz} Hz of {frame_bytes}-byte frames"
        ));
    }
    Ok(format)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::io::Cursor;

    /// A plain `WAVE_FORMAT_PCM` file.
    pub(crate) fn pcm_wav(channels: u16, rate_hz: u32, bits: u16, data: &[u8]) -> Vec<u8> {
        let block_align = channels * bits / 8;
        let mut w = Vec::new();
        w.extend_from_slice(b"RIFF");
        w.extend_from_slice(&(36 + data.len() as u32).to_le_bytes());
        w.extend_from_slice(b"WAVE");
        w.extend_from_slice(b"fmt ");
        w.extend_from_slice(&16u32.to_le_bytes());
        w.extend_from_slice(&WAVE_FORMAT_PCM.to_le_bytes());
        w.extend_from_slice(&channels.to_le_bytes());
        w.extend_from_slice(&rate_hz.to_le_bytes());
        w.extend_from_slice(&(rate_hz * u32::from(block_align)).to_le_bytes());
        w.extend_from_slice(&block_align.to_le_bytes());
        w.extend_from_slice(&bits.to_le_bytes());
        w.extend_from_slice(b"data");
        w.extend_from_slice(&(data.len() as u32).to_le_bytes());
        w.extend_from_slice(data);
        w
    }

    fn read(bytes: Vec<u8>) -> Result<WavReader<Cursor<Vec<u8>>>, WavError> {
        WavReader::new(Cursor::new(bytes))
    }

    fn refusal(bytes: Vec<u8>, needle: &str) {
        match read(bytes) {
            Err(WavError::Refused(why)) => assert!(why.contains(needle), "{why} lacks {needle:?}"),
            Err(e) => panic!("{e}"),
            Ok(_) => panic!("accepted; expected a refusal naming {needle:?}"),
        }
    }

    #[test]
    fn a_pcm_file_reads_at_16_and_32_bits() {
        let data: Vec<u8> = (0..24).collect();
        for (bits, sample_format, frames) in
            [(16, SampleFormat::S16Le, 6), (32, SampleFormat::S32Le, 3)]
        {
            let mut r = read(pcm_wav(2, 8000, bits, &data)).unwrap();
            assert_eq!(
                r.format(),
                WavFormat {
                    rate_hz: 8000,
                    channels: 2,
                    sample_format
                }
            );
            assert_eq!(r.frames(), frames);
            let first = r.read_frames(1).unwrap();
            let rest = r.read_frames(100).unwrap();
            assert_eq!([first, rest].concat(), data);
            assert!(r.read_frames(1).unwrap().is_empty());
        }
    }

    #[test]
    fn the_header_this_writes_reads_back() {
        let data = [1u8, 2, 3, 4, 5, 6];
        let mut bytes = header(3, 16_000, 16, 96_000, 6);
        bytes.extend_from_slice(&data);
        let mut r = read(bytes).unwrap();
        assert_eq!(r.format().channels, 3);
        assert_eq!(r.format().sample_format, SampleFormat::S16Le);
        assert_eq!(r.frames(), 1);
        assert_eq!(r.read_frames(1).unwrap(), data);
    }

    #[test]
    fn other_chunks_are_skipped_with_their_pad_byte() {
        let plain = pcm_wav(1, 1000, 16, &[7, 0]);
        let mut bytes = plain[..12].to_vec();
        bytes.extend_from_slice(b"LIST");
        bytes.extend_from_slice(&3u32.to_le_bytes());
        bytes.extend_from_slice(&[9, 9, 9, 0]);
        bytes.extend_from_slice(&plain[12..]);
        let mut r = read(bytes).unwrap();
        assert_eq!(r.read_frames(1).unwrap(), [7, 0]);
    }

    #[test]
    fn a_skip_passes_over_frames_and_the_read_after_it_continues() {
        let data: Vec<u8> = (0..24).collect();
        let mut r = read(pcm_wav(2, 8000, 16, &data)).unwrap();
        assert_eq!(r.read_frames(1).unwrap(), data[0..4]);
        r.skip_frames(2).unwrap();
        assert_eq!(r.read_frames(1).unwrap(), data[12..16]);
        r.skip_frames(0).unwrap();
        r.skip_frames(2).unwrap();
        assert!(r.read_frames(1).unwrap().is_empty());
    }

    #[test]
    fn a_skip_past_the_data_is_refused_and_moves_nothing() {
        let data: Vec<u8> = (0..24).collect();
        let mut r = read(pcm_wav(2, 8000, 16, &data)).unwrap();
        r.skip_frames(4).unwrap();
        let e = r.skip_frames(3).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidInput);
        assert!(
            e.to_string().contains("3 frames to skip and 2 remain"),
            "{e}"
        );
        assert!(r.skip_frames(u64::MAX).is_err());
        assert_eq!(r.read_frames(2).unwrap(), data[16..24]);
    }

    #[test]
    fn data_that_shrinks_after_the_header_is_an_error_not_short_samples() {
        let data: Vec<u8> = (0..24).collect();
        let wav = pcm_wav(2, 8000, 16, &data);
        let cut = wav.len() - 6;
        let mut r = read(wav).unwrap();
        r.inner.get_mut().truncate(cut);
        assert_eq!(r.read_frames(4).unwrap(), data[0..16]);
        let e = r.read_frames(2).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn each_malformed_shape_is_refused() {
        refusal(b"RIFX\0\0\0\0WAVE".to_vec(), "not a RIFF/WAVE");
        refusal(b"RIFF".to_vec(), "ends inside the RIFF header");

        let mut float = pcm_wav(1, 1000, 32, &[0; 4]);
        float[20..22].copy_from_slice(&3u16.to_le_bytes());
        refusal(float, "format tag 0x0003");

        refusal(pcm_wav(1, 1000, 8, &[0; 2]), "8-bit samples");
        refusal(pcm_wav(1, 1000, 24, &[0; 3]), "24-bit samples");

        refusal(pcm_wav(2, 1000, 16, &[0; 6]), "not whole 4-byte frames");

        let mut short = pcm_wav(1, 1000, 16, &[0; 4]);
        short.truncate(short.len() - 2);
        refusal(short, "the data chunk states 4 bytes and the file holds 2");

        let plain = pcm_wav(1, 1000, 16, &[0; 2]);
        let mut data_first = plain[..12].to_vec();
        data_first.extend_from_slice(&plain[36..]);
        data_first.extend_from_slice(&plain[12..36]);
        refusal(data_first, "before any fmt chunk");

        let mut extensible = header(1, 1000, 16, 2000, 2);
        extensible.extend_from_slice(&[0; 2]);
        extensible[44] = 0x03;
        refusal(extensible, "not integer PCM");

        let mut padded = header(1, 1000, 32, 4000, 4);
        padded.extend_from_slice(&[0; 4]);
        padded[38..40].copy_from_slice(&24u16.to_le_bytes());
        refusal(padded, "24 valid bits in 32-bit samples");

        refusal(plain[..36].to_vec(), "no data chunk");
    }
}
