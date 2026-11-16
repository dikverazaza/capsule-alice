//! Streaming MP3 -> signed 16-bit little-endian PCM decoder for Capsula Mini.
//!
//! This helper deliberately does not open PulseAudio, /dev/log, sysfs, or any
//! voice-agent resource. It writes raw 44.1 kHz stereo PCM to stdout; the
//! Rust PlaybackSupervisor pipes stdout into the firmware's existing paplay.

use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use reqwest::blocking::{Client, Response};
use symphonia::core::codecs::audio::{AudioDecoder, AudioDecoderOptions};
use symphonia::core::errors::Error as SymphoniaError;
use symphonia::core::formats::probe::Hint;
use symphonia::core::formats::{FormatOptions, FormatReader, TrackType};
use symphonia::core::io::{MediaSource, MediaSourceStream};
use symphonia::core::meta::MetadataOptions;
use symphonia::default::{get_codecs, get_probe};

const OUTPUT_RATE: u32 = 44_100;
const USER_AGENT: &str = "Mozilla/5.0 (X11; Linux aarch64) AppleWebKit/537.36 CapsulaAudio/0.1";
const MAX_CONSECUTIVE_DECODE_ERRORS: usize = 8;

/// Reqwest's blocking Response is a streaming Read. Mutex supplies Sync for
/// Symphonia's MediaSource bound; seeking is intentionally unsupported.
struct HttpMediaSource {
    response: Mutex<Response>,
    content_length: Option<u64>,
}

impl HttpMediaSource {
    fn new(response: Response) -> Self {
        let content_length = response.content_length();
        Self {
            response: Mutex::new(response),
            content_length,
        }
    }
}

impl Read for HttpMediaSource {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let mut response = self
            .response
            .lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "HTTP stream lock poisoned"))?;
        response.read(buffer).map_err(|error| {
            // reqwest can retain the full signed URL in nested body errors.
            // Keep logs useful without leaking a temporary Yandex token URL.
            io::Error::new(error.kind(), "HTTP audio stream read failed")
        })
    }
}

impl Seek for HttpMediaSource {
    fn seek(&mut self, _position: SeekFrom) -> io::Result<u64> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Yandex HTTP stream is not seekable",
        ))
    }
}

impl MediaSource for HttpMediaSource {
    fn is_seekable(&self) -> bool {
        false
    }

    fn byte_len(&self) -> Option<u64> {
        self.content_length
    }
}

/// Streaming linear resampler. It keeps one boundary frame between packets,
/// so packet boundaries do not introduce clicks or gaps.
struct StereoResampler {
    input_rate: u32,
    step: f64,
    position: f64,
    input_frames: u64,
    output_frames: u64,
    pending: Vec<[f32; 2]>,
}

impl StereoResampler {
    fn new(input_rate: u32) -> Result<Self> {
        if input_rate == 0 {
            bail!("MP3 stream reported a zero sample rate");
        }
        Ok(Self {
            input_rate,
            step: input_rate as f64 / OUTPUT_RATE as f64,
            position: 0.0,
            input_frames: 0,
            output_frames: 0,
            pending: Vec::with_capacity(4096),
        })
    }

    fn push(&mut self, input: &[[f32; 2]]) -> Vec<[f32; 2]> {
        self.input_frames = self.input_frames.saturating_add(input.len() as u64);
        self.pending.extend_from_slice(input);
        let mut output = Vec::with_capacity(
            ((input.len() as f64 * OUTPUT_RATE as f64 / self.input_rate as f64).ceil() as usize)
                .saturating_add(2),
        );

        while self.position + 1.0 < self.pending.len() as f64 {
            let left_index = self.position.floor() as usize;
            let fraction = (self.position - left_index as f64) as f32;
            let a = self.pending[left_index];
            let b = self.pending[left_index + 1];
            output.push([
                a[0] + (b[0] - a[0]) * fraction,
                a[1] + (b[1] - a[1]) * fraction,
            ]);
            self.position += self.step;
        }

        // Retain the last source frame for interpolation with the next packet.
        let consumed = (self.position.floor() as usize)
            .min(self.pending.len().saturating_sub(1));
        if consumed > 0 {
            self.pending.drain(0..consumed);
            self.position -= consumed as f64;
        }
        self.output_frames = self.output_frames.saturating_add(output.len() as u64);
        output
    }

    fn finish(&mut self) -> Vec<[f32; 2]> {
        let target_frames = ((self.input_frames as u128 * OUTPUT_RATE as u128
            + (self.input_rate as u128 / 2))
            / self.input_rate as u128) as u64;
        let mut output = Vec::new();
        while !self.pending.is_empty() && self.output_frames < target_frames {
            let left_index = (self.position.floor() as usize).min(self.pending.len() - 1);
            let fraction = (self.position - left_index as f64).clamp(0.0, 1.0) as f32;
            let a = self.pending[left_index];
            let b = self.pending[(left_index + 1).min(self.pending.len() - 1)];
            output.push([
                a[0] + (b[0] - a[0]) * fraction,
                a[1] + (b[1] - a[1]) * fraction,
            ]);
            self.position += self.step;
            self.output_frames += 1;
        }
        self.pending.clear();
        output
    }
}

/// Dynamic loudness normalizer and smooth crossfade processor.
/// Applies target ReplayGain linear scaling, smooth equal-power fade-in (anti-click),
/// and an analog-style soft-knee saturation limiter to prevent digital clipping on peaks.
struct LoudnessProcessor {
    gain_linear: f32,
    fade_in_frames: u64,
    current_frame: u64,
}

impl LoudnessProcessor {
    fn new(gain_db: f32, fade_in_ms: u64) -> Self {
        let clamped_db = gain_db.clamp(-24.0, 12.0);
        let gain_linear = 10.0f32.powf(clamped_db / 20.0);
        let fade_in_frames = (fade_in_ms as f64 * OUTPUT_RATE as f64 / 1000.0).round() as u64;
        Self {
            gain_linear,
            fade_in_frames,
            current_frame: 0,
        }
    }

    #[inline(always)]
    fn soft_limit(x: f32) -> f32 {
        const THRESHOLD: f32 = 0.85;
        const MARGIN: f32 = 1.0 - THRESHOLD;
        if x.abs() <= THRESHOLD {
            x
        } else if x > THRESHOLD {
            THRESHOLD + MARGIN * ((x - THRESHOLD) / MARGIN).tanh()
        } else {
            -THRESHOLD - MARGIN * ((-x - THRESHOLD) / MARGIN).tanh()
        }
    }

    fn process(&mut self, frames: &[[f32; 2]]) -> Vec<[f32; 2]> {
        let mut out = Vec::with_capacity(frames.len());
        for frame in frames {
            let fade = if self.fade_in_frames > 0 && self.current_frame < self.fade_in_frames {
                let t = self.current_frame as f32 / self.fade_in_frames as f32;
                (t * std::f32::consts::FRAC_PI_2).sin()
            } else {
                1.0
            };
            self.current_frame = self.current_frame.saturating_add(1);

            let mult = self.gain_linear * fade;
            let left = Self::soft_limit(frame[0] * mult);
            let right = Self::soft_limit(frame[1] * mult);
            out.push([left, right]);
        }
        out
    }
}

fn write_pcm<W: Write>(writer: &mut W, frames: &[[f32; 2]]) -> io::Result<()> {
    let mut bytes = Vec::with_capacity(frames.len().saturating_mul(4));
    for frame in frames {
        for sample in frame {
            let clipped = sample.clamp(-1.0, 1.0);
            let value = if clipped < 0.0 {
                (clipped * 32768.0).round() as i16
            } else {
                (clipped * 32767.0).round() as i16
            };
            bytes.extend_from_slice(&value.to_le_bytes());
        }
    }
    writer.write_all(&bytes)
}

fn open_stream(url: &str) -> Result<HttpMediaSource> {
    if !(url.starts_with("https://") || url.starts_with("http://")) {
        bail!("audio URL must use HTTP or HTTPS");
    }

    let client = Client::builder()
        .user_agent(USER_AGENT)
        .connect_timeout(Duration::from_secs(12))
        // A music track is a long-lived response. Disable reqwest's total
        // request timeout so playback is not cut off after the default period.
        .timeout(None::<Duration>)
        .redirect(reqwest::redirect::Policy::limited(8))
        .build()
        .context("failed to create HTTP client")?;

    let response = client
        .get(url)
        .send()
        .map_err(|error| anyhow!("failed to request Yandex audio stream: {}", error.without_url()))?;
    let status = response.status();
    if !status.is_success() {
        bail!("Yandex audio server returned HTTP {status}");
    }
    Ok(HttpMediaSource::new(response))
}

fn decode_url(url: &str, gain_db: f32, fade_in_ms: u64) -> Result<u64> {
    let source = open_stream(url)?;
    let media_stream = MediaSourceStream::new(Box::new(source), Default::default());

    let mut hint = Hint::new();
    hint.with_extension("mp3");

    let probed = get_probe()
        .probe(
            &hint,
            media_stream,
            FormatOptions::default(),
            MetadataOptions::default(),
        )
        .context("stream is not a supported MP3 audio stream")?;
    let mut format = probed;

    let track = format
        .default_track(TrackType::Audio)
        .ok_or_else(|| anyhow!("MP3 stream does not contain an audio track"))?;
    let track_id = track.id;
    let audio_params = track
        .codec_params
        .as_ref()
        .ok_or_else(|| anyhow!("MP3 audio codec parameters are missing"))?
        .audio()
        .ok_or_else(|| anyhow!("stream does not have audio codec parameters"))?;

    let mut decoder = get_codecs()
        .make_audio_decoder(audio_params, &AudioDecoderOptions::default())
        .context("Symphonia could not create the MP3 decoder")?;

    let mut stdout = io::BufWriter::with_capacity(8 * 1024, io::stdout().lock());
    let mut resampler: Option<StereoResampler> = None;
    let mut processor = LoudnessProcessor::new(gain_db, fade_in_ms);
    let mut decoded_frames = 0_u64;
    let mut consecutive_decode_errors = 0_usize;
    let mut logged_input_format = false;

    loop {
        let packet = match format.next_packet() {
            Ok(Some(packet)) => packet,
            Ok(None) => break,
            Err(SymphoniaError::ResetRequired) => {
                bail!("MP3 stream changed format while playing; refusing unsafe decoder reset")
            }
            Err(SymphoniaError::IoError(error))
                if error.kind() == io::ErrorKind::UnexpectedEof =>
            {
                break
            }
            Err(error) => return Err(anyhow!("failed to read MP3 stream: {error}")),
        };

        if packet.track_id != track_id {
            continue;
        }

        let decoded = match decoder.decode(&packet) {
            Ok(decoded) => {
                consecutive_decode_errors = 0;
                decoded
            }
            Err(SymphoniaError::DecodeError(_)) => {
                consecutive_decode_errors += 1;
                if consecutive_decode_errors > MAX_CONSECUTIVE_DECODE_ERRORS {
                    bail!("too many consecutive MP3 frame decode errors");
                }
                continue;
            }
            Err(SymphoniaError::IoError(error))
                if error.kind() == io::ErrorKind::UnexpectedEof =>
            {
                break
            }
            Err(error) => return Err(anyhow!("MP3 decoder failed: {error}")),
        };

        let spec = decoded.spec();
        let sample_rate = spec.rate();
        let channel_count = spec.channels().count();
        if channel_count == 0 {
            bail!("MP3 decoder returned an audio buffer without channels");
        }
        if !logged_input_format {
            eprintln!(
                "[capsula-audio] input={sample_rate}Hz/{channel_count}ch; output={OUTPUT_RATE}Hz/2ch/s16le; gain={gain_db:+.2}dB; fade_in={fade_in_ms}ms"
            );
            logged_input_format = true;
        }

        let active_resampler = match resampler.as_mut() {
            Some(existing) if existing.input_rate == sample_rate => existing,
            Some(_) => bail!("MP3 sample rate changed during playback"),
            None => {
                resampler = Some(StereoResampler::new(sample_rate)?);
                resampler.as_mut().expect("resampler was just initialized")
            }
        };

        let mut interleaved = vec![0.0_f32; decoded.samples_interleaved()];
        decoded.copy_to_slice_interleaved(&mut interleaved);
        if interleaved.len() % channel_count != 0 {
            bail!("MP3 decoder returned an incomplete audio frame");
        }

        let mut stereo = Vec::with_capacity(interleaved.len() / channel_count);
        for frame in interleaved.chunks_exact(channel_count) {
            let left = frame[0];
            let right = if channel_count > 1 { frame[1] } else { left };
            stereo.push([left, right]);
        }

        let output = active_resampler.push(&stereo);
        if !output.is_empty() {
            let processed = processor.process(&output);
            write_pcm(&mut stdout, &processed).context("failed to write PCM to paplay")?;
            decoded_frames += output.len() as u64;
        }
    }

    if let Some(resampler) = resampler.as_mut() {
        let tail = resampler.finish();
        if !tail.is_empty() {
            let processed_tail = processor.process(&tail);
            write_pcm(&mut stdout, &processed_tail).context("failed to flush final PCM frames")?;
            decoded_frames += tail.len() as u64;
        }
    }
    stdout.flush().context("failed to flush PCM output")?;

    if decoded_frames == 0 {
        bail!("MP3 stream ended without producing audio samples");
    }
    Ok(decoded_frames)
}

fn run() -> Result<()> {
    let mut gain_db: f32 = 0.0;
    let mut fade_in_ms: u64 = 800;
    let mut target_url: Option<String> = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--version" || arg == "-V" {
            println!("capsula-audio {} (Symphonia MP3 -> PCM)", env!("CARGO_PKG_VERSION"));
            return Ok(());
        } else if arg == "--help" || arg == "-h" {
            eprintln!("Usage: capsula-audio [--gain <dB>] [--fade-in <ms>] <Yandex-MP3-URL>\nWrites 44100 Hz stereo s16le PCM to stdout.");
            return Ok(());
        } else if arg == "--gain" {
            let val = args.next().ok_or_else(|| anyhow!("missing value for --gain"))?;
            gain_db = val.parse::<f32>().context("invalid float for --gain")?;
        } else if arg == "--fade-in" {
            let val = args.next().ok_or_else(|| anyhow!("missing value for --fade-in"))?;
            fade_in_ms = val.parse::<u64>().context("invalid integer for --fade-in")?;
        } else if arg.starts_with("--") {
            bail!("unknown option: {arg}");
        } else if target_url.is_none() {
            target_url = Some(arg);
        } else {
            bail!("unexpected extra argument: {arg}");
        }
    }

    let url = target_url.ok_or_else(|| anyhow!("missing audio URL"))?;
    eprintln!("[capsula-audio] Rust MP3 decoder started; PCM=44100Hz/stereo/s16le gain={gain_db:+.2}dB fade_in={fade_in_ms}ms");
    let frames = decode_url(&url, gain_db, fade_in_ms)?;
    eprintln!("[capsula-audio] stream completed; decoded_frames={frames}");
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        // Never print the supplied URL: Yandex stream links can contain tokens.
        eprintln!("[capsula-audio] ERROR: {error:#}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::{write_pcm, StereoResampler, OUTPUT_RATE};

    #[test]
    fn same_rate_preserves_frame_count_and_order() {
        let input = vec![[0.25_f32, -0.25_f32]; 2048];
        let mut resampler = StereoResampler::new(OUTPUT_RATE).unwrap();
        let mut output = resampler.push(&input);
        output.extend(resampler.finish());
        assert_eq!(output.len(), input.len());
        assert!((output[0][0] - 0.25).abs() < 0.0001);
        assert!((output[0][1] + 0.25).abs() < 0.0001);
    }

    #[test]
    fn resamples_48khz_to_44100_without_packet_boundary_loss() {
        let input: Vec<[f32; 2]> = (0..48_000)
            .map(|index| {
                let sample = (index as f32 / 48_000.0 * std::f32::consts::TAU * 440.0).sin();
                [sample, -sample]
            })
            .collect();

        let mut single_packet = StereoResampler::new(48_000).unwrap();
        let mut expected = single_packet.push(&input);
        expected.extend(single_packet.finish());

        let mut chunked = StereoResampler::new(48_000).unwrap();
        let mut actual = Vec::new();
        for packet in input.chunks(1152) {
            actual.extend(chunked.push(packet));
        }
        actual.extend(chunked.finish());

        assert_eq!(actual.len(), OUTPUT_RATE as usize);
        assert_eq!(actual.len(), expected.len());
        for (actual_frame, expected_frame) in actual.iter().zip(expected.iter()) {
            assert!((actual_frame[0] - expected_frame[0]).abs() < 0.00001);
            assert!((actual_frame[1] - expected_frame[1]).abs() < 0.00001);
        }
    }

    #[test]
    fn pcm_is_signed_16_bit_little_endian_stereo() {
        let mut bytes = Vec::new();
        write_pcm(&mut bytes, &[[1.0, -1.0]]).unwrap();
        assert_eq!(bytes, vec![0xff, 0x7f, 0x00, 0x80]);
    }

    #[test]
    fn loudness_processor_gain_and_soft_limiting() {
        use super::LoudnessProcessor;
        let mut proc = LoudnessProcessor::new(6.0, 0); // +6dB is ~2.0x linear
        let frames = vec![[0.1, -0.1], [0.8, -0.8]];
        let processed = proc.process(&frames);
        // 0.1 * 2.0 = ~0.2 (under threshold 0.85, linear)
        assert!((processed[0][0] - 0.2).abs() < 0.01);
        assert!((processed[0][1] + 0.2).abs() < 0.01);
        // 0.8 * 2.0 = 1.6 (exceeds threshold, compressed below 1.0 by soft limiter)
        assert!(processed[1][0] < 1.0);
        assert!(processed[1][0] > 0.85);
        assert!(processed[1][1] > -1.0);
        assert!(processed[1][1] < -0.85);
    }

    #[test]
    fn loudness_processor_fade_in() {
        use super::LoudnessProcessor;
        let mut proc = LoudnessProcessor::new(0.0, 100); // 100ms fade-in
        let input = vec![[1.0, 1.0]; 4410]; // 100ms at 44.1kHz = 4410 frames
        let processed = proc.process(&input);
        // First frame should be practically zero
        assert!(processed[0][0] < 0.001);
        // Last frame should be near 1.0 (soft limited to ~0.95)
        assert!(processed[4409][0] > 0.90);
    }
}
