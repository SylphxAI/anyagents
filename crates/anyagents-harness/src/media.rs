//! Media an agent was given, and the form a model takes an image in.
//!
//! A channel that receives a file keeps the server's own copy directly inside
//! `<data>/media` and names it in the message body with one line,
//! [`local_media_line`]. Nothing more reaches the model with the message: the
//! agent decides whether to look, and an image it reads (`code`'s
//! `workspace.read`) is given to its model as [`model_image`] prepares it.

use std::io::Cursor;
use std::path::Path;

use image::{DynamicImage, ImageDecoder, ImageFormat, ImageReader, imageops::FilterType};

/// The directory inside the data directory that holds the server's copies.
pub const MEDIA_DIR: &str = "media";

/// The longest edge of an image a model is given. Every provider the fleet
/// reaches takes it: Anthropic refuses an edge over 8,000 pixels, and over
/// 2,000 once a request holds more than 20 images; OpenAI and Gemini scale
/// larger images down themselves.
pub const MAX_IMAGE_EDGE: u32 = 2_000;

/// The most bytes of one image a model is given. Anthropic refuses an image
/// whose base64 is over 5 MiB; this many bytes encode to under that.
pub const MAX_IMAGE_BYTES: usize = 3_900_000;

/// The line a message body names a received file with.
pub fn local_media_line(kind: &str, path: &Path) -> String {
    format!("[Local media ({kind}) file: {}]", path.display())
}

/// An image as a model is given it: a format every provider decodes, within
/// [`MAX_IMAGE_EDGE`] and [`MAX_IMAGE_BYTES`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelImage {
    /// `image/png`, `image/jpeg`, `image/gif` or `image/webp`.
    pub mime: &'static str,
    pub data: Vec<u8>,
    pub width: u32,
    pub height: u32,
    /// The original was re-encoded (scaled, or converted from a format a
    /// model does not take); otherwise `data` is the file's own bytes.
    pub converted: bool,
}

impl ModelImage {
    /// The bytes, base64 encoded (standard alphabet, padded).
    pub fn base64(&self) -> String {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD.encode(&self.data)
    }
}

/// Why bytes were not given to a model as an image.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MediaError {
    #[error("not an image a model takes")]
    NotAnImage,
    #[error("the image cannot be decoded: {0}")]
    Undecodable(String),
}

/// The image a model is given for `bytes`. A PNG, JPEG, GIF or WebP within
/// the limits that decodes whole is passed through untouched. Anything
/// larger is scaled to fit [`MAX_IMAGE_EDGE`], with its EXIF orientation
/// applied, and anything in another format a decoder here reads (BMP) is
/// converted: to PNG when it has transparency and fits, else to JPEG, whose
/// quality and then size step down until it fits [`MAX_IMAGE_BYTES`]. A body
/// that does not decode is refused, so no model is handed bytes its provider
/// rejects. The same bytes always give the same result.
pub fn model_image(bytes: &[u8]) -> Result<ModelImage, MediaError> {
    let format = image::guess_format(bytes).map_err(|_| MediaError::NotAnImage)?;
    let mime = match format {
        ImageFormat::Png => Some("image/png"),
        ImageFormat::Jpeg => Some("image/jpeg"),
        ImageFormat::Gif => Some("image/gif"),
        ImageFormat::WebP => Some("image/webp"),
        ImageFormat::Bmp => None,
        _ => return Err(MediaError::NotAnImage),
    };
    let reader = || {
        let mut reader = ImageReader::new(Cursor::new(bytes));
        reader.set_format(format);
        reader
    };
    let undecodable = |err: image::ImageError| MediaError::Undecodable(err.to_string());
    let (width, height) = reader().into_dimensions().map_err(undecodable)?;
    if let Some(mime) = mime
        && width.max(height) <= MAX_IMAGE_EDGE
        && bytes.len() <= MAX_IMAGE_BYTES
    {
        // Decoded whole all the same: a provider refuses a body that does not
        // decode, and the refusal fails the request that carries it.
        reader().decode().map_err(undecodable)?;
        return Ok(ModelImage {
            mime,
            data: bytes.to_vec(),
            width,
            height,
            converted: false,
        });
    }
    let mut decoder = reader().into_decoder().map_err(undecodable)?;
    let orientation = decoder.orientation().map_err(undecodable)?;
    let mut image = DynamicImage::from_decoder(decoder).map_err(undecodable)?;
    image.apply_orientation(orientation);
    let mut edge = MAX_IMAGE_EDGE.min(image.width().max(image.height())).max(1);
    loop {
        let scaled = fit(&image, edge);
        if scaled.color().has_alpha()
            && let Some(png) = encode_png(&scaled)
            && png.len() <= MAX_IMAGE_BYTES
        {
            return Ok(converted("image/png", png, &scaled));
        }
        for quality in [85u8, 70, 55, 40] {
            let jpeg = encode_jpeg(&scaled, quality).map_err(undecodable)?;
            if jpeg.len() <= MAX_IMAGE_BYTES {
                return Ok(converted("image/jpeg", jpeg, &scaled));
            }
        }
        if edge <= 64 {
            return Err(MediaError::Undecodable(
                "no encoding fits the size limit".into(),
            ));
        }
        edge = edge * 3 / 4;
    }
}

fn fit(image: &DynamicImage, edge: u32) -> DynamicImage {
    if image.width().max(image.height()) <= edge {
        image.clone()
    } else {
        image.resize(edge, edge, FilterType::Triangle)
    }
}

fn converted(mime: &'static str, data: Vec<u8>, image: &DynamicImage) -> ModelImage {
    ModelImage {
        mime,
        data,
        width: image.width(),
        height: image.height(),
        converted: true,
    }
}

fn encode_png(image: &DynamicImage) -> Option<Vec<u8>> {
    let mut out = Cursor::new(Vec::new());
    image.write_to(&mut out, ImageFormat::Png).ok()?;
    Some(out.into_inner())
}

fn encode_jpeg(image: &DynamicImage, quality: u8) -> image::ImageResult<Vec<u8>> {
    let mut out = Vec::new();
    // JPEG has no alpha: a transparent image is flattened onto white.
    let rgb = if image.color().has_alpha() {
        let rgba = image.to_rgba8();
        let mut flat = image::RgbImage::new(rgba.width(), rgba.height());
        for (to, from) in flat.pixels_mut().zip(rgba.pixels()) {
            let alpha = u16::from(from[3]);
            for (to, from) in to.0.iter_mut().zip(from.0) {
                let value = u16::from(from) * alpha + 255 * (255 - alpha);
                *to = (value / 255) as u8;
            }
        }
        flat
    } else {
        image.to_rgb8()
    };
    image::codecs::jpeg::JpegEncoder::new_with_quality(&mut out, quality).encode_image(&rgb)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn png(width: u32, height: u32) -> Vec<u8> {
        let image = image::RgbImage::from_fn(width, height, |x, y| {
            image::Rgb([(x % 256) as u8, (y % 256) as u8, ((x * y) % 256) as u8])
        });
        let mut out = Cursor::new(Vec::new());
        DynamicImage::ImageRgb8(image)
            .write_to(&mut out, ImageFormat::Png)
            .unwrap();
        out.into_inner()
    }

    #[test]
    fn a_received_file_is_named_by_one_line() {
        assert_eq!(
            local_media_line("photo", Path::new("/d/media/complete_photo_a.jpg")),
            "[Local media (photo) file: /d/media/complete_photo_a.jpg]"
        );
    }

    #[test]
    fn an_image_within_the_limits_is_passed_through() {
        let bytes = png(40, 30);
        let image = model_image(&bytes).unwrap();
        assert_eq!(image.mime, "image/png");
        assert_eq!((image.width, image.height), (40, 30));
        assert!(!image.converted);
        assert_eq!(image.data, bytes);
        assert!(image.base64().starts_with("iVBORw0KGgo"));
    }

    #[test]
    fn a_large_image_is_scaled_to_fit_and_the_same_bytes_give_the_same_result() {
        let bytes = png(MAX_IMAGE_EDGE * 2, 100);
        let first = model_image(&bytes).unwrap();
        assert!(first.converted);
        assert_eq!(first.mime, "image/jpeg");
        assert_eq!((first.width, first.height), (MAX_IMAGE_EDGE, 50));
        assert!(first.data.len() <= MAX_IMAGE_BYTES);
        assert_eq!(model_image(&bytes).unwrap(), first);
    }

    #[test]
    fn an_image_whose_body_does_not_decode_is_given_to_no_model() {
        let mut bytes = png(40, 30);
        bytes.truncate(bytes.len() / 2);
        assert!(matches!(
            model_image(&bytes),
            Err(MediaError::Undecodable(_))
        ));
    }

    #[test]
    fn a_format_a_model_does_not_take_is_converted_and_text_is_refused() {
        let mut bmp = Cursor::new(Vec::new());
        DynamicImage::ImageRgb8(image::RgbImage::new(8, 8))
            .write_to(&mut bmp, ImageFormat::Bmp)
            .unwrap();
        let image = model_image(bmp.get_ref()).unwrap();
        assert!(image.converted);
        assert_eq!(image.mime, "image/jpeg");
        assert_eq!(model_image(b"%PDF-1.7\n"), Err(MediaError::NotAnImage));
        assert_eq!(model_image(b"hello"), Err(MediaError::NotAnImage));
    }
}
