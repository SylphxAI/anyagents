//! Local CPU inference: an ONNX encoder and its tokenizer, as an embedder
//! (text to an L2-normalised vector) or a cross-encoder (query and passage to
//! a relevance score). ONNX Runtime is loaded from the pinned, verified shared
//! library; no model or runtime is fetched here.
use crate::model::{Head, ModelPin, RuntimePin};
use ort::{environment::Environment, session::Session, value::Tensor};
use std::{path::Path, sync::OnceLock};
use tokenizers::{
    EncodeInput, Tokenizer, TruncationDirection, TruncationParams, TruncationStrategy,
};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

static ENVIRONMENT: OnceLock<Result<Environment, String>> = OnceLock::new();

/// Load the pinned ONNX Runtime library once per process and keep its
/// environment for every session.
pub fn init(models: &Path, runtime: &RuntimePin) -> Result<&'static Environment, Error> {
    let library = runtime.library_path(models);
    ENVIRONMENT
        .get_or_init(|| {
            ort::init_from(&library)
                .map_err(|e| e.to_string())?
                .with_name("anyagents-memory")
                .build()
                .map_err(|e| e.to_string())
        })
        .as_ref()
        .map_err(|error| format!("ONNX Runtime {}: {error}", library.display()).into())
}

/// One loaded encoder. `threads` bounds the intra-op pool; with one thread
/// inference runs on the calling thread, at that thread's priority.
pub struct Encoder {
    pin: ModelPin,
    session: Session,
    tokenizer: Tokenizer,
    /// The same tokenizer without truncation, to measure and cut text.
    counter: Tokenizer,
    wants_token_types: bool,
}

impl Encoder {
    pub fn load(
        environment: &Environment,
        models: &Path,
        pin: &ModelPin,
        threads: usize,
    ) -> Result<Self, Error> {
        let failed = |e: ort::Error<ort::session::builder::SessionBuilder>| e.to_string();
        let session = Session::builder(environment)?
            .with_intra_threads(threads.max(1))
            .map_err(failed)?
            .with_inter_threads(1)
            .map_err(failed)?
            .commit_from_file(pin.onnx_path(models))?;
        let wants_token_types = session
            .inputs()
            .iter()
            .any(|i| i.name() == "token_type_ids");
        let mut counter = Tokenizer::from_file(pin.tokenizer_path(models))?;
        counter.with_padding(None);
        counter.with_truncation(None).map_err(|e| e.to_string())?;
        let mut tokenizer = counter.clone();
        // A pair is cut from its longer side: the passage, not the query.
        tokenizer
            .with_truncation(Some(TruncationParams {
                max_length: pin.max_tokens,
                strategy: TruncationStrategy::LongestFirst,
                direction: TruncationDirection::Right,
                stride: 0,
            }))
            .map_err(|e| e.to_string())?;
        Ok(Self {
            pin: *pin,
            session,
            tokenizer,
            counter,
            wants_token_types,
        })
    }

    pub fn pin(&self) -> &ModelPin {
        &self.pin
    }

    /// The model's tokenizer without truncation.
    pub fn counter(&self) -> &Tokenizer {
        &self.counter
    }

    /// Run one batch of already-built inputs; returns one row per input: a
    /// pooled vector for an embedder, one logit for a cross-encoder.
    fn run(&mut self, inputs: Vec<EncodeInput<'_>>) -> Result<Vec<Vec<f32>>, Error> {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }
        let encodings = self
            .tokenizer
            .encode_batch(inputs, true)
            .map_err(|e| e.to_string())?;
        let batch = encodings.len();
        let width = encodings.iter().map(|e| e.len()).max().unwrap_or(0).max(1);
        let pad = self
            .tokenizer
            .get_padding()
            .map(|p| p.pad_id)
            .or_else(|| self.tokenizer.token_to_id("<pad>"))
            .unwrap_or(0) as i64;
        let mut ids = vec![pad; batch * width];
        let mut mask = vec![0i64; batch * width];
        let mut types = vec![0i64; batch * width];
        for (row, encoding) in encodings.iter().enumerate() {
            for (col, (&id, &ty)) in encoding
                .get_ids()
                .iter()
                .zip(encoding.get_type_ids())
                .enumerate()
            {
                ids[row * width + col] = id as i64;
                mask[row * width + col] = 1;
                types[row * width + col] = ty as i64;
            }
        }
        let shape = [batch, width];
        let mut feeds: Vec<(
            std::borrow::Cow<'static, str>,
            ort::session::SessionInputValue<'_>,
        )> = vec![
            ("input_ids".into(), Tensor::from_array((shape, ids))?.into()),
            (
                "attention_mask".into(),
                Tensor::from_array((shape, mask.clone()))?.into(),
            ),
        ];
        if self.wants_token_types {
            feeds.push((
                "token_type_ids".into(),
                Tensor::from_array((shape, types))?.into(),
            ));
        }
        let outputs = self.session.run(feeds)?;
        let (out_shape, data) = outputs[0].try_extract_tensor::<f32>()?;
        let dims: Vec<usize> = out_shape.iter().map(|&d| d as usize).collect();
        let mut rows = Vec::with_capacity(batch);
        match (self.pin.head, dims.as_slice()) {
            (Head::Logit, [b, 1]) | (Head::Logit, [b]) if *b == batch => {
                rows.extend(data.iter().take(batch).map(|&logit| vec![logit]));
            }
            (Head::Cls | Head::Mean, [b, h]) if *b == batch => {
                for row in 0..batch {
                    rows.push(normalize(data[row * h..(row + 1) * h].to_vec()));
                }
            }
            (Head::Cls, [b, s, h]) if *b == batch => {
                for row in 0..batch {
                    let start = row * s * h;
                    rows.push(normalize(data[start..start + h].to_vec()));
                }
            }
            (Head::Mean, [b, s, h]) if *b == batch => {
                for row in 0..batch {
                    let mut sum = vec![0f32; *h];
                    let mut count = 0f32;
                    for t in 0..*s {
                        if mask[row * width + t] == 0 {
                            continue;
                        }
                        count += 1.0;
                        let at = (row * s + t) * h;
                        for (acc, x) in sum.iter_mut().zip(&data[at..at + h]) {
                            *acc += x;
                        }
                    }
                    rows.push(normalize(
                        sum.into_iter().map(|x| x / count.max(1.0)).collect(),
                    ));
                }
            }
            (head, other) => {
                return Err(format!(
                    "{}: output shape {other:?} does not fit {head:?}",
                    self.pin.name
                )
                .into());
            }
        }
        Ok(rows)
    }

    /// Embed queries, each read with the model's query prefix.
    pub fn embed_queries(&mut self, queries: &[&str]) -> Result<Vec<Vec<f32>>, Error> {
        let prefix = self.pin.query_prefix;
        self.run(
            queries
                .iter()
                .map(|q| EncodeInput::Single(format!("{prefix}{q}").into()))
                .collect(),
        )
    }

    /// Embed passages, each read with the model's passage prefix.
    pub fn embed_passages(&mut self, passages: &[&str]) -> Result<Vec<Vec<f32>>, Error> {
        let prefix = self.pin.passage_prefix;
        self.run(
            passages
                .iter()
                .map(|p| EncodeInput::Single(format!("{prefix}{p}").into()))
                .collect(),
        )
    }

    /// Cross-encoder scores of each passage for one query; the passage is
    /// truncated, never the query.
    pub fn score(&mut self, query: &str, passages: &[&str]) -> Result<Vec<f32>, Error> {
        Ok(self
            .run(
                passages
                    .iter()
                    .map(|p| EncodeInput::Dual(query.to_owned().into(), (*p).to_owned().into()))
                    .collect(),
            )?
            .into_iter()
            .map(|row| row[0])
            .collect())
    }
}

fn normalize(mut v: Vec<f32>) -> Vec<f32> {
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in &mut v {
            *x /= norm;
        }
    }
    v
}
