//! Optional in-process CPU reranker. Missing/corrupt pinned assets are a visible
//! lexical degradation, never a remote model call or a different model fallback.
//!
//! Primary specifications read 2026-11-07: Candle 0.9.1 BERT implementation;
//! intfloat/multilingual-e5-small at REVISION (MIT), README pooling/prefix recipe.
use candle_core::{DType, Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::bert::{BertModel, Config};
use sha2::{Digest, Sha256};
use std::{
    io::{self, Read},
    path::Path,
};
use tokenizers::{PaddingDirection, PaddingParams, PaddingStrategy, Tokenizer, TruncationParams};

pub const REVISION: &str = "614241f622f53c4eeff9890bdc4f31cfecc418b3";
pub const DIMENSIONS: usize = 384;
/// Bump for any prefix, truncation, padding, mask or pooling change.
pub const PREPROCESSING: &str = "e5-prefix-right512-maskmean-specials-l2-f32-v1";
pub const ASSETS: [(&str, u64, &str); 3] = [
    (
        "config.json",
        655,
        "69137736cab8b8903a07fe8afaafdda25aac55415a12a55d1bffa9f581abf959",
    ),
    (
        "tokenizer.json",
        17082730,
        "0b44a9d7b51c3c62626640cda0e2c2f70fdacdc25bbbd68038369d14ebdf4c39",
    ),
    (
        "model.safetensors",
        470641600,
        "1a55775f53449dac10a2bcbc312469fac40b96d53198c407081a831f81c98477",
    ),
];

#[derive(Debug, thiserror::Error)]
pub enum DenseError {
    #[error("dense assets unavailable or invalid: {0}")]
    Assets(String),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Candle(#[from] candle_core::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("dense tokenizer: {0}")]
    Tokenizer(String),
}

pub struct E5Model {
    model: BertModel,
    tokenizer: Tokenizer,
}
impl E5Model {
    /// Only host-provisioned, digest-pinned assets; no subprocess or network.
    /// Descriptor-safe reads reject symlink ancestors, symlink leaves and hardlinks.
    pub fn load(directory: &Path) -> Result<Self, DenseError> {
        let mut assets = Vec::new();
        for (name, length, expected) in ASSETS {
            let mut file = crate::raw_segment::open_readonly(&directory.join(name))?;
            if file.metadata()?.len() != length {
                return Err(DenseError::Assets(format!("{name}: length mismatch")));
            }
            let mut bytes = Vec::with_capacity(length as usize);
            file.read_to_end(&mut bytes)?;
            if format!("{:x}", Sha256::digest(&bytes)) != expected {
                return Err(DenseError::Assets(format!("{name}: SHA-256 mismatch")));
            }
            assets.push(bytes);
        }
        let config: Config = serde_json::from_slice(&assets[0])?;
        let mut tokenizer =
            Tokenizer::from_bytes(&assets[1]).map_err(|e| DenseError::Tokenizer(e.to_string()))?;
        let pad_id = tokenizer
            .token_to_id("<pad>")
            .ok_or_else(|| DenseError::Assets("tokenizer has no <pad> token".into()))?;
        tokenizer
            .with_truncation(Some(TruncationParams {
                max_length: 512,
                ..Default::default()
            }))
            .map_err(|e| DenseError::Tokenizer(e.to_string()))?;
        tokenizer.with_padding(Some(PaddingParams {
            strategy: PaddingStrategy::BatchLongest,
            direction: PaddingDirection::Right,
            pad_id,
            pad_type_id: 0,
            pad_token: "<pad>".into(),
            pad_to_multiple_of: None,
        }));
        let weights = assets.pop().expect("three pinned assets");
        let vb = VarBuilder::from_buffered_safetensors(weights, DType::F32, &Device::Cpu)?;
        let model = BertModel::load(vb, &config)?;
        Ok(Self { model, tokenizer })
    }

    /// Small batches bound attention memory even for 512-token candidates.
    /// Special tokens participate in pooling, matching the primary recipe.
    pub fn embed(&self, texts: &[&str], query: bool) -> Result<Vec<Vec<f32>>, DenseError> {
        let mut result = Vec::with_capacity(texts.len());
        for chunk in texts.chunks(4) {
            let prefixed: Vec<String> = chunk
                .iter()
                .map(|s| format!("{}: {s}", if query { "query" } else { "passage" }))
                .collect();
            let encoded = self
                .tokenizer
                .encode_batch(prefixed, true)
                .map_err(|e| DenseError::Tokenizer(e.to_string()))?;
            let ids: Vec<_> = encoded
                .iter()
                .map(|e| Tensor::new(e.get_ids(), &Device::Cpu))
                .collect::<Result<_, _>>()?;
            let masks: Vec<_> = encoded
                .iter()
                .map(|e| Tensor::new(e.get_attention_mask(), &Device::Cpu))
                .collect::<Result<_, _>>()?;
            let ids = Tensor::stack(&ids, 0)?;
            let mask = Tensor::stack(&masks, 0)?;
            let types = ids.zeros_like()?;
            let hidden = self.model.forward(&ids, &types, Some(&mask))?;
            let float_mask = mask.to_dtype(DType::F32)?;
            let sums = hidden.broadcast_mul(&float_mask.unsqueeze(2)?)?.sum(1)?;
            let means = sums.broadcast_div(&float_mask.sum(1)?.unsqueeze(1)?)?;
            for mut vector in means.to_vec2::<f32>()? {
                normalize(&mut vector)?;
                result.push(vector);
            }
        }
        Ok(result)
    }
}

fn normalize(vector: &mut [f32]) -> Result<(), DenseError> {
    let norm = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
    if vector.len() != DIMENSIONS || !norm.is_finite() || norm <= 0.0 {
        return Err(DenseError::Assets(
            "nonfinite/zero/wrong-dimension embedding".into(),
        ));
    }
    for value in vector {
        *value /= norm;
    }
    Ok(())
}
pub fn cosine(a: &[f32], b: &[f32]) -> Option<f32> {
    if a.len() != DIMENSIONS || b.len() != DIMENSIONS {
        return None;
    }
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    dot.is_finite().then_some(dot)
}
pub fn cache_key(content_sha: &str, policy: &str) -> String {
    let identity = format!(
        "{REVISION}:{}:{PREPROCESSING}:{content_sha}:{policy}",
        ASSETS[2].2
    );
    format!("{:x}", Sha256::digest(identity.as_bytes()))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn keys_include_policy_and_content_and_invalid_vectors_refuse() {
        assert_ne!(cache_key("same", "a"), cache_key("same", "b"));
        assert_ne!(cache_key("old", "a"), cache_key("new", "a"));
        assert!(cosine(&[1.0], &[1.0]).is_none());
        assert!(normalize(&mut vec![f32::NAN; DIMENSIONS]).is_err());
        let mut v = vec![2.0; DIMENSIONS];
        normalize(&mut v).unwrap();
        assert!((cosine(&v, &v).unwrap() - 1.0).abs() < 0.00001);
    }
    #[test]
    fn missing_assets_are_explicit_not_a_fake_embedding() {
        let directory = tempfile::tempdir().unwrap();
        assert!(matches!(
            E5Model::load(directory.path()),
            Err(DenseError::Io(_))
        ));
    }
}
