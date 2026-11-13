//! The embedder and reranker this build uses, and the ONNX Runtime library
//! that runs them, each pinned by source revision and SHA-256. Changing a
//! model changes its name, which empties the dense index for a rebuild.
use crate::model::{Head, ModelPin, PinnedFile, RuntimePin};

/// Embedders with a published ONNX export and a permissive licence, compared
/// on real history (docs/host-memory.md). The first is the default.
pub const EMBEDDERS: &[ModelPin] = &[
    ModelPin {
        name: "granite-embedding-97m-multilingual-r2-int8",
        repo: "ibm-granite/granite-embedding-97m-multilingual-r2",
        revision: "835ad14087e140460703cf0fae09f97d469d65c2",
        onnx: PinnedFile {
            path: "onnx/model_quint8_avx2.onnx",
            size: 98_247_878,
            sha256: "a6022dd8220ea6f6595562a1328ee216f4a94faa55362f2f4747c80f1e78772e",
        },
        tokenizer: PinnedFile {
            path: "tokenizer.json",
            size: 25_301_672,
            sha256: "4f2842d568e2724370aec203652a42ac783c7937f8347a1a2cc7506d71f1582f",
        },
        head: Head::Cls,
        max_tokens: 512,
        query_prefix: "",
        passage_prefix: "",
        dims: 384,
    },
    ModelPin {
        name: "granite-embedding-97m-multilingual-r2",
        repo: "ibm-granite/granite-embedding-97m-multilingual-r2",
        revision: "835ad14087e140460703cf0fae09f97d469d65c2",
        onnx: PinnedFile {
            path: "onnx/model.onnx",
            size: 390_004_608,
            sha256: "68e592b160673d30250824c1116bc6ab33f70efb22b97c9e1d7ce1e69c1c9d70",
        },
        tokenizer: PinnedFile {
            path: "tokenizer.json",
            size: 25_301_672,
            sha256: "4f2842d568e2724370aec203652a42ac783c7937f8347a1a2cc7506d71f1582f",
        },
        head: Head::Cls,
        max_tokens: 512,
        query_prefix: "",
        passage_prefix: "",
        dims: 384,
    },
    ModelPin {
        name: "granite-embedding-311m-multilingual-r2-int8",
        repo: "ibm-granite/granite-embedding-311m-multilingual-r2",
        revision: "44399559930365213510b1ee2eb15ded83374f0e",
        onnx: PinnedFile {
            path: "onnx/model_quint8_avx2.onnx",
            size: 313_421_909,
            sha256: "f1fdd44e7e1ac51f12ab7957c7bd092e064d596c288513bf9d326842f669edee",
        },
        tokenizer: PinnedFile {
            path: "tokenizer.json",
            size: 33_384_821,
            sha256: "0087c868b33bad550a78a08d19798cfd7f713cde4f020803b8f51f405503e15f",
        },
        head: Head::Cls,
        max_tokens: 512,
        query_prefix: "",
        passage_prefix: "",
        dims: 768,
    },
    ModelPin {
        name: "granite-embedding-107m-multilingual",
        repo: "ibm-granite/granite-embedding-107m-multilingual",
        revision: "d6cffd338414d6a1c1f5decfad5fec62eebc90d5",
        onnx: PinnedFile {
            path: "model.onnx",
            size: 428_102_968,
            sha256: "387e6f91dce3c651cb3dc06661d74fb1d3947f20ed9c3ecf49d2b3b77086b954",
        },
        tokenizer: PinnedFile {
            path: "tokenizer.json",
            size: 9_081_382,
            sha256: "7fc9e475f2ac473f070052d92b0c46ec2b91d9cd672935feb6fd837a65d33f78",
        },
        head: Head::Cls,
        max_tokens: 512,
        query_prefix: "",
        passage_prefix: "",
        dims: 384,
    },
    ModelPin {
        name: "multilingual-e5-small-int8",
        repo: "intfloat/multilingual-e5-small",
        revision: "614241f622f53c4eeff9890bdc4f31cfecc418b3",
        onnx: PinnedFile {
            path: "onnx/model_qint8_avx512_vnni.onnx",
            size: 118_346_824,
            sha256: "dd476dd0c2514e9b9be83aeb3853fac0763e0bdf4a71645407587d77c48a2d88",
        },
        tokenizer: PinnedFile {
            path: "tokenizer.json",
            size: 17_082_730,
            sha256: "0b44a9d7b51c3c62626640cda0e2c2f70fdacdc25bbbd68038369d14ebdf4c39",
        },
        head: Head::Mean,
        max_tokens: 512,
        query_prefix: "query: ",
        passage_prefix: "passage: ",
        dims: 384,
    },
];

/// Cross-encoders, compared the same way. The first is the default.
pub const RERANKERS: &[ModelPin] = &[
    ModelPin {
        name: "hs-rerank-1",
        repo: "hindsight-ai/hs-rerank-1",
        revision: "927f5be31280298a51cf29af8a29cb111ed812c5",
        onnx: PinnedFile {
            path: "onnx/model.onnx",
            size: 160_582_339,
            sha256: "c866580692c6bf274b11ca622db4329f4cbfd6f08f96af964badc31d748ef243",
        },
        tokenizer: PinnedFile {
            path: "tokenizer.json",
            size: 34_363_188,
            sha256: "609d8f4c067cd3950f88594c5a802616cea245823836ef5848ee4fc40aab5b6f",
        },
        head: Head::Logit,
        max_tokens: 512,
        query_prefix: "",
        passage_prefix: "",
        dims: 1,
    },
    ModelPin {
        name: "multilingual-reranker-small-int8",
        repo: "Horizon-Labs/multilingual-reranker-small",
        revision: "23139fec6f1f448cdf89a5ac2d2c6bf5e8c7b398",
        onnx: PinnedFile {
            path: "onnx/model_quantized.onnx",
            size: 268_407_694,
            sha256: "f913154682468bfe92d6a7e02ed4e994e548c112fddc74452b573b17c949046e",
        },
        tokenizer: PinnedFile {
            path: "tokenizer.json",
            size: 34_363_439,
            sha256: "7925f638b5444897efe3b4608eed56c1652f0ed39c2662c8302de1bc264c6088",
        },
        head: Head::Logit,
        max_tokens: 384,
        query_prefix: "",
        passage_prefix: "",
        dims: 1,
    },
    ModelPin {
        name: "mmarco-mminilmv2-l12-h384-int8",
        repo: "cross-encoder/mmarco-mMiniLMv2-L12-H384-v1",
        revision: "1427fd652930e4ba29e8149678df786c240d8825",
        onnx: PinnedFile {
            path: "onnx/model_qint8_avx512_vnni.onnx",
            size: 118_620_017,
            sha256: "1825907d6c1a9001ff78124780bbde20a614a8c3df3b63409cf3c72c6fe5c8b4",
        },
        tokenizer: PinnedFile {
            path: "tokenizer.json",
            size: 17_082_660,
            sha256: "62c24cdc13d4c9952d63718d6c9fa4c287974249e16b7ade6d5a85e7bbb75626",
        },
        head: Head::Logit,
        max_tokens: 512,
        query_prefix: "",
        passage_prefix: "",
        dims: 1,
    },
    ModelPin {
        name: "bge-reranker-v2-m3-int8",
        repo: "onnx-community/bge-reranker-v2-m3-ONNX",
        revision: "6f5ff65298512715a1e669753bc754d2bc8f367b",
        onnx: PinnedFile {
            path: "onnx/model_int8.onnx",
            size: 570_727_094,
            sha256: "912fc1215c2dbff6499700534bd8d31253af01573861abbfc43afd1fab6cce5d",
        },
        tokenizer: PinnedFile {
            path: "tokenizer.json",
            size: 17_082_900,
            sha256: "8bf8afbfd11306bd872018c53bfdf2e160a56f8edbcf49933324404791c148d3",
        },
        head: Head::Logit,
        max_tokens: 512,
        query_prefix: "",
        passage_prefix: "",
        dims: 1,
    },
];

pub const EMBEDDER: ModelPin = EMBEDDERS[0];
pub const RERANKER: ModelPin = RERANKERS[0];

/// A pinned model by name, from either table.
pub fn by_name(name: &str) -> Option<ModelPin> {
    EMBEDDERS
        .iter()
        .chain(RERANKERS)
        .find(|p| p.name == name)
        .copied()
}

/// ONNX Runtime 1.31.0, the version `ort` 2.0.0-rc.15 is built against,
/// from Microsoft's official release assets: `(os, arch, pin)`.
pub const RUNTIMES: &[(&str, &str, RuntimePin)] = &[
    (
        "linux",
        "x86_64",
        RuntimePin {
            version: "1.31.0",
            url: "https://github.com/microsoft/onnxruntime/releases/download/v1.31.0/onnxruntime-linux-x64-1.31.0.tgz",
            sha256: "cc5c72baf5ae5c8238a6841f0897227be2d02826b9cf98eaf02fdefaeeb03a57",
            library: "onnxruntime-linux-x64-1.31.0/lib/libonnxruntime.so.1.31.0",
        },
    ),
    (
        "linux",
        "aarch64",
        RuntimePin {
            version: "1.31.0",
            url: "https://github.com/microsoft/onnxruntime/releases/download/v1.31.0/onnxruntime-linux-aarch64-1.31.0.tgz",
            sha256: "c5b8b3cca31f3d643a3b313b8f42e2d0f16a4fcefd02cda343d97bd5306afaab",
            library: "onnxruntime-linux-aarch64-1.31.0/lib/libonnxruntime.so.1.31.0",
        },
    ),
    (
        "macos",
        "aarch64",
        RuntimePin {
            version: "1.31.0",
            url: "https://github.com/microsoft/onnxruntime/releases/download/v1.31.0/onnxruntime-osx-arm64-1.31.0.tgz",
            sha256: "1031b36dd3bdaa8d976a6130d9cf2a05e9be320b93ae804f8427b91dc3432afc",
            library: "onnxruntime-osx-arm64-1.31.0/lib/libonnxruntime.1.31.0.dylib",
        },
    ),
];
