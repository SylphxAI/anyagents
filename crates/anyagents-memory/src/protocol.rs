//! The memory service's whole interface: newline-delimited JSON requests and
//! responses over a Unix socket owned by the operator. The kernel sends the
//! owners it has already authorized; the service never decides access and
//! never returns raw text, only coordinates the kernel verifies before it
//! reads a byte.
use serde::{Deserialize, Serialize};
use std::{
    io::{BufRead, BufReader, Write},
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    time::Duration,
};

/// Wire version. A service answering another version is refused.
pub const VERSION: u32 = 1;

/// The service socket inside a data directory.
pub fn socket_path(data_dir: &Path) -> PathBuf {
    data_dir.join("memory-service.sock")
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    /// Dense candidates for `query` among the documents of `owners`.
    Search {
        query: String,
        /// Authorized agent ids; an empty list selects nothing.
        owners: Vec<String>,
        /// `memory`, `transcripts`, or `None` for both.
        #[serde(default)]
        source: Option<String>,
        #[serde(default)]
        since_ms: Option<i64>,
        #[serde(default)]
        until_ms: Option<i64>,
        limit: usize,
    },
    /// Cross-encoder relevance of each passage to `query`, in input order.
    Rerank {
        query: String,
        passages: Vec<String>,
    },
    Status,
}

/// One dense candidate: an indexed document and the exact raw byte range of
/// the piece that matched, as the kernel's host index knows them.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DenseHit {
    pub document_id: String,
    pub agent_id: String,
    /// Absolute byte range in the document's segment, with its SHA-256.
    pub offset: u64,
    pub length: u64,
    pub sha256: String,
    pub score: f32,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Status {
    pub embedder: String,
    pub reranker: String,
    /// Documents embedded, and documents the host index holds that are not.
    pub embedded_documents: u64,
    pub pending_documents: u64,
    pub vectors: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Reply {
    Search { hits: Vec<DenseHit>, status: Status },
    Rerank { scores: Vec<f32> },
    Status { status: Status },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Envelope<T> {
    v: u32,
    #[serde(flatten)]
    body: T,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum Outcome {
    Ok { ok: bool, reply: Reply },
    Err { ok: bool, error: String },
}

/// Encode one request line.
pub fn encode_request(request: &Request) -> serde_json::Result<String> {
    let mut line = serde_json::to_string(&Envelope {
        v: VERSION,
        body: request,
    })?;
    line.push('\n');
    Ok(line)
}

/// Decode one request line; a wrong version is an error.
pub fn decode_request(line: &str) -> Result<Request, String> {
    let envelope: Envelope<Request> = serde_json::from_str(line).map_err(|e| e.to_string())?;
    if envelope.v != VERSION {
        return Err(format!(
            "memory service speaks v{VERSION}, not v{}",
            envelope.v
        ));
    }
    Ok(envelope.body)
}

pub fn encode_reply(result: &Result<Reply, String>) -> String {
    let outcome = match result {
        Ok(reply) => Outcome::Ok {
            ok: true,
            reply: reply.clone(),
        },
        Err(error) => Outcome::Err {
            ok: false,
            error: error.clone(),
        },
    };
    let mut line = serde_json::to_string(&Envelope {
        v: VERSION,
        body: outcome,
    })
    .unwrap_or_else(|e| format!(r#"{{"v":{VERSION},"ok":false,"error":"{e}"}}"#));
    line.push('\n');
    line
}

fn decode_reply(line: &str) -> Result<Reply, String> {
    let envelope: Envelope<Outcome> =
        serde_json::from_str(line).map_err(|e| format!("bad memory service reply: {e}"))?;
    if envelope.v != VERSION {
        return Err(format!("memory service answered v{}", envelope.v));
    }
    match envelope.body {
        Outcome::Ok { ok: true, reply } => Ok(reply),
        Outcome::Err { error, .. } => Err(error),
        Outcome::Ok { .. } => Err("memory service reply without a result".into()),
    }
}

/// A connection to a running memory service. Each call is one request and
/// one reply within `deadline`; a slow or absent service is reported to the
/// caller, which answers from lexical retrieval and says so.
pub struct Client {
    socket: PathBuf,
    deadline: Duration,
}

impl Client {
    pub fn new(socket: PathBuf, deadline: Duration) -> Self {
        Self { socket, deadline }
    }

    /// A client for the service of `data_dir`, or `None` when no service has
    /// a socket there.
    pub fn for_data_dir(data_dir: &Path, deadline: Duration) -> Option<Self> {
        let socket = socket_path(data_dir);
        socket.exists().then(|| Self::new(socket, deadline))
    }

    pub fn call(&self, request: &Request) -> Result<Reply, String> {
        let stream = UnixStream::connect(&self.socket)
            .map_err(|e| format!("memory service unreachable: {e}"))?;
        stream
            .set_read_timeout(Some(self.deadline))
            .and_then(|()| stream.set_write_timeout(Some(self.deadline)))
            .map_err(|e| e.to_string())?;
        let line = encode_request(request).map_err(|e| e.to_string())?;
        (&stream)
            .write_all(line.as_bytes())
            .map_err(|e| format!("memory service write: {e}"))?;
        let mut reply = String::new();
        BufReader::new(&stream).read_line(&mut reply).map_err(|e| {
            format!(
                "memory service did not answer within {:?}: {e}",
                self.deadline
            )
        })?;
        decode_reply(&reply)
    }

    pub fn search(
        &self,
        query: &str,
        owners: &[String],
        source: Option<&str>,
        since_ms: Option<i64>,
        until_ms: Option<i64>,
        limit: usize,
    ) -> Result<(Vec<DenseHit>, Status), String> {
        match self.call(&Request::Search {
            query: query.into(),
            owners: owners.to_vec(),
            source: source.map(str::to_owned),
            since_ms,
            until_ms,
            limit,
        })? {
            Reply::Search { hits, status } => Ok((hits, status)),
            _ => Err("memory service answered another request".into()),
        }
    }

    pub fn rerank(&self, query: &str, passages: &[String]) -> Result<Vec<f32>, String> {
        match self.call(&Request::Rerank {
            query: query.into(),
            passages: passages.to_vec(),
        })? {
            Reply::Rerank { scores } if scores.len() == passages.len() => Ok(scores),
            Reply::Rerank { .. } => Err("memory service scored another number of passages".into()),
            _ => Err("memory service answered another request".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_and_replies_round_trip_and_versions_are_checked() {
        let request = Request::Search {
            query: "點解要再註冊".into(),
            owners: vec!["agt_1".into()],
            source: Some("transcripts".into()),
            since_ms: None,
            until_ms: Some(5),
            limit: 10,
        };
        let line = encode_request(&request).unwrap();
        assert!(
            line.ends_with('\n') && line.contains(r#""v":1"#) && line.contains(r#""op":"search""#)
        );
        assert_eq!(decode_request(line.trim()).unwrap(), request);
        assert!(decode_request(&line.replace(r#""v":1"#, r#""v":2"#)).is_err());

        let reply = Reply::Rerank {
            scores: vec![0.5, -1.0],
        };
        assert_eq!(
            decode_reply(&encode_reply(&Ok(reply.clone()))).unwrap(),
            reply
        );
        let error = decode_reply(&encode_reply(&Err("model missing".into())));
        assert_eq!(error, Err("model missing".into()));
        let status = Reply::Search {
            hits: vec![DenseHit {
                document_id: "transcript:a:b:1".into(),
                agent_id: "agt_1".into(),
                offset: 10,
                length: 20,
                sha256: "ab".into(),
                score: 0.9,
            }],
            status: Status::default(),
        };
        assert_eq!(
            decode_reply(&encode_reply(&Ok(status.clone()))).unwrap(),
            status
        );
    }

    #[test]
    fn an_absent_service_is_an_error_not_a_wait() {
        let dir = tempfile::tempdir().unwrap();
        assert!(Client::for_data_dir(dir.path(), Duration::from_secs(1)).is_none());
        let client = Client::new(dir.path().join("missing.sock"), Duration::from_secs(1));
        let started = std::time::Instant::now();
        assert!(client.rerank("q", &["p".into()]).is_err());
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
