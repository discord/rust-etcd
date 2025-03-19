//! Contains the etcd client. All API calls are made via the client.

use std::{future::Future, sync::Arc, time::Duration};

use base64::{prelude::BASE64_STANDARD, Engine};
use http::{
    header::{HeaderMap, HeaderValue},
    StatusCode, Uri,
};
use rand::{prelude::SliceRandom, rng};
use reqwest::{Certificate, Identity, IntoUrl};
use serde::de::DeserializeOwned;
use serde_derive::{Deserialize, Serialize};
use serde_json;
use tracing::error;

use crate::{
    error::{ApiError, Error},
    VersionInfo,
};

const XETCD_CLUSTER_ID: &str = "X-Etcd-Cluster-Id";
const XETCD_INDEX: &str = "X-Etcd-Index";
const XRAFT_INDEX: &str = "X-Raft-Index";
const XRAFT_TERM: &str = "X-Raft-Term";

/// API client for etcd.
///
/// All API calls require a client.
#[derive(Clone, Debug)]
pub struct Client {
    endpoints: Arc<Vec<Uri>>,
    http_client: reqwest::Client,
}

/// A username and password to use for HTTP basic authentication.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct BasicAuth {
    /// The username to use for authentication.
    username: String,
    /// The password to use for authentication.
    password: String,
}

/// A value returned by the health check API endpoint to indicate a healthy cluster member.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Health {
    /// The health status of the cluster member.
    pub health: String,
}

/// A client builder is used to configure and create a client.
///
/// Use with [`ClientBuilder::new`], however if you don't require advanced configuration,
/// a client can be created with [`Client::new`].
#[derive(Debug)]
pub struct ClientBuilder {
    endpoints: Vec<Uri>,
    basic_auth: Option<BasicAuth>,
    tcp_keepalive: Option<Duration>,
    request_timeout: Option<Duration>,
    connect_timeout: Duration,
    #[cfg(feature = "tls")]
    tls_client_identity: Option<Identity>,
    #[cfg(feature = "tls")]
    tls_root_certificates: Vec<Certificate>,
}

impl ClientBuilder {
    /// Creates a new client builder that can be used to configure and customize the etcd client.
    /// # Errors
    ///
    /// Panics if no endpoints are provided or if any of the endpoints is an invalid URL.
    pub fn new(endpoints: &[&str]) -> Self {
        if endpoints.is_empty() {
            panic!("invariant: no endpoints provided")
        }

        let endpoints = endpoints
            .into_iter()
            .map(|e| {
                e.parse()
                    .expect(&format!("invariant: could not parse endpoint: {}", e))
            })
            .collect();

        Self {
            endpoints,
            basic_auth: None,
            connect_timeout: Duration::from_secs(90),
            tcp_keepalive: None,
            request_timeout: None,
            #[cfg(feature = "tls")]
            tls_client_identity: None,
            #[cfg(feature = "tls")]
            tls_root_certificates: Vec::new(),
        }
    }

    /// Configures the client to use basic auth, with the given username and password.
    pub fn with_basic_auth(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.basic_auth = Some(BasicAuth {
            username: username.into(),
            password: password.into(),
        });
        self
    }

    /// Configures the client to use a specific connect timeout.
    ///
    /// The default is 90 seconds.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Configures the underlying http client to use `SO_KEEPALIVE` with the
    /// supplied duration.
    pub fn with_tcp_keepalive(mut self, timeout: Duration) -> Self {
        self.tcp_keepalive = Some(timeout);
        self
    }

    /// Configures the client to have a request timeout (doesn't affect kv::watch calls).
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    #[cfg(feature = "tls")]
    /// Uses a specific client certificate ([`Identity`]) for TLS connections to etcd.
    pub fn with_client_identity(mut self, identity: Identity) -> Self {
        self.tls_client_identity = Some(identity);
        self
    }

    #[cfg(feature = "tls")]
    /// Adds a specific root certificate that will be accepted by the client.
    ///
    /// Useful if your etcd server is using TLS with self-signed certificates.
    ///
    /// NOTE: Calling this function multiple times adds each certificate to the list of
    /// allowed root certificate authorities.
    pub fn with_root_certificate(mut self, certificate: Certificate) -> Self {
        self.tls_root_certificates.push(certificate);
        self
    }

    /// Constructs a client from the builder.
    pub fn build(self) -> Client {
        let client_builder = reqwest::ClientBuilder::new();
        let client_builder = client_builder.connect_timeout(self.connect_timeout);
        let client_builder = match self.basic_auth {
            Some(auth) => {
                let mut headers = HeaderMap::new();
                let basic_auth =
                    BASE64_STANDARD.encode(format!("{}:{}", auth.username, auth.password));
                headers.insert(
                    reqwest::header::AUTHORIZATION,
                    HeaderValue::from_str(&format!("Basic {}", basic_auth))
                        .expect("invariant: could not create basic auth header."),
                );
                client_builder.default_headers(headers)
            }
            None => client_builder,
        };
        let client_builder = match self.request_timeout {
            Some(timeout) => client_builder.timeout(timeout),
            None => client_builder,
        };
        let client_builder = match self.tcp_keepalive {
            Some(timeout) => client_builder.tcp_keepalive(timeout),
            None => client_builder,
        };

        #[cfg(feature = "tls")]
        let client_builder = {
            let client_builder = if let Some(identity) = self.tls_client_identity {
                client_builder.identity(identity)
            } else {
                client_builder
            };

            self.tls_root_certificates
                .into_iter()
                .fold(client_builder, |client_builder, certificate| {
                    client_builder.add_root_certificate(certificate)
                })
        };

        let http_client = client_builder
            .build()
            .expect("invariant: could not create http client");

        Client {
            endpoints: Arc::new(self.endpoints),
            http_client,
        }
    }
}

impl Client {
    /// Constructs a new client using the HTTP protocol. For more advanced configuration, use [`ClientBuilder`]
    ///
    /// # Parameters
    ///
    /// * endpoints: URLs for one or more cluster members. When making an API call, the client will
    /// make the call to each member in order until it receives a successful respponse.
    ///
    /// # Errors
    ///
    /// Panics if no endpoints are provided or if any of the endpoints is an invalid URL.
    pub fn new(endpoints: &[&str]) -> Self {
        ClientBuilder::new(endpoints).build()
    }

    /// Lets other internal code access the `HttpClient`.
    pub(crate) fn http_client(&self) -> &reqwest::Client {
        &self.http_client
    }

    /// Runs a basic health check against each etcd member.
    pub async fn health(&self) -> Vec<Result<Response<Health>, Error>> {
        self.request_on_each_endpoint("health").await
    }

    /// Returns version information from each etcd cluster member the client was initialized with.
    pub async fn versions(&self) -> Vec<Result<Response<VersionInfo>, Error>> {
        self.request_on_each_endpoint("version").await
    }

    fn shuffled_endpoints(&self) -> Vec<&Uri> {
        // Shallow copy the endpoints, so we can shuffle them.
        let mut endpoints: Vec<&Uri> = self.endpoints.iter().collect();
        let mut rng = rng();
        endpoints.shuffle(&mut rng);
        endpoints
    }

    pub(crate) async fn first_ok<'a, H, F, T, E>(&'a self, handler: H) -> Result<T, Vec<E>>
    where
        F: Future<Output = Result<T, E>> + 'a,
        H: Fn(&'a Client, &'a Uri) -> F,
    {
        let mut errors = Vec::new();

        for endpoint in self.shuffled_endpoints() {
            let result = (handler)(&self, endpoint).await;
            match result {
                Ok(response) => return Ok(response),
                Err(err) => errors.push(err),
            }
        }

        Err(errors)
    }

    /// Attempts to issue a GET request to the given path on all endpoints, returning the result of the first successful request.
    pub(crate) async fn request_first_ok<T, P>(&self, path: P) -> Result<Response<T>, Error>
    where
        P: AsRef<str>,
        T: DeserializeOwned,
    {
        let path = path.as_ref();
        let result = self
            .first_ok(|client, endpoint| client.request(format!("{}{}", endpoint, path)))
            .await;

        match result {
            Ok(response) => Ok(response),
            Err(errors) => Err(errors
                .into_iter()
                .next()
                .expect("invariant: errors array should never be empty.")),
        }
    }

    /// Attempts to issue a GET request to the given path on all endpoints, returning results from each endpoint.
    pub(crate) async fn request_on_each_endpoint<T, P>(
        &self,
        path: P,
    ) -> Vec<Result<Response<T>, Error>>
    where
        P: AsRef<str>,
        T: DeserializeOwned,
    {
        let path = path.as_ref();
        let mut results = Vec::with_capacity(self.endpoints.len());

        for endpoint in self.endpoints.iter() {
            let result = self.request(build_url(endpoint, path)).await;
            results.push(result);
        }

        results
    }

    /// Lets other internal code make basic HTTP requests.
    pub(crate) async fn request<T, U>(&self, uri: U) -> Result<Response<T>, Error>
    where
        U: IntoUrl,
        T: DeserializeOwned,
    {
        let response = self.http_client.get(uri).send().await?;
        parse_etcd_response(response, |s| s == StatusCode::OK).await
    }
}

pub(crate) async fn parse_etcd_response<T>(
    response: reqwest::Response,
    status_code_is_success: impl FnOnce(StatusCode) -> bool,
) -> Result<Response<T>, Error>
where
    T: DeserializeOwned,
{
    let status_code = response.status();
    let cluster_info = ClusterInfo::from(response.headers());
    let body = response.bytes().await?;
    if status_code_is_success(status_code) {
        match serde_json::from_slice::<T>(&body) {
            Ok(data) => Ok(Response { data, cluster_info }),
            Err(error) => Err(Error::Serialization(error)),
        }
    } else {
        match serde_json::from_slice::<ApiError>(&body) {
            Ok(error) => Err(Error::Api(error)),
            Err(error) => Err(Error::Serialization(error)),
        }
    }
}

/// A wrapper type returned by all API calls.
///
/// Contains the primary data of the response along with information about the cluster extracted
/// from the HTTP response headers.
#[derive(Clone, Debug)]
pub struct Response<T> {
    /// Information about the state of the cluster.
    pub cluster_info: ClusterInfo,
    /// The primary data of the response.
    pub data: T,
}

/// Information about the state of the etcd cluster from an API response's HTTP headers.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ClusterInfo {
    /// An internal identifier for the cluster.
    pub cluster_id: Option<String>,
    /// A unique, monotonically-incrementing integer created for each change to etcd.
    pub etcd_index: Option<u64>,
    /// A unique, monotonically-incrementing integer used by the Raft protocol.
    pub raft_index: Option<u64>,
    /// The current Raft election term.
    pub raft_term: Option<u64>,
}

impl<'a> From<&'a HeaderMap<HeaderValue>> for ClusterInfo {
    fn from(headers: &'a HeaderMap<HeaderValue>) -> Self {
        let cluster_id = headers.get(XETCD_CLUSTER_ID).and_then(|v| {
            match String::from_utf8(v.as_bytes().to_vec()) {
                Ok(s) => Some(s),
                Err(e) => {
                    error!("{} header decode error: {:?}", XETCD_CLUSTER_ID, e);
                    None
                }
            }
        });

        let etcd_index = headers.get(XETCD_INDEX).and_then(|v| {
            match String::from_utf8(v.as_bytes().to_vec())
                .map_err(|e| format!("{:?}", e))
                .and_then(|s| s.parse().map_err(|e| format!("{:?}", e)))
            {
                Ok(i) => Some(i),
                Err(e) => {
                    error!("{} header decode error: {}", XETCD_INDEX, e);
                    None
                }
            }
        });

        let raft_index = headers.get(XRAFT_INDEX).and_then(|v| {
            match String::from_utf8(v.as_bytes().to_vec())
                .map_err(|e| format!("{:?}", e))
                .and_then(|s| s.parse().map_err(|e| format!("{:?}", e)))
            {
                Ok(i) => Some(i),
                Err(e) => {
                    error!("{} header decode error: {}", XRAFT_INDEX, e);
                    None
                }
            }
        });

        let raft_term = headers.get(XRAFT_TERM).and_then(|v| {
            match String::from_utf8(v.as_bytes().to_vec())
                .map_err(|e| format!("{:?}", e))
                .and_then(|s| s.parse().map_err(|e| format!("{:?}", e)))
            {
                Ok(i) => Some(i),
                Err(e) => {
                    error!("{} header decode error: {}", XRAFT_TERM, e);
                    None
                }
            }
        });

        ClusterInfo {
            cluster_id,
            etcd_index,
            raft_index,
            raft_term,
        }
    }
}

pub(crate) async fn parse_empty_response(
    response: reqwest::Response,
) -> Result<Response<()>, Error> {
    let status_code = response.status();
    let cluster_info = ClusterInfo::from(response.headers());
    let body = response.bytes().await?;
    match status_code {
        StatusCode::NO_CONTENT | StatusCode::OK => Ok(Response {
            data: (),
            cluster_info,
        }),
        _ => match serde_json::from_slice::<ApiError>(&body) {
            Ok(error) => Err(Error::Api(error)),
            Err(error) => Err(Error::Serialization(error)),
        },
    }
}

/// Constructs the full URL for the versions API call.
fn build_url(endpoint: &Uri, path: &str) -> String {
    format!("{}{}", endpoint, path)
}
