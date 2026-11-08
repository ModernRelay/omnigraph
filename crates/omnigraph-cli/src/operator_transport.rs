use std::collections::BTreeMap;
use std::io::Read;
use std::net::IpAddr;

use color_eyre::eyre::{Result, bail, eyre};
use reqwest::{Certificate, ClientBuilder, Url};

use crate::operator_schema::{CaMode, Proxy, Transport};

const MAX_CA_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct CapturedTransport {
    endpoint: Url,
    proxy: Option<Url>,
    certificates: Vec<Certificate>,
    replace_roots: bool,
}

impl CapturedTransport {
    pub(crate) fn capture(
        endpoint: &str,
        transport: Option<&Transport>,
        environment: &BTreeMap<String, String>,
    ) -> Result<Self> {
        let endpoint = Url::parse(endpoint)?;
        let default = Transport::default();
        let transport = transport.unwrap_or(&default);
        let proxy = match &transport.proxy {
            Proxy::Direct => None,
            Proxy::Url { url } => Some(proxy_url(url)?),
            Proxy::Environment => environment_proxy(&endpoint, environment)?,
        };
        let mut certificates = Vec::new();
        let mut replace_roots = false;
        if let Some(ca) = transport.tls.as_ref().and_then(|tls| tls.ca.as_ref()) {
            if proxy.as_ref().is_some_and(|url| url.scheme() == "https") {
                bail!(
                    "version 1 refuses custom destination CA with HTTPS proxy; separate HTTPS proxy and destination CA trust is unsupported"
                );
            }
            let mut options = std::fs::OpenOptions::new();
            options.read(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.custom_flags(libc::O_NONBLOCK | libc::O_NOCTTY);
            }
            let file = options
                .open(&ca.file)
                .map_err(|_| eyre!("could not open configured CA bundle"))?;
            if !file.metadata()?.is_file() {
                bail!("configured CA bundle must be a regular file");
            }
            let mut bytes = Vec::new();
            file.take(MAX_CA_BYTES + 1).read_to_end(&mut bytes)?;
            if bytes.len() as u64 > MAX_CA_BYTES {
                bail!("configured CA bundle exceeds 1 MiB");
            }
            certificates = Certificate::from_pem_bundle(&bytes)
                .map_err(|_| eyre!("configured CA bundle is not valid PEM certificates"))?;
            if certificates.is_empty() {
                bail!("configured CA bundle contains no certificates");
            }
            replace_roots = ca.mode == CaMode::Replace;
        }
        Ok(Self {
            endpoint,
            proxy,
            certificates,
            replace_roots,
        })
    }

    pub(crate) fn apply(&self, builder: ClientBuilder) -> Result<ClientBuilder> {
        let mut builder = builder
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .tls_built_in_root_certs(!self.replace_roots)
            .tls_built_in_webpki_certs(false)
            .tls_built_in_native_certs(!self.replace_roots);
        for certificate in &self.certificates {
            builder = builder.add_root_certificate(certificate.clone());
        }
        if let Some(proxy) = &self.proxy {
            let endpoint = self.endpoint.clone();
            let selected = proxy.clone();
            builder = builder.proxy(reqwest::Proxy::custom(move |request| {
                (request.scheme() == endpoint.scheme()
                    && request.host_str() == endpoint.host_str()
                    && request.port_or_known_default() == endpoint.port_or_known_default())
                .then(|| selected.clone())
            }));
        }
        Ok(builder)
    }

    #[cfg(test)]
    pub(crate) fn proxy_url(&self) -> Option<&str> {
        self.proxy.as_ref().map(Url::as_str)
    }
}

fn proxy_url(raw: &str) -> Result<Url> {
    let url = Url::parse(raw).map_err(|_| eyre!("configured proxy URL is invalid"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !matches!(url.path(), "" | "/")
    {
        bail!("proxy requires HTTP(S), a host, and no credentials, path, query or fragment");
    }
    Ok(url)
}

fn environment_value<'a>(
    environment: &'a BTreeMap<String, String>,
    lower: &str,
    upper: &str,
) -> Result<Option<&'a str>> {
    match (environment.get(lower), environment.get(upper)) {
        (Some(left), Some(right)) if left != right => {
            bail!("conflicting proxy environment variable spellings: {lower}/{upper}")
        }
        (Some(value), _) | (_, Some(value)) => Ok(Some(value)),
        _ => Ok(None),
    }
}

fn environment_proxy(
    endpoint: &Url,
    environment: &BTreeMap<String, String>,
) -> Result<Option<Url>> {
    let raw = match endpoint.scheme() {
        "https" => environment_value(environment, "https_proxy", "HTTPS_PROXY")?,
        "http" => environment.get("http_proxy").map(String::as_str),
        _ => bail!("environment proxy mode requires an HTTP(S) endpoint"),
    };
    let bypass = environment_value(environment, "no_proxy", "NO_PROXY")?;
    let bypasses = bypass
        .map(|value| bypass_matches(endpoint, value))
        .transpose()?
        .unwrap_or(false);
    let proxy = raw
        .filter(|value| !value.is_empty())
        .map(proxy_url)
        .transpose()?;
    Ok(if bypasses { None } else { proxy })
}

fn bypass_matches(endpoint: &Url, raw: &str) -> Result<bool> {
    let destination = endpoint
        .host_str()
        .ok_or_else(|| eyre!("endpoint lacks host"))?;
    let mut matches = false;
    for entry in raw
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        if entry == "*" {
            matches = true;
            continue;
        }
        if entry.contains(['/', '*', '@', '?', '#']) {
            bail!("unsupported proxy bypass pattern");
        }
        let suffix = entry.starts_with('.');
        let host_port = entry.strip_prefix('.').unwrap_or(entry);
        if host_port.is_empty() || host_port.chars().any(char::is_whitespace) {
            bail!("invalid proxy bypass host");
        }
        if host_port.matches(':').count() > 1 && !host_port.starts_with('[') {
            bail!("IPv6 proxy bypass addresses require brackets");
        }
        let parsed = Url::parse(&format!("http://{host_port}"))
            .map_err(|_| eyre!("invalid proxy bypass host or port"))?;
        if parsed.host_str().is_none() || parsed.path() != "/" {
            bail!("invalid proxy bypass host");
        }
        let host = parsed.host_str().expect("host checked");
        if suffix && host.trim_matches(['[', ']']).parse::<IpAddr>().is_ok() {
            bail!("proxy suffix patterns require DNS names");
        }
        let explicit_port = if host_port.starts_with('[') {
            host_port
                .split_once(']')
                .and_then(|(_, tail)| tail.strip_prefix(':'))
        } else {
            host_port.split_once(':').map(|(_, port)| port)
        };
        let port_matches = match explicit_port {
            Some(port) => {
                endpoint.port_or_known_default()
                    == Some(
                        port.parse::<u16>()
                            .map_err(|_| eyre!("invalid proxy bypass port"))?,
                    )
            }
            None => true,
        };
        let host_matches = destination.eq_ignore_ascii_case(host)
            || (suffix
                && destination
                    .to_ascii_lowercase()
                    .ends_with(&format!(".{}", host.to_ascii_lowercase())));
        matches |= port_matches && host_matches;
    }
    Ok(matches)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn read_headers(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
        use tokio::io::AsyncReadExt;
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            request.push(socket.read_u8().await.unwrap());
            assert!(request.len() < 8192);
        }
        request
    }

    fn environment(values: &[(&str, &str)]) -> BTreeMap<String, String> {
        values
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn direct_ignores_ambient_proxy_configuration() {
        let captured = CapturedTransport::capture(
            "https://graph.example.com",
            None,
            &environment(&[("HTTPS_PROXY", "not a URL")]),
        )
        .unwrap();
        assert!(captured.proxy_url().is_none());
        captured
            .apply(reqwest::Client::builder())
            .unwrap()
            .build()
            .unwrap();
    }

    #[test]
    fn explicit_proxy_ignores_ambient_bypass() {
        let transport = Transport {
            proxy: Proxy::Url {
                url: "http://proxy.example.com:8080".into(),
            },
            ..Default::default()
        };
        let captured = CapturedTransport::capture(
            "https://graph.example.com",
            Some(&transport),
            &environment(&[("NO_PROXY", "*")]),
        )
        .unwrap();
        assert_eq!(captured.proxy_url(), Some("http://proxy.example.com:8080/"));
    }

    #[test]
    fn environment_proxy_refuses_conflicts_without_echoing_values() {
        let result = environment_proxy(
            &Url::parse("https://graph.example.com").unwrap(),
            &environment(&[("https_proxy", "SECRET-A"), ("HTTPS_PROXY", "SECRET-B")]),
        );
        let message = result.unwrap_err().to_string();
        assert!(message.contains("conflicting"));
        assert!(!message.contains("SECRET"));
    }

    #[test]
    fn bypass_matching_preserves_host_and_port_boundaries() {
        for (endpoint, pattern, expected) in [
            ("https://graph.example.com", ".example.com", true),
            ("https://example.com", ".example.com", true),
            ("https://notexample.com", ".example.com", false),
            ("https://graph.example.com", "example.com", false),
            ("https://graph.example.com", "graph.example.com:443", true),
            ("https://graph.example.com", "graph.example.com:80", false),
            ("http://[::1]:8080", "[::1]:8080", true),
            ("https://graph.example.com", "*", true),
        ] {
            assert_eq!(
                bypass_matches(&Url::parse(endpoint).unwrap(), pattern).unwrap(),
                expected,
                "{endpoint} {pattern}"
            );
        }
        for pattern in [
            "10.0.0.0/8",
            "*.example.com",
            "::1",
            ".127.0.0.1",
            "host:wrong",
            "*,bad/path",
        ] {
            assert!(
                bypass_matches(&Url::parse("https://graph.example.com").unwrap(), pattern).is_err(),
                "{pattern}"
            );
        }
    }

    #[test]
    fn uppercase_http_proxy_and_all_proxy_are_never_used() {
        let values = environment(&[
            ("HTTP_PROXY", "http://untrusted.example.com"),
            ("ALL_PROXY", "http://untrusted.example.com"),
        ]);
        assert!(
            environment_proxy(&Url::parse("http://127.0.0.1:8080").unwrap(), &values)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn custom_destination_ca_cannot_change_https_proxy_trust() {
        for mode in [CaMode::Append, CaMode::Replace] {
            for proxy in [
                Proxy::Url {
                    url: "https://proxy.example.com".into(),
                },
                Proxy::Environment,
            ] {
                let transport = Transport {
                    proxy,
                    tls: Some(crate::operator_schema::Tls {
                        ca: Some(crate::operator_schema::Ca {
                            mode,
                            file: "/nonexistent/ca.pem".into(),
                        }),
                    }),
                };
                let error = CapturedTransport::capture(
                    "https://graph.example.com",
                    Some(&transport),
                    &environment(&[("HTTPS_PROXY", "https://proxy.example.com")]),
                )
                .unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("separate HTTPS proxy and destination CA trust")
                );
            }
        }
    }

    #[tokio::test]
    async fn captured_ca_is_immutable_and_redirects_are_not_followed() {
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream = format!("http://{}", listener.local_addr().unwrap());
        let gateway = crate::tls_gateway::TlsGateway::new(&upstream);
        let directory = tempfile::tempdir().unwrap();
        let ca_file = directory.path().join("ca.pem");
        std::fs::copy(crate::tls_gateway::ca_path(), &ca_file).unwrap();
        let transport = Transport {
            proxy: Proxy::Direct,
            tls: Some(crate::operator_schema::Tls {
                ca: Some(crate::operator_schema::Ca {
                    mode: CaMode::Replace,
                    file: ca_file.clone(),
                }),
            }),
        };
        let captured =
            CapturedTransport::capture(&gateway.origin, Some(&transport), &BTreeMap::new())
                .unwrap();
        std::fs::write(ca_file, "replaced after invocation binding").unwrap();
        let origin = gateway.origin.clone();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_headers(&mut socket).await;
            let reply = format!(
                "HTTP/1.1 307 Temporary Redirect\r\nLocation: {origin}/other\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            socket.write_all(reply.as_bytes()).await.unwrap();
            request
        });
        let client = captured
            .apply(reqwest::Client::builder())
            .unwrap()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();
        let response = client
            .get(&gateway.origin)
            .bearer_auth("synthetic-test-token")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::TEMPORARY_REDIRECT);
        let request = server.await.unwrap();
        assert!(
            String::from_utf8(request)
                .unwrap()
                .contains("Bearer synthetic-test-token")
        );
    }

    #[tokio::test]
    async fn http_connect_proxy_preserves_custom_ca_and_hides_origin_credentials() {
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let gateway = crate::tls_gateway::TlsGateway::new(&format!(
            "http://{}",
            listener.local_addr().unwrap()
        ));
        let proxy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let transport = Transport {
            proxy: Proxy::Url {
                url: format!("http://{}", proxy.local_addr().unwrap()),
            },
            tls: Some(crate::operator_schema::Tls {
                ca: Some(crate::operator_schema::Ca {
                    mode: CaMode::Replace,
                    file: crate::tls_gateway::ca_path().into(),
                }),
            }),
        };
        let endpoint = Url::parse(&gateway.origin).unwrap();
        let authority = format!("127.0.0.1:{}", endpoint.port().unwrap());
        let proxy_task = tokio::spawn(async move {
            let (mut downstream, _) = proxy.accept().await.unwrap();
            let request = String::from_utf8(read_headers(&mut downstream).await).unwrap();
            assert!(request.starts_with(&format!("CONNECT {authority} HTTP/1.1\r\n")));
            assert!(!request.to_ascii_lowercase().contains("authorization"));
            assert!(!request.contains("synthetic-test-token"));
            let mut upstream = tokio::net::TcpStream::connect(authority).await.unwrap();
            downstream
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .await
                .unwrap();
            tokio::io::copy_bidirectional(&mut downstream, &mut upstream)
                .await
                .unwrap();
        });
        let server_task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = String::from_utf8(read_headers(&mut socket).await).unwrap();
            assert!(request.contains("Bearer synthetic-test-token"));
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await
                .unwrap();
        });
        let captured =
            CapturedTransport::capture(&gateway.origin, Some(&transport), &BTreeMap::new())
                .unwrap();
        let client = captured
            .apply(reqwest::Client::builder())
            .unwrap()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();
        let response = client
            .get(&gateway.origin)
            .bearer_auth("synthetic-test-token")
            .send()
            .await
            .unwrap();
        assert_eq!(response.text().await.unwrap(), "ok");
        server_task.await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), proxy_task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn system_trust_does_not_accept_an_unconfigured_private_ca() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream = format!("http://{}", listener.local_addr().unwrap());
        let gateway = crate::tls_gateway::TlsGateway::new(&upstream);
        let captured = CapturedTransport::capture(&gateway.origin, None, &BTreeMap::new()).unwrap();
        let client = captured
            .apply(reqwest::Client::builder())
            .unwrap()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();
        assert!(client.get(&gateway.origin).send().await.is_err());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), listener.accept())
                .await
                .is_err()
        );
    }

    #[test]
    fn ca_bundle_is_bounded_and_requires_certificates() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("ca.pem");
        let transport = Transport {
            proxy: Proxy::Direct,
            tls: Some(crate::operator_schema::Tls {
                ca: Some(crate::operator_schema::Ca {
                    mode: CaMode::Append,
                    file: path.clone(),
                }),
            }),
        };
        for bytes in [
            Vec::new(),
            b"not a certificate".to_vec(),
            vec![b' '; MAX_CA_BYTES as usize + 1],
        ] {
            std::fs::write(&path, bytes).unwrap();
            assert!(
                CapturedTransport::capture(
                    "https://graph.example.com",
                    Some(&transport),
                    &BTreeMap::new()
                )
                .is_err()
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn ca_fifo_is_refused_without_waiting_for_a_writer() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("ca.pipe");
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&path)
                .status()
                .unwrap()
                .success()
        );
        let transport = Transport {
            proxy: Proxy::Direct,
            tls: Some(crate::operator_schema::Tls {
                ca: Some(crate::operator_schema::Ca {
                    mode: CaMode::Replace,
                    file: path,
                }),
            }),
        };
        let error = CapturedTransport::capture(
            "https://graph.example.com",
            Some(&transport),
            &BTreeMap::new(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("regular file"));
    }

    #[test]
    fn proxy_urls_reject_secrets_and_unsupported_schemes() {
        for raw in [
            "http://user:secret@proxy.example.com",
            "socks5://proxy.example.com",
            "https://proxy.example.com/path",
            "http://proxy.example.com?token=secret",
        ] {
            let error = proxy_url(raw).unwrap_err().to_string();
            assert!(!error.contains("secret"));
        }
    }
}
