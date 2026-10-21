use std::net::IpAddr;

use url::{Host, Url};

use super::*;

fn invalid(path: &str, message: &str) -> ConfigError {
    ConfigError(format!("{path}: {message}"))
}

fn nonempty(value: &str, path: &str) -> Result<(), ConfigError> {
    if value.trim().is_empty() || value.chars().any(char::is_control) {
        return Err(invalid(
            path,
            "must be nonempty and contain no control characters",
        ));
    }
    Ok(())
}

fn name(value: &str, path: &str) -> Result<(), ConfigError> {
    let mut bytes = value.bytes();
    if !bytes.next().is_some_and(|c| c.is_ascii_alphabetic())
        || !bytes.all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c))
    {
        return Err(invalid(path, "name must match [A-Za-z][A-Za-z0-9_.-]*"));
    }
    Ok(())
}

fn reference<T>(value: &str, entries: &BTreeMap<String, T>, path: &str) -> Result<(), ConfigError> {
    name(value, path)?;
    if !entries.contains_key(value) {
        return Err(invalid(path, "unknown local reference"));
    }
    Ok(())
}

pub(super) fn validate(config: &mut OperatorConfig, base: &Path) -> Result<(), ConfigError> {
    if config.version != 1 {
        return Err(invalid("version", "must be the integer 1"));
    }
    if !base.is_absolute() {
        return Err(invalid("base_dir", "must be absolute"));
    }
    for (key, profile) in &mut config.profiles {
        let path = format!("profiles.{key}");
        name(key, &path)?;
        managed_identifier(
            &profile.organization_id,
            128,
            &format!("{path}.organization_id"),
        )?;
        profile.api = endpoint(&profile.api, false, &format!("{path}.api"))?;
        auth_provider(&mut profile.auth, true, base, &format!("{path}.auth"))?;
        transport(&mut profile.transport, base, &path)?;
    }
    for (key, target) in &mut config.targets {
        let path = format!("targets.{key}");
        name(key, &path)?;
        match target {
            Target::Managed {
                profile,
                cluster_id,
                cluster_incarnation,
                endpoint: route,
                transport: network,
                default_graph,
            } => {
                reference(profile, &config.profiles, &format!("{path}.profile"))?;
                managed_identifier(cluster_id, 256, &format!("{path}.cluster_id"))?;
                managed_identifier(
                    cluster_incarnation,
                    256,
                    &format!("{path}.cluster_incarnation"),
                )?;
                if let Some(route) = route {
                    *route = endpoint(route, false, &format!("{path}.endpoint"))?;
                }
                transport(network, base, &path)?;
                graph(default_graph, true, &path)?;
            }
            Target::Server {
                endpoint: route,
                auth: profiles,
                default_auth,
                transport: network,
                default_graph,
            } => {
                auth(profiles, default_auth, false, base, &path)?;
                let anonymous = profiles
                    .values()
                    .all(|p| matches!(p, AuthProfile::Anonymous));
                *route = endpoint(route, anonymous, &format!("{path}.endpoint"))?;
                transport(network, base, &path)?;
                graph(default_graph, true, &path)?;
            }
            Target::Direct {
                scope,
                uri,
                default_graph,
            } => {
                if *scope == DirectScope::Graph && default_graph.is_some() {
                    return Err(invalid(&path, "direct graph targets forbid default_graph"));
                }
                *uri = local_uri(uri, &format!("{path}.uri"))?;
                graph(default_graph, false, &path)?;
            }
        }
    }
    for (key, alias) in &config.aliases {
        let path = format!("aliases.{key}");
        name(key, &path)?;
        reference(&alias.target, &config.targets, &format!("{path}.target"))?;
        nonempty(&alias.query, &format!("{path}.query"))?;
        let direct_graph = matches!(
            config.targets.get(&alias.target),
            Some(Target::Direct {
                scope: DirectScope::Graph,
                ..
            })
        );
        match (&alias.graph, direct_graph) {
            (Some(_), true) => return Err(invalid(&path, "direct graph aliases forbid graph")),
            (None, false) => return Err(invalid(&path, "multi-graph aliases require graph")),
            (Some(value), false) => graph_id(
                value,
                !matches!(
                    config.targets.get(&alias.target),
                    Some(Target::Direct { .. })
                ),
                &format!("{path}.graph"),
            )?,
            (None, true) => {}
        }
        for key in alias.params.keys() {
            nonempty(key, &format!("{path}.params"))?;
        }
    }
    if let Some(value) = &config.defaults.profile {
        reference(value, &config.profiles, "defaults.profile")?;
    }
    if let Some(value) = &config.defaults.target {
        reference(value, &config.targets, "defaults.target")?;
    }
    Ok(())
}

fn managed_identifier(value: &str, max_len: usize, path: &str) -> Result<(), ConfigError> {
    if !crate::helpers::valid_managed_identifier(value, max_len) {
        return Err(invalid(
            path,
            &format!("must contain 1–{max_len} ASCII letters, digits, underscores, or hyphens"),
        ));
    }
    Ok(())
}

fn graph_id(value: &str, served: bool, path: &str) -> Result<(), ConfigError> {
    let result = if served {
        crate::helpers::require_graph_segment(value)
    } else {
        omnigraph_server::GraphId::try_from(value).map(|_| ())
    };
    result.map_err(|error| invalid(path, &error.to_string()))
}

fn graph(value: &Option<String>, served: bool, path: &str) -> Result<(), ConfigError> {
    if let Some(value) = value {
        graph_id(value, served, &format!("{path}.default_graph"))?;
    }
    Ok(())
}

fn auth(
    profiles: &mut BTreeMap<String, AuthProfile>,
    default: &Option<String>,
    managed: bool,
    base: &Path,
    path: &str,
) -> Result<(), ConfigError> {
    if profiles.is_empty() {
        return Err(invalid(
            path,
            "auth must declare at least one named profile",
        ));
    }
    if let Some(default) = default {
        reference(default, profiles, &format!("{path}.default_auth"))?;
    }
    for (key, profile) in profiles {
        let path = format!("{path}.auth.{key}");
        name(key, &path)?;
        auth_provider(profile, managed, base, &path)?;
    }
    Ok(())
}

fn auth_provider(
    profile: &mut AuthProfile,
    managed: bool,
    base: &Path,
    path: &str,
) -> Result<(), ConfigError> {
    match profile {
        AuthProfile::Omnigraph if !managed => {
            return Err(invalid(path, "omnigraph requires a managed profile"));
        }
        AuthProfile::Anonymous if managed => {
            return Err(invalid(path, "anonymous requires a standalone server"));
        }
        AuthProfile::Token { source } => match source {
            TokenSource::Store { .. } => {}
            TokenSource::Env { env } => {
                let mut bytes = env.bytes();
                if !bytes
                    .next()
                    .is_some_and(|c| c.is_ascii_alphabetic() || c == b'_')
                    || !bytes.all(|c| c.is_ascii_alphanumeric() || c == b'_')
                {
                    return Err(invalid(path, "env must match [A-Za-z_][A-Za-z0-9_]*"));
                }
            }
            TokenSource::File { file } => filesystem_path(file, base, path)?,
        },
        AuthProfile::Exec { command, timeout } => {
            let Some(executable) = command.first() else {
                return Err(invalid(path, "command must be a nonempty argv array"));
            };
            nonempty(executable, path)?;
            if !Path::new(executable).is_absolute() || command.iter().any(|arg| arg.contains('\0'))
            {
                return Err(invalid(
                    path,
                    "command requires an absolute executable and NUL-free arguments",
                ));
            }
            let seconds = timeout
                .strip_suffix('s')
                .filter(|s| !s.is_empty() && s.bytes().all(|c| c.is_ascii_digit()))
                .and_then(|s| s.parse::<u32>().ok());
            if !seconds.is_some_and(|s| (1..=120).contains(&s)) {
                return Err(invalid(
                    path,
                    "timeout must be integer seconds from 1s through 120s",
                ));
            }
        }
        AuthProfile::Omnigraph | AuthProfile::Anonymous => {}
    }
    Ok(())
}

fn filesystem_path(file: &mut PathBuf, base: &Path, path: &str) -> Result<(), ConfigError> {
    let value = file
        .to_str()
        .ok_or_else(|| invalid(path, "path must be UTF-8"))?;
    nonempty(value, path)?;
    if !file.is_absolute() {
        *file = base.join(&*file);
    }
    Ok(())
}

fn transport(value: &mut Option<Transport>, base: &Path, path: &str) -> Result<(), ConfigError> {
    let Some(value) = value else { return Ok(()) };
    if let Some(Tls { ca: Some(ca) }) = &mut value.tls {
        filesystem_path(&mut ca.file, base, &format!("{path}.transport.tls.ca.file"))?;
    }
    if let Proxy::Url { url } = &mut value.proxy {
        let parsed = checked_url(url, &format!("{path}.transport.proxy.url"))?;
        if !matches!(parsed.scheme(), "http" | "https")
            || parsed.host().is_none()
            || parsed.path() != "/"
        {
            return Err(invalid(
                path,
                "proxy URL requires HTTP(S), a host, and no non-root path",
            ));
        }
        if parsed.scheme() == "https" && value.tls.as_ref().is_some_and(|tls| tls.ca.is_some()) {
            return Err(invalid(
                &format!("{path}.transport"),
                "custom destination tls.ca is not supported with an HTTPS proxy",
            ));
        }
        *url = parsed.to_string();
    }
    Ok(())
}

fn checked_url(value: &str, path: &str) -> Result<Url, ConfigError> {
    if value.is_empty()
        || value.chars().any(|c| c.is_whitespace() || c.is_control())
        || value.contains('\\')
    {
        return Err(invalid(
            path,
            "URL contains whitespace, controls, or backslashes",
        ));
    }
    let (_, rest) = value
        .split_once("://")
        .ok_or_else(|| invalid(path, "URL must be absolute with an authority"))?;
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    if authority.contains('@') || authority.contains('%') {
        return Err(invalid(
            path,
            "URL authority forbids userinfo and percent encoding",
        ));
    }
    let raw_path = &rest[authority.len()..];
    validate_path(raw_path, path)?;
    let parsed = Url::parse(value).map_err(|_| invalid(path, "invalid URL"))?;
    if !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(invalid(path, "URL forbids userinfo, query, and fragment"));
    }
    Ok(parsed)
}

fn validate_path(value: &str, path: &str) -> Result<(), ConfigError> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes
                .get(i + 1..i + 3)
                .ok_or_else(|| invalid(path, "incomplete percent encoding"))?;
            let high = (hex[0] as char).to_digit(16);
            let low = (hex[1] as char).to_digit(16);
            let (Some(high), Some(low)) = (high, low) else {
                return Err(invalid(path, "invalid percent encoding"));
            };
            let c = (high * 16 + low) as u8;
            if matches!(c, b'/' | b'\\' | b'%' | b'?' | b'#') || c.is_ascii_control() {
                return Err(invalid(
                    path,
                    "encoded separators, controls, and ambiguous encoding are forbidden",
                ));
            }
            decoded.push(c);
            i += 3;
        } else {
            decoded.push(bytes[i]);
            i += 1;
        }
    }
    let decoded =
        std::str::from_utf8(&decoded).map_err(|_| invalid(path, "path encoding must be UTF-8"))?;
    if decoded
        .split('/')
        .any(|segment| matches!(segment, "." | ".."))
        || decoded.contains("//")
    {
        return Err(invalid(
            path,
            "path traversal and empty path segments are forbidden",
        ));
    }
    Ok(())
}

fn endpoint(value: &str, allow_loopback: bool, path: &str) -> Result<String, ConfigError> {
    let mut parsed = checked_url(value, path)?;
    let loopback = match parsed.host() {
        Some(Host::Domain(host)) => host == "localhost",
        Some(Host::Ipv4(ip)) => IpAddr::V4(ip).is_loopback(),
        Some(Host::Ipv6(ip)) => IpAddr::V6(ip).is_loopback(),
        None => false,
    };
    if parsed.host().is_none()
        || !(parsed.scheme() == "https"
            || (parsed.scheme() == "http" && allow_loopback && loopback))
    {
        return Err(invalid(
            path,
            "endpoint requires HTTPS; HTTP is only allowed for anonymous loopback servers",
        ));
    }
    if !parsed.path().ends_with('/') {
        let prefix = format!("{}/", parsed.path());
        parsed.set_path(&prefix);
    }
    Ok(parsed.to_string())
}

fn local_uri(value: &str, path: &str) -> Result<String, ConfigError> {
    let parsed = checked_url(value, path)?;
    let (_, rest) = value
        .split_once("://")
        .ok_or_else(|| invalid(path, "expected local file URI"))?;
    if parsed.scheme() != "file"
        || !rest.starts_with('/')
        || parsed.host().is_some()
        || parsed.to_file_path().is_err()
    {
        return Err(invalid(
            path,
            "direct URI must be an absolute local file:// URI without authority",
        ));
    }
    Ok(parsed.to_string())
}
