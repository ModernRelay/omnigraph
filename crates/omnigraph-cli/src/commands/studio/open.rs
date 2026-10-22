//! Handing a Studio link to the platform. Each platform opens it only through
//! a handler it names for Studio: on macOS the application with Studio's
//! bundle identifier, elsewhere the scheme's registered handler. The generic
//! openers are never asked to open an unregistered scheme, since they hand
//! one to a web browser.

use std::io::{self, Read};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

/// Studio's macOS bundle identifier, the one name `open -b` resolves.
pub(crate) const BUNDLE_ID: &str = "com.modernrelay.omnigraph-studio";
/// The URL scheme of Studio's links.
pub(crate) const SCHEME: &str = "omnigraph-studio";
/// Where to get Studio when nothing here opens its links.
pub(crate) const INSTALL_URL: &str = "https://github.com/ModernRelay/omnigraph-studio/releases";

/// How long one platform command may run before the opener stops waiting:
/// a handler that keeps running has received the link.
const WAIT: Duration = Duration::from_secs(10);
/// The most output a handler query may print.
const OUTPUT_MAX_BYTES: u64 = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Platform {
    MacOs,
    /// Linux and the other Unix desktops that follow freedesktop.org's
    /// shared MIME database.
    Freedesktop,
    Windows,
    Unsupported,
}

impl Platform {
    pub(crate) fn current() -> Self {
        if cfg!(target_os = "macos") {
            Self::MacOs
        } else if cfg!(windows) {
            Self::Windows
        } else if cfg!(unix) {
            Self::Freedesktop
        } else {
            Self::Unsupported
        }
    }
}

/// How a platform command ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Exit {
    Exited {
        success: bool,
        stdout: String,
    },
    /// Still running when the opener stopped waiting.
    Running,
}

/// Runs the platform's commands; tests substitute one that records them.
pub(crate) trait Runner {
    /// Run a query whose answer is its exit status and stdout.
    fn query(&mut self, program: &str, args: &[&str]) -> io::Result<Exit>;
    /// Start the command that opens the link. It gets none of this
    /// process's standard streams, since the application it starts may
    /// inherit them and outlive this process.
    fn launch(&mut self, program: &str, args: &[&str]) -> io::Result<Exit>;
}

/// What became of the link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Opening {
    Opened,
    /// Nothing here opens it, and why.
    Unavailable(String),
}

/// Open `link` on `platform`. The link is the only argument a command gets
/// that this process did not fix, and it carries no secret.
pub(crate) fn open(platform: Platform, link: &str, runner: &mut dyn Runner) -> Opening {
    match platform {
        Platform::MacOs => launched(
            runner.launch("open", &["-b", BUNDLE_ID, link]),
            format!("no application has the bundle identifier {BUNDLE_ID}"),
        ),
        Platform::Freedesktop => {
            let kind = format!("x-scheme-handler/{SCHEME}");
            match runner.query("xdg-mime", &["query", "default", kind.as_str()]) {
                Ok(Exit::Exited {
                    success: true,
                    stdout,
                }) if !stdout.trim().is_empty() => launched(
                    runner.launch("xdg-open", &[link]),
                    format!("xdg-open could not open the link with {}", stdout.trim()),
                ),
                _ => Opening::Unavailable(format!(
                    "no application is registered for {SCHEME} links \
                     (`xdg-mime query default {kind}` names none)"
                )),
            }
        }
        Platform::Windows => {
            let key = format!(r"HKCR\{SCHEME}");
            let command = format!(r"{key}\shell\open\command");
            let registered = [
                vec!["query", key.as_str(), "/v", "URL Protocol"],
                vec!["query", command.as_str(), "/ve"],
            ]
            .iter()
            .all(|args| {
                matches!(
                    runner.query("reg", args),
                    Ok(Exit::Exited { success: true, .. })
                )
            });
            if !registered {
                return Opening::Unavailable(format!(
                    "no application is registered for {SCHEME} links \
                     (the registry has no {SCHEME} URL protocol with an open command)"
                ));
            }
            launched(
                runner.launch("rundll32", &["url.dll,FileProtocolHandler", link]),
                format!("the {SCHEME} protocol handler could not open the link"),
            )
        }
        Platform::Unsupported => {
            Opening::Unavailable("this platform has no way to open Studio links".into())
        }
    }
}

fn launched(result: io::Result<Exit>, refused: String) -> Opening {
    match result {
        Ok(Exit::Exited { success: true, .. } | Exit::Running) => Opening::Opened,
        Ok(Exit::Exited { success: false, .. }) => Opening::Unavailable(refused),
        Err(error) => Opening::Unavailable(format!("{refused}: {error}")),
    }
}

/// The process's own platform commands, found on `PATH`.
pub(crate) struct SystemRunner;

impl Runner for SystemRunner {
    fn query(&mut self, program: &str, args: &[&str]) -> io::Result<Exit> {
        let mut child = Command::new(program)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let Some(status) = wait(&mut child)? else {
            let _ = child.kill();
            let _ = child.wait();
            return Ok(Exit::Running);
        };
        let mut stdout = Vec::new();
        if let Some(output) = child.stdout.take() {
            output.take(OUTPUT_MAX_BYTES).read_to_end(&mut stdout)?;
        }
        Ok(Exit::Exited {
            success: status.success(),
            stdout: String::from_utf8_lossy(&stdout).into_owned(),
        })
    }

    fn launch(&mut self, program: &str, args: &[&str]) -> io::Result<Exit> {
        let mut child = Command::new(program)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        // A handler still running has the link; it is left to run.
        Ok(match wait(&mut child)? {
            Some(status) => Exit::Exited {
                success: status.success(),
                stdout: String::new(),
            },
            None => Exit::Running,
        })
    }
}

fn wait(child: &mut Child) -> io::Result<Option<ExitStatus>> {
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(Some(status));
        }
        if Instant::now() >= deadline {
            return Ok(None);
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    const LINK: &str = "omnigraph-studio://open?target=staging&binding=sha256:b&home=sha256:h";

    /// A runner that answers from a script and records every command, so a
    /// test opens nothing.
    #[derive(Default)]
    pub(crate) struct Recorded {
        answers: Vec<(&'static str, io::Result<Exit>)>,
        pub(crate) calls: Vec<Vec<String>>,
    }

    impl Recorded {
        pub(crate) fn answering(answers: Vec<(&'static str, io::Result<Exit>)>) -> Self {
            Self {
                answers,
                calls: Vec::new(),
            }
        }

        fn answer(&mut self, program: &str, args: &[&str]) -> io::Result<Exit> {
            self.calls.push(
                std::iter::once(program)
                    .chain(args.iter().copied())
                    .map(str::to_owned)
                    .collect(),
            );
            let position = self
                .answers
                .iter()
                .position(|(name, _)| *name == program)
                .unwrap_or_else(|| panic!("unexpected command {program} {args:?}"));
            self.answers.remove(position).1
        }
    }

    impl Runner for Recorded {
        fn query(&mut self, program: &str, args: &[&str]) -> io::Result<Exit> {
            self.answer(program, args)
        }
        fn launch(&mut self, program: &str, args: &[&str]) -> io::Result<Exit> {
            self.answer(program, args)
        }
    }

    pub(crate) fn exited(success: bool, stdout: &str) -> io::Result<Exit> {
        Ok(Exit::Exited {
            success,
            stdout: stdout.into(),
        })
    }

    fn missing() -> io::Result<Exit> {
        Err(io::Error::new(io::ErrorKind::NotFound, "not found"))
    }

    fn argv(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|part| (*part).to_owned()).collect()
    }

    fn unavailable(opening: Opening) -> String {
        match opening {
            Opening::Unavailable(reason) => reason,
            Opening::Opened => panic!("expected the link to stay unopened"),
        }
    }

    #[test]
    fn macos_opens_studio_by_its_bundle_identifier() {
        for answer in [exited(true, ""), Ok(Exit::Running)] {
            let mut runner = Recorded::answering(vec![("open", answer)]);
            assert_eq!(open(Platform::MacOs, LINK, &mut runner), Opening::Opened);
            assert_eq!(
                runner.calls,
                [argv(&[
                    "open",
                    "-b",
                    "com.modernrelay.omnigraph-studio",
                    LINK
                ])]
            );
        }
        for answer in [exited(false, ""), missing()] {
            let mut runner = Recorded::answering(vec![("open", answer)]);
            let reason = unavailable(open(Platform::MacOs, LINK, &mut runner));
            assert!(reason.contains(BUNDLE_ID), "{reason}");
            assert_eq!(runner.calls.len(), 1);
        }
    }

    #[test]
    fn freedesktop_opens_only_through_a_named_scheme_handler() {
        let query = argv(&[
            "xdg-mime",
            "query",
            "default",
            "x-scheme-handler/omnigraph-studio",
        ]);
        let mut runner = Recorded::answering(vec![
            ("xdg-mime", exited(true, "omnigraph-studio.desktop\n")),
            ("xdg-open", exited(true, "")),
        ]);
        assert_eq!(
            open(Platform::Freedesktop, LINK, &mut runner),
            Opening::Opened
        );
        assert_eq!(runner.calls, [query.clone(), argv(&["xdg-open", LINK])]);
        // Without a named handler xdg-open is never run: its generic path
        // would hand the link to a browser.
        for answer in [
            exited(true, ""),
            exited(true, " \n"),
            exited(false, ""),
            missing(),
        ] {
            let mut runner = Recorded::answering(vec![("xdg-mime", answer)]);
            let reason = unavailable(open(Platform::Freedesktop, LINK, &mut runner));
            assert!(reason.contains("names none"), "{reason}");
            assert_eq!(runner.calls, std::slice::from_ref(&query));
        }
        let mut runner = Recorded::answering(vec![
            ("xdg-mime", exited(true, "omnigraph-studio.desktop\n")),
            ("xdg-open", exited(false, "")),
        ]);
        let reason = unavailable(open(Platform::Freedesktop, LINK, &mut runner));
        assert!(reason.contains("omnigraph-studio.desktop"), "{reason}");
    }

    #[test]
    fn windows_opens_only_through_a_registered_url_protocol() {
        let protocol = argv(&[
            "reg",
            "query",
            r"HKCR\omnigraph-studio",
            "/v",
            "URL Protocol",
        ]);
        let command = argv(&[
            "reg",
            "query",
            r"HKCR\omnigraph-studio\shell\open\command",
            "/ve",
        ]);
        let mut runner = Recorded::answering(vec![
            ("reg", exited(true, "")),
            ("reg", exited(true, "")),
            ("rundll32", exited(true, "")),
        ]);
        assert_eq!(open(Platform::Windows, LINK, &mut runner), Opening::Opened);
        assert_eq!(
            runner.calls,
            [
                protocol.clone(),
                command.clone(),
                argv(&["rundll32", "url.dll,FileProtocolHandler", LINK])
            ]
        );
        for (answers, asked) in [
            (vec![("reg", exited(false, ""))], vec![protocol.clone()]),
            (vec![("reg", missing())], vec![protocol.clone()]),
            (
                vec![("reg", exited(true, "")), ("reg", exited(false, ""))],
                vec![protocol.clone(), command.clone()],
            ),
        ] {
            let mut runner = Recorded::answering(answers);
            let reason = unavailable(open(Platform::Windows, LINK, &mut runner));
            assert!(reason.contains("URL protocol"), "{reason}");
            assert_eq!(runner.calls, asked);
        }
    }

    #[test]
    fn an_unsupported_platform_runs_nothing() {
        let mut runner = Recorded::default();
        unavailable(open(Platform::Unsupported, LINK, &mut runner));
        assert!(runner.calls.is_empty());
    }
}
