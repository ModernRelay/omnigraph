//! Handing a Studio link to the platform. Each platform opens it only through
//! a handler it names for Studio: on macOS the application with Studio's
//! bundle identifier, on a freedesktop.org desktop the program the scheme's
//! registered desktop entry runs, on Windows the scheme's registered URL
//! protocol. No generic opener is asked, since one hands a link it cannot
//! place to a web browser.

use std::io::{self, Read};
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

/// Studio's macOS bundle identifier, the one name `open -b` resolves.
pub(crate) const BUNDLE_ID: &str = "com.modernrelay.omnigraph-studio";
/// The URL scheme of Studio's links.
pub(crate) const SCHEME: &str = "omnigraph-studio";
/// Where to get Studio when nothing here opens its links.
pub(crate) const INSTALL_URL: &str = "https://github.com/ModernRelay/omnigraph-studio/releases";

/// How long a registration query may run before it counts as unanswered.
const QUERY_WAIT: Duration = Duration::from_secs(10);
/// How long the opener waits for the command it starts: one that refuses at
/// once is seen, and one still running, such as a freedesktop handler that
/// is Studio itself, has received the link.
const LAUNCH_WAIT: Duration = Duration::from_secs(2);
/// The most output a handler query may print.
const OUTPUT_MAX_BYTES: u64 = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Platform {
    MacOs,
    /// Linux and the other Unix desktops that follow freedesktop.org's
    /// shared MIME database, with the base directories desktop entries are
    /// looked up in.
    Freedesktop {
        data_dirs: Vec<PathBuf>,
    },
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
            Self::Freedesktop {
                data_dirs: super::desktop::data_dirs(
                    std::env::var_os("XDG_DATA_HOME"),
                    std::env::home_dir(),
                    std::env::var_os("XDG_DATA_DIRS"),
                ),
            }
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
pub(crate) fn open(platform: &Platform, link: &str, runner: &mut dyn Runner) -> Opening {
    match platform {
        Platform::MacOs => launched(
            runner.launch("open", &["-b", BUNDLE_ID, link]),
            format!("no application has the bundle identifier {BUNDLE_ID}"),
        ),
        Platform::Freedesktop { data_dirs } => freedesktop(data_dirs, link, runner),
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

/// The program the registered desktop entry runs, started with the link.
/// `xdg-open` is never run: without a display its generic path skips the
/// registered handler and tries web browsers.
fn freedesktop(data_dirs: &[PathBuf], link: &str, runner: &mut dyn Runner) -> Opening {
    use super::desktop;
    let kind = format!("x-scheme-handler/{SCHEME}");
    let answer = runner.query("xdg-mime", &["query", "default", kind.as_str()]);
    let id = match &answer {
        Ok(Exit::Exited {
            success: true,
            stdout,
        }) => desktop::queried_id(stdout),
        _ => None,
    };
    let Some(id) = id else {
        return Opening::Unavailable(format!(
            "no application is registered for {SCHEME} links \
             (`xdg-mime query default {kind}` names none)"
        ));
    };
    let Some(path) = desktop::find_entry(id, data_dirs) else {
        return Opening::Unavailable(format!(
            "{id}, the handler xdg-mime names for {SCHEME} links, is in no \
             applications directory of XDG_DATA_HOME or XDG_DATA_DIRS"
        ));
    };
    let command =
        match desktop::read_entry(&path).and_then(|entry| desktop::command_line(&entry, link)) {
            Ok(command) => command,
            Err(why) => {
                return Opening::Unavailable(format!(
                    "the desktop entry {} for {SCHEME} links {why}",
                    path.display()
                ));
            }
        };
    let (program, args) = command
        .split_first()
        .expect("a command line names its program");
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    launched(
        runner.launch(program, &args),
        format!("{program}, the program {id} runs for {SCHEME} links, could not open the link"),
    )
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
        let Some(status) = wait(&mut child, QUERY_WAIT)? else {
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
        let mut command = Command::new(program);
        command
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        // In its own process group, as a desktop launcher starts an
        // application, so a signal to this terminal job does not reach it.
        #[cfg(unix)]
        std::os::unix::process::CommandExt::process_group(&mut command, 0);
        let mut child = command.spawn()?;
        // A handler still running has the link; it is left to run.
        Ok(match wait(&mut child, LAUNCH_WAIT)? {
            Some(status) => Exit::Exited {
                success: status.success(),
                stdout: String::new(),
            },
            None => Exit::Running,
        })
    }
}

fn wait(child: &mut Child, bound: Duration) -> io::Result<Option<ExitStatus>> {
    let deadline = Instant::now() + bound;
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
            assert_eq!(open(&Platform::MacOs, LINK, &mut runner), Opening::Opened);
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
            let reason = unavailable(open(&Platform::MacOs, LINK, &mut runner));
            assert!(reason.contains(BUNDLE_ID), "{reason}");
            assert_eq!(runner.calls.len(), 1);
        }
    }

    /// The program Studio's desktop entry runs in these tests; the recorded
    /// runner never executes it.
    const HANDLER: &str = "/opt/omnigraph-studio/bin/omnigraph-studio";

    /// A freedesktop platform whose one data directory holds
    /// `applications/omnigraph-studio.desktop` with `exec`, when given.
    fn freedesktop(exec: Option<&str>) -> (tempfile::TempDir, Platform) {
        let dir = tempfile::tempdir().unwrap();
        if let Some(exec) = exec {
            let applications = dir.path().join("applications");
            std::fs::create_dir(&applications).unwrap();
            std::fs::write(
                applications.join("omnigraph-studio.desktop"),
                format!(
                    "[Desktop Entry]\nType=Application\nName=OmniGraph Studio\nExec={exec}\n\
                     MimeType=x-scheme-handler/omnigraph-studio;\n"
                ),
            )
            .unwrap();
        }
        let platform = Platform::Freedesktop {
            data_dirs: vec![dir.path().to_owned()],
        };
        (dir, platform)
    }

    /// The handler `xdg-mime` names is started from its desktop entry's
    /// `Exec`, with the link in place of `%u`. `xdg-open` never runs, so no
    /// web browser can stand in for Studio: the recorded runner refuses any
    /// command it was not given an answer for.
    #[test]
    fn freedesktop_runs_the_registered_handler_and_no_generic_opener() {
        let query = argv(&[
            "xdg-mime",
            "query",
            "default",
            "x-scheme-handler/omnigraph-studio",
        ]);
        let named = || exited(true, "omnigraph-studio.desktop\n");
        let (_dir, platform) = freedesktop(Some(&format!("\"{HANDLER}\" --open %u")));
        for answer in [exited(true, ""), Ok(Exit::Running)] {
            let mut runner = Recorded::answering(vec![("xdg-mime", named()), (HANDLER, answer)]);
            assert_eq!(open(&platform, LINK, &mut runner), Opening::Opened);
            assert_eq!(
                runner.calls,
                [query.clone(), argv(&[HANDLER, "--open", LINK])]
            );
        }
        let mut runner =
            Recorded::answering(vec![("xdg-mime", named()), (HANDLER, exited(false, ""))]);
        let reason = unavailable(open(&platform, LINK, &mut runner));
        assert!(reason.contains(HANDLER), "{reason}");

        // Nothing registered: only the query runs.
        for answer in [
            exited(true, ""),
            exited(true, " \n"),
            exited(false, ""),
            missing(),
        ] {
            let mut runner = Recorded::answering(vec![("xdg-mime", answer)]);
            let reason = unavailable(open(&platform, LINK, &mut runner));
            assert!(reason.contains("names none"), "{reason}");
            assert_eq!(runner.calls, std::slice::from_ref(&query));
        }
        // A registration whose desktop entry is missing, or does not take a
        // URL, runs nothing either.
        for (exec, why) in [
            (None, "in no applications directory"),
            (Some(format!("{HANDLER} %f")), "exactly one URL"),
            (Some(format!("{HANDLER} --url=%u")), "field code inside"),
        ] {
            let (_dir, platform) = freedesktop(exec.as_deref());
            let mut runner = Recorded::answering(vec![("xdg-mime", named())]);
            let reason = unavailable(open(&platform, LINK, &mut runner));
            assert!(reason.contains(why), "{reason}");
            assert_eq!(runner.calls, std::slice::from_ref(&query));
        }
    }

    /// The registration query is recorded; the launch is real, of a handler
    /// script the desktop entry names by its absolute path.
    #[cfg(unix)]
    struct RealLaunch(Recorded);

    #[cfg(unix)]
    impl Runner for RealLaunch {
        fn query(&mut self, program: &str, args: &[&str]) -> io::Result<Exit> {
            self.0.query(program, args)
        }
        fn launch(&mut self, program: &str, args: &[&str]) -> io::Result<Exit> {
            SystemRunner.launch(program, args)
        }
    }

    /// The program the desktop entry names really runs, receives the link as
    /// one argument, and decides the outcome by its exit.
    #[cfg(unix)]
    #[test]
    fn freedesktop_launches_the_program_its_desktop_entry_names_with_the_link() {
        use std::os::unix::fs::PermissionsExt;
        let bin = tempfile::tempdir().unwrap();
        let received = bin.path().join("received");
        for (name, exit) in [("handler", 0), ("refusing-handler", 1)] {
            let script = bin.path().join(name);
            std::fs::write(
                &script,
                format!(
                    "#!/bin/sh\nfor arg in \"$@\"; do printf '%s\\n' \"$arg\"; done > '{}'\nexit {exit}\n",
                    received.display()
                ),
            )
            .unwrap();
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
            // A platform may hold a new executable's first run for a malware
            // scan longer than the opener waits; run each once, unbounded.
            std::process::Command::new(&script).status().unwrap();
        }
        for (name, opened) in [("handler", true), ("refusing-handler", false)] {
            let _ = std::fs::remove_file(&received);
            let exec = format!("\"{}\" --link %u", bin.path().join(name).display());
            let (_dir, platform) = freedesktop(Some(&exec));
            let mut runner = RealLaunch(Recorded::answering(vec![(
                "xdg-mime",
                exited(true, "omnigraph-studio.desktop\n"),
            )]));
            let opening = open(&platform, LINK, &mut runner);
            assert_eq!(opening == Opening::Opened, opened, "{name}: {opening:?}");
            assert_eq!(
                std::fs::read_to_string(&received).unwrap(),
                format!("--link\n{LINK}\n")
            );
        }
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
        assert_eq!(open(&Platform::Windows, LINK, &mut runner), Opening::Opened);
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
            let reason = unavailable(open(&Platform::Windows, LINK, &mut runner));
            assert!(reason.contains("URL protocol"), "{reason}");
            assert_eq!(runner.calls, asked);
        }
    }

    #[test]
    fn an_unsupported_platform_runs_nothing() {
        let mut runner = Recorded::default();
        unavailable(open(&Platform::Unsupported, LINK, &mut runner));
        assert!(runner.calls.is_empty());
    }
}
