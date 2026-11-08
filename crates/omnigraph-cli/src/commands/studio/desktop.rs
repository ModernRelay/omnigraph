//! Studio's registered handler on a freedesktop.org desktop: the desktop
//! entry `xdg-mime` names for the link scheme, found as the Desktop Entry
//! Specification finds a desktop file ID, and the command line its `Exec`
//! key gives for one URL. The opener runs that command itself. It never runs
//! `xdg-open`, whose generic path skips the registered handler when there is
//! no display and hands the link to a web browser.

use std::ffi::OsString;
use std::io::Read;
use std::path::{Path, PathBuf};

/// The most bytes of a desktop entry that are read.
const ENTRY_MAX_BYTES: u64 = 64 * 1024;
/// The most paths one desktop file ID is looked up as.
const CANDIDATES_MAX: usize = 64;
/// Characters the specification allows in an argument only inside quotes.
const RESERVED: &[char] = &[
    '\t', '\n', '"', '\'', '\\', '>', '<', '~', '|', '&', ';', '$', '*', '?', '#', '(', ')', '`',
];

/// The base directories desktop entries are looked up in, most important
/// first: `$XDG_DATA_HOME` (default `~/.local/share`), then each entry of
/// `$XDG_DATA_DIRS` (default `/usr/local/share:/usr/share`). An empty
/// variable is unset, and a relative path is ignored.
pub(crate) fn data_dirs(
    data_home: Option<OsString>,
    home: Option<PathBuf>,
    data_dirs: Option<OsString>,
) -> Vec<PathBuf> {
    let user = data_home
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .or_else(|| {
            home.filter(|home| home.is_absolute())
                .map(|home| home.join(".local/share"))
        });
    let system = data_dirs
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "/usr/local/share:/usr/share".into());
    user.into_iter()
        .chain(std::env::split_paths(&system).filter(|path| path.is_absolute()))
        .collect()
}

/// The first desktop file ID in `xdg-mime query default`'s answer.
pub(crate) fn queried_id(stdout: &str) -> Option<&str> {
    let id = stdout.trim().split(';').next()?.trim();
    (!id.is_empty()).then_some(id)
}

/// The file a desktop file ID names, from the most important base directory
/// that holds one. The ID is the file's path below `applications` with each
/// `/` written `-`, so each `-` is tried as a subdirectory as well.
pub(crate) fn find_entry(id: &str, data_dirs: &[PathBuf]) -> Option<PathBuf> {
    if !valid_id(id) {
        return None;
    }
    let candidates = candidates(id);
    data_dirs.iter().find_map(|dir| {
        let applications = dir.join("applications");
        candidates
            .iter()
            .map(|candidate| applications.join(candidate))
            .find(|path| path.is_file())
    })
}

fn valid_id(id: &str) -> bool {
    id.len() > ".desktop".len()
        && id.ends_with(".desktop")
        && !id.starts_with('.')
        && !id.chars().any(|c| c == '/' || c == '\\' || c.is_control())
        && id.split('-').all(|part| part != "." && part != "..")
}

/// `vendor-app.desktop` itself first, then with each `-` in turn read as a
/// subdirectory: `vendor/app.desktop`.
fn candidates(id: &str) -> Vec<PathBuf> {
    fn split(rest: &str, prefix: &Path, found: &mut Vec<PathBuf>) {
        for (at, _) in rest.match_indices('-') {
            let (dir, tail) = (&rest[..at], &rest[at + 1..]);
            if dir.is_empty() || tail.is_empty() || found.len() >= CANDIDATES_MAX {
                continue;
            }
            let base = prefix.join(dir);
            found.push(base.join(tail));
            split(tail, &base, found);
        }
    }
    let mut found = vec![PathBuf::from(id)];
    split(id, Path::new(""), &mut found);
    found
}

/// The desktop entry's text, bounded and UTF-8 as the specification requires.
pub(crate) fn read_entry(path: &Path) -> Result<String, String> {
    let file = std::fs::File::open(path).map_err(|error| format!("cannot be read: {error}"))?;
    let mut bytes = Vec::new();
    file.take(ENTRY_MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("cannot be read: {error}"))?;
    if bytes.len() as u64 > ENTRY_MAX_BYTES {
        return Err("exceeds 64 KiB".into());
    }
    String::from_utf8(bytes).map_err(|_| "is not UTF-8".into())
}

/// The program and arguments the entry's `Exec` key runs for `link`: the
/// link replaces the one `%u` or `%U`, the other field codes are dropped,
/// and quoting is undone. An entry the opener cannot run exactly as written,
/// or that does not take a URL, is refused.
pub(crate) fn command_line(entry: &str, link: &str) -> Result<Vec<String>, String> {
    let keys = desktop_entry_keys(entry)?;
    let key = |name: &str| {
        keys.iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    };
    if key("Hidden") == Some("true") {
        return Err("is hidden".into());
    }
    if key("Type").is_some_and(|kind| kind != "Application") {
        return Err("is not an application".into());
    }
    if key("Terminal") == Some("true") {
        return Err("runs in a terminal".into());
    }
    let exec = key("Exec").ok_or("has no Exec key")?;
    let mut command = Vec::new();
    let mut urls = 0;
    let mut files = 0;
    for (index, (argument, quoted)) in arguments(&unescape(exec))?.into_iter().enumerate() {
        if !quoted {
            let code = match argument.as_str() {
                "%u" | "%U" => {
                    urls += 1;
                    Some(Some(link.to_owned()))
                }
                "%f" | "%F" => {
                    files += 1;
                    Some(None)
                }
                "%i" | "%c" | "%k" | "%d" | "%D" | "%n" | "%N" | "%v" | "%m" => Some(None),
                _ => None,
            };
            if let Some(expansion) = code {
                if index == 0 {
                    return Err("names no program in its Exec key".into());
                }
                command.extend(expansion);
                continue;
            }
        }
        command.push(literal(&argument)?);
    }
    if urls != 1 || files != 0 {
        return Err("does not take exactly one URL through %u or %U in its Exec key".into());
    }
    let program = command.first().ok_or("has an empty Exec key")?;
    if program.is_empty() || (program.contains('/') && !Path::new(program).is_absolute()) {
        return Err("names its program by a relative path".into());
    }
    Ok(command)
}

/// The keys of the `[Desktop Entry]` group, refusing one given twice.
fn desktop_entry_keys(entry: &str) -> Result<Vec<(String, String)>, String> {
    let mut group: Option<&str> = None;
    let mut seen_group = false;
    let mut keys: Vec<(String, String)> = Vec::new();
    for line in entry.lines() {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.trim().is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(name) = line
            .strip_prefix('[')
            .and_then(|rest| rest.strip_suffix(']'))
        {
            group = Some(name);
            seen_group |= name == "Desktop Entry";
            continue;
        }
        if group != Some("Desktop Entry") {
            continue;
        }
        let (key, value) = line.split_once('=').ok_or("has a line that is not a key")?;
        let (key, value) = (key.trim_end(), value.trim_start());
        if keys.iter().any(|(seen, _)| seen == key) {
            return Err(format!("gives {key} twice"));
        }
        keys.push((key.to_owned(), value.to_owned()));
    }
    if !seen_group {
        return Err("has no [Desktop Entry] group".into());
    }
    Ok(keys)
}

/// A string value's escapes, undone before the `Exec` quoting rules apply. An
/// escape the string type does not define stays, for the quoting rules.
fn unescape(value: &str) -> String {
    let mut unescaped = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            unescaped.push(c);
            continue;
        }
        match chars.next() {
            Some('s') => unescaped.push(' '),
            Some('n') => unescaped.push('\n'),
            Some('t') => unescaped.push('\t'),
            Some('r') => unescaped.push('\r'),
            Some('\\') => unescaped.push('\\'),
            Some(other) => {
                unescaped.push('\\');
                unescaped.push(other);
            }
            None => unescaped.push('\\'),
        }
    }
    unescaped
}

/// The `Exec` value's arguments, each with whether it was quoted. Arguments
/// are separated by spaces; a quoted one is quoted whole, and inside it only
/// `"`, `` ` ``, `$` and `\` are escaped, each with a backslash.
fn arguments(exec: &str) -> Result<Vec<(String, bool)>, String> {
    let mut arguments = Vec::new();
    let mut chars = exec.chars().peekable();
    loop {
        while chars.next_if_eq(&' ').is_some() {}
        let Some(&first) = chars.peek() else { break };
        let mut argument = String::new();
        if first == '"' {
            chars.next();
            loop {
                match chars.next() {
                    None => return Err("has an unterminated quoted argument".into()),
                    Some('"') => break,
                    Some('\\') => match chars.next() {
                        Some(c @ ('"' | '`' | '$' | '\\')) => argument.push(c),
                        _ => return Err("has an escape a quoted argument cannot hold".into()),
                    },
                    Some('`' | '$') => {
                        return Err("has an unescaped ` or $ in a quoted argument".into());
                    }
                    Some(c) => argument.push(c),
                }
            }
            if chars.peek().is_some_and(|&c| c != ' ') {
                return Err("quotes only part of an argument".into());
            }
            arguments.push((argument, true));
        } else {
            while let Some(c) = chars.next_if(|&c| c != ' ') {
                if RESERVED.contains(&c) {
                    return Err(format!("has `{c}` outside quotes in its Exec key"));
                }
                argument.push(c);
            }
            arguments.push((argument, false));
        }
    }
    Ok(arguments)
}

/// An argument with `%%` written as `%`. Any other `%` is a field code inside
/// an argument, whose expansion the specification leaves undefined.
fn literal(argument: &str) -> Result<String, String> {
    let mut literal = String::with_capacity(argument.len());
    let mut chars = argument.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            literal.push(c);
        } else if chars.next() == Some('%') {
            literal.push('%');
        } else {
            return Err(format!(
                "has a field code inside the argument `{argument}` in its Exec key"
            ));
        }
    }
    Ok(literal)
}

#[cfg(test)]
mod tests {
    use super::*;

    const LINK: &str = "omnigraph-studio://open?target=staging&binding=sha256:b&home=sha256:h";

    fn entry(exec: &str) -> String {
        format!("[Desktop Entry]\nType=Application\nName=OmniGraph Studio\nExec={exec}\n")
    }

    fn run(exec: &str) -> Result<Vec<String>, String> {
        command_line(&entry(exec), LINK)
    }

    fn argv(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|part| (*part).to_owned()).collect()
    }

    #[cfg(unix)]
    #[test]
    fn data_dirs_follow_the_base_directory_specification() {
        let home = Some(PathBuf::from("/home/me"));
        assert_eq!(
            data_dirs(None, home.clone(), None),
            [
                PathBuf::from("/home/me/.local/share"),
                "/usr/local/share".into(),
                "/usr/share".into()
            ]
        );
        assert_eq!(
            data_dirs(Some("".into()), home.clone(), Some("".into())),
            data_dirs(None, home.clone(), None)
        );
        assert_eq!(
            data_dirs(
                Some("/data/home".into()),
                home.clone(),
                Some("/opt/share:relative:/srv/share".into())
            ),
            [
                PathBuf::from("/data/home"),
                "/opt/share".into(),
                "/srv/share".into()
            ]
        );
        // A relative XDG_DATA_HOME is ignored, as if it were unset.
        assert_eq!(
            data_dirs(Some("relative".into()), home, Some("/opt/share".into())),
            [PathBuf::from("/home/me/.local/share"), "/opt/share".into()]
        );
    }

    #[test]
    fn a_desktop_file_id_is_found_in_the_most_important_directory_and_in_subdirectories() {
        let root = tempfile::tempdir().unwrap();
        let (user, system) = (root.path().join("user"), root.path().join("system"));
        let write = |dir: &Path, relative: &str| {
            let path = dir.join("applications").join(relative);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, entry("/bin/true %u")).unwrap();
            path
        };
        let dirs = [user.clone(), system.clone()];
        assert_eq!(find_entry("omnigraph-studio.desktop", &dirs), None);
        let nested = write(&system, "omnigraph/studio.desktop");
        assert_eq!(find_entry("omnigraph-studio.desktop", &dirs), Some(nested));
        let flat = write(&system, "omnigraph-studio.desktop");
        assert_eq!(find_entry("omnigraph-studio.desktop", &dirs), Some(flat));
        let user_entry = write(&user, "omnigraph-studio.desktop");
        assert_eq!(
            find_entry("omnigraph-studio.desktop", &dirs),
            Some(user_entry)
        );
        let deep = write(&system, "com/modernrelay/studio.desktop");
        assert_eq!(
            find_entry("com-modernrelay-studio.desktop", &dirs),
            Some(deep)
        );
        for id in [
            "",
            ".desktop",
            "studio",
            "../studio.desktop",
            "..-studio.desktop",
            "a/b.desktop",
            ".hidden.desktop",
        ] {
            assert_eq!(find_entry(id, &dirs), None, "{id}");
        }
        assert_eq!(
            queried_id("omnigraph-studio.desktop;other.desktop\n"),
            Some("omnigraph-studio.desktop")
        );
        assert_eq!(queried_id(" \n"), None);
    }

    #[test]
    fn exec_runs_its_program_with_the_link_for_its_url_field_code() {
        for (exec, expected) in [
            (
                "/opt/studio/bin/omnigraph-studio %u",
                argv(&["/opt/studio/bin/omnigraph-studio", LINK]),
            ),
            ("omnigraph-studio %U", argv(&["omnigraph-studio", LINK])),
            // Other field codes are dropped and `%%` is a literal percent.
            (
                "studio --open %u %i %c %k --rate=100%%",
                argv(&["studio", "--open", LINK, "--rate=100%"]),
            ),
            // Quoting is undone: escapes of the string type apply first.
            (
                r#""/opt/Omni Graph/studio" "a \\$b \\"c\\" \\\\d" %u"#,
                argv(&["/opt/Omni Graph/studio", r#"a $b "c" \d"#, LINK]),
            ),
            // `\s` is a space, which separates arguments outside quotes.
            (
                r"/opt/omni\sgraph/studio %u",
                argv(&["/opt/omni", "graph/studio", LINK]),
            ),
            ("  studio   %u  ", argv(&["studio", LINK])),
        ] {
            assert_eq!(run(exec).unwrap(), expected, "{exec}");
        }
        let crlf = "# comment\r\n[Desktop Entry]\r\nExec = studio %u\r\n[Desktop Action new]\r\nExec=other\r\n";
        assert_eq!(command_line(crlf, LINK).unwrap(), argv(&["studio", LINK]));
    }

    #[test]
    fn an_entry_the_opener_cannot_run_as_written_is_refused() {
        for (exec, why) in [
            ("studio", "exactly one URL"),
            ("studio %f", "exactly one URL"),
            ("studio %u %f", "exactly one URL"),
            ("studio %u %U", "exactly one URL"),
            ("studio --url=%u", "field code inside"),
            (r#"studio "%u""#, "field code inside"),
            ("studio %x %u", "field code inside"),
            ("%u studio", "names no program"),
            ("bin/studio %u", "relative path"),
            ("studio > log %u", "outside quotes"),
            ("studio $HOME %u", "outside quotes"),
            (r#""studio %u"#, "unterminated"),
            (r#""studio"x %u"#, "part of an argument"),
            (r#""studio $x" %u"#, "unescaped"),
            (r#""studio \\q" %u"#, "cannot hold"),
            ("", "exactly one URL"),
        ] {
            let error = run(exec).unwrap_err();
            assert!(error.contains(why), "{exec}: {error}");
        }
        for (text, why) in [
            ("[Desktop Entry]\nType=Application\n", "no Exec"),
            ("[Other]\nExec=studio %u\n", "no [Desktop Entry]"),
            (
                "[Desktop Entry]\nExec=studio %u\nExec=other %u\n",
                "Exec twice",
            ),
            ("[Desktop Entry]\nExec=studio %u\nHidden=true\n", "hidden"),
            (
                "[Desktop Entry]\nExec=studio %u\nTerminal=true\n",
                "terminal",
            ),
            (
                "[Desktop Entry]\nType=Link\nExec=studio %u\n",
                "not an application",
            ),
            ("[Desktop Entry]\nExec studio %u\n", "not a key"),
        ] {
            let error = command_line(text, LINK).unwrap_err();
            assert!(error.contains(why), "{text}: {error}");
        }
    }

    #[test]
    fn an_entry_is_read_bounded_and_as_utf8() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("e.desktop");
        std::fs::write(&path, entry("studio %u")).unwrap();
        assert!(read_entry(&path).unwrap().contains("Exec=studio %u"));
        std::fs::write(&path, vec![b'#'; ENTRY_MAX_BYTES as usize + 1]).unwrap();
        assert!(read_entry(&path).unwrap_err().contains("64 KiB"));
        std::fs::write(&path, [0xff, 0xfe]).unwrap();
        assert!(read_entry(&path).unwrap_err().contains("UTF-8"));
        assert!(
            read_entry(&dir.path().join("absent.desktop"))
                .unwrap_err()
                .contains("cannot be read")
        );
    }
}
