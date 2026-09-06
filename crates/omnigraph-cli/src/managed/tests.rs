use super::*;
use clap::Parser;

#[test]
fn origins_are_canonical_and_credentials_cannot_change_destination() {
    for (input, expected) in [
        ("https://CONTROL.example:443/", "https://control.example"),
        ("http://127.0.0.1:3000", "http://127.0.0.1:3000"),
        ("http://[::1]:3000/", "http://[::1]:3000"),
    ] {
        assert_eq!(canonical_origin(input).unwrap(), expected);
    }
    for bad in [
        "http://control.example",
        "http://127.0.0.2",
        "http://localhost.evil",
        "https://user@control.example",
        "https://control.example/path",
        "https://control.example?x",
        "https://control.example#x",
        "file:///tmp/root",
    ] {
        assert!(canonical_origin(bad).is_err(), "accepted {bad}");
    }
    for bad in ["", "../other", "run?x", "run#x", "run:cancel", "run/other"] {
        assert!(identifier(bad).is_err());
    }
}

#[test]
fn managed_outcomes_preserve_public_exit_contract() {
    for (state, expected) in [
        ("converged", 0),
        ("failed", 1),
        ("refused", 2),
        ("blocked", 2),
        ("partially_converged", 3),
        ("recovery_required", 4),
        ("stalled", 5),
        ("cancelled", 6),
    ] {
        assert_eq!(outcome_exit(state).unwrap(), Some(expected));
    }
    for state in ["proposed", "offered", "running"] {
        assert_eq!(outcome_exit(state).unwrap(), None);
    }
    assert!(outcome_exit("done").is_err());
}

#[test]
fn legacy_login_and_managed_login_are_exclusive() {
    assert!(Cli::try_parse_from(["omnigraph", "login", "prod", "--token", "legacy"]).is_ok());
    assert!(
        Cli::try_parse_from(["omnigraph", "login", "--api", "https://control.example"]).is_ok()
    );
    for args in [
        vec!["login"],
        vec!["login", "prod", "--api", "https://control.example"],
        vec![
            "login",
            "--api",
            "https://control.example",
            "--token",
            "legacy",
        ],
        vec!["logout", "prod", "--api", "https://control.example"],
    ] {
        assert!(Cli::try_parse_from(std::iter::once("omnigraph").chain(args)).is_err());
    }
    for value in ["0", "3601"] {
        assert!(Cli::try_parse_from(["omnigraph", "cluster", "plan", "--timeout", value]).is_err());
    }
}

#[test]
fn contexts_are_exact_and_cannot_hide_unknown_authority() {
    let dir = tempfile::tempdir().unwrap();
    let context = Context {
        version: 1,
        cluster: "cluster-a".into(),
        api: "https://control.example".into(),
    };
    save_context(dir.path(), &context).unwrap();
    assert_eq!(
        read_context(dir.path()).unwrap().unwrap().cluster,
        "cluster-a"
    );
    let child = dir.path().join("child");
    std::fs::create_dir(&child).unwrap();
    assert!(read_context(&child).unwrap().is_none());
    std::fs::write(
        dir.path().join(".omnigraph/context"),
        "version: 1\ncluster: cluster-a\napi: https://control.example\nactor: trusted\n",
    )
    .unwrap();
    assert!(read_context(dir.path()).is_err());
}

#[tokio::test]
async fn explicit_direct_is_the_only_context_override() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join(".omnigraph")).unwrap();
    std::fs::write(dir.path().join(".omnigraph/context"), "malformed").unwrap();
    let config = dir.path().to_str().unwrap();
    let managed = Cli::try_parse_from([
        "omnigraph",
        "cluster",
        "status",
        "--config",
        config,
        "--json",
    ])
    .unwrap();
    assert_eq!(dispatch(&managed).await.unwrap().exit, 2);
    let direct = Cli::try_parse_from([
        "omnigraph",
        "cluster",
        "status",
        "--config",
        config,
        "--direct",
    ])
    .unwrap();
    assert!(dispatch(&direct).await.is_none());
    let forbidden = Cli::try_parse_from([
        "omnigraph",
        "cluster",
        "apply",
        "--config",
        config,
        "--direct",
        "--plan",
        "plan-id",
    ])
    .unwrap();
    assert_eq!(dispatch(&forbidden).await.unwrap().exit, 2);
}
