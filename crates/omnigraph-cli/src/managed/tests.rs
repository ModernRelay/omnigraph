use super::*;
use clap::{CommandFactory, FromArgMatches, Parser};

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
        assert!(
            Cli::try_parse_from([
                "omnigraph",
                "cluster",
                "plan",
                "--managed",
                "--timeout",
                value
            ])
            .is_err()
        );
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

fn parse_cluster(args: &[&str]) -> std::result::Result<Cli, clap::Error> {
    let matches = Cli::command().try_get_matches_from(args)?;
    let cli = Cli::from_arg_matches(&matches)?;
    let mut command_matches = &matches;
    while let Some((_, sub_matches)) = command_matches.subcommand() {
        command_matches = sub_matches;
    }
    crate::validate_cluster_arguments(&cli, command_matches)?;
    Ok(cli)
}

#[tokio::test]
async fn managed_flag_is_explicit_and_local_commands_ignore_context() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join(".omnigraph")).unwrap();
    std::fs::write(dir.path().join(".omnigraph/context"), "malformed").unwrap();
    let config = dir.path().to_str().unwrap();
    for args in [
        vec![
            "omnigraph",
            "cluster",
            "status",
            "--managed",
            "--config",
            config,
        ],
        vec![
            "omnigraph",
            "cluster",
            "--managed",
            "status",
            "--config",
            config,
        ],
    ] {
        let managed = parse_cluster(&args).unwrap();
        let output = dispatch(&managed).await.unwrap();
        assert_eq!(output.exit, 2);
        assert_eq!(output.body["type"], "context_invalid");
    }
    for verb in ["validate", "plan", "apply", "status", "observe"] {
        let local = parse_cluster(&["omnigraph", "cluster", verb, "--config", config]).unwrap();
        assert!(dispatch(&local).await.is_none(), "{verb}");
    }
    // Conflicting selectors refuse before the deliberately malformed context is read.
    for extra in [
        vec!["--direct"],
        vec!["--as", "actor"],
        vec!["--server", "https://data.example"],
        vec!["--graph", "knowledge"],
        vec!["--profile", "prod"],
        vec!["--store", "file:///unused-store"],
        vec!["--cluster", "file:///unused-cluster"],
    ] {
        let args = [
            "omnigraph",
            "cluster",
            "status",
            "--managed",
            "--config",
            config,
        ]
        .into_iter()
        .chain(extra.iter().copied())
        .collect::<Vec<_>>();
        let cli = parse_cluster(&args).unwrap();
        let output = dispatch(&cli).await.unwrap();
        assert_eq!(output.exit, 2, "{extra:?}");
        assert_eq!(output.body["type"], "managed_scope_conflict", "{extra:?}");
    }
    for (extra, kind) in [
        (vec!["--graph", "knowledge"], "token_profile_conflict"),
        (
            vec!["--graph", "knowledge", "--clear"],
            "token_clear_conflict",
        ),
    ] {
        let args = [
            "omnigraph",
            "cluster",
            "token",
            "--managed",
            "--config",
            config,
        ]
        .into_iter()
        .chain(extra)
        .collect::<Vec<_>>();
        let cli = parse_cluster(&args).unwrap();
        let output = dispatch(&cli).await.unwrap();
        assert_eq!(output.exit, 2);
        assert_eq!(output.body["type"], kind);
    }
    for args in [
        vec![
            "cluster",
            "create",
            "demo",
            "--api",
            "https://control.example",
        ],
        vec!["cluster", "apply", "--plan", "saved-plan"],
        vec!["cluster", "plan", "--rev", "revision"],
        vec!["cluster", "status", "run-id"],
        vec!["cluster", "status", "--operation", "operation-id"],
        vec![
            "cluster",
            "push",
            "--expected-revision",
            "rev",
            "--message",
            "update",
        ],
        vec!["cluster", "delete", "--incarnation", "inc-one"],
        vec![
            "cluster",
            "undo-delete",
            "--incarnation",
            "inc-one",
            "--deletion-id",
            "delete-one",
        ],
        vec!["cluster", "token"],
        vec!["cluster", "operation", "op-id"],
        vec!["cluster", "history"],
        vec!["cluster", "cancel", "run-id"],
        vec!["cluster", "apply", "--managed"],
        vec![
            "cluster",
            "apply",
            "--managed",
            "--plan",
            "saved-plan",
            "--deployment-id",
            "id",
        ],
        vec!["cluster", "status", "--managed", "--deployment-id", "id"],
        vec!["cluster", "status", "--managed", "--operation", "id"],
        vec!["cluster", "validate", "--managed"],
        vec!["cluster", "observe", "--managed"],
        vec!["cluster", "force-unlock", "lock-id", "--managed"],
        vec![
            "cluster",
            "upgrade-ledger",
            "--writers-stopped",
            "--managed",
        ],
        vec!["managed", "plan"],
        vec!["managed", "operation", "op-id"],
    ] {
        let args = std::iter::once("omnigraph").chain(args).collect::<Vec<_>>();
        assert!(parse_cluster(&args).is_err(), "accepted {args:?}");
    }
    for args in [
        vec!["omnigraph", "cluster", "operation", "op-id", "--managed"],
        vec![
            "omnigraph",
            "cluster",
            "plan",
            "--managed",
            "--rev",
            "revision",
        ],
        vec![
            "omnigraph",
            "cluster",
            "apply",
            "--managed",
            "--plan",
            "saved-plan",
        ],
        vec!["omnigraph", "cluster", "status", "--managed", "run-id"],
    ] {
        assert!(parse_cluster(&args).is_ok(), "refused {args:?}");
    }
}
