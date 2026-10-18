use color_eyre::eyre::{Result, bail, eyre};
use omnigraph::db::{InitOptions, Omnigraph};
use std::{fs, io::Write};

use super::InitArgs;
use crate::helpers::ensure_local_graph_parent;

pub(crate) async fn run_init(args: InitArgs, stdout: &mut dyn Write) -> Result<()> {
    let InitArgs { schema, uri, force } = args;
    // RFC-010 Slice 3: graphs inside an established cluster are created
    // by `cluster apply` (which records deployment ledger), not
    // by hand-running `init` into the cluster's storage layout.
    if let Some(root) = omnigraph_cluster::cluster_root_for_graph_uri(&uri)
        .await
        .map_err(|diagnostic| {
            eyre!(
                "could not check cluster ownership for `{}`: {}",
                diagnostic.path,
                diagnostic.message
            )
        })?
    {
        bail!(
            "`{uri}` is inside cluster `{root}`. Graphs in a cluster are created by \
             `cluster apply` (which records the deployment ledger), not `init`. \
             Declare the graph in cluster.yaml and run `cluster apply`."
        );
    }
    let schema_source = fs::read_to_string(&schema)?;
    ensure_local_graph_parent(&uri)?;
    Omnigraph::init_with_options(&uri, &schema_source, InitOptions { force }).await?;
    writeln!(stdout, "initialized {}", uri)?;
    Ok(())
}
