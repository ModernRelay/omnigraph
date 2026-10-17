//! Blob command input capture and execution against one resolved graph client.
use super::{BlobCommand, TransferScope};
use crate::blob_cli::{self, BlobRangeRequest, DeferredOutputFile};
use crate::client::GraphClient;
use crate::output::{write_blob_stat_human, write_blob_write_human, write_json};
use color_eyre::eyre::Result;
use omnigraph::BlobPrecondition;
use omnigraph_api_types::{BlobReadQuery, BlobWriteQuery};
use std::io::{Read, Write};
use std::path::PathBuf;

pub(crate) enum BlobOperation {
    Get {
        query: BlobReadQuery,
        range: Option<BlobRangeRequest>,
        out: Option<PathBuf>,
    },
    Stat {
        query: BlobReadQuery,
        json: bool,
    },
    Write {
        query: BlobWriteQuery,
        bytes: Option<bytes::Bytes>,
        if_match: Option<String>,
        precondition: Option<BlobPrecondition>,
        json: bool,
    },
}

/// Preserve the CLI's pre-resolution refusal order. This is the explicit input
/// capture boundary: only `put` reads its bounded file/stdin payload here.
pub(crate) fn capture_blob(
    command: BlobCommand,
    stdin: &mut dyn Read,
    stdin_is_terminal: bool,
) -> Result<BlobOperation> {
    Ok(match command {
        BlobCommand::Get {
            entity,
            type_name,
            id,
            property,
            branch,
            snapshot,
            offset,
            length,
            out,
        } => {
            // Reject invalid range arithmetic before opening any graph or server.
            let range = BlobRangeRequest::new(offset, length)?;
            let query =
                blob_cli::blob_query(entity.into(), type_name, id, property, branch, snapshot);
            BlobOperation::Get { query, range, out }
        }
        BlobCommand::Stat {
            entity,
            type_name,
            id,
            property,
            branch,
            snapshot,
            json,
        } => {
            let query =
                blob_cli::blob_query(entity.into(), type_name, id, property, branch, snapshot);
            BlobOperation::Stat { query, json }
        }
        BlobCommand::Put {
            entity,
            type_name,
            id,
            property,
            branch,
            file,
            if_match,
            json,
        } => {
            let precondition = blob_cli::parse_if_match(if_match.as_deref())?;
            let bytes = blob_cli::read_put_input(file.as_deref(), stdin, stdin_is_terminal)?;
            let query = blob_cli::blob_write_query(entity.into(), type_name, id, property, branch);
            BlobOperation::Write {
                query,
                bytes: Some(bytes),
                if_match,
                precondition,
                json,
            }
        }
        BlobCommand::Clear {
            entity,
            type_name,
            id,
            property,
            branch,
            if_match,
            json,
        } => {
            let precondition = blob_cli::parse_if_match(if_match.as_deref())?;
            let query = blob_cli::blob_write_query(entity.into(), type_name, id, property, branch);
            BlobOperation::Write {
                query,
                bytes: None,
                if_match,
                precondition,
                json,
            }
        }
    })
}

pub(crate) async fn run_blob(
    command: BlobCommand,
    scope: TransferScope<'_>,
    stdin: &mut dyn Read,
    stdin_is_terminal: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    let operation = capture_blob(command, stdin, stdin_is_terminal)?;
    let client = match &operation {
        BlobOperation::Get { .. } | BlobOperation::Stat { .. } => {
            GraphClient::resolve(
                scope.operator,
                scope.capability,
                scope.server,
                scope.graph,
                None,
                scope.profile,
                scope.store,
                stderr,
            )
            .await?
        }
        BlobOperation::Write { .. } => {
            GraphClient::resolve_with_policy(
                scope.operator,
                scope.capability,
                scope.server,
                scope.graph,
                None,
                scope.actor,
                scope.profile,
                scope.store,
                stderr,
            )
            .await?
        }
    };
    execute_blob(&client, operation, stdout, stderr).await
}

pub(crate) async fn execute_blob(
    client: &GraphClient,
    operation: BlobOperation,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    match operation {
        BlobOperation::Get { query, range, out } => match out {
            Some(path) => {
                let mut file = DeferredOutputFile::new(path);
                client.blob_get(&query, range, &mut file).await?;
            }
            None => client.blob_get(&query, range, stdout).await?,
        },
        BlobOperation::Stat { query, json } => {
            let output = client.blob_stat(&query).await?;
            if json {
                write_json(&output, stdout)?;
            } else {
                write_blob_stat_human(&output, stdout)?;
            }
        }
        BlobOperation::Write {
            query,
            bytes,
            if_match,
            precondition,
            json,
        } => {
            let output = client
                .blob_write(&query, bytes, if_match.as_deref(), precondition, stderr)
                .await?;
            if json {
                write_json(&output, stdout)?;
            } else {
                write_blob_write_human(&output, stdout)?;
            }
        }
    }
    Ok(())
}
