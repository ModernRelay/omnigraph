//! Existing organization CLI grammar.
use clap::{Args, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Args)]
pub(crate) struct OrganizationArgs {
    /// Emit the validated API envelope instead of readable text.
    #[arg(long, global = true)]
    pub(crate) json: bool,
}

#[derive(Debug, Subcommand)]
pub(crate) enum OrganizationCommand {
    /// Inspect or change local enrollment, admission and control permissions.
    Members {
        #[command(subcommand)]
        command: OrganizationMembersCommand,
    },
    /// Manage provider invitations; acceptance does not grant cluster access.
    Invitations {
        #[command(subcommand)]
        command: OrganizationInvitationsCommand,
    },
    /// Manage organization-owned workload admission; graph policy stays in Cedar.
    Workloads {
        #[command(subcommand)]
        command: OrganizationWorkloadsCommand,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum OrganizationWorkloadsCommand {
    /// List enrolled and revoked workloads with their current grant versions.
    List,
    /// Enroll an organization-owned application using a versioned YAML patch.
    Enroll {
        file: PathBuf,
        #[arg(long)]
        idempotency_key: String,
    },
    /// Update an enabled workload's local grants using a versioned YAML patch.
    Update {
        file: PathBuf,
        #[arg(long)]
        idempotency_key: String,
    },
    /// Disable local access and clear grants; provider secrets and Cedar are unchanged.
    Revoke {
        client_id: String,
        #[arg(long, value_parser = clap::value_parser!(i64).range(1..))]
        expected_version: i64,
        #[arg(long)]
        idempotency_key: String,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum OrganizationMembersCommand {
    /// List provider members and retained local enrollment, including current versions.
    List,
    /// Apply a versioned YAML add/update/remove patch; graph policy is unchanged.
    Apply {
        file: PathBuf,
        /// Stable request key; retain it if the result is uncertain.
        #[arg(long)]
        idempotency_key: String,
    },
    /// Remove local access, organization membership and its sessions.
    /// Deploy Cedar revocation and verify old graph credentials first.
    Offboard {
        user_id: String,
        /// Current local grant version; zero for an unenrolled person.
        #[arg(long, value_parser = clap::value_parser!(i64).range(0..))]
        expected_version: i64,
        /// Stable request key; retain it if the result is uncertain.
        #[arg(long)]
        idempotency_key: String,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum OrganizationInvitationsCommand {
    /// List one provider page of up to 100 invitations.
    List {
        /// Continue from the previous page's exact cursor.
        #[arg(long)]
        after: Option<String>,
    },
    /// Email an invitation to join this organization.
    Send {
        email: String,
        /// Stable request key; retain it if the result is uncertain.
        #[arg(long)]
        idempotency_key: String,
    },
    /// Resend one exact pending invitation.
    Resend {
        invitation_id: String,
        #[arg(long)]
        idempotency_key: String,
    },
    /// Revoke one exact pending invitation.
    Revoke {
        invitation_id: String,
        #[arg(long)]
        idempotency_key: String,
    },
}
