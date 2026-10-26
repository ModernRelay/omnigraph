//! Existing organization CLI grammar.
use clap::{Args, Subcommand};

#[derive(Debug, Args)]
pub(crate) struct OrganizationArgs {
    /// Emit the validated API envelope instead of readable text.
    #[arg(long, global = true)]
    pub(crate) json: bool,
}

#[derive(Debug, Subcommand)]
pub(crate) enum OrganizationCommand {
    /// Inspect organization membership or offboard a member.
    Members {
        #[command(subcommand)]
        command: OrganizationMembersCommand,
    },
    /// Manage provider invitations; native permissions remain in cluster policy.
    Invitations {
        #[command(subcommand)]
        command: OrganizationInvitationsCommand,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum OrganizationMembersCommand {
    /// List provider members, stable identities and organization service permissions.
    List,
    /// Remove organization membership and its organization-bound sessions.
    /// Deploy Cedar revocation and verify old graph credentials first.
    Offboard {
        user_id: String,
        /// Exact current membership ID, or `absent` after observing no membership.
        #[arg(long)]
        expected_membership: String,
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
