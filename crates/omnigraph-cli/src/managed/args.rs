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
    /// List members, set service roles or offboard a member.
    Members {
        #[command(subcommand)]
        command: OrganizationMembersCommand,
    },
    /// Manage organization invitations; native permissions remain in cluster policy.
    Invitations {
        #[command(subcommand)]
        command: OrganizationInvitationsCommand,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum OrganizationMembersCommand {
    /// List members, stable identities, service roles and permissions.
    List,
    /// Set an existing active member's service role; graph policy is unchanged.
    Set {
        /// Organization member email or exact user ID.
        selector: String,
        /// Existing organization service-role slug.
        #[arg(long)]
        role: String,
        /// Stable request key; retain it if the result is uncertain.
        #[arg(long)]
        idempotency_key: String,
    },
    /// Remove organization membership and its organization-bound sessions.
    /// Deploy Cedar revocation and verify old graph credentials first.
    Offboard {
        /// Organization member email or exact user ID.
        selector: String,
        /// Assert an exact membership ID, or `absent` with an exact user ID.
        #[arg(long)]
        expected_membership: Option<String>,
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
    /// Resend a pending invitation by email or exact invitation ID.
    Resend {
        selector: String,
        #[arg(long)]
        idempotency_key: String,
    },
    /// Revoke a pending invitation by email or exact invitation ID.
    Revoke {
        selector: String,
        #[arg(long)]
        idempotency_key: String,
    },
}
