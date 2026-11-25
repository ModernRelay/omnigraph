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
    /// List the organizations the signed-in person is an active member of,
    /// marking the selected profile's.
    List,
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
    /// Manage the organization's machine applications: unattended client
    /// identities. A machine has no graph access until applied Cedar names it.
    Machines {
        #[command(subcommand)]
        command: OrganizationMachinesCommand,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum OrganizationMembersCommand {
    /// List members, stable identities, service roles and permissions.
    List,
    /// Replace an existing active member's service roles; graph policy is unchanged.
    Set {
        /// Organization member email or exact user ID.
        selector: String,
        /// Existing service-role slug; repeat for every role the member should retain.
        #[arg(long = "role", value_name = "ROLE", required = true)]
        roles: Vec<String>,
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
        /// Existing service-role slug; required for new invitations.
        #[arg(long)]
        role: Option<String>,
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

#[derive(Debug, Subcommand)]
pub(crate) enum OrganizationMachinesCommand {
    /// List machines with their client IDs, service scopes and Cedar principals.
    List,
    /// Create a machine and its first secret, written only to --secret-file.
    Create {
        /// Display name, 1–256 bytes without surrounding whitespace.
        name: String,
        /// Organization service permission the machine receives; repeat for
        /// each. At most five, each one you hold, never `members_manage`.
        #[arg(long = "scope", value_name = "SCOPE")]
        scopes: Vec<String>,
        /// File that receives the secret, created with owner-only permission.
        #[arg(long, value_name = "PATH")]
        secret_file: std::path::PathBuf,
        /// Replace an existing --secret-file.
        #[arg(long)]
        replace_secret_file: bool,
        /// Add an `oauth_client_credentials` profile NAME for this machine.
        #[arg(long, value_name = "NAME", value_parser = crate::invocation::single_selector("--create-profile"))]
        create_profile: Option<String>,
        /// Replace an existing profile NAME.
        #[arg(long, requires = "create_profile")]
        replace_profile: bool,
        /// Stable request key; retain it if the result is uncertain.
        #[arg(long)]
        idempotency_key: String,
    },
    /// Manage a machine's secrets.
    Secret {
        #[command(subcommand)]
        command: OrganizationMachineSecretCommand,
    },
    /// Retire a machine by exact name or client ID. Issued tokens last until
    /// they expire, and Cedar policy naming it is unchanged.
    Revoke {
        /// Machine name or exact client ID.
        selector: String,
        /// Stable request key; retain it if the result is uncertain.
        #[arg(long)]
        idempotency_key: String,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum OrganizationMachineSecretCommand {
    /// Generate a new secret for a machine profile, write it to the profile's
    /// secret file, prove a token exchange with it, then revoke the old secret.
    Rotate {
        /// The machine's `oauth_client_credentials` profile, whose
        /// client_secret is a `{file: PATH}` source.
        #[arg(long, value_name = "NAME", value_parser = crate::invocation::single_selector("--machine-profile"))]
        machine_profile: String,
        /// Secret ID to revoke after the proof; repeat for each. Required
        /// when the machine had more than one secret before rotation.
        #[arg(long = "revoke", value_name = "SECRET_ID")]
        revoke: Vec<String>,
        /// Stable request key; retain it if the result is uncertain.
        #[arg(long)]
        idempotency_key: String,
    },
}
