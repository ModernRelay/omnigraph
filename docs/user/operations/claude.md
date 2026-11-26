# Connect Claude to Omnigraph Cloud

Connect Claude to your Omnigraph Cloud graph. Claude can then answer questions from it, remember things for you, and keep it up to date as you chat. It works in claude.ai, Claude Desktop and Claude Code.

Claude talks directly to your own cluster's [MCP endpoint](mcp.md). Omnigraph runs no service between them, and every request runs with your own permissions.

## Before you start

You need:

- an **Omnigraph Cloud account** and a **cluster** in your organization;
- the cluster's **MCP URL**. In the console, open your cluster and go to the **Connect** tab. The URL looks like `https://<id>-<incarnation>.graphs.platform.omnigraph.dev/mcp`;
- a Claude plan that supports connectors.

**No MCP URL on the Connect tab?** Claude access isn't enabled for that cluster yet. Clusters created before it was available need an administrator to switch it on. Email hello@modernrelay.com: enabling it takes a short, scheduled restart of the cluster and doesn't touch your data or policies.

## Connect

### claude.ai and Claude Desktop

1. Copy your cluster's MCP URL from the console's **Connect** tab.
2. Open [Omnigraph in the Claude directory](https://claude.ai/directory/connectors/omnigraph) and click **Connect**. Until the listing is published, add it as a custom connector instead: in Claude, open **Settings → Connectors**, choose **Add custom connector**, and name it Omnigraph.
3. Paste the URL when Claude asks for it. The directory listing accepts only Omnigraph cluster URLs.
4. Sign in with your Omnigraph account and approve access.

Connectors you add in claude.ai also appear in Claude Desktop.

**Team and Enterprise.** An Owner of your Claude organization can add the connector for everyone. Each member signs in with their own Omnigraph account and can list the cluster's graph names. Your cluster's policy controls what each member can read or change in a graph.

### Claude Code

If you use Claude Code with your claude.ai account, the connector you added there is already available. Otherwise add it directly:

```bash
claude mcp add --transport http omnigraph <your-mcp-url>
```

Claude Code opens a browser for sign-in the first time you use it.

## Try it

- "What graphs do I have, and what's in them?"
- "Who are my close friends, and when did I last see them?"
- "What do I still owe Theo?"
- "Remember that Priya recommended *Thinking in Systems*."
- "Show the tasks due this week, grouped by project."
- "What changed in the graph since Monday?"

Claude reads the graph's schema and its stored queries first, then picks the right query and its parameters. Ask in your own words: you don't need to know the query names.

## Create a second brain

If your cluster has no graphs yet, Claude tells you so. Create one with the Omnigraph CLI from the `second-brain` cookbook:

1. Install the [Omnigraph CLI](../install.md) and [sign in](../cli/login.md): `omnigraph login --create-profile work`.
2. Register your cluster as a target: `omnigraph target add brain --profile work --cluster <cluster-id>`.
3. Pull the cluster's configuration, add the `brain` graph from the [second-brain cookbook](https://github.com/ModernRelay/omnigraph-cookbooks/tree/main/second-brain) (schema, queries and policy), then push, plan and apply it, as in [managed configuration](../cli/managed-lifecycle.md): `omnigraph cluster pull`, `cluster push --expected-revision <rev>`, `cluster plan --rev <rev>`, `cluster apply --plan <preview-id>`.
4. Optionally load the example data: `omnigraph load --target brain --graph brain --data seed.jsonl --mode append`.

Claude Code can run these steps for you: ask it to set up the second brain and point it at the cookbook.

Creating a graph needs permission to manage the cluster's configuration. If you don't have it, ask an organization admin.

## What Claude can do

Claude acts **as you**. Every call runs under your cluster's [policy](policy.md), exactly as if you ran it from the Omnigraph CLI:

- **Read:** the schema, stored queries, graph data, branches and history, as far as your policy allows.
- **Write:** stored and ad hoc mutations, loads, branches and merges, if your policy grants you write access to that graph or branch. Depending on your Claude settings, Claude asks for your approval before it calls a tool that changes data.
- **Not available:** changing the schema, creating or deleting graphs, or managing the cluster. Those stay in the console and CLI.

If Claude can only read, your policy doesn't grant you `change` on that graph. Ask your administrator.

**Limiting what Claude can do.** Claude has exactly the rights of the account it signed in with: the policy can't tell Claude apart from you using the CLI. To give Claude less, sign in to the connector with a separate Omnigraph account, for example an `agent` user in your organization. Then ask your administrator to grant that account only what Claude should do, such as `change` on unprotected branches while people merge into `main`.

Every change Claude makes is a normal commit attributed to the account it signed in with, so you can see it in the graph's history and revert it like any other change.

## Privacy and data flow

- Claude connects straight to your cluster's `/mcp` endpoint over HTTPS. Omnigraph runs no proxy in between.
- Sign-in uses Omnigraph's identity provider. Claude receives a short-lived token that works only for that one cluster.
- What a query returns goes into your Claude conversation and is handled under Anthropic's terms for your Claude plan.
- Disconnect anytime in Claude's connector settings. Your organization can also remove your access in Omnigraph.

What Omnigraph collects and keeps is described in the [privacy policy](https://www.omnigraph.dev/privacy).

## Troubleshooting

| What you see | What it means | What to do |
|---|---|---|
| Claude rejects the URL | It isn't an Omnigraph cluster URL | Copy it again from the **Connect** tab. It must end in `/mcp`, with no trailing slash |
| Sign-in works, then "not admitted" | Your account isn't a member of the cluster's organization | Ask an organization admin to invite you |
| "Forbidden" / 403 on a request | The cluster's policy doesn't allow that action for you | Signing in again won't help. Ask an admin for the permission named in the error |
| "Service unavailable" | Your cluster is restarting, or Claude access is being updated | Wait a minute and try again |
| No graphs listed | The cluster has no registered graphs | See "Create a second brain" or ask an administrator to create a graph |
| "Result too large" | The query returned more than the server sends in one result | Ask for fewer rows or a narrower filter, for example "only this month" |
| Claude keeps asking you to sign in | Your Omnigraph session expired or was revoked | Sign in again. If it repeats, disconnect and reconnect the connector |

Questions or problems: hello@modernrelay.com.
