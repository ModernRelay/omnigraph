//! What the bridge admits and how it narrows answers: one graph, and only
//! the tools the server annotates read-only.
//!
//! The bridge adds no tool and runs none: a call it admits reaches the
//! server's own handler, which keeps every refusal. It decides only whether a
//! message is sent, fills the graph a call omits, and removes other graphs
//! from listings.

use std::collections::HashMap;

use serde_json::{Value, json};

use super::wire::INVALID_PARAMS;

/// The resource that lists the server's graphs.
const GRAPHS_RESOURCE: &str = "omnigraph://graphs";
/// The prefix of a resource that names one graph.
const GRAPH_RESOURCE_PREFIX: &str = "omnigraph://graphs/";
/// The tool that lists the server's graphs.
const GRAPHS_TOOL: &str = "graphs";

/// What the server says about one tool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ToolFacts {
    /// Annotated `readOnlyHint: true`.
    read_only: bool,
    /// Its input schema declares a `graph` argument.
    takes_graph: bool,
}

/// What to do with one request.
#[derive(Debug, PartialEq)]
pub(super) enum Admit {
    /// Send it: rewritten when `Some`, else as the agent wrote it.
    Send(Option<Value>),
    /// Answer the agent with this JSON-RPC error instead of sending it.
    Refuse {
        code: i64,
        message: String,
        data: Value,
    },
    /// The server's tools are not known yet: list them, then admit again.
    ListTools,
}

/// The bridge's one target and graph, and the server's tools as listed.
pub(super) struct Binding {
    target: String,
    graph: String,
    tools: HashMap<String, ToolFacts>,
}

impl Binding {
    pub(super) fn new(target: String, graph: String) -> Self {
        Self {
            target,
            graph,
            tools: HashMap::new(),
        }
    }

    /// Decide whether a request is sent.
    pub(super) fn admit(&self, method: &str, message: &Value) -> Admit {
        match method {
            "tools/call" => self.admit_tool_call(message),
            "resources/read" | "resources/subscribe" | "resources/unsubscribe" => {
                self.admit_resource(message)
            }
            _ => Admit::Send(None),
        }
    }

    /// The refusal of a call to a tool the server did not list.
    pub(super) fn unlisted(&self, message: &Value) -> Admit {
        let name = tool_name(message).unwrap_or_default();
        refuse(
            format!("unknown MCP tool `{name}`: the server lists no such tool"),
            json!({"tool": name}),
        )
    }

    fn admit_tool_call(&self, message: &Value) -> Admit {
        let Some(name) = tool_name(message) else {
            return refuse(
                "a `tools/call` request names its tool in `params.name`".to_string(),
                Value::Null,
            );
        };
        let Some(facts) = self.tools.get(name) else {
            return Admit::ListTools;
        };
        if !facts.read_only {
            return refuse(
                format!(
                    "`{name}` is not a read-only tool: `omnigraph mcp` admits only the tools the server \
                     annotates read-only, and writes need `omnigraph mcp --write`, which is not available yet"
                ),
                json!({"tool": name, "read_only": false}),
            );
        }
        if !facts.takes_graph {
            return Admit::Send(None);
        }
        match message.pointer("/params/arguments/graph") {
            Some(Value::String(graph)) if *graph == self.graph => Admit::Send(None),
            Some(Value::String(other)) => self.other_graph(other),
            None | Some(Value::Null) => {
                let mut message = message.clone();
                let params = &mut message["params"];
                if !params["arguments"].is_object() && !params["arguments"].is_null() {
                    // Not an argument object: the server refuses it as it is.
                    return Admit::Send(None);
                }
                params["arguments"]["graph"] = Value::String(self.graph.clone());
                Admit::Send(Some(message))
            }
            // A graph that is not a string is the server's to refuse.
            Some(_) => Admit::Send(None),
        }
    }

    fn admit_resource(&self, message: &Value) -> Admit {
        match message
            .pointer("/params/uri")
            .and_then(Value::as_str)
            .and_then(resource_graph)
        {
            Some(graph) if graph != self.graph => self.other_graph(graph),
            _ => Admit::Send(None),
        }
    }

    fn other_graph(&self, other: &str) -> Admit {
        refuse(
            format!(
                "this connection serves graph `{}` of target '{}', not `{other}`; start another \
                 `omnigraph mcp --graph {other}` to use it",
                self.graph, self.target
            ),
            json!({"graph": self.graph, "requested_graph": other}),
        )
    }

    /// Record the tools a `tools/list` result names, so a later call can be
    /// admitted or refused by them.
    pub(super) fn learn_tools(&mut self, result: &Value) {
        let Some(tools) = result.get("tools").and_then(Value::as_array) else {
            return;
        };
        for tool in tools {
            let Some(name) = tool.get("name").and_then(Value::as_str) else {
                continue;
            };
            let facts = ToolFacts {
                read_only: tool.pointer("/annotations/readOnlyHint") == Some(&Value::Bool(true)),
                takes_graph: tool.pointer("/inputSchema/properties/graph").is_some(),
            };
            self.tools.insert(name.to_string(), facts);
        }
    }

    /// Narrow the server's answer to a request the bridge sent. Returns
    /// whether the answer changed.
    pub(super) fn narrow(&mut self, method: &str, request: &Value, answer: &mut Value) -> bool {
        let Some(result) = answer.get_mut("result") else {
            return false;
        };
        match method {
            "initialize" | "server/discover" => self.describe(result),
            "tools/list" => {
                self.learn_tools(result);
                let Some(tools) = result.get_mut("tools").and_then(Value::as_array_mut) else {
                    return false;
                };
                let listed = tools.len();
                tools.retain(|tool| {
                    tool.pointer("/annotations/readOnlyHint") == Some(&Value::Bool(true))
                });
                tools.len() != listed
            }
            "tools/call" if tool_name(request) == Some(GRAPHS_TOOL) => {
                let mut changed = false;
                if let Some(structured) = result.get_mut("structuredContent") {
                    changed |= narrow_listing(structured, &self.graph);
                }
                if let Some(content) = result.get_mut("content").and_then(Value::as_array_mut) {
                    for item in content {
                        changed |= narrow_text(item, &self.graph, false);
                    }
                }
                changed
            }
            "resources/read"
                if request.pointer("/params/uri").and_then(Value::as_str)
                    == Some(GRAPHS_RESOURCE) =>
            {
                let mut changed = false;
                if let Some(contents) = result.get_mut("contents").and_then(Value::as_array_mut) {
                    for item in contents {
                        changed |= narrow_text(item, &self.graph, true);
                    }
                }
                changed
            }
            "resources/list" => {
                let Some(resources) = result.get_mut("resources").and_then(Value::as_array_mut)
                else {
                    return false;
                };
                let listed = resources.len();
                resources.retain(|resource| {
                    resource
                        .get("uri")
                        .and_then(Value::as_str)
                        .and_then(resource_graph)
                        .is_none_or(|named| named == self.graph)
                });
                resources.len() != listed
            }
            _ => false,
        }
    }

    /// Tell the agent which graph and tools this connection serves.
    fn describe(&self, result: &mut Value) -> bool {
        let note = format!(
            "This connection is `omnigraph mcp` for target '{}', bound to graph `{}`: a tool that takes \
             `graph` uses `{}` when the call omits it and refuses any other graph. Only read-only tools are \
             listed and admitted.",
            self.target, self.graph, self.graph
        );
        let instructions = match result.get("instructions").and_then(Value::as_str) {
            Some(text) => format!("{text}\n\n{note}"),
            None => note,
        };
        result["instructions"] = Value::String(instructions);
        true
    }
}

fn refuse(message: String, data: Value) -> Admit {
    Admit::Refuse {
        code: INVALID_PARAMS,
        message,
        data,
    }
}

fn tool_name(message: &Value) -> Option<&str> {
    message.pointer("/params/name").and_then(Value::as_str)
}

/// The graph a resource URI names, as the server parses it: the segment
/// after `omnigraph://graphs/` and before the next `/`.
fn resource_graph(uri: &str) -> Option<&str> {
    let (graph, _) = uri.strip_prefix(GRAPH_RESOURCE_PREFIX)?.split_once('/')?;
    Some(graph)
}

/// Keep only `graph` in a `{"graphs": [...]}` listing.
fn narrow_listing(listing: &mut Value, graph: &str) -> bool {
    let Some(graphs) = listing.get_mut("graphs").and_then(Value::as_array_mut) else {
        return false;
    };
    let listed = graphs.len();
    graphs.retain(|entry| entry.get("graph_id").and_then(Value::as_str) == Some(graph));
    graphs.len() != listed
}

/// Narrow a text item that holds a listing as JSON, in the same layout.
fn narrow_text(item: &mut Value, graph: &str, pretty: bool) -> bool {
    let Some(text) = item.get("text").and_then(Value::as_str) else {
        return false;
    };
    let Ok(mut listing) = serde_json::from_str::<Value>(text) else {
        return false;
    };
    if !narrow_listing(&mut listing, graph) {
        return false;
    }
    let text = if pretty {
        serde_json::to_string_pretty(&listing)
    } else {
        serde_json::to_string(&listing)
    };
    match text {
        Ok(text) => {
            item["text"] = Value::String(text);
            true
        }
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn listed() -> Value {
        json!({"tools": [
            {"name": "graphs", "inputSchema": {"type": "object", "properties": {}}, "annotations": {"readOnlyHint": true}},
            {"name": "schema", "inputSchema": {"type": "object", "properties": {"graph": {"type": "string"}}}, "annotations": {"readOnlyHint": true}},
            {"name": "mutate", "inputSchema": {"type": "object", "properties": {"graph": {"type": "string"}}}, "annotations": {"readOnlyHint": false, "destructiveHint": true}},
            {"name": "unannotated", "inputSchema": {"type": "object"}},
        ]})
    }

    fn call(name: &str, arguments: Option<Value>) -> Value {
        let mut params = json!({"name": name});
        if let Some(arguments) = arguments {
            params["arguments"] = arguments;
        }
        json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": params})
    }

    fn bound() -> Binding {
        let mut binding = Binding::new("local".into(), "knowledge".into());
        binding.learn_tools(&listed());
        binding
    }

    #[test]
    fn only_listed_read_only_tools_are_admitted_and_listed() {
        let mut binding = Binding::new("local".into(), "knowledge".into());
        // Unknown until the server's tools are listed.
        assert_eq!(
            binding.admit("tools/call", &call("schema", None)),
            Admit::ListTools
        );
        let mut answer = json!({"jsonrpc": "2.0", "id": 2, "result": listed()});
        assert!(binding.narrow("tools/list", &json!({}), &mut answer));
        let names: Vec<&str> = answer["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["graphs", "schema"]);

        for tool in ["mutate", "unannotated"] {
            let Admit::Refuse { code, message, .. } =
                binding.admit("tools/call", &call(tool, None))
            else {
                panic!("{tool} is refused");
            };
            assert_eq!(code, INVALID_PARAMS);
            assert!(message.contains("mcp --write"), "{message}");
        }
        assert_eq!(
            binding.admit("tools/call", &call("graphs", Some(json!({})))),
            Admit::Send(None)
        );
        let Admit::Refuse { message, .. } = binding.unlisted(&call("load", None)) else {
            panic!("an unlisted tool is refused");
        };
        assert!(message.contains("`load`"), "{message}");
        let Admit::Refuse { .. } = binding.admit(
            "tools/call",
            &json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {}}),
        ) else {
            panic!("a call without a tool name is refused");
        };
    }

    #[test]
    fn a_call_is_bound_to_the_connections_graph() {
        let binding = bound();
        // An omitted or null graph is filled; the rest of the call is kept.
        for arguments in [
            None,
            Some(json!({"branch": "main"})),
            Some(json!({"graph": null})),
        ] {
            let Admit::Send(Some(sent)) =
                binding.admit("tools/call", &call("schema", arguments.clone()))
            else {
                panic!("{arguments:?} is filled");
            };
            assert_eq!(sent["params"]["arguments"]["graph"], "knowledge");
            assert_eq!(sent["params"]["name"], "schema");
            if arguments.is_some_and(|arguments| arguments.get("branch").is_some()) {
                assert_eq!(sent["params"]["arguments"]["branch"], "main");
            }
        }
        assert_eq!(
            binding.admit(
                "tools/call",
                &call("schema", Some(json!({"graph": "knowledge"})))
            ),
            Admit::Send(None)
        );
        let Admit::Refuse { message, data, .. } = binding.admit(
            "tools/call",
            &call("schema", Some(json!({"graph": "other"}))),
        ) else {
            panic!("another graph is refused");
        };
        assert!(message.contains("`knowledge`"), "{message}");
        assert_eq!(data["requested_graph"], "other");
        // A tool without a `graph` argument is sent as written.
        assert_eq!(
            binding.admit("tools/call", &call("graphs", None)),
            Admit::Send(None)
        );

        let read = |uri: &str| json!({"method": "resources/read", "params": {"uri": uri}});
        for uri in [
            "omnigraph://graphs",
            "omnigraph://graphs/knowledge/schema",
            "omnigraph://best-practices/index",
        ] {
            assert_eq!(
                binding.admit("resources/read", &read(uri)),
                Admit::Send(None)
            );
        }
        for uri in [
            "omnigraph://graphs/other/schema",
            "omnigraph://graphs/knowledge%2F../branches",
        ] {
            assert!(
                matches!(
                    binding.admit("resources/read", &read(uri)),
                    Admit::Refuse { .. }
                ),
                "{uri}"
            );
        }
    }

    #[test]
    fn listings_name_only_the_connections_graph() {
        let mut binding = bound();
        let listing = json!({"graphs": [
            {"graph_id": "knowledge", "display_name": "knowledge"},
            {"graph_id": "other", "display_name": "other"},
        ]});
        let mut answer = json!({"id": 3, "result": {
            "content": [{"type": "text", "text": listing.to_string()}],
            "structuredContent": listing,
            "isError": false,
        }});
        assert!(binding.narrow("tools/call", &call("graphs", None), &mut answer));
        let only = json!({"graphs": [{"graph_id": "knowledge", "display_name": "knowledge"}]});
        assert_eq!(answer["result"]["structuredContent"], only);
        let text: Value =
            serde_json::from_str(answer["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(text, only);

        let mut resource = json!({"id": 4, "result": {"contents": [{
            "uri": "omnigraph://graphs",
            "mimeType": "application/json",
            "text": serde_json::to_string_pretty(&listing).unwrap(),
        }]}});
        let request = json!({"params": {"uri": "omnigraph://graphs"}});
        assert!(binding.narrow("resources/read", &request, &mut resource));
        let text: Value =
            serde_json::from_str(resource["result"]["contents"][0]["text"].as_str().unwrap())
                .unwrap();
        assert_eq!(text, only);

        let mut resources = json!({"id": 5, "result": {"resources": [
            {"uri": "omnigraph://graphs"},
            {"uri": "omnigraph://graphs/other/schema"},
            {"uri": "omnigraph://graphs/knowledge/schema"},
            {"uri": "omnigraph://best-practices/index"},
        ]}});
        assert!(binding.narrow("resources/list", &json!({}), &mut resources));
        assert_eq!(
            resources["result"]["resources"].as_array().unwrap().len(),
            3
        );

        // An error answer, or another tool's result, is left as it is.
        let mut refused = json!({"id": 6, "error": {"code": -32602, "message": "no"}});
        assert!(!binding.narrow("tools/call", &call("graphs", None), &mut refused));
        let mut schema = json!({"id": 7, "result": {"structuredContent": listing}});
        assert!(!binding.narrow("tools/call", &call("schema", None), &mut schema));
    }

    #[test]
    fn initialize_tells_the_agent_its_graph() {
        let mut binding = bound();
        let mut answer = json!({"id": 0, "result": {"protocolVersion": "2025-11-25", "instructions": "Server."}});
        assert!(binding.narrow("initialize", &json!({}), &mut answer));
        let instructions = answer["result"]["instructions"].as_str().unwrap();
        assert!(instructions.starts_with("Server.\n\n"), "{instructions}");
        assert!(instructions.contains("graph `knowledge`"), "{instructions}");
        assert_eq!(answer["result"]["protocolVersion"], "2025-11-25");
    }
}
