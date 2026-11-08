- Operator configuration validation now rejects unsupported identifiers and YAML
anchors and aliases before execution. Alias parameters must be a JSON object;
unsupported positional alias arguments fail at parsing instead of implying a
binding the configuration cannot declare. Legacy configuration errors point to
the upgrade guide without importing old credentials or changing configuration.
