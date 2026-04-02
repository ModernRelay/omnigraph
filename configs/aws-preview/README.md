Use this config to talk to the live AWS preview server with the `mr-omni` alias pack.

Example:

```bash
cd /Users/andrew/code/omnigraph/configs/aws-preview
cp .env.omni.example .env.omni

/Users/andrew/code/omnigraph/target/debug/omnigraph snapshot --json
/Users/andrew/code/omnigraph/target/debug/omnigraph read --alias sem-dec "open source launch"
/Users/andrew/code/omnigraph/target/debug/omnigraph branch list --json
```
