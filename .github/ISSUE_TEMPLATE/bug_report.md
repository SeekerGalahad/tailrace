---
name: Bug report
about: Something in tailrace does not behave as documented or expected
title: ''
labels: bug
assignees: ''
---

**Describe the bug**

A clear and concise description of what the bug is.

**To reproduce**

Steps to reproduce the behavior:

1. ...
2. ...

Ideally as a pipeline definition (SQL), a `curl` against the console/engine
API, or the smallest snippet of input data that triggers it.

**Expected behavior**

What you expected to happen instead.

**Environment**

- tailrace version (`tailrace-console --version` / tarball `VERSION` file):
- OS + arch (e.g. linux amd64, macOS arm64):
- How you run it: single binary / `scripts/dev.sh` / deploy chain:
- If Kafka/Doris involved: broker / FE versions:

**Logs**

```
paste relevant console/node stderr here (strip credentials and tokens!)
```

**Security note**: never paste tokens, passwords, or connection credentials —
see [SECURITY.md](../SECURITY.md) for vulnerability reports.
