# Security Policy

## Supported versions

tailrace is pre-1.0 (`0.1.x`). Security fixes are applied to the default branch
only; there are no backport branches yet.

## Reporting a vulnerability

**Do not open a public issue for anything security-related.**

Please use GitHub's private reporting channel instead:

1. Go to the **Security** tab of this repository.
2. Select **Report a vulnerability** to open a private GitHub Security Advisory.

This reaches the maintainers privately and lets us coordinate a fix and a
controlled disclosure (credit is given by GitHub unless you prefer to stay
anonymous). Please include reproduction steps, affected versions/commits, and
impact assessment where possible. We aim to respond within 7 days.

## Scope

tailrace moves data between message queues, filesystems, and OLAP engines.
Reports about the engine API (`/v1`), the console (auth, credential storage,
deploy chain), connector credential handling, and the packaged binaries are all
in scope.
