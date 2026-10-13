# Security policy

## Report a vulnerability

Email `cort@infertap.ai` with `SECURITY` in the subject. Do not report vulnerabilities
in public issues. Include the affected version, reproduction steps, and potential impact.
Remove production keys, prompts, and credentials from the initial report.

We acknowledge reports within three business days and aim to complete an initial
assessment within ten business days. We coordinate disclosure with the reporter and
credit them unless they request otherwise. The target disclosure window is no more than
90 days, with earlier disclosure when a fix is available.

## Scope

This policy covers the tap binary, release artifacts, container image, systemd units,
and Kubernetes manifests. Reports may concern input-content exposure, pseudonymization,
network isolation, resource controls, or other security defects. Vulnerabilities in
bundled dependencies are tracked here and reported upstream as appropriate.

Operator-supplied labels and provenance can contain arbitrary text. Keep sensitive content
out of those fields. See [Security and resource bounds](docs/trust.md) for the threat model,
controls, and limitations, and [Installation](docs/install.md#get-the-binary) for artifact
verification.

## Supported versions

The latest released version receives security fixes. Upgrade older installations to
receive fixes. Before the first release, report vulnerabilities against the current main
branch and include the commit identifier.
