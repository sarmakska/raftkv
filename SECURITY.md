# Security Policy

I take the security of raftkv seriously and I welcome reports of any issue that could affect the safety or correctness of the system.

## Reporting a vulnerability

Please email me privately at security@sarmalinux.com with a description of the issue, the steps to reproduce it, and the impact you believe it has. Do not open a public issue for a security report.

I commit to acknowledging your report within 7 days, and I will keep you updated as I investigate and work on a fix. Once a fix is ready I will coordinate a disclosure timeline with you and credit you in the release notes unless you prefer to remain anonymous.

## Supported versions

| Version | Supported |
| --- | --- |
| 0.1.x | Yes |
| < 0.1 | No |

Because raftkv is currently pre-1.0, security fixes land on the latest 0.1.x line. Once a 1.0 release is cut I will maintain the most recent minor version.

## Scope

The consensus core, the persistence layer, the fault-injection harness and the linearizability checker are all in scope. A correctness bug that lets the cluster violate linearizability is treated as a security issue, not just a functional one.
