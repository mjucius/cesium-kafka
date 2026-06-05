# Security Policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately via GitHub Security Advisories
("Report a vulnerability" on the repository's Security tab). Do not open public issues for
security reports.

## Deployment requirements

cesium-kafka's internal **tracker topic is the system's durable scheduler state**. Write access to
it must be restricted to the cesium principal (Kafka ACLs):

- a forged ADD record is a duplicate-injection primitive;
- a forged completion tombstone is a data-loss primitive.

See `docs/operations.md` (and the design document) for the normative ACL requirement and
principal/credential guidance.
