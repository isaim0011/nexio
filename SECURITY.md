# Security policy

Please report suspected vulnerabilities privately through GitHub's
["Report a vulnerability"](https://github.com/isaim0011/nexio/security/advisories/new) form on this repository
rather than a public issue. Include a description, affected versions and, if possible, a reproduction.

nexio is pre-1.0; the latest published release receives fixes. Memory-safety bugs in the completion/cancellation
layer and any behavior where I/O touches the wrong descriptor or buffer are treated as the highest priority.
