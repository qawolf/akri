# Third-party code (QA Wolf fork)

## h2

`h2/` is a copy of https://github.com/project-akri/h2 at commit
`e01e8842a37bc3590228e9ab408fc1710018c031` (branch `patch-authority`). Upstream Akri v0.14.0 pins the same commit
as a git patch. The root `Cargo.toml` uses this copy as a path patch for `h2` 0.4.15.

- Why: the agent serves the kubelet device-plugin gRPC. Stock `h2` rejects the `:authority` header that kubelet
  sends. The patch accepts it for all methods except CONNECT.
- Why a copy: upstream force-pushed the branch before. The commit is reachable only by SHA, and the upstream
  project can go away.
- Left out: `fixtures/` and `ci/` (h2's own `Cargo.toml` excludes them from its package, and `fixtures/` is
  58 MB) and `.github/`. All other files are unchanged.

Check the copy:

```sh
git clone https://github.com/project-akri/h2 /tmp/h2 && git -C /tmp/h2 checkout e01e8842a37bc3590228e9ab408fc1710018c031
diff -r -x .git -x fixtures -x ci -x .github /tmp/h2 third-party/h2
```
