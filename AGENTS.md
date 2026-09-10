# eventsdb — pointers for coding agents

Claude Code reads `CLAUDE.md` rather than this name. Point it here by writing
`.claude/CLAUDE.md` yourself, with `@AGENTS.md` as its first line: `.claude/`
is already on the ignore list, so the file stays yours and nothing is tracked.
Write it rather than symlinking this file — a symlink has no room of its own,
and the instructions that belong to your machine alone would have to go into
this file to be read at all.

- Architecture, the shape of an event, and what this design will not do:
  [README.md](README.md)
- Disclosure policy — read before anything ships:
  [PUBLIC_DEVELOPMENT.md](PUBLIC_DEVELOPMENT.md)
- Issue conventions, including how an umbrella issue and its children divide the
  work: [CONTRIBUTING.md](CONTRIBUTING.md#issue-conventions)
- Branch, commit, and pull request conventions, preferred commit format
  included: [CONTRIBUTING.md](CONTRIBUTING.md)
- API detail lives in the rustdoc (`cargo doc`); code documentation outranks
  stale issue text.
- Never work on `main`; one branch per issue, cut from `origin/main` after a
  fetch — [CONTRIBUTING.md](CONTRIBUTING.md#branches) names the four prefixes.
  Where that branch is checked out is the local setup's business, not this
  file's.
- The four verification commands are in
  [CONTRIBUTING.md](CONTRIBUTING.md#verification) and the whole workspace is
  small enough that all four run over all of it. `--all-features` is not
  optional: a backend behind a feature is a path nothing checks without it.
- Publishing, pushing, tagging and opening a pull request are human actions. An
  agent's part ends at the last thing that writes nothing remote — which
  includes running the gates and writing the pull request body to an untracked
  file for the human to read before it is posted.
- Before proposing a feature, check it against what this design gives up on
  purpose. A single writer is why the global order has no holes and why a
  projection can be exactly-once; a feature whose reason for existing is network
  delivery does not automatically transfer. The store also does not interpret
  values it is handed, and the schema ladder only goes up. `Limitations` in the
  README states these, and
  [#1](https://github.com/ynishi/eventsdb/issues/1) works through what they rule
  in and out.
