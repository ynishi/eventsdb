# Contributing

Shared conventions for changes to this repository, for humans and coding agents
alike. The disclosure policy in [PUBLIC_DEVELOPMENT.md](PUBLIC_DEVELOPMENT.md)
outranks this file.

## Issue conventions

### Labels

Five categories. Assign at least one when you open an issue.

| Label           | The change                                             |
| --------------- | ------------------------------------------------------ |
| `bug`           | behaviour contradicts what it promises                 |
| `enhancement`   | behaviour that does not exist yet                      |
| `refactor`      | behaviour unchanged                                    |
| `documentation` | prose only — README, rustdoc, these files              |
| `chore`         | production code untouched — CI, tests, tooling, release |

`documentation` and `chore` are both "production code untouched"; the split is
whether a reader is the audience. A rewritten doc comment is `documentation`, a
changed `include` list is `chore`.

### Umbrella issues

An issue covering more than one change is an umbrella: it holds the analysis and
the unsettled questions, and implementation issues split off from it as they are
picked up, each carrying its own scope. The reasoning stays in one place instead
of being repeated in every child, and the umbrella's `Open` section is what the
next split is chosen from. Children link back with `Refs #<umbrella>`.

This is how [#1](https://github.com/ynishi/eventsdb/issues/1) works, and reading
it is faster than reading this paragraph.

## Branches

Never work on `main`. One branch per issue, cut from `origin/main` after a
fetch, named `<type>/<slug>` where `<type>` is `fix`, `feat`, `docs`, or
`chore`.

Whether that branch lives in a second worktree or in the checkout you already
have is yours to decide, and so is how its build directory gets there. One
thing about the choice is not: if you do run several checkouts, give each its
own `target`. Cargo treats path dependencies with the same name, version and
workspace-relative path as the same crate even across checkouts
([cargo#12516](https://github.com/rust-lang/cargo/issues/12516)), which
`eventsdb-core` satisfies against every other checkout of this repository — so
two of them pointed at one build directory can report a gate green against the
other branch's binaries, with no error to notice.

## Verification

Four gates, and the whole workspace is small enough to run all of them over all
of it. `cargo test --workspace --all-features` takes 19 s once the tree is
built, which is not a budget anybody needs to manage.

```bash
cargo fmt --all --check
cargo clippy --workspace --all-features --all-targets
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

`--all-features` is load-bearing in all three that take it: a feature-gated path
nothing enables is a path nothing checks, and a backend behind a feature is
exactly that. The rustdoc gate is not decoration — three intra-doc links were
already found broken once (`5ac8618`), and a broken link is a promise the
published documentation does not keep.

The MSRV is 1.82, and the manifest says why: `libsqlite3-sys` declares none of
its own and uses `unsafe extern "C"`, so 1.78 cannot build it. It is a claim
this repository makes to everyone who depends on it. When a change reaches for
something newer, raise the number deliberately in the same commit rather than
letting the build discover it.

Report what was actually run. "I did not verify X" is a usable report; a green
claim resting on a command nobody ran is not.

## Documentation

API detail lives in the rustdoc. `cargo doc` is where a reader goes for what a
type is and what a function promises. When two texts disagree the code wins, and
after that the doc comment: a comment contradicting an issue is a finding about
the issue.

Four rules for writing that prose.

**State a rule once, where the thing it constrains is defined.** Everywhere
else, link to that statement and write only what this site adds. A second full
statement is the copy nobody edits the day the rule moves.

**Do not write the state of the tree where nothing maintains it.** "Not yet",
"neither has one", "the only caller" are true when written and go false in
silence. Keep the rule and cut the clause about today.

**A number describing a list belongs in the file holding the list, or nowhere.**
The list is the answer and the number is a copy of it. Point at the list
instead.

**A sentence recording that a rule changed stays.** It is a constraint written
in the past tense, and it is what stands between the next reader and undoing the
rule. The test is whether deleting it lets somebody repeat the mistake. If
deleting it costs nothing it is a report on how a change went, and that belongs
in the commit message.

## Preferred commit format

```text
<subject: what changed, one line>

<prose: the problem, why this fix and not the alternative, what it
cost. This is where the reasoning lives.>

Verified: <the commands actually run, and their outcome>

Refs #<issue>
```

- `cargo fmt` output and clippy fixes go in their own commits, separate from
  behaviour changes.
- Update `CHANGELOG.md` under `## [Unreleased]` as its own commit.
- Never commit working notes, another checkout, or local agent state. If a
  commit needs `git add -f`, stop: something is filed wrong.

## Versions and publishing

The three crates share one version through `[workspace.package]`, so they move
together and a release republishes all of them whether or not each one changed.
That is the cost of the shared number, and it is the reason a facade crate can
join the family without a version of its own to reason about.

Publishing is a human action. `cargo publish`, `git push --tags`, and
`gh release create` are outside what a coding agent does here, and no
classification in [PUBLIC_DEVELOPMENT.md](PUBLIC_DEVELOPMENT.md) changes that.
crates.io is also where the consequences of a mistake stop being editable:
`cargo yank` is not deletion, and a yanked version stays downloadable with
existing lockfiles still resolving to it.

## Pull requests

A coding agent's part ends with everything that does not write to anything
remote, and that includes the gate:

1. `git fetch origin`, then run the four verification commands. None of them
   writes to anything remote, so being denied `git push` is no reason to skip
   them. Report the result.
2. **Write the PR body to a file**, somewhere the tree does not track, and say
   where it is. Not a summary in the chat, not "a draft" — a file, so the
   command handed over can be `--body-file <path>` and the human reads the same
   bytes that will be posted.
3. Hand over the literal commands. These two are the only ones that write to
   anything remote:

   ```bash
   git push -u origin <branch>
   gh pr create --base main --head <branch> \
     --title "<subject>" --body-file <path>
   ```

The PR body records what changed, what was verified, and what it deliberately
does not cover — under the same disclosure policy as everything else.
