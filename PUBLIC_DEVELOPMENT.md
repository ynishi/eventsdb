# Public development and information boundaries

eventsdb develops in public wherever the information is ours to disclose. This
document defines what may cross from private planning into public issues, pull
requests, code, and documentation. It is also the policy a coding agent uses
when deciding whether a reference is a disclosure problem.

This policy is about **information visibility**. Licensing is a separate
decision: see the licence section of the [README](README.md) for this
repository's terms, and [Third-party material](#third-party-material) for files
brought in from elsewhere.

An outcome under this policy is never permission to act. ALLOW means a piece of
information may appear in a public artifact; it says nothing about whether a
push, publish, or release may happen, or who may run it. Those operations are
governed by their own rules — coding agents do not push, publish, or open pull
requests regardless of how a diff classifies — and no outcome in this document
loosens them.

Vulnerabilities do not go through this document. Use GitHub's private
vulnerability reporting on this repository, so a report never sits in a public
issue while a fix is being written. If protected content has already been
published, [After a disclosure](#after-a-disclosure) picks up where the
classification below stops: removal is no longer the remedy on its own.

## Default

Design choices, roadmaps, alternatives that were rejected, code, and
implementation plans are public by default when their disclosure can be decided
here without exposing information entrusted to us by someone else.

Private does not mean "commercial" or "important." It means that another person
or organisation has not agreed to publication, or that disclosure would expose
credentials or an unresolved security issue.

Examples include the identities and conversations of people who report things
privately, contracts and NDA material, access credentials, and non-public
security reports. These inputs may inform public work, but their protected
contents do not cross the publication boundary.

## Classification

Publication review uses three outcomes. Confidentiality and public-document
quality are deliberately separate: an opaque reference is not automatically a
leak, but it may still make a public artifact incomplete.

### ALLOW

The following may appear in public issues, pull requests, commits, code
comments, and documentation:

- public code, design alternatives, roadmaps, and decisions;
- an internal task identifier;
- a plain internal issue URL, without credentials or access-bearing query
  parameters;
- public-safe task bookkeeping and summaries;
- an anonymised statement of a need, such as "callers need this capability";
- facts that can be supported by public sources or by the public repository —
  which includes a measurement anybody can reproduce from a command in
  [CONTRIBUTING.md](CONTRIBUTING.md#verification).

An internal identifier or plain URL **is not a leak by itself**. Its presence
does not make the surrounding artifact private.

Anonymising a source must not invent evidence. Do not turn one request into
"multiple users," or state any other count, unless that count is both accurate
and safe to disclose.

### WARN

The following are allowed to exist. They are worth correcting because a
self-contained explanation makes the design easier to understand and maintain,
not because they are disclosures:

- the problem, requirement, acceptance criteria, or design rationale exists only
  in an internal note;
- an internal link is the sole explanation for a public code change;
- a copied planning note has not been rewritten as a self-contained public
  explanation.

A WARN is not a leak, a security incident, or a publication blocker. On a
best-effort basis, add the public problem, constraints, and decision rationale
before pushing the change; the internal reference may remain as supplemental
provenance. If the change has already been pushed, either improve the
explanation inline when that is the useful fix, or open an issue when the
missing context needs separate work.

Do not build an elaborate guard or review process to guarantee that no WARN is
ever published. Human and AI contributors will both produce incomplete context
occasionally, and preventing every instance costs more than the defect. Find and
improve these cases as ordinary documentation work.

### BLOCK

The following must not be copied into a public artifact:

- the body, comments, attachments, or screenshots of a private issue unless each
  disclosed part has independently been cleared for publication;
- link text that itself reveals protected information, even when the plain URL
  would have been allowed;
- the identity, private conversation, or identifying account data of a
  contributor or reporter, without permission;
- contract, NDA, or other third-party-confidential material;
- credentials, session material, signed URLs, API keys, or URLs containing
  access-bearing tokens;
- non-public vulnerability details before coordinated disclosure;
- private measurements or business data whose disclosure rights have not been
  established.

If a BLOCK item is found **before** publication, remove or redact the protected
content and carry on; nothing else is owed. If it is found **after**, removal is
no longer the remedy on its own — see [After a disclosure](#after-a-disclosure).
Do not classify an allowed internal identifier or plain URL as an incident.

## Third-party material

The boundary above is about information. A file asks a second question that the
classification does not: whether this repository may redistribute it at all.

Committing a file redistributes it as part of this repository. The file does not
have to use the repository's default licence: different files may carry
different terms. Those terms must permit the distribution being made, and every
required licence, copyright, attribution, and notice must be preserved. Two
things make this easy to miss: a file can be freely downloadable and still not
freely redistributable, and a fixture fetched at test time becomes
redistribution the moment somebody commits it.

Before adding a file that did not originate here, state where it came from,
under what terms it is distributed, and which notices must accompany it, beside
the file or in the script that produces it. If these cannot be stated, do not
commit it: generate an equivalent, or fetch it at build time and leave it
untracked. Fetching rather than committing avoids redistribution by this
repository; it does not remove any terms governing use of the fetched file.

A dependency is not covered by this section — it is resolved rather than
redistributed, and its terms reach the user through their own build.

## After a disclosure

Once protected content has been published, it has been disclosed. Deleting the
commit, force-pushing over it, or making the repository private does not undo
that, and treating any of those as the fix is the error this section exists to
prevent. What follows is an incident, not an edit.

The first question is whether the exposed thing can be **invalidated**.

**It can** — credentials, tokens, signed URLs, session material. Revoke or
rotate before touching history. A published secret is compromised the moment it
is published, and removing it from the code stops nobody who already copied it;
rotation is the only step that actually contains anything. Once rotated, the
secret cannot be used, and that may be the entire remedy — rewriting history is
then a tidiness decision rather than a containment one. Weigh it knowing what it
cannot reach: it does not touch other people's clones (they have to be told), a
fork keeps the commit until its owner removes it, and somebody who pulls an old
clone and pushes can restore what was purged.

If the content reached crates.io, treat external copies as potentially
permanent. `cargo yank` is not deletion — the version stays downloadable and
existing lockfiles keep resolving to it. crates.io may delete an entire crate
when either it was first published less than 72 hours ago, or it has a single
owner, has fewer than 500 downloads for each month it has been published, and no
other crates depend on it. Even a successful deletion does not retract copies
already downloaded or mirrored.

**It cannot** — a person's data, a private conversation, third-party
confidential material, an unpatched vulnerability. Nothing can be rotated, so
the disclosure is irreversible and the work moves from the file to the people it
affects. Establish what was exposed, over what window, and how far it could have
travelled; mirrors, forks, archives, and scrapers are part of that answer rather
than a footnote. Then meet the obligations the _content_ carries, which are not
this repository's to waive — personal data carries the duties of whichever law
applies to whoever is acting as controller, third-party confidential material is
owed to its owner under whatever agreement covers it, and an unpatched
vulnerability follows coordinated disclosure rather than this document.

Removal still happens. It is mitigation evidence, not the resolution.

Write down the exposure window, the reach, and every takedown attempted. That
record is what the obligations above are answered with. Where that record is
public it is an issue, and it closes once everything it describes is done — the
takedowns, and the obligations the content carried, which outlast them. What it
says stays readable and searchable after it closes; an issue left open says
instead that the disclosure is still being dealt with.

## Internal references

Internal tracking and public explanation serve different purposes and may
coexist.

```text
Allowed:
  Internal tracking: TASK-123
  Related internal issue: https://tracker.example/TASK-123

Incomplete but not a leak:
  Implements https://tracker.example/TASK-123

Blocked:
  Reporter Name asked for this in TASK-123: "verbatim private message"
  https://tracker.example/TASK-123?access_token=...
```

Use an internal reference as a provenance pointer, never as the only readable
source of the public change. Do not fetch protected issue content and repeat it
merely because the identifier or URL itself is allowed.

## From private evidence to public work

Some design work necessarily begins with private context: a private benchmark,
an internal experiment, a report that arrived privately. That context is
converted at a publication gate before a coding agent begins public-facing
implementation.

```text
Private evidence
    -> public-safe issue draft
    -> public implementation issue
    -> public-safe local task
    -> code / pull request
    -> current contract in code documentation
```

The gate produces a new, self-contained artifact rather than sanitising and
publishing an entire private plan. Its minimum output is:

1. the problem;
2. public evidence or an anonymised description of the need;
3. the desired property or outcome;
4. constraints that can be stated publicly;
5. unresolved questions;
6. relevant locations in the public repository.

An internal issue may remain linked after this conversion. Coding should use the
public artifact as its contract, so the implementation context does not need the
protected source material.

A note being kept locally rather than published says where it is, not whether it
is confidential. Once a task has passed the publication gate, keep its contents
public-safe even if the note itself is never published. This lets human and AI
contributors use the same context without repeatedly reclassifying it.

## Role of each public artifact

- **Issue:** explore the problem, alternatives, and trade-offs, and record the
  accepted change, scope, and acceptance criteria.
- **Pull request and commit:** record the implementation delta and chronology.
- **Code documentation:** state the current contract, invariants, rationale,
  examples, compatibility notes, and migration path.

There is no separate discussion venue. Exploration belongs in the issue next to
the change it argues for, and what is still unsettled stays there in an `Open`
section rather than moving somewhere else to be resolved. Reading an issue
should not require finding the conversation that preceded it. How an umbrella
issue and its children divide that work is in
[CONTRIBUTING.md](CONTRIBUTING.md#umbrella-issues).

Public work should be understandable from these artifacts without access to an
internal system. Internal references may preserve additional lineage, but they
do not replace the public explanation.

## Agent publication check

Before producing a public-facing artifact, an agent checks in this order:

1. **BLOCK:** Does the draft contain protected third-party content, credentials,
   token-bearing URLs, or non-public security material?
2. **WARN:** Can a reader understand the problem, constraints, and decision
   without opening an internal system?
3. **ALLOW:** Keep internal IDs and plain URLs when they add useful provenance;
   do not report them as leaks merely because they are internal.
4. **Redistribution:** Does the change commit a file that originated elsewhere?
   That is a licensing question, not a disclosure one, so none of the three
   outcomes above answers it — see
   [Third-party material](#third-party-material).

If a BLOCK item turns out to be already published, stop applying this check and
go to [After a disclosure](#after-a-disclosure). At that point the question is
no longer what the next artifact may contain.

This check is intentionally narrow. It prevents disclosure of protected contents
while avoiding an unbounded search for every string that merely looks internal.
