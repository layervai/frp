# LayerV frp fork — branching, release, and upstream policy

This repository is a narrow fork of [fatedier/frp](https://github.com/fatedier/frp).
It is *not* a GitHub fork: it is a standalone repository so that pull requests
default to this repo rather than upstream, and so branch protection is ours.
`UPSTREAM_BASE` records the upstream release the fork currently sits on.

## One live line

`layerv/main` is the only live branch. Everything merges into it; every release
is tagged on it.

Between 2026-07 and 2026-08 the fork ran version-named base branches
(`layerv/base-v0.68.1`, `layerv/base-v0.70.0`, `layerv/base-v0.71.0`), one per
upstream release, with the layerv patches replayed onto each new one. That went
wrong in a specific, predictable way:

* Two lines were live at once and drifted three upstream releases apart — the
  frps-side consumer stayed on the v0.68.1 line while the frpc-side consumers
  moved to v0.70.0 — and nothing forced them to converge.
* Replaying gave the same fix a different SHA on every line, which defeats the
  point of a provenance chain built on signed tags that peel to a reviewed
  commit.
* Retired branches stayed pushable and unmarked, so a reviewer could not tell a
  live base from a dead one without tracing consumer pins across repositories.

Do not create another version-named base branch.

## Upstream refreshes: merge, do not rebase

Refresh by **merging** the upstream tag into `layerv/main`:

```bash
git fetch upstream --tags
git checkout layerv/main
git merge v0.72.0          # resolve conflicts, keep upstream's version of
                           # anything upstream has since fixed itself
```

Rebasing is tempting and wrong here. It re-authors every layerv commit, so the
reviewed-commit identity that the release tags and the consumers'
provenance-verification chain depend on restarts from scratch on every
upstream bump. Merging keeps commit identity stable forever; the cost is a
messier graph, which is the cheaper half of the trade.

While resolving, check each layerv patch against what upstream now does. Two
of the fork's patches were deleted during the v0.71.0 refresh because upstream
had absorbed them (`fatedier/frp#5428` and `#5424`); a third was kept because
upstream's similarly-titled change turned out to be unrelated. Shrinking the
fork is the goal — verify supersession by running our regression test against
pristine upstream, not by comparing commit titles.

Update `UPSTREAM_BASE` in the same merge.

## Releases

Tags follow the fork's **own** semver line: `v1.4.0`, not `v0.71.0-layerv.1`.

The old scheme encoded the upstream base in the tag, but `v0.71.0-layerv.1` is
a semver *pre-release* of `v0.71.0` — it sorts *below* the upstream version it
is built from, despite containing strictly more. That only ever worked because
every consumer pins through a `replace` directive, which bypasses minimal
version selection entirely. Under the old scheme `go get -u`, `go list -m -u`,
and Dependabot were all blind to new fork releases.

The upstream base now lives in `UPSTREAM_BASE` and in the tag message, where it
does not have to fight semver.

Release tags are annotated and PGP-signed, and must peel to a reviewed merge
commit on `layerv/main`:

```bash
git tag -s v1.1.0 -m "LayerV frp v1.1.0 (upstream v0.72.0)

<what changed>"
git push origin v1.1.0
```

Consumers verify this chain with `scripts/verify-frp-provenance.sh`. That
script lives in each **consumer** repository, not here. It checks the
`go.mod` pin, the `go.sum` hashes, the public proxy's metadata from a cold
module cache, and that the live tag still resolves to the recorded commit.
Never move a published tag.

When cutting a release, every consumer's copy of that script needs its pinned
constants refreshed. The script derives the version and hashes from `go.mod`
and `go.sum`, so only `commit` is hand-maintained.

## Retired branches

`retired/*` branches are frozen history, kept only so old release tags remain
reachable and auditable. They are protected against pushes. Never merge into
one; never cut a release from one.

## History

The version-named base branches were collapsed on 2026-08-26:

| old branch | now | notes |
|---|---|---|
| `layerv/base-v0.71.0` | `layerv/main` | the live line |
| `layerv/base-v0.70.0` | `retired/base-v0.70.0` | locked; content replayed onto v0.71.0 in #21 |
| `layerv/base-v0.68.1` | `retired/base-v0.68.1` | locked; content ported onto v0.71.0 in #22 |

Release tags on the retired lines (`v0.68.1-layerv.*`, `v0.70.0-layerv.*`)
remain valid and reachable; branch renames do not move tags. They are frozen —
new releases come from `layerv/main` on the `v1.x` line.
