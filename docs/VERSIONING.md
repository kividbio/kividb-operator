# Docs versioning

There is exactly one copy of each user-facing doc, at `docs/*.md`. There
is no `docs/versions/vX.Y/` duplication — versioning comes from git
itself: every release is a tag, every tag's `docs/` tree is exactly the
docs as they were when that release shipped, and an external site (e.g.
kivi-web) picks a version by fetching `docs/` at that tag's ref instead
of at a branch. This is the same model GitHub itself uses for "view this
file as of this release" and avoids ever having two copies of a doc to
keep in sync by hand.

## Contract for an external consumer

1. Know (from wherever it tracks releases — this repo's tags, GitHub
   Releases, `CHANGELOG.md`) which ref corresponds to which operator
   version.
2. For a selected version, fetch `docs/manifest.json` at that ref:

   ```json
   {
     "docs": [
       { "slug": "architecture", "title": "Architecture", "file": "ARCHITECTURE.md" },
       { "slug": "install", "title": "Installing kividb-operator", "file": "INSTALL.md" },
       ...
     ]
   }
   ```

   `docs[].file` is relative to `docs/`. `slug` is a stable page identity
   for routing; `title` is what to show in a nav/sidebar; array order is
   the intended nav order.
3. For a selected doc, fetch `docs/{file}` at the **same ref** used for
   the manifest. Two standard ways to do this against GitHub, both work
   identically since this is plain Markdown with no build step:
   - `https://raw.githubusercontent.com/kividbio/kividb-operator/<ref>/docs/{file}`
   - the Contents API: `GET /repos/kividbio/kividb-operator/contents/docs/{file}?ref=<ref>`

`<ref>` is a tag (`v0.1.3`) for a specific released version, or a branch
(`main`) for in-development docs matching the default branch's current
state — both are valid, ordinary git refs; nothing here is
version-scheme-specific.

## Cross-doc links

Docs link to each other with plain relative filenames (e.g.
`[CONFIGURATION.md](CONFIGURATION.md)`,
`[ROADMAP.md](ROADMAP.md#before-1.0.0)`). These resolve correctly as long
as a consumer keeps fetching sibling files at the **same ref** — never mix
`docs/CONFIGURATION.md` from one tag with `docs/ROADMAP.md` from another
when resolving a link between them.

## What this means day to day

Nothing extra. Edit `docs/*.md` in place alongside whatever code change
motivated the edit, same as any other file in the repo — there is no
separate versioned copy to remember to update. `docs/manifest.json` only
needs a change when a doc file is added, removed, or renamed (not on
every content edit).

`CONTRIBUTING.md` and `RELEASING.md` are maintainer-facing, not
end-user-facing, and are intentionally **not** listed in
`docs/manifest.json` — they describe how to work on this repo, which only
has one current answer regardless of which released version a user runs.
