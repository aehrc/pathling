Draft a GitHub release for the tag: $ARGUMENTS

## Instructions

1. **Determine the release type.** Inspect the tag to determine whether this is
   a core library release (e.g. `v9.5.0`) or a server release (e.g.
   `server-v1.3.0`).

2. **Find the previous release.** Use `gh release list` to find the most recent
   release of the same type (core or server). Store its tag for use in the
   comparison.

3. **Gather changes.** Run `git log <previous-tag>..HEAD --oneline` to list all
   commits since the previous release. Also run
   `git diff <previous-tag>..HEAD --stat` to understand the scope of changes.

4. **Read the changed code.** For each commit that is not a trivial chore
   (version bump, CI config, dependency update), read the relevant source files
   or diffs to understand the substance of the change. Use
   `git show <commit> --stat` and `git show <commit>` as needed.

5. **Cross-reference issues.** Look for GitHub issue references in commit
   messages (e.g. `#1234`, `fixes #1234`). Use `gh issue view` to get the title
   and description of referenced issues. Include issue links in the release
   notes.

6. **Draft the release notes.** Follow the style of previous releases exactly:
    - Start with a single-sentence summary describing the focus of the release.
    - Organise changes into sections using `##` headings. Use only the sections
      that apply:
        - `## New features`
        - `## Bug fixes`
        - `## Documentation`
        - `## Dependencies`
        - `## Infrastructure and tooling`
    - Within sections, use concise bullet points. Link to issues where
      applicable using the format
      `[#123](https://github.com/aehrc/pathling/issues/123)`.
    - Use `###` sub-headings within a section only for major features that need
      additional detail.
    - End with a full changelog link:
      `**Full Changelog**: https://github.com/aehrc/pathling/compare/<previous-tag>...<new-tag>`

7. **Create the draft release.** Run:

    ```
    gh release create <tag> --draft --title "<title>" --notes "<notes>"
    ```

    The title should be the tag with the `v` prefix removed for core releases
    (e.g. `9.5.0`), or the full display name for server releases (e.g.
    `Server v1.3.0`).

8. **Report the result.** Show the user the draft release URL and the full
   release notes content for review.
