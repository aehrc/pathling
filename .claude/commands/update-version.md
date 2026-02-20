Update the Pathling core version across all required files and commit the
change. The new version is: $ARGUMENTS

Read the "Updating the version" section of `RELEASE_CHECKLIST.md` to determine
which files to update. Read the root `pom.xml` to determine the current
version.

If the new version ends with `-SNAPSHOT`, update only the POM files. If it is a
release version (no `-SNAPSHOT` suffix), also update the release-only files. For
release-only files, replace the previous release version (the version currently
in those files) with the new version.

After making all changes, stage the modified files and commit with the message:

```
chore: Update core version to <version>
```
