# Running Pathling at a version or a commit

Shared by the skills in the `bugs` plugin. Record the Pathling, Python, Java and Spark versions of
every run, and the commit for anything built from source.

## A released version

- Install it in a throwaway `uv venv` in the scratchpad: `uv pip install pathling==<version>`.
  Java 21 is needed.
- The latest release is the newest `pathling` on PyPI. The server is released separately, under
  `server-v*` tags, as a Docker image.
- The installed version: `python -c "import importlib.metadata as m; print(m.version('pathling'))"`.
  This is also the command to give a reporter; `pathling.__version__` does not exist in every
  release.

## A branch or a commit

- Build each commit in its own worktree under `.claude/worktrees/` with `scripts/mvn-isolated.sh`
  (see `docs/agents/local-and-worktrees.md`), so the build leaves other checkouts' SNAPSHOT jars in
  `~/.m2` alone. Run `mvn install` in the main checkout only for the commit it has checked out.
- A SNAPSHOT already installed from that branch may be reused when the commit it was built from is
  known. Record that commit.
- A variant is often cheaper as a Java unit test in the target worktree, which skips Ivy entirely.

## Python against a source build

- A Python script run outside the isolated build resolves the jar through the shared Ivy cache.
  Clear it as CONTRIBUTING.md "Clearing the Ivy cache" describes, or the script runs a stale jar
  and passes.
- The generated `lib/python/pathling/_version.py` can lag the POM version. Set
  `pathling._version.__java_version__` to the POM version before importing `PathlingContext`; the
  symptom of a mismatch is `'JavaPackage' object is not callable`.
- A script that creates DataFrames needs `PYSPARK_PYTHON` set to the venv's interpreter when it
  differs from the system Python, or the Spark workers fail with a `PythonException`.
- Spark progress bars end lines with `\r`. Pipe output through `tr '\r' '\n'` before `grep`.
