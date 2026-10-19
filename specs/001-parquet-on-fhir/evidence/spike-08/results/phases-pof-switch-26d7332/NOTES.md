# phases-pof-switch-26d7332: the SC-011 bar, re-measured with the phase harness (spike 08)

Commit `26d7332e79`, the PoF switch, built under `9.9.0-pofswitch-26d7332` in the detached worktree
`.claude/worktrees/bench-26d7332`, which is the build `pof-switch-26d7332` used. Its POMs are the only change, so
`pathlingDirty` is `true`. It was not rebuilt. Built jar SHA-256 `5180983e6e7e…`, the same jar as the one resolved
into the private Ivy home `ivy/phases-pof-switch-26d7332/` (`resolved-jar.sha256`). Venv `venv-26d7332`.

Run on 2026-10-19 from 13:11 to 14:14, the first of three labels run back to back by one batch script
(`run-phases.sh <label> venv-26d7332 pathling`). The sources were JSON, XML lines and both Bundle shapes, with
extensions off and on. The definitions are in `scripts/phases.py` and in the spike's evidence README.

Other processes: two `jdtls` language servers (idle). IntelliJ and SonarLint, resident at 12:31, had exited
before the run. Before the run, macOS agents (AmbientDisplayAgent, WallpaperAgent and coreaudiod) briefly used
about four cores. The batch started once they were idle. During the runs the load average reached 80 to 90. That it
came from the benchmark JVM's own threads is an inference, not established: no other process was above 12% CPU
when checked at 14:24. Swap was in use after the batch (2.79 GB of 4 GB at 16:20), which is a possible lead.
