#!/bin/bash
#
# Runs Maven isolated from the other checkouts on this machine, so that builds in several git
# worktrees can run concurrently.
#
# Usage: scripts/mvn-isolated.sh <Maven arguments>.
#
# The checkout gets its own Maven local repository, with the shared one as a read-only fallback,
# so that "install" does not replace the artifacts that other checkouts build and test against.
# The Python and R tests get their own Ivy cache, and resolve Pathling only from that repository.
# Everything is kept under .local/isolated in the checkout.
#
# The shared repository defaults to ~/.m2/repository; set MVN_ISOLATED_SHARED_REPO to use another.
#

set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
state="$root/.local/isolated"
conf="$state/spark-conf"
shared_repo="${MVN_ISOLATED_SHARED_REPO:-$HOME/.m2/repository}"

# The paths below go unescaped into file URLs and XML attributes.
case "$state$shared_repo" in
  *[[:space:]\&\<\"]*)
    echo "mvn-isolated.sh: paths containing spaces, &, < or \" are not supported." >&2
    exit 1
    ;;
esac

# Replaces a file with standard input only when the content differs, by renaming, so that a
# concurrent run in the same checkout never reads a partly written file.
write_if_changed() {
  local tmp="$1.$$"
  cat > "$tmp"
  if cmp -s "$tmp" "$1"; then
    rm -f "$tmp"
  else
    mv "$tmp" "$1"
  fi
}

# Spark reads these settings when spark-submit starts, which covers the sessions that the Python
# and R tests launch. They replace Spark's default resolvers, so they must reach Maven Central
# themselves, and spark-packages and the local Ivy repository are not searched.
mkdir -p "$conf"
write_if_changed "$conf/spark-defaults.conf" <<EOF
spark.jars.ivy          $state/ivy
spark.jars.ivySettings  $conf/ivysettings.xml
EOF
# The SNAPSHOT filename never changes, so the isolated repository's resolver checks the
# modification time to pick up a rebuilt jar. Pathling modules come only from that repository, so
# a missing install fails instead of silently using another checkout's build.
write_if_changed "$conf/ivysettings.xml" <<EOF
<ivysettings>
  <settings defaultResolver="chain"/>
  <resolvers>
    <ibiblio name="isolated-m2" m2compatible="true" usepoms="true"
      root="file://$state/m2" checkmodified="true" changingPattern=".*-SNAPSHOT"/>
    <chain name="chain" returnFirst="true">
      <resolver ref="isolated-m2"/>
      <ibiblio name="shared-m2" m2compatible="true" usepoms="true" root="file://$shared_repo"/>
      <ibiblio name="central" m2compatible="true" usepoms="true"/>
    </chain>
  </resolvers>
  <modules>
    <module organisation="au.csiro.pathling" resolver="isolated-m2"/>
  </modules>
</ivysettings>
EOF
export SPARK_CONF_DIR="$conf"

exec mvn "-Dmaven.repo.local=$state/m2" "-Dmaven.repo.local.tail=$shared_repo" "$@"
