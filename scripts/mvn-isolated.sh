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
# Everything is kept under .private/mvn-isolated in the checkout, which no other checkout shares.
#
# The shared repository defaults to ~/.m2/repository; set MVN_ISOLATED_SHARED_REPO to use another.
# A SPARK_CONF_DIR of your own is carried over, with the isolated Ivy settings taking precedence.
#

set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
state="$root/.private/mvn-isolated"
conf="$state/spark-conf"
shared_repo="${MVN_ISOLATED_SHARED_REPO:-$HOME/.m2/repository}"

# The paths below go unescaped into file URLs and XML attributes.
case "$state$shared_repo" in
  *[[:space:]\&\<\"\#\?%]*)
    echo "mvn-isolated.sh: paths containing spaces, &, <, \", #, ? or % are not supported." >&2
    exit 1
    ;;
esac

# Replaces a file with standard input only when the content differs, by renaming, so that a
# concurrent run in the same checkout never reads a partly written file. The temporary file is kept
# outside the Spark configuration directory, where a concurrent run could prune it.
write_if_changed() {
  local tmp="$state/.tmp.$$.${1##*/}"
  cat > "$tmp"
  if cmp -s "$tmp" "$1"; then
    rm -f "$tmp"
  else
    mv "$tmp" "$1"
  fi
}

# The configuration that Spark would otherwise use, if one is set. A directory that does not exist
# is ignored, as Spark ignores it, and so is this script's own, which a nested run would see.
mkdir -p "$conf"
user_conf=""
if [[ -n "${SPARK_CONF_DIR:-}" && -d "$SPARK_CONF_DIR" ]]; then
  user_conf=$(cd -P "$SPARK_CONF_DIR" && pwd)
  [[ "$user_conf" == "$(cd -P "$conf" && pwd)" ]] && user_conf=""
fi

# Spark reads the configuration directory's top-level files, so those are copied, and copies of
# files that have since been removed are deleted.
for f in "$conf"/*; do
  name=${f##*/}
  [[ "$name" == spark-defaults.conf ]] && continue
  [[ -n "$user_conf" && -f "$user_conf/$name" ]] || rm -f "$f"
done
if [[ -n "$user_conf" ]]; then
  for f in "$user_conf"/*; do
    name=${f##*/}
    if [[ -f "$f" && "$name" != spark-defaults.conf ]]; then
      write_if_changed "$conf/$name" < "$f"
    fi
  done
fi

# Spark reads these settings when spark-submit starts, which covers the sessions that the Python
# and R tests launch. They come after any settings of your own, so they take precedence. They
# replace Spark's default resolvers, so they must reach Maven Central themselves, and
# spark-packages and the local Ivy repository are not searched.
{
  if [[ -n "$user_conf" && -f "$user_conf/spark-defaults.conf" ]]; then
    cat "$user_conf/spark-defaults.conf"
    echo
  fi
  echo "spark.jars.ivy          $state/ivy"
  echo "spark.jars.ivySettings  $state/ivysettings.xml"
} | write_if_changed "$conf/spark-defaults.conf"
# A rebuild keeps the jar's filename, whether the version is a SNAPSHOT or a release, so the
# isolated repository's resolver treats every version as changing and checks the modification time
# to pick up a rebuilt jar. Pathling modules come only from that repository, so a missing install
# fails instead of silently using another checkout's build.
write_if_changed "$state/ivysettings.xml" <<EOF
<ivysettings>
  <settings defaultResolver="chain"/>
  <resolvers>
    <ibiblio name="isolated-m2" m2compatible="true" usepoms="true"
      root="file://$state/m2" checkmodified="true" changingPattern=".*"/>
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
