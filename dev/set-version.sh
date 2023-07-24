#!/bin/sh
set -x -e

PROJ_DIR=$(cd "`dirname $0`"/..; pwd)

VERSION="${1?Version is required}"
R_VERSION=${VERSION/-SNAPSHOT/.9000}

echo "Project dir is: ${PROJ_DIR}"
echo "Setting version to: ${VERSION}"
echo "Setting R version to: ${R_VERSION}"


for f in `find "${PROJ_DIR}" -name pom.xml`; do
  sed -i ''  "1,28 s/<version>.*<\/version>/<version>$VERSION<\/version>/" $f
done

for f in `find "${PROJ_DIR}" -name DESCRIPTION`; do
  sed -i '' "s/^Version:.*/Version: $R_VERSION/" $f
done

