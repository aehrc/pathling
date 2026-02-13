#!/usr/bin/env bash
# Downloads and extracts Apache Spark to a specified directory.
# Tries the CDN first, then falls back to the archive mirror. Each source is
# retried up to 3 times with exponential backoff.
#
# Usage: install-spark.sh <spark-version> <hadoop-version> <target-dir>
#
# Author: John Grimes

set -euo pipefail

readonly SPARK_VERSION="${1:?Usage: install-spark.sh <spark-version> <hadoop-version> <target-dir>}"
readonly HADOOP_VERSION="${2:?Usage: install-spark.sh <spark-version> <hadoop-version> <target-dir>}"
readonly TARGET_DIR="${3:?Usage: install-spark.sh <spark-version> <hadoop-version> <target-dir>}"

readonly FILENAME="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
readonly CDN_URL="https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${FILENAME}"
readonly ARCHIVE_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${FILENAME}"

# Attempts to download a URL with retries and exponential backoff.
download_with_retries() {
  local url="$1"
  local output="$2"
  local max_retries=3
  local delays=(10 30 90)

  for ((i = 0; i < max_retries; i++)); do
    echo "Attempt $((i + 1))/${max_retries}: downloading ${url}"
    if curl -fSL --retry 0 -o "${output}" "${url}"; then
      echo "Download succeeded."
      return 0
    fi
    if ((i < max_retries - 1)); then
      echo "Download failed. Retrying in ${delays[$i]}s..."
      sleep "${delays[$i]}"
    fi
  done

  echo "All ${max_retries} attempts failed for ${url}."
  return 1
}

mkdir -p "${TARGET_DIR}"
readonly TARBALL="${TARGET_DIR}/${FILENAME}"

echo "=== Installing Spark ${SPARK_VERSION} (Hadoop ${HADOOP_VERSION}) ==="

# Try CDN first, then archive.
if download_with_retries "${CDN_URL}" "${TARBALL}"; then
  echo "Downloaded from CDN."
elif download_with_retries "${ARCHIVE_URL}" "${TARBALL}"; then
  echo "Downloaded from archive."
else
  echo "ERROR: Failed to download Spark from both CDN and archive." >&2
  exit 1
fi

echo "Extracting ${TARBALL} to ${TARGET_DIR}..."
tar -xzf "${TARBALL}" -C "${TARGET_DIR}"
rm -f "${TARBALL}"

echo "Spark ${SPARK_VERSION} installed to ${TARGET_DIR}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"
