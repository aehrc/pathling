#!/bin/bash
# Runs one shard of the #2879 benchmark matrix: one benchmark phase and one pass, across the four
# cells (Pathling 10.0.0 and 11 on Spark 4.0.2 and 4.1.0) back to back in one allocation, so that
# every ratio between cells is measured on the same node. Pass 2 runs the cells in reverse. The
# allocation is 32 cores of a shared node rather than an exclusive node, because exclusive nodes
# were queued for about 16 hours.
#
# Author: John Grimes
#SBATCH --job-name=pathling-2879-bench
#SBATCH --account=OD-221174
#SBATCH --time=1:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=64g
#SBATCH --array=0-7
#SBATCH --output=logs/%A_%a.out
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=gri306@csiro.au

set -euo pipefail
module load amazon-corretto/21.0.0.35.1
cd "$SCRATCH3DIR/pathling-2879-bench"

phases=(encode decode viewPlanning viewExecution)
phase=${phases[$((SLURM_ARRAY_TASK_ID % 4))]}
pass=$((SLURM_ARRAY_TASK_ID / 4 + 1))
cells=(10-spark402 11-spark402 10-spark410 11-spark410)
if [ "$pass" -eq 2 ]; then cells=(11-spark410 10-spark410 11-spark402 10-spark402); fi

echo "node $(hostname), phase ${phase}, pass ${pass}, $(java -version 2>&1 | head -1)"
lscpu | grep -E "Model name|^CPU\(s\)|Socket"
free -g | head -2
mkdir -p results
for cell in "${cells[@]}"; do
  echo "=== $(date '+%F %T') ${cell} (load: $(cut -d' ' -f1-3 /proc/loadavg))"
  java -Xshare:off -Xmx8g -ea -Duser.timezone=UTC -Djava.io.tmpdir="${TMPDIR:-/tmp}" \
    --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
    --add-opens=java.base/java.net=ALL-UNNAMED \
    -jar "bench-${cell}.jar" "PathlingBenchmark\.${phase}\$" \
    -rf json -rff "results/bench-${cell}-${phase}-pass${pass}.json" \
    > "results/bench-${cell}-${phase}-pass${pass}.log" 2>&1
done
echo "=== $(date '+%F %T') done"
