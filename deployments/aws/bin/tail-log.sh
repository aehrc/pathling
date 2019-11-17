#!/bin/bash
set -e

BASE_DIR="$(cd `dirname $0`/.. && pwd)" 

"${BASE_DIR}/bin/ssh-master.sh" tail -f clinsight.log
