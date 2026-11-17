#!/bin/bash
# Puts the probe jar on the driver classpath. Jar libraries from /Workspace are refused on Assigned
# (dedicated) Unity Catalog clusters, and there is no UC volume. The Pathling jar (128 MB, over the
# 50 MB workspace import limit) is read from the DBFS root, and added only on the cluster that runs the engine views (LPR_ENGINE=1). The script always exits
# 0, so that a failure shows in the log and not as a cluster that never starts. LPR_USER is the
# workspace user that owns the uploaded files, set on the cluster by the submit script.
WS=/Workspace/Users/$LPR_USER
cp -v $WS/lpr/*.jar /databricks/jars/ 2>&1
if [ "$LPR_ENGINE" = 1 ]; then ls -la /dbfs/lpr/engine/ 2>&1; cp -v /dbfs/lpr/engine/*.jar /databricks/jars/ 2>&1; fi
ls -la /databricks/jars/lpr*.jar /databricks/jars/library-runtime*.jar 2>&1
exit 0
