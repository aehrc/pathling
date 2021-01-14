#!/usr/bin/env bash

#
# Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
# Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
# Software Licence Agreement.
#

java $JAVA_OPTS -Djava.library.path=/opt/hadoop/lib/native -ea -jar /pathling.jar