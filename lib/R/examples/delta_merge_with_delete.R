# Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
# Organisation (CSIRO) ABN 41 687 119 230.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This example demonstrates using delete_on_merge with Delta Lake tables.
#
# When merging data with delete_on_merge=TRUE, resources present in the destination
# but not in the source will be deleted. This is useful for synchronising a subset
# of data or removing stale records.

library(sparklyr)
library(pathling)
library(dplyr)

# Create Pathling context.
pc <- pathling_connect()

# Read NDJSON data.
ndjson_dir <- pathling_examples("ndjson")
full_data <- pc %>% pathling_read_ndjson(ndjson_dir)

# Create a temporary directory for the Delta tables.
temp_dir <- tempfile(pattern = "delta_merge_delete_")

# Write the full dataset to Delta format.
full_data %>% ds_write_delta(temp_dir, save_mode = SaveMode$OVERWRITE)

# Read back the data and count patients.
initial_data <- pc %>% pathling_read_delta(temp_dir)
initial_count <- initial_data %>%
    ds_read("Patient") %>%
    summarise(count = n()) %>%
    collect()

cat("Initial patient count:", initial_count$count, "\n")

# Create a subset with only 3 specific patients.
subset_patients <- full_data %>%
    ds_read("Patient") %>%
    filter(id %in% c(
        "8ee183e2-b3c0-4151-be94-b945d6aa8c6d",
        "beff242e-580b-47c0-9844-c1a68c36c5bf",
        "e62e52ae-2d75-4070-a0ae-3cc78d35ed08"
    ))

# Create a new datasource with the subset of patients and all conditions.
subset_data <- pc %>% pathling_read_datasets(list(
    Patient = subset_patients,
    Condition = full_data %>% ds_read("Condition")
))

# Merge with delete_on_merge=TRUE - this will delete patients not in the subset.
subset_data %>% ds_write_delta(
    temp_dir,
    save_mode = SaveMode$MERGE,
    delete_on_merge = TRUE
)

# Read back the merged data and verify only 3 patients remain.
merged_data <- pc %>% pathling_read_delta(temp_dir)
final_count <- merged_data %>%
    ds_read("Patient") %>%
    summarise(count = n()) %>%
    collect()

cat("Final patient count after merge with delete:", final_count$count, "\n")
cat("Deleted", initial_count$count - final_count$count, "patients\n")

# Show the remaining patient IDs.
cat("\nRemaining patients:\n")
merged_data %>%
    ds_read("Patient") %>%
    select(id) %>%
    show()

# Clean up the temporary directory.
unlink(temp_dir, recursive = TRUE)

pc %>% pathling_disconnect()
