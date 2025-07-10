/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.sql;

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * Pathling-specific SQL functions that extend Spark SQL functionality.
 * <p>
 * This interface provides utility functions for working with Spark SQL columns in the context of
 * FHIR data processing. These functions handle common operations like pruning annotations, safely
 * concatenating maps, and collecting maps during aggregation.
 */
public interface SqlFunctions {

    /**
     * Removes all fields starting with '_' (underscore) from struct values.
     * <p>
     * This function is used to clean up internal/synthetic fields from FHIR resources
     * before presenting them to users. Fields that don't start with underscore are preserved.
     * Non-struct values are not affected by this function.
     *
     * @param col The column containing struct values to prune
     * @return A new column with underscore-prefixed fields removed from structs
     */
    @Nonnull
    static Column prune_annotations(@Nonnull final Column col) {
        return new Column(new PruneSyntheticFields(col.expr()));
    }
}
