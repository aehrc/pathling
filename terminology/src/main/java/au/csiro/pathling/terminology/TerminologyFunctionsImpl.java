/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.terminology;

import static au.csiro.pathling.sql.TerminologySupport.parseCsvEquivalences;

import au.csiro.pathling.sql.Terminology;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * An implementation of the library terminology functions interface that uses the UDFs.
 *
 * @author John Grimes
 */
class TerminologyFunctionsImpl implements TerminologyFunctions {

  TerminologyFunctionsImpl() {}

  @Nonnull
  @Override
  public Dataset<Row> memberOf(
      @Nonnull final Column codingArrayCol,
      @Nonnull final String valueSetUri,
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final String outputColumnName) {
    return dataset.withColumn(outputColumnName, Terminology.member_of(codingArrayCol, valueSetUri));
  }

  @Nonnull
  @Override
  public Dataset<Row> translate(
      @Nonnull final Column codingArrayCol,
      @Nonnull final String conceptMapUrl,
      final boolean reverse,
      @Nonnull final String equivalencesCsv,
      @Nullable final String target,
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final String outputColumnName) {
    return dataset.withColumn(
        outputColumnName,
        Terminology.translate(
            codingArrayCol, conceptMapUrl, reverse, parseCsvEquivalences(equivalencesCsv), target));
  }

  @Override
  @Nonnull
  public Dataset<Row> subsumes(
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column codingArrayA,
      @Nonnull final Column codingArrayB,
      @Nonnull final String outputColumnName,
      final boolean inverted) {
    return dataset.withColumn(
        outputColumnName,
        inverted
            ? Terminology.subsumed_by(codingArrayA, codingArrayB)
            : Terminology.subsumes(codingArrayA, codingArrayB));
  }
}
