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

package au.csiro.pathling.terminology;

import static au.csiro.pathling.sql.TerminologySupport.parseCsvEquivalences;

import au.csiro.pathling.sql.Terminology;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Describes the interface to the terminology functions from a library context.
 */
public interface TerminologyFunctions {

  /**
   * Tests whether the codings within the specified column are members of the specified value set.
   * Creates a new column containing the result.
   *
   * @param codingArrayCol the column containing the codings to test
   * @param valueSetUri the URI of the value set to test against
   * @param dataset the dataset containing the codings
   * @param outputColumnName the name of the output column
   * @return a new dataset with a new column containing the result
   * @see <a href="https://www.hl7.org/fhir/valueset-operation-validate-code.html">Operation
   * $validate-code on ValueSet</a>
   */
  @Nonnull
  Dataset<Row> memberOf(@Nonnull final Column codingArrayCol, @Nonnull final String valueSetUri,
      @Nonnull final Dataset<Row> dataset, @Nonnull final String outputColumnName);

  /**
   * Translates the codings within the specified column using a concept map known to the terminology
   * service.
   *
   * @param codingArrayCol the column containing the codings to translate
   * @param conceptMapUrl the URL of the concept map to use for translation
   * @param reverse if true, the translation will be reversed
   * @param equivalencesCsv the CSV representation of the equivalences to use for translation
   * @param target the target value set to translate to
   * @param dataset the dataset containing the codings
   * @param outputColumnName the name of the output column
   * @return a new dataset with a new column containing the result
   * @see <a href="https://www.hl7.org/fhir/conceptmap-operation-translate.html">Operation
   * $translate on ConceptMap</a>
   */
  @Nonnull
  Dataset<Row> translate(@Nonnull final Column codingArrayCol, @Nonnull final String conceptMapUrl,
      final boolean reverse, @Nonnull final String equivalencesCsv, @Nullable final String target,
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final String outputColumnName);

  /**
   * Tests whether one or more of a set of codings subsume one or more of another set of codings.
   *
   * @param dataset the dataset containing the codings
   * @param codingArrayA the column containing the first set of codings
   * @param codingArrayB the column containing the second set of codings
   * @param outputColumnName the name of the output column
   * @param inverted if true, the subsumption test will be inverted
   * @return a new dataset with a new column containing the result
   * @see <a href="https://www.hl7.org/fhir/codesystem-operation-subsumes.html">Operation $subsumes
   * on CodeSystem</a>
   */
  @Nonnull
  Dataset<Row> subsumes(@Nonnull final Dataset<Row> dataset,
      @Nonnull final Column codingArrayA, @Nonnull final Column codingArrayB,
      @Nonnull final String outputColumnName, final boolean inverted);

  @Nonnull
  static TerminologyFunctions build() {
    return new TerminologyFunctionsImpl();
  }
}

/**
 * An implementation of the library terminology functions interface that uses the UDFs.
 */
class TerminologyFunctionsImpl implements TerminologyFunctions {

  TerminologyFunctionsImpl() {
  }

  @Nonnull
  @Override
  public Dataset<Row> memberOf(@Nonnull final Column codingArrayCol,
      @Nonnull final String valueSetUri, @Nonnull final Dataset<Row> dataset,
      @Nonnull final String outputColumnName) {
    return dataset.withColumn(outputColumnName, Terminology.member_of(codingArrayCol, valueSetUri));
  }

  @Nonnull
  @Override
  public Dataset<Row> translate(@Nonnull final Column codingArrayCol,
      @Nonnull final String conceptMapUrl, final boolean reverse,
      @Nonnull final String equivalencesCsv,
      @Nullable final String target,
      @Nonnull final Dataset<Row> dataset, @Nonnull final String outputColumnName) {
    return dataset.withColumn(outputColumnName,
        Terminology.translate(codingArrayCol, conceptMapUrl, reverse,
            parseCsvEquivalences(equivalencesCsv), target));
  }

  @Override
  @Nonnull
  public Dataset<Row> subsumes(@Nonnull final Dataset<Row> dataset,
      @Nonnull final Column codingArrayA, @Nonnull final Column codingArrayB,
      @Nonnull final String outputColumnName, final boolean inverted) {
    return dataset.withColumn(outputColumnName,
        inverted
        ? Terminology.subsumed_by(codingArrayA, codingArrayB)
        : Terminology.subsumes(codingArrayA, codingArrayB));
  }

}
