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

package au.csiro.pathling.fhirpath.parser;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;

/**
 * Context and dependencies used in the parsing of a FHIRPath expression. The parser context can
 * change through the parsing of an expression as it passes through different states (e.g.
 * aggregation, inside the arguments to a function). Change to the parser context is only permitted
 * through a limited set of public setter methods.
 *
 * @author John Grimes
 */
@Getter
public class ParserContext {

  /**
   * The input context from which the FHIRPath is to be evaluated, which is then referred to through
   * {@code %resource} or {@code %context}.
   *
   * @see <a href="https://hl7.org/fhirpath/2018Sep/index.html#path-selection">Path selection</a>
   * @see <a href="https://hl7.org/fhirpath/2018Sep/index.html#environment-variables">Environment
   * variables</a>
   * @see <a href="https://hl7.org/fhir/R4/fhirpath.html#variables">FHIR Specific Variables</a>
   */
  @Nonnull
  private final FhirPath inputContext;

  /**
   * A FHIR context that can be used to do FHIR stuff.
   */
  @Nonnull
  private final FhirContext fhirContext;

  /**
   * The Spark session that can be used to resolve Spark queries required for this expression.
   */
  @Nonnull
  private final SparkSession sparkSession;

  /**
   * A table resolver for retrieving Datasets for resource references.
   */
  @Nonnull
  private final DataSource dataSource;

  /**
   * A factory for creating new {@link TerminologyService} objects, which is needed within blocks of
   * code that are run in parallel. Will only be present if a terminology service has been
   * configured.
   */
  @Nonnull
  private final Optional<TerminologyServiceFactory> terminologyServiceFactory;

  /**
   * The context may contain zero or more grouping columns. If the context of evaluation is a single
   * resource, the resource identity column should be the one and only column in this list.
   * Otherwise, columns that provide the relevant groupings should be in here.
   */
  @Nonnull
  private final List<Column> groupingColumns;

  /**
   * When within the context of function arguments, this is the {@link FhirPath} that represents the
   * item in a collection currently being iterated over, denoted by the {@code $this} keyword.
   */
  @Nonnull
  private Optional<FhirPath> thisContext = Optional.empty();

  /**
   * Stores away columns relating to the identity of resources and elements, for later retrieval
   * using the string path. This is used for element and resource-identity aware joins.
   */
  @Nonnull
  private final Map<String, Column> nodeIdColumns;

  /**
   * @param inputContext the input context from which the FHIRPath is to be evaluated
   * @param fhirContext a {@link FhirContext} that can be used to do FHIR stuff
   * @param sparkSession a {@link SparkSession} that can be used to resolve Spark queries required
   * for this expression
   * @param dataSource for retrieving data relating to resource references
   * @param terminologyServiceFactory a factory for {@link TerminologyService} objects, used for
   * parallel processing
   * @param groupingColumns the list of columns to group on when aggregating
   * @param nodeIdColumns columns relating to the identity of resources and elements for different
   * paths parsed within this context
   */
  public ParserContext(@Nonnull final FhirPath inputContext, @Nonnull final FhirContext fhirContext,
      @Nonnull final SparkSession sparkSession, @Nonnull final DataSource dataSource,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory,
      @Nonnull final List<Column> groupingColumns,
      @Nonnull final Map<String, Column> nodeIdColumns) {
    this.inputContext = inputContext;
    this.fhirContext = fhirContext;
    this.sparkSession = sparkSession;
    this.dataSource = dataSource;
    this.terminologyServiceFactory = terminologyServiceFactory;
    this.groupingColumns = groupingColumns;
    this.nodeIdColumns = nodeIdColumns;
  }

  public void setThisContext(@Nonnull final FhirPath thisContext) {
    this.thisContext = Optional.of(thisContext);
  }

}
