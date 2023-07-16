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
import au.csiro.pathling.fhirpath.FhirPathAndContext;
import au.csiro.pathling.fhirpath.Nesting;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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

  @Nonnull
  private final UnnestBehaviour unnestBehaviour;

  @Nonnull
  private final Map<String, FhirPathAndContext> variables;

  @Nonnull
  private final Nesting nesting;

  /**
   * @param inputContext the input context from which the FHIRPath is to be evaluated
   * @param fhirContext a {@link FhirContext} that can be used to do FHIR stuff
   * @param sparkSession a {@link SparkSession} that can be used to resolve Spark queries required
   * for this expression
   * @param dataSource for retrieving data relating to resource references
   * @param terminologyServiceFactory a factory for {@link TerminologyService} objects, used for
   * parallel processing
   * @param groupingColumns the list of columns to group on when aggregating paths parsed within
   * this context
   */
  public ParserContext(@Nonnull final FhirPath inputContext, @Nonnull final FhirContext fhirContext,
      @Nonnull final SparkSession sparkSession, @Nonnull final DataSource dataSource,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory,
      @Nonnull final List<Column> groupingColumns) {
    this(inputContext, fhirContext, sparkSession, dataSource, terminologyServiceFactory,
        groupingColumns, UnnestBehaviour.UNNEST, new HashMap<>(), new Nesting());
  }

  /**
   * @param inputContext the input context from which the FHIRPath is to be evaluated
   * @param fhirContext a {@link FhirContext} that can be used to do FHIR stuff
   * @param sparkSession a {@link SparkSession} that can be used to resolve Spark queries required
   * for this expression
   * @param dataSource for retrieving data relating to resource references
   * @param terminologyServiceFactory a factory for {@link TerminologyService} objects, used for
   * parallel processing
   * @param groupingColumns the list of columns to group on when aggregating
   * @param unnestBehaviour the execution context to use
   */
  public ParserContext(@Nonnull final FhirPath inputContext, @Nonnull final FhirContext fhirContext,
      @Nonnull final SparkSession sparkSession, @Nonnull final DataSource dataSource,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory,
      @Nonnull final List<Column> groupingColumns,
      @Nonnull final UnnestBehaviour unnestBehaviour,
      @Nonnull final Map<String, FhirPathAndContext> variables,
      @Nonnull final Nesting nesting) {
    this.inputContext = inputContext;
    this.fhirContext = fhirContext;
    this.sparkSession = sparkSession;
    this.dataSource = dataSource;
    this.terminologyServiceFactory = terminologyServiceFactory;
    this.groupingColumns = groupingColumns;
    this.unnestBehaviour = unnestBehaviour;
    this.variables = variables;
    this.nesting = nesting;
  }

  public void setThisContext(@Nonnull final FhirPath thisContext) {
    this.thisContext = Optional.of(thisContext);
  }

  /**
   * Creates a copy of the current parser context, but with the specified unnest behaviour.
   *
   * @param unnestBehaviour the {@link UnnestBehaviour} to use
   * @return a new #{link ParserContext}
   */
  public ParserContext withUnnestBehaviour(@Nonnull final UnnestBehaviour unnestBehaviour) {
    return new ParserContext(inputContext, fhirContext, sparkSession, dataSource,
        terminologyServiceFactory, groupingColumns, unnestBehaviour, variables, nesting);
  }

  /**
   * Creates a copy of the current parser context, but with a different input dataset and the node
   * IDs emptied out.
   *
   * @return a new {@link ParserContext}
   */
  public ParserContext withContextDataset(@Nonnull final Dataset<Row> contextDataset) {
    final FhirPath newInputContext = inputContext.withDataset(contextDataset);
    return new ParserContext(newInputContext, fhirContext, sparkSession, dataSource,
        terminologyServiceFactory, groupingColumns, unnestBehaviour, variables, nesting);
  }

}
