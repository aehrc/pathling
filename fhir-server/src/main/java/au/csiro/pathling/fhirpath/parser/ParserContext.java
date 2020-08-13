/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.parser;

import static au.csiro.pathling.utilities.Preconditions.check;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ThisPath;
import au.csiro.pathling.io.ResourceReader;
import ca.uhn.fhir.context.FhirContext;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;

/**
 * Context and dependencies used in the parsing of a FHIRPath expression.
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
   * The item from an input collection currently under evaluation, e.g. an item from the input to
   * the `where` function. Used for the implementation of the {@code $this} element.
   *
   * @see <a href="https://hl7.org/fhirpath/2018Sep/index.html#functions-2">Functions</a>
   */
  @Nonnull
  private final Optional<ThisPath> thisContext;

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
  private final ResourceReader resourceReader;

  /**
   * The terminology client that should be used to resolve terminology queries within this
   * expression. Will only be present if a terminology service has been configured.
   */
  @Nonnull
  private final Optional<TerminologyClient> terminologyClient;

  /**
   * A factory for creating new TerminologyClient objects, which is needed within blocks of code
   * that are run in parallel. Will only be present if a terminology service has been configured.
   */
  @Nonnull
  private final Optional<TerminologyClientFactory> terminologyClientFactory;

  @Nonnull
  private Column groupByColumn;

  /**
   * @param inputContext The input context from which the FHIRPath is to be evaluated
   * @param thisContext The item from an input collection currently under evaluation
   * @param fhirContext A {@link FhirContext} that can be used to do FHIR stuff
   * @param sparkSession A {@link SparkSession} that can be used to resolve Spark queries required
   * for this expression
   * @param resourceReader For retrieving data relating to resource references
   * @param terminologyClient The {@link TerminologyClient} that should be used to resolve
   * terminology queries
   * @param terminologyClientFactory A factory for {@link TerminologyClient} objects, used for
   * parallel processing
   */
  public ParserContext(@Nonnull final FhirPath inputContext,
      @Nonnull final Optional<ThisPath> thisContext, @Nonnull final FhirContext fhirContext,
      @Nonnull final SparkSession sparkSession, @Nonnull final ResourceReader resourceReader,
      @Nonnull final Optional<TerminologyClient> terminologyClient,
      @Nonnull final Optional<TerminologyClientFactory> terminologyClientFactory) {
    check(inputContext.getIdColumn().isPresent());
    this.inputContext = inputContext;
    this.thisContext = thisContext;
    this.fhirContext = fhirContext;
    this.sparkSession = sparkSession;
    this.resourceReader = resourceReader;
    this.terminologyClient = terminologyClient;
    this.terminologyClientFactory = terminologyClientFactory;
    groupByColumn = inputContext.getIdColumn().get();
  }

  /**
   * @return The set of {@link Column} objects that should be used for grouping, when performing
   * aggregations
   */
  @Nonnull
  public Column[] getGroupBy() {
    return new Column[]{groupByColumn};
  }

  public void setGroupingColumns(@Nonnull final List<Column> columns) {
    check(columns.size() == 1);
    groupByColumn = columns.get(0);
  }

}
