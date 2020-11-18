/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.parser;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.io.ResourceReader;
import ca.uhn.fhir.context.FhirContext;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
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

  /**
   * The context may contain zero or more grouping columns. If there are columns in this list, the
   * data will be aggregated using them rather than the resource identity.
   */
  @Nonnull
  private final Optional<List<Column>> groupingColumns;

  /**
   * When within the context of function arguments, this is the {@link FhirPath} that represents the
   * item in a collection currently being iterated over, denoted by the {@code $this} keyword.
   */
  @Nonnull
  private Optional<FhirPath> thisContext = Optional.empty();

  /**
   * @param inputContext The input context from which the FHIRPath is to be evaluated
   * @param fhirContext A {@link FhirContext} that can be used to do FHIR stuff
   * @param sparkSession A {@link SparkSession} that can be used to resolve Spark queries required
   * for this expression
   * @param resourceReader For retrieving data relating to resource references
   * @param terminologyClient The {@link TerminologyClient} that should be used to resolve
   * terminology queries
   * @param terminologyClientFactory A factory for {@link TerminologyClient} objects, used for
   * parallel processing
   * @param groupingColumns the list of columns to group on when aggregating
   */
  public ParserContext(@Nonnull final FhirPath inputContext, @Nonnull final FhirContext fhirContext,
      @Nonnull final SparkSession sparkSession, @Nonnull final ResourceReader resourceReader,
      @Nonnull final Optional<TerminologyClient> terminologyClient,
      @Nonnull final Optional<TerminologyClientFactory> terminologyClientFactory,
      @Nonnull final Optional<List<Column>> groupingColumns) {
    this.inputContext = inputContext;
    this.fhirContext = fhirContext;
    this.sparkSession = sparkSession;
    this.resourceReader = resourceReader;
    this.terminologyClient = terminologyClient;
    this.terminologyClientFactory = terminologyClientFactory;
    this.groupingColumns = groupingColumns;
  }

  public void setThisContext(@Nonnull final FhirPath thisContext) {
    this.thisContext = Optional.of(thisContext);
  }

}
