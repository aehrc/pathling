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

package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ReferenceCollection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.registry.FunctionRegistry;
import au.csiro.pathling.fhirpath.function.registry.NoSuchFunctionException;
import au.csiro.pathling.fhirpath.parser.ConstantReplacer;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import lombok.Getter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Context and dependencies used in the evaluation of a FHIRPath expression.
 *
 * @author John Grimes
 */
@Getter
public class LegacyEvaluationContext implements EvaluationContext {

  /**
   * The input context from which the FHIRPath is to be evaluated, referred to through
   * {@code %context}.
   *
   * @see <a href="https://hl7.org/fhirpath/2018Sep/index.html#path-selection">Path selection</a>
   * @see <a href="https://hl7.org/fhirpath/2018Sep/index.html#environment-variables">Environment
   * variables</a>
   * @see <a href="https://hl7.org/fhir/R4/fhirpath.html#variables">FHIR Specific Variables</a>
   */
  @Nonnull
  private final Collection inputContext;

  /**
   * The resource that contains the input context, referred to through {@code %resource} or
   * {@code %rootResource}.
   *
   * @see <a href="https://hl7.org/fhirpath/2018Sep/index.html#path-selection">Path selection</a>
   * @see <a href="https://hl7.org/fhirpath/2018Sep/index.html#environment-variables">Environment
   * variables</a>
   * @see <a href="https://hl7.org/fhir/R4/fhirpath.html#variables">FHIR Specific Variables</a>
   */
  @Nonnull
  private final ResourceCollection resource;

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
   * A data source that can be used to resolve FhirPath queries required for this expression.
   */
  @Nonnull
  private final DataSource dataSource;

  /**
   * A table resolver for retrieving Datasets for resource references.
   */
  @Nonnull
  private final Dataset<Row> dataset;

  /**
   * A registry of FHIRPath function implementations.
   */
  @Nonnull
  private final FunctionRegistry<NamedFunction> functionRegistry;

  /**
   * A factory for creating new {@link TerminologyService} objects, which is needed within blocks of
   * code that are run in parallel. Will only be present if a terminology service has been
   * configured.
   */
  @Nonnull
  private final Optional<TerminologyServiceFactory> terminologyServiceFactory;

  /**
   * A list of constants and the expressions that they should be replaced with.
   */
  @Nonnull
  private final Optional<ConstantReplacer> constantReplacer;

  /**
   * @param inputContext the input context from which the FHIRPath is to be evaluated
   * @param fhirContext a {@link FhirContext} that can be used to do FHIR stuff
   * @param sparkSession a {@link SparkSession} that can be used to resolve Spark queries required
   * for this expression
   * @param dataset for retrieving data relating to resource references
   * @param terminologyServiceFactory a factory for {@link TerminologyService} objects, used for
   * parallel processing
   * @param constantReplacer a list of constants and the expressions that they should be replaced
   * with
   */
  public LegacyEvaluationContext(@Nonnull final Collection inputContext,
      @Nonnull final ResourceCollection resource,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final SparkSession sparkSession,
      @Nonnull final DataSource dataSource,
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final FunctionRegistry<NamedFunction> functionRegistry,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory,
      @Nonnull final Optional<ConstantReplacer> constantReplacer) {
    this.inputContext = inputContext;
    this.resource = resource;
    this.fhirContext = fhirContext;
    this.sparkSession = sparkSession;
    this.dataSource = dataSource;
    this.dataset = dataset;
    this.functionRegistry = functionRegistry;
    this.terminologyServiceFactory = terminologyServiceFactory;
    this.constantReplacer = constantReplacer;
  }

  /**
   * @return a new {@link LegacyEvaluationContext} with the same properties as this one, but with a
   * different input context
   */
  @Nonnull
  public LegacyEvaluationContext withInputContext(@Nonnull final Collection inputContext) {
    return new LegacyEvaluationContext(inputContext, resource, fhirContext, sparkSession,
        dataSource,
        dataset, functionRegistry, terminologyServiceFactory, constantReplacer);
  }


  @Nonnull
  @Override
  public NamedFunction<Collection> resolveFunction(@Nonnull final String name)
      throws NoSuchFunctionException {
    return functionRegistry.getInstance(name);
  }

  @Nonnull
  @Override
  public Collection resolveVariable(@Nonnull final String name) {
    if (name.equals("%context")) {
      return getInputContext();
    } else if (name.equals("%resource") || name.equals("%rootResource")) {
      return getResource();
    } else {
      throw new IllegalArgumentException("Unknown constant: " + name);
    }
  }

  @Nonnull
  @Override
  public Optional<ResourceCollection> resolveResource(@Nonnull final String resourceCode) {
    throw new UnsupportedOperationException("resolveResource() not supported in this context");
  }

  @Override
  @Nonnull
  public Collection resolveJoin(
      @Nonnull final ReferenceCollection referenceCollection) {
    throw new UnsupportedOperationException("resolveJoin() not supported in this context");
  }

  @Nonnull
  @Override
  public ResourceCollection resolveReverseJoin(@Nonnull final ResourceCollection parentResource,
      @Nonnull final String expression) {
    throw new UnsupportedOperationException("resolveReverseJoin() not supported in this context");
  }
}
