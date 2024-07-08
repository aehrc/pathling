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

package au.csiro.pathling.test.builders;

import static org.mockito.Mockito.mock;

import au.csiro.pathling.fhirpath.LegacyEvaluationContext;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.registry.FunctionRegistry;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.fhirpath.parser.ConstantReplacer;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.DefaultAnswer;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author John Grimes
 */
public class EvaluationContextBuilder {

  @Nonnull
  private final DataSource dataSource;
  @Nonnull
  private Collection inputContext;

  @Nonnull
  private ResourceCollection resource;

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final SparkSession spark;

  @Nonnull
  private Dataset<Row> dataset;

  @Nonnull
  private FunctionRegistry<NamedFunction> functionRegistry;

  @Nullable
  private TerminologyServiceFactory terminologyServiceFactory;

  @Nullable
  private ConstantReplacer constantReplacer;

  public EvaluationContextBuilder(@Nonnull final SparkSession spark,
      @Nonnull final FhirContext fhirContext) {
    inputContext = resource;
    resource = mock(ResourceCollection.class);
    this.fhirContext = fhirContext;
    this.spark = spark;
    this.dataSource = mock(DataSource.class);
    //noinspection unchecked
    dataset = mock(Dataset.class, new DefaultAnswer());
    functionRegistry = new StaticFunctionRegistry();
    terminologyServiceFactory = mock(TerminologyServiceFactory.class);
    constantReplacer = mock(ConstantReplacer.class);
  }

  @Nonnull
  public EvaluationContextBuilder inputContext(@Nonnull final Collection inputContext) {
    this.inputContext = inputContext;
    return this;
  }

  @Nonnull
  public EvaluationContextBuilder resource(@Nonnull final ResourceCollection resource) {
    this.resource = resource;
    return this;
  }

  @Nonnull
  public EvaluationContextBuilder dataset(@Nonnull final Dataset<Row> dataset) {
    this.dataset = dataset;
    return this;
  }

  @Nonnull
  public EvaluationContextBuilder functionRegistry(
      @Nonnull final FunctionRegistry<NamedFunction> functionRegistry) {
    this.functionRegistry = functionRegistry;
    return this;
  }

  @Nonnull
  public EvaluationContextBuilder terminologyServiceFactory(
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    this.terminologyServiceFactory = terminologyServiceFactory;
    return this;
  }

  @Nonnull
  public EvaluationContextBuilder constantReplacer(
      @Nonnull final ConstantReplacer constantReplacer) {
    this.constantReplacer = constantReplacer;
    return this;
  }

  @Nonnull
  public LegacyEvaluationContext build() {
    return new LegacyEvaluationContext(inputContext, resource, fhirContext, spark, dataSource,
        dataset, functionRegistry,
        Optional.ofNullable(terminologyServiceFactory), Optional.ofNullable(constantReplacer));
  }

}
