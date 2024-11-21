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

package au.csiro.pathling.fhirpath.execution;

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.context.FhirPathContext;
import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.fhirpath.context.ViewEvaluationContext;
import au.csiro.pathling.fhirpath.function.registry.FunctionRegistry;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.variable.VariableResolverChain;
import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.stream.Stream;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;


@Value
public class SingleFhirPathExecutor implements FhirPathExecutor {

  @Nonnull
  ResourceType subjectResource;

  @Nonnull
  FhirContext fhirContext;

  @Nonnull
  FunctionRegistry<?> functionRegistry;

  @Nonnull
  Map<String, Collection> variables;

  @Nonnull
  DataSource dataSource;

  @Override
  @Nonnull
  public Collection validate(@Nonnull final FhirPath path) {

    final ResourceResolver resourceResolver = new UnsupportedResourceResolver();
    final FhirPathContext fhirpathContext = FhirPathContext.ofResource(
        resolveResource(subjectResource), variables);
    final EvaluationContext evalContext = new ViewEvaluationContext(fhirpathContext,
        functionRegistry, resourceResolver);
    return path.apply(fhirpathContext.getInputContext(), evalContext);
  }


  static class UnsupportedResourceResolver implements ResourceResolver {

    @Nonnull
    @Override
    public ResourceCollection resolveResource(@Nonnull final ResourceType resourceType) {
      throw new UnsupportedOperationException("resolveResource() is not supported");
    }

    @Nonnull
    @Override
    public ResourceCollection resolveReverseJoin(@Nonnull final ResourceType resourceType,
        @Nonnull final String expression) {
      throw new UnsupportedOperationException("resolveReverseJoin() is not supported");
    }
  }


  @Nonnull
  Dataset<Row> resourceDataset(@Nonnull final ResourceType resourceType) {
    final Dataset<Row> dataset = dataSource.read(resourceType);

    final Stream<Column> explicitColumns = Stream.of(
        dataset.col("id"),
        dataset.col("id_versioned").alias("key"),
        functions.struct(
            Stream.of(dataset.columns()).filter(c -> !c.startsWith("_"))
                .map(dataset::col).toArray(Column[]::new)
        ).alias(resourceType.toCode())
    );
    final Stream<Column> implicitColumns = Stream.of(dataset.columns())
        .filter(c -> c.startsWith("_"))
        .map(dataset::col);

    return dataset.select(Stream.concat(explicitColumns, implicitColumns)
        .toArray(Column[]::new));
  }

  ResourceCollection resolveResource(@Nonnull final ResourceType resourceType) {
    return ResourceCollection.build(
        new DefaultRepresentation(functions.col(resourceType.toCode())),
        fhirContext, resourceType);
  }


  @Override
  @Nonnull
  public Dataset<Row> execute(@Nonnull final FhirPath path) {
    // just as above ... but with a more intelligent resourceResolver
    final ResourceResolver resourceResolver = new UnsupportedResourceResolver();

    // we will need to extract the dependencies and create the map for and the dataset;
    // but for now just make it work for the main resource
    final Dataset<Row> dataset = resourceDataset(subjectResource);
    final FhirPathContext fhirpathContext = FhirPathContext.ofResource(
        resolveResource(subjectResource), variables);
    final EvaluationContext evalContext = new ViewEvaluationContext(
        fhirpathContext,
        functionRegistry,
        resourceResolver);
    final Collection result = path.apply(fhirpathContext.getInputContext(), evalContext);
    return dataset.select(functions.col("id"), result.getColumnValue().alias("value"));
  }


  @Override
  @Nonnull
  public CollectionDataset evaluate(@Nonnull final FhirPath path) {
    // just as above ... but with a more intelligent resourceResolver
    final ResourceResolver resourceResolver = new UnsupportedResourceResolver();

    // we will need to extract the dependencies and create the map for and the dataset;
    // but for now just make it work for the main resource
    final Dataset<Row> dataset = resourceDataset(subjectResource);
    final FhirPathContext fhirpathContext = FhirPathContext.ofResource(
        resolveResource(subjectResource), variables);
    final EvaluationContext evalContext = new ViewEvaluationContext(
        fhirpathContext,
        functionRegistry,
        resourceResolver);
    final Collection result = path.apply(fhirpathContext.getInputContext(), evalContext);
    return CollectionDataset.of(dataset, result);
  }

  @Nonnull
  public CollectionDataset evaluate(@Nonnull final String fhirpathExpression) {
    return evaluate(new Parser().parse(fhirpathExpression));
  }

  @Nonnull
  @Override
  public Collection evaluate(@Nonnull final FhirPath path, @Nonnull final Collection inputContext) {
    // just as above ... but with a more intelligent resourceResolver
    final ResourceResolver resourceResolver = new UnsupportedResourceResolver();
    final ResourceCollection resource = resolveResource(subjectResource);
    final VariableResolverChain variableResolverChain =
        VariableResolverChain.withDefaults(resource, inputContext, variables);
    final FhirPathContext fhirpathContext = FhirPathContext.of(
        resource, inputContext, variableResolverChain);
    final EvaluationContext evalContext = new ViewEvaluationContext(
        fhirpathContext,
        functionRegistry,
        resourceResolver);
    return path.apply(inputContext, evalContext);
  }

  @Nonnull
  @Override
  public Collection createDefaultInputContext() {
    return resolveResource(subjectResource);
  }

  @Nonnull
  @Override
  public Dataset<Row> createInitialDataset() {
    return resourceDataset(subjectResource);
  }


}
