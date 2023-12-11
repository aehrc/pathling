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

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.PathEvalContext;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.column.StdColumnCtx;
import au.csiro.pathling.fhirpath.context.DefaultPathEvalContext;
import au.csiro.pathling.fhirpath.context.FhirpathContext;
import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.fhirpath.function.registry.FunctionRegistry;
import au.csiro.pathling.fhirpath.path.Paths;
import au.csiro.pathling.fhirpath.path.Paths.EvalFunction;
import au.csiro.pathling.fhirpath.path.Paths.Resource;
import au.csiro.pathling.fhirpath.path.Paths.Traversal;
import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static au.csiro.pathling.utilities.Functions.maybeCast;


@Value
public class FhirPathExecutor {

  List<EvalFunction> findJoins(@Nonnull final FhirPath<Collection> path) {
    return path.asStream()
        .flatMap(p -> p.children().flatMap(x -> x.asStream()))
        .map(maybeCast(EvalFunction.class)).flatMap(Optional::stream)
        .filter(ef -> "reverseResolve".equals(ef.getFunctionIdentifier()))
        .collect(Collectors.toUnmodifiableList());
  }

  class EmptyResourceResolver implements ResourceResolver {

    @Nonnull
    @Override
    public ResourceCollection resolveResource(@Nonnull final ResourceType resourceType) {
      return ResourceCollection.build(StdColumnCtx.of(functions.col(resourceType.toCode())),
          fhirContext, resourceType);
    }

    @Nonnull
    @Override
    public ResourceCollection resolveReverseJoin(@Nonnull final ResourceType resourceType,
        @Nonnull final String expression) {

      // TODO: implement this
      final String resourceName = expression.split("\\.")[0];
      final ResourceType foreignResourceType = ResourceType.fromCode(resourceName);

      return ResourceCollection.build(StdColumnCtx.of(
              functions.col(resourceType.toCode() + "@" + expression.replace('.', '_'))),
          fhirContext, foreignResourceType);
    }
  }

  @Nonnull
  FhirContext fhirContext;

  @Nonnull
  FunctionRegistry<?> functionRegistry;

  @Nonnull
  ResourceType subjectResource;

  @Nonnull
  public Collection validate(@Nonnull final FhirPath<Collection> path) {

    final ResourceResolver resourceResolver = new EmptyResourceResolver();
    final FhirpathContext fhirpathContext = FhirpathContext.ofResource(
        resourceResolver.resolveResource(subjectResource));
    final PathEvalContext evalContext = new DefaultPathEvalContext(
        fhirpathContext,
        functionRegistry,
        resourceResolver);
    return path.apply(fhirpathContext.getInputContext(), evalContext);
  }


  @Nonnull
  public Dataset<Row> resourceDataset(@Nonnull final ResourceType resourceType,
      @Nonnull final DataSource dataSource) {
    final Dataset<Row> dataset = dataSource.read(resourceType);
    return dataset.select(
        dataset.col("id"),
        dataset.col("id_versioned").alias("key"),
        functions.struct(
            Stream.of(dataset.columns()).filter(c -> !c.startsWith("_"))
                .map(dataset::col).toArray(Column[]::new)
        ).alias(resourceType.toCode()));
  }


  @Nonnull
  public Dataset<Row> execute(@Nonnull final FhirPath<Collection> path,
      @Nonnull final DataSource dataSource) {

    // just as above ... but with a more intelligent resourceResolver
    final ResourceResolver resourceResolver = new EmptyResourceResolver();

    // we will need to extract the dependencies and create the map for and the dataset;
    // but for now just make it work for the main resource
    final Dataset<Row> dataset = dataSource.read(subjectResource);

    final Dataset<Row> patients = resourceDataset(subjectResource, dataSource);

    final FhirpathContext fhirpathContext = FhirpathContext.ofResource(
        resourceResolver.resolveResource(subjectResource));
    final PathEvalContext evalContext = new DefaultPathEvalContext(
        fhirpathContext,
        functionRegistry,
        resourceResolver);

    final List<EvalFunction> reverseJoins = findJoins(path);
    // for each join eval the reference column

    Dataset<Row> derivedDataset = patients;
    for (EvalFunction reverseJoin : reverseJoins) {
      final FhirPath<Collection> reference = reverseJoin.getArguments().get(0);
      final Resource foreingResource = (Resource) reference.first();
      final Dataset<Row> foreignDataset = resourceDataset(foreingResource.getResourceType(),
          dataSource);
      final Collection foreignKey = reference.andThen(new Traversal("reference"))
          .apply(fhirpathContext.getInputContext(), evalContext);
      final String joinTag =
          subjectResource.toCode() + "@" + reference.toExpression().replace('.', '_');
      final Dataset<Row> foreignDatasetJoin =
          foreignDataset.groupBy(foreignKey.getColumn().alias(joinTag + "_key"))
              .agg(
                  functions.collect_list(
                      foreignDataset.col(foreingResource.getResourceType().toCode())
                  ).alias(joinTag)
              );
      derivedDataset = derivedDataset.join(
          foreignDatasetJoin,
          patients.col("key").equalTo(foreignDatasetJoin.col(joinTag + "_key")),
          "left_outer"
      ).drop(joinTag + "_key");
    }

    final Collection result = path.apply(fhirpathContext.getInputContext(), evalContext);
    return derivedDataset.select(functions.col("id"), result.getColumn().alias("value"));
  }
}
