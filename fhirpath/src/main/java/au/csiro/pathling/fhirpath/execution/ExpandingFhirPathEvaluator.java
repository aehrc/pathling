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
import au.csiro.pathling.fhirpath.FhirPathStreamVisitor;
import au.csiro.pathling.fhirpath.FhirPathVisitor;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ReferenceCollection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.context.FhirPathContext;
import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.fhirpath.context.ViewEvaluationContext;
import au.csiro.pathling.fhirpath.execution.DataRoot.ResourceRoot;
import au.csiro.pathling.fhirpath.execution.DataRoot.ReverseResolveRoot;
import au.csiro.pathling.fhirpath.function.registry.FunctionRegistry;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.path.Paths;
import au.csiro.pathling.fhirpath.path.Paths.EvalFunction;
import au.csiro.pathling.fhirpath.path.Paths.This;
import au.csiro.pathling.fhirpath.path.Paths.Traversal;
import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.jetbrains.annotations.NotNull;


@Value
public class ExpandingFhirPathEvaluator implements FhirPathEvaluator {

  // TODO: Move somewhere else

  @Nonnull
  public static Column collect_map(@Nonnull final Column mapColumn) {
    // TODO: try to implement this more eccielty and in a way 
    // that does not requiere:
    // .config("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
    return functions.reduce(
        functions.collect_list(mapColumn),
        functions.any_value(mapColumn),
        (acc, elem) -> functions.when(acc.isNull(), elem).otherwise(functions.map_concat(acc, elem))
    );
  }


  @Nonnull
  public Dataset<Row> createInitialDataset() {
    return resourceDataset(subjectResource);
  }


  @Override
  public @NotNull CollectionDataset evaluate(@NotNull final String fhirpathExpression) {

    final FhirPath fhirPath = parser.parse(fhirpathExpression);
    final Set<DataRoot> joinRoots = findJoinsRoots(fhirPath);
    System.out.println("Join roots: " + joinRoots);
    joinRoots.forEach(System.out::println);

    Dataset<Row> resolvedDataset =
        joinRoots.size() == 1
        ? reverseResolveJoin((ReverseResolveRoot) joinRoots.iterator().next(),
            createInitialDataset(), null)
        : createInitialDataset();

    final ResourceResolver resourceResolver = new EmptyResourceResolver();
    final FhirPathContext fhirpathContext = FhirPathContext.ofResource(
        resourceResolver.resolveResource(subjectResource));
    final EvaluationContext evalContext = new ViewEvaluationContext(
        fhirpathContext,
        functionRegistry,
        resourceResolver);
    return CollectionDataset.of(
        resolvedDataset,
        fhirPath.apply(fhirpathContext.getInputContext(), evalContext)
    );
  }


  @Nonnull
  private Dataset<Row> computeReverseJoin(@Nonnull final Dataset<Row> parentDataset,
      @Nullable final Dataset<Row> maybeChildDataset, @Nonnull final ReverseResolveRoot joinRoot) {

    System.out.println("Computing reverse join for: " + joinRoot);

    final FhirPathExecutor childExecutor = createExecutor(joinRoot.getForeignResourceType(),
        dataSource);

    final Dataset<Row> childDataset = maybeChildDataset == null
                                      ? childExecutor.createInitialDataset()
                                      : maybeChildDataset;

    final CollectionDataset referenceResult =
        childExecutor.evaluate(parser.parse(joinRoot.getForeignKeyPath()), childDataset);

    // check the type of the reference matches
    final Set<ResourceType> allowedReferenceTypes = ((ReferenceCollection) referenceResult.getValue()).getReferenceTypes();
    if (!allowedReferenceTypes.contains(joinRoot.getMaster().getResourceType())) {
      throw new IllegalArgumentException(
          "Reference type does not match. Expected: " + allowedReferenceTypes + " but got: "
              + joinRoot.getMaster().getResourceType());
    }

    final CollectionDataset childParentKeyResult =
        childExecutor.evaluate(joinRoot.getForeignKeyPath() + "." + "reference");
    final Collection childResource = childExecutor.createDefaultInputContext();

    final Dataset<Row> childInput = referenceResult.getDataset();
    System.out.println("Child input:");
    childInput.show();

    final Column[] passThroughColumns = Stream.of(childInput.columns())
        .filter(c -> c.contains("@"))
        // TODO: replace with the map_combine function
        .map(c -> collect_map(functions.col(c)).alias(c))
        .toArray(Column[]::new);

    final Dataset<Row> childResult = childInput
        .groupBy(childParentKeyResult.getValueColumn().alias(joinRoot.getChildKeyTag()))
        .agg(
            functions.map_from_entries(
                // wrap the element into array to create the map
                functions.array(
                    functions.struct(
                        functions.any_value(childParentKeyResult.getValueColumn()).alias("key"),
                        functions.collect_list(childResource.getColumnValue()).alias("value")
                    )
                )
            ).alias(joinRoot.getValueTag()),
            passThroughColumns
            // pass through for any existing columns that require
            // map aggregation
        );

    childResult.show();
    final Dataset<Row> joinedDataset = parentDataset.join(childResult,
            functions.col("key")
                .equalTo(childResult.col(joinRoot.getChildKeyTag())),
            "left_outer")
        .drop(joinRoot.getChildKeyTag());
    joinedDataset.show();
    return joinedDataset;

  }


  @Nonnull
  private Dataset<Row> reverseResolveJoin(@Nonnull final ReverseResolveRoot joinRoot,
      @Nonnull final Dataset<Row> parentDataset, @Nullable final Dataset<Row> maybeChildDataset) {

    // 

    // for nested joins we need to do it recursively aggregating all the existing join map columns
    // the problem is that I need to do it in reverse order 

    // Ineed to to do deep unnestign

    if (joinRoot.getMaster() instanceof ResourceRoot) {
      return computeReverseJoin(parentDataset, maybeChildDataset, joinRoot);
    } else if (joinRoot.getMaster() instanceof ReverseResolveRoot rrr) {

      final Dataset<Row> childDataset = computeReverseJoin(
          createExecutor(rrr.getResourceType(), dataSource).createInitialDataset(),
          maybeChildDataset, joinRoot);

      return reverseResolveJoin(rrr, parentDataset, childDataset);
      // compute the dataset for the current resolve root with the master type as the subject
      // and default master resource as parent dataset

      //  resulting dataset then needs to be passed as the child result for the deeper join
      //  if there is no child result then we create one from the default child dataset (so initially it's null)

    } else {
      throw new UnsupportedOperationException(
          "Not implemented - unknown root type: " + joinRoot.getMaster());
    }
  }


  @Nonnull
  private FhirPathExecutor createExecutor(final ResourceType subjectResourceType,
      final DataSource dataSource) {
    return new SingleFhirPathExecutor(subjectResourceType,
        fhirContext, functionRegistry,
        Collections.emptyMap(), dataSource);
  }

  @Value
  static class DataRootFinderVisitor implements FhirPathStreamVisitor<DataRoot> {

    @Nonnull
    ResourceType subjectResource;

    @Nonnull
    @Override
    public Stream<DataRoot> visitPath(@Nonnull final FhirPath path) {
      if (path instanceof EvalFunction && "reverseResolve" .equals(((EvalFunction) path)
          .getFunctionIdentifier())) {
        // actually we know we do not want to visit the children of this function
        return Stream.of(ExecutorUtils.fromPath(subjectResource, (EvalFunction) path));
      } else {
        return visitChildren(path);
      }
    }
  }

  @Value
  static class DependencyFinderVisitor implements FhirPathStreamVisitor<DataDependency> {

    @Nonnull
    Optional<DataRoot> resourceContext;

    @Nonnull
    @Override
    public Stream<DataDependency> visitPath(@Nonnull final FhirPath path) {
      if (path instanceof Traversal) {
        return resourceContext.map(r -> DataDependency.of(r, ((Traversal) path).getPropertyName()))
            .stream();
      } else if (path instanceof EvalFunction && "reverseResolve" .equals(((EvalFunction) path)
          .getFunctionIdentifier())) {
        return Stream.empty();
      } else {
        return visitChildren(path);
      }
    }

    @Nonnull
    @Override
    public FhirPathVisitor<Stream<DataDependency>> enterContext(@Nonnull final FhirPath path) {
      if (path instanceof Paths.Resource) {
        return new DependencyFinderVisitor(Optional.of(ResourceRoot.of(
            ((Paths.Resource) path).getResourceType())));
      } else if (isTransparent(path)) {
        return this;
      } else if (path instanceof EvalFunction && "reverseResolve" .equals(((EvalFunction) path)
          .getFunctionIdentifier())) {
        return new DependencyFinderVisitor(
            Optional.of(ExecutorUtils.fromPath(resourceContext.orElseThrow(),
                (EvalFunction) path)));
      } else {
        return resourceContext.isEmpty()
               ? this
               : new DependencyFinderVisitor(Optional.empty());
      }
    }

    private static final Set<String> TRANSPARENT_FUNCTIONS = Set.of("first", "last", "where");

    static boolean isTransparent(@Nonnull final FhirPath path) {
      return
          (path instanceof EvalFunction && TRANSPARENT_FUNCTIONS.contains(((EvalFunction) path)
              .getFunctionIdentifier()))
              || (path instanceof This);
    }
  }

  class EmptyResourceResolver implements ResourceResolver {

    @Nonnull
    @Override
    public ResourceCollection resolveResource(@Nonnull final ResourceType resourceType) {
      return ResourceCollection.build(
          new DefaultRepresentation(functions.col(resourceType.toCode())),
          fhirContext, resourceType);
    }

    @Nonnull
    @Override
    public ResourceCollection resolveReverseJoin(@Nonnull final ResourceCollection parentResource,
        @Nonnull final String expression) {

      final ResourceType resourceType = parentResource.getResourceType();
      // TODO: implement this
      final String resourceName = expression.split("\\.")[0];
      final ResourceType foreignResourceType = ResourceType.fromCode(resourceName);

      final ReverseResolveRoot root = ReverseResolveRoot.of(parentResource.getDataRoot(),
          foreignResourceType, expression.split("\\.")[1]);

      return ResourceCollection.build(
          parentResource.getColumn().traverse("id_versioned")
              .applyTo(functions.col(root.getValueTag())),
          fhirContext, root);
    }
  }

  @Nonnull
  ResourceType subjectResource;

  @Nonnull
  FhirContext fhirContext;

  @Nonnull
  FunctionRegistry<?> functionRegistry;

  @Nonnull
  DataSource dataSource;


  @Nonnull
  Parser parser = new Parser();

  @Nonnull
  public Collection validate(@Nonnull final FhirPath path) {

    final ResourceResolver resourceResolver = new EmptyResourceResolver();
    final FhirPathContext fhirpathContext = FhirPathContext.ofResource(
        resourceResolver.resolveResource(subjectResource));
    final EvaluationContext evalContext = new ViewEvaluationContext(
        fhirpathContext,
        functionRegistry,
        resourceResolver);
    return path.apply(fhirpathContext.getInputContext(), evalContext);
  }


  @Nonnull
  Dataset<Row> resourceDataset(@Nonnull final ResourceType resourceType) {
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
  Dataset<Row> reverseJoinDataset(@Nonnull final ReverseResolveRoot dataRoot,
      @Nonnull final List<String> dependencies) {
    final Dataset<Row> dataset = dataSource.read(dataRoot.getForeignResourceType());
    final Set<String> columns = Stream.of(dataset.columns())
        .collect(Collectors.toUnmodifiableSet());
    final String joinTag = dataRoot.getTag();

    // TODO: make it better
    final Column referenceColumn = dataset.col(dataRoot.getForeignKeyPath())
        .getField("reference");
    return dataset.groupBy(referenceColumn.alias(dataRoot.getParentKeyTag()))
        .agg(
            functions.collect_list(
                functions.struct(
                    dependencies.stream().filter(columns::contains)
                        .map(dataset::col).toArray(Column[]::new)
                )
            ).alias(joinTag)
        );
  }


  @Nonnull
  public Dataset<Row> execute(@Nonnull final FhirPath path) {
    // just as above ... but with a more intelligent resourceResolver
    final ResourceResolver resourceResolver = new EmptyResourceResolver();

    // we will need to extract the dependencies and create the map for and the dataset;
    // but for now just make it work for the main resource
    final Dataset<Row> patients = resourceDataset(subjectResource);

    final FhirPathContext fhirpathContext = FhirPathContext.ofResource(
        resourceResolver.resolveResource(subjectResource));
    final EvaluationContext evalContext = new ViewEvaluationContext(
        fhirpathContext,
        functionRegistry,
        resourceResolver);

    final List<DataView> reverseJoinsViews = findDataViews(path).stream()
        .filter(dv -> dv.getRoot() instanceof ReverseResolveRoot)
        .toList();

    // for each join eval the reference column

    Dataset<Row> derivedDataset = patients;
    for (DataView reverseJoinsView : reverseJoinsViews) {
      final ReverseResolveRoot reverseJoin = (ReverseResolveRoot) reverseJoinsView.getRoot();

      if (!ResourceRoot.of(subjectResource).equals(reverseJoin.getMaster())) {
        throw new IllegalStateException("Only reverse resolve to subject resource");
      }

      final Dataset<Row> foreignDatasetJoin = reverseJoinDataset(
          reverseJoin,
          reverseJoinsView.getDependencies()
      );

      derivedDataset = derivedDataset.join(
          foreignDatasetJoin,
          patients.col("key").equalTo(foreignDatasetJoin.col(reverseJoin.getParentKeyTag())),
          "left_outer"
      ).drop(reverseJoin.getParentKeyTag());
    }
    final Collection result = path.apply(fhirpathContext.getInputContext(), evalContext);
    return derivedDataset.select(functions.col("id"), result.getColumnValue().alias("value"));
  }

  public Dataset<Row> execute(@NotNull final FhirPath path,
      @NotNull final Dataset<Row> subjectDataset) {
    throw new UnsupportedOperationException("Not implemented");
  }

  public @NotNull CollectionDataset evaluate(@NotNull final FhirPath path,
      @NotNull final Dataset<Row> subjectDataset) {
    throw new UnsupportedOperationException("Not implemented");
  }


  @Nonnull
  public CollectionDataset evaluate(@Nonnull final FhirPath path) {
    // just as above ... but with a more intelligent resourceResolver
    final ResourceResolver resourceResolver = new EmptyResourceResolver();

    // we will need to extract the dependencies and create the map for and the dataset;
    // but for now just make it work for the main resource
    final Dataset<Row> patients = resourceDataset(subjectResource);

    final FhirPathContext fhirpathContext = FhirPathContext.ofResource(
        resourceResolver.resolveResource(subjectResource));
    final EvaluationContext evalContext = new ViewEvaluationContext(
        fhirpathContext,
        functionRegistry,
        resourceResolver);

    final List<DataView> reverseJoinsViews = findDataViews(path).stream()
        .filter(dv -> dv.getRoot() instanceof ReverseResolveRoot)
        .toList();

    // for each join eval the reference column

    Dataset<Row> derivedDataset = patients;
    for (final DataView reverseJoinsView : reverseJoinsViews) {
      final ReverseResolveRoot reverseJoin = (ReverseResolveRoot) reverseJoinsView.getRoot();

      if (!ResourceRoot.of(subjectResource).equals(reverseJoin.getMaster())) {
        throw new IllegalStateException("Only reverse resolve to subject resource");
      }

      final Dataset<Row> foreignDatasetJoin = reverseJoinDataset(
          reverseJoin,
          reverseJoinsView.getDependencies()
      );

      derivedDataset = derivedDataset.join(
          foreignDatasetJoin,
          patients.col("key").equalTo(foreignDatasetJoin.col(reverseJoin.getParentKeyTag())),
          "left_outer"
      ).drop(reverseJoin.getParentKeyTag());
    }
    final Collection result = path.apply(fhirpathContext.getInputContext(), evalContext);
    return CollectionDataset.of(derivedDataset, result);
  }

  @Nonnull
  public Collection evaluate(@Nonnull final FhirPath path, @Nonnull final Collection inputContext) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Nonnull
  public Collection createDefaultInputContext() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Nonnull
  public Set<DataRoot> findJoinsRoots(@Nonnull final FhirPath path) {
    return path.accept(new DependencyFinderVisitor(Optional.of(ResourceRoot.of(subjectResource))))
        .map(DataDependency::getRoot)
        .filter(ReverseResolveRoot.class::isInstance)
        .collect(Collectors.toUnmodifiableSet());
  }

  @Nonnull
  public Set<DataDependency> findDataDependencies(@Nonnull final FhirPath path) {
    return path.accept(new DependencyFinderVisitor(Optional.of(ResourceRoot.of(subjectResource))))
        .collect(Collectors.toUnmodifiableSet());
  }


  @Nonnull
  List<DataView> findDataViews(@Nonnull final FhirPath path) {
    return findDataDependencies(path).stream().collect(
            Collectors.groupingBy(DataDependency::getRoot,
                Collectors.mapping(DataDependency::getElement, Collectors.toUnmodifiableList()))
        ).entrySet().stream().map(e -> DataView.of(e.getKey(), e.getValue()))
        .toList();
  }


}
