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
import au.csiro.pathling.fhirpath.collection.ReferenceCollection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.collection.mixed.MixedResourceCollection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.context.FhirPathContext;
import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.fhirpath.context.ViewEvaluationContext;
import au.csiro.pathling.fhirpath.execution.DataRoot.JoinRoot;
import au.csiro.pathling.fhirpath.execution.DataRoot.ResolveRoot;
import au.csiro.pathling.fhirpath.execution.DataRoot.ReverseResolveRoot;
import au.csiro.pathling.fhirpath.function.registry.FunctionRegistry;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.path.Paths.Traversal;
import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import com.google.common.collect.Streams;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.jetbrains.annotations.NotNull;


/**
 * A FHIRPath evaluator that can handle multiple joins.
 */
@Value
public class MultiFhirPathEvaluator implements FhirPathEvaluator {


  @Nonnull
  static Column ns_map_concat(@Nonnull final Column left, @Nonnull final Column right) {
    return functions.when(left.isNull(), right)
        .when(right.isNull(), left)
        .otherwise(functions.map_concat(left, right));
  }

  // TODO: Move somewhere else

  @Nonnull
  public static Column collect_map(@Nonnull final Column mapColumn) {
    // TODO: try to implement this more eccielty and in a way 
    // that does not requiere:
    // .config("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
    return functions.reduce(
        functions.collect_list(mapColumn),
        functions.any_value(mapColumn),
        (acc, elem) -> functions.when(acc.isNull(), elem).otherwise(ns_map_concat(acc, elem))
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

    // Dataset<Row> resolvedDataset =
    //     joinRoots.size() == 1
    //     ? resolveJoins((JoinRoot) joinRoots.iterator().next(),
    //         createInitialDataset(), null)
    //     : createInitialDataset();

    Dataset<Row> resolvedDataset = resolveJoins(
        JoinSet.mergeRoots(joinRoots).iterator().next(),
        createInitialDataset());

    System.out.println("Resolved dataset:");
    resolvedDataset.show();
    System.out.println(resolvedDataset.queryExecution().executedPlan().toString());

    final ResourceResolver resourceResolver = new DefaultResourceResolver();
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
  private Dataset<Row> computeJoin(@Nonnull final Dataset<Row> parentDataset,
      @Nullable final Dataset<Row> maybeChildDataset, @Nonnull final JoinRoot joinRoot) {
    if (joinRoot instanceof ReverseResolveRoot rrr) {
      return computeReverseJoin(parentDataset, maybeChildDataset, rrr);
    } else if (joinRoot instanceof ResolveRoot rr) {
      return computeResolveJoin(parentDataset, maybeChildDataset, rr);
    } else {
      throw new UnsupportedOperationException(
          "Not implemented - unknown root type: " + joinRoot);
    }
  }


  @Nonnull
  private Dataset<Row> computeResolveJoin(@Nonnull final Dataset<Row> parentDataset,
      @Nullable final Dataset<Row> maybeChildDataset, @Nonnull final ResolveRoot joinRoot) {

    System.out.println("Computing resolve join for: " + joinRoot);

    final FhirPathExecutor parentExecutor = createExecutor(joinRoot.getMaster().getResourceType(),
        dataSource);
    final ReferenceCollection referenceCollection = (ReferenceCollection)
        parentExecutor.evaluate(joinRoot.getMasterResourcePath()).getValue();
    final Set<ResourceType> referenceTypes = referenceCollection.getReferenceTypes();
    System.out.println(
        "Reference types: " + referenceTypes);

    final ResolveRoot typedRoot = joinRoot;
    if (!referenceTypes.contains(typedRoot.getResourceType())) {
      throw new IllegalArgumentException(
          "Reference type does not match. Expected: " + referenceTypes + " but got: "
              + typedRoot.getResourceType());
    }

    Dataset<Row> resultDataset;

    // unfortunatelly singularity cannot be determined from the reference alone as its parents 
    // may not be singular

    // TODO: optimize
    // for not we will just select referecne column on the parent dataset to check the representation

    final Dataset<Row> referenceDataset = parentDataset.select(
        referenceCollection.getColumnValue().alias("reference"));
    final boolean isSingular = !(referenceDataset.schema().apply(0)
        .dataType() instanceof ArrayType);
    if (isSingular) {

      // TODO: this should be replaced with call to evalPath() with not grouping context
      final FhirPathExecutor childExecutor = createExecutor(
          typedRoot.getForeignResourceType(),
          dataSource);

      final Dataset<Row> childDataset = maybeChildDataset == null
                                        ? childExecutor.createInitialDataset()
                                        : maybeChildDataset;

      // this shoukld point to the resource column
      final Collection childResource = childExecutor.createDefaultInputContext();

      // we essentially need to join the child result (with a map) to the parent dataset
      // using the resolved reference as the joining key

      final Stream<Column> passThroughColumns = Stream.of(childDataset.columns())
          .filter(c -> c.contains("@"))
          // TODO: replace with the map_combine function
          .map(functions::col);

      final String uniqueValueTag = typedRoot.getValueTag() + "_unique";
      // create one key-value pair map a the value
      final Stream<Column> keyValuesColumns = Stream.of(
          childDataset.col("key").alias(typedRoot.getChildKeyTag()),
          functions.map_from_arrays(
              functions.array(childDataset.col("key")),
              // maybe need to be wrapped in another array
              functions.array(childResource.getColumnValue())
          ).alias(uniqueValueTag)
      );

      final Dataset<Row> childResult = childDataset.select(
          Streams.concat(keyValuesColumns, passThroughColumns)
              .toArray(Column[]::new));

      // and now join to the parent reference

      final Collection parentRegKey = parentExecutor.evaluate(new Traversal("reference"),
          referenceCollection);

      final Dataset<Row> joinedDataset = parentDataset.join(childResult,
              parentRegKey.getColumnValue().equalTo(functions.col(typedRoot.getChildKeyTag())),
              "left_outer")
          .drop(typedRoot.getChildKeyTag());

      final Dataset<Row> finalDataset = mergeMapColumns(joinedDataset, typedRoot.getValueTag(),
          uniqueValueTag);

      System.out.println("Final dataset:");
      finalDataset.show();
      return finalDataset;
    } else {

      final String uniqueValueTag = typedRoot.getValueTag() + "_unique";
      // TODO: this should be replaced with call to evalPath() with not grouping context
      final FhirPathExecutor childExecutor = createExecutor(
          typedRoot.getForeignResourceType(),
          dataSource);

      final Dataset<Row> childDataset = maybeChildDataset == null
                                        ? childExecutor.createInitialDataset()
                                        : maybeChildDataset;

      // this shoukld point to the resource column
      final Collection childResource = childExecutor.createDefaultInputContext();

      // we essentially need to join the child result (with a map) to the parent dataset
      // using the resolved reference as the joining key

      final Stream<Column> childPassThroughColumns = Stream.of(childDataset.columns())
          .filter(c -> c.contains("@"))
          // TODO: replace with the map_combine function
          .map(functions::col);

      // create one key-value pair map a the value
      final Stream<Column> keyValuesColumns = Stream.of(
          childDataset.col("key").alias(typedRoot.getChildKeyTag()),
          functions.map_from_arrays(
              functions.array(childDataset.col("key")),
              // maybe need to be wrapped in another array
              functions.array(childResource.getColumnValue())
          ).alias(uniqueValueTag)
      );

      final Dataset<Row> childResult =
          childDataset.select(
              Streams.concat(keyValuesColumns, childPassThroughColumns)
                  .toArray(Column[]::new));

      // but we also need to map_concat child maps to the current join if exits

      // and now join to the parent reference

      final Collection parentRegKey = parentExecutor.evaluate(new Traversal("reference"),
          referenceCollection);

      final Dataset<Row> expandedParent = parentDataset.withColumn(typedRoot.getParentKeyTag(),
          functions.explode_outer(parentRegKey.getColumnValue()));

      final Dataset<Row> joinedDataset = joinWithMapMerge(expandedParent, childResult,
          expandedParent.col(typedRoot.getParentKeyTag())
              .equalTo(childResult.col(typedRoot.getChildKeyTag())))
          .drop(typedRoot.getChildKeyTag(), typedRoot.getParentKeyTag());
      joinedDataset.show();

      final Stream<Column> passThroughColumns = Stream.of(childDataset.columns())
          .filter(c -> c.contains("@"))
          // TODO: replace with the map_combine function
          .map(c -> collect_map(functions.col(c)).alias(c));

      final Stream<Column> parentColumns = Stream.of(parentDataset.columns())
          .filter(c -> !"key".equals(c) && !"id".equals(c))
          // TODO: replace with the map_combine function
          .map(c -> functions.any_value(functions.col(c)).alias(c));

      final Column[] allPassColumns = Stream.concat(
              parentColumns,
              Stream.concat(Stream.of(
                      collect_map(functions.col(uniqueValueTag)).alias(uniqueValueTag)),
                  passThroughColumns))
          .toArray(Column[]::new);

      final Dataset<Row> regroupedDataset = joinedDataset.groupBy(joinedDataset.col("id"))
          .agg(
              functions.any_value(joinedDataset.col("key")).alias("key"),
              allPassColumns
          );

      resultDataset = mergeMapColumns(regroupedDataset, typedRoot.getValueTag(),
          uniqueValueTag);
      resultDataset.show();
      return resultDataset;
    }
  }

  // I Need to be able to make a smart join where the map columns are merged
  // and the other columns are passed through

  @Nonnull
  private Dataset<Row> joinWithMapMerge(@Nonnull final Dataset<Row> leftDataset,
      @Nonnull final Dataset<Row> rightDataset,
      @Nonnull final Column on) {

    // deduplicate columns
    // for @map colums map_contat
    // for others keep the left
    final Set<String> commonColumns = new HashSet<>(List.of(leftDataset.columns()));
    commonColumns.retainAll(Set.of(rightDataset.columns()));

    final Set<String> commonMapColumns = commonColumns.stream()
        .filter(c -> c.contains("@"))
        .collect(Collectors.toUnmodifiableSet());

    final Column[] uniqueSelection = Stream.concat(
        Stream.of(leftDataset.columns())
            .map(c -> commonMapColumns.contains(c)
                      ? ns_map_concat(leftDataset.col(c), rightDataset.col(c)).alias(c)
                      : leftDataset.col(c)),
        Stream.of(rightDataset.columns())
            .filter(c -> !commonColumns.contains(c))
            .map(rightDataset::col)

    ).toArray(Column[]::new);
    return leftDataset.join(rightDataset, on, "left_outer")
        .select(uniqueSelection);
  }


  @Nonnull
  private Dataset<Row> mergeMapColumns(@Nonnull final Dataset<Row> dataset,
      @Nonnull final String finalColumn, @Nonnull final String tempColumn) {
    if (List.of(dataset.columns()).contains(finalColumn)) {
      return dataset.withColumn(finalColumn,
              ns_map_concat(functions.col(finalColumn), functions.col(tempColumn)))
          .drop(tempColumn);
    } else {
      return dataset.withColumnRenamed(tempColumn, finalColumn);
    }
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
  private Dataset<Row> resolveJoins(@Nonnull final JoinSet joinSet,
      @Nonnull final Dataset<Row> parentDataset) {

    // now just reduce current children
    return joinSet.getChildren().stream()
        .reduce(parentDataset, (dataset, subset) ->
            // the parent dataset for subjoin should be different
            computeJoin(dataset,
                resolveJoins(subset, resourceDataset(subset.getMaster().getResourceType())),
                (JoinRoot) subset.getMaster()), (dataset1, dataset2) -> dataset1);
  }

  @Nonnull
  private FhirPathExecutor createExecutor(final ResourceType subjectResourceType,
      final DataSource dataSource) {
    return new SingleFhirPathExecutor(subjectResourceType,
        fhirContext, functionRegistry,
        Collections.emptyMap(), dataSource);
  }

  class DefaultResourceResolver implements ResourceResolver {

    @Nonnull
    @Override
    public ResourceCollection resolveResource(@Nonnull final ResourceType resourceType) {
      return ResourceCollection.build(
          new DefaultRepresentation(functions.col(resourceType.toCode())),
          fhirContext, resourceType);
    }


    @Nonnull
    ResourceCollection resolveTypedJoin(@Nonnull final ReferenceCollection referenceCollection,
        @Nonnull final ResourceType referenceType) {

      if (!referenceCollection.getReferenceTypes().contains(referenceType)) {
        throw new IllegalArgumentException(
            "Reference type does not match. Expected: " + referenceType + " but got: "
                + referenceCollection.getReferenceTypes());
      }
      // TODO: get from the reference collection
      final JoinTag valueTag = JoinTag.ResolveTag.of(referenceType);

      return ResourceCollection.build(
          referenceCollection.getColumn().traverse("reference")
              .applyTo(functions.col(valueTag.getTag())),
          fhirContext, referenceType);
    }


    @Override
    public @Nonnull Collection resolveJoin(
        @Nonnull final ReferenceCollection referenceCollection) {
      final Set<ResourceType> referenceTypes = referenceCollection.getReferenceTypes();
      return referenceTypes.size() == 1
             ? resolveTypedJoin(referenceCollection, referenceTypes.iterator().next())
             : new MixedResourceCollection(referenceCollection, this::resolveTypedJoin);
    }

    @Nonnull
    @Override
    public ResourceCollection resolveReverseJoin(@Nonnull final ResourceCollection parentResource,
        @Nonnull final String expression) {

      final ResourceType resourceType = parentResource.getResourceType();
      // TODO: implement this
      final String resourceName = expression.split("\\.")[0];
      final String masterKeyPath = expression.split("\\.")[1];
      final ResourceType childResourceType = ResourceType.fromCode(resourceName);

      final JoinTag valueTag = JoinTag.ReverseResolveTag.of(childResourceType, masterKeyPath);

      return ResourceCollection.build(
          parentResource.getColumn().traverse("id_versioned")
              .applyTo(functions.col(valueTag.getTag())),
          fhirContext, childResourceType);
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
  Dataset<Row> resourceDataset(@Nonnull final ResourceType resourceType) {
    final Dataset<Row> dataset = dataSource.read(resourceType);
    return dataset.select(
        dataset.col("id"),
        dataset.col("id_versioned").alias("key"),
        functions.struct(
            Stream.of(dataset.columns())
                .map(dataset::col).toArray(Column[]::new)
        ).alias(resourceType.toCode()));
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
  public Collection evaluate(@Nonnull final FhirPath path, @Nonnull final Collection inputContext) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Nonnull
  public Set<DataRoot> findJoinsRoots(@Nonnull final FhirPath path) {
    final DataRootResolver dataRootResolver = new DataRootResolver(subjectResource, fhirContext);
    // TODO: create the actual hierarchy of the joins
    // for now find the longest root path 
    return dataRootResolver.findDataRoots(path);
  }

}
