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

import static au.csiro.pathling.fhirpath.execution.BaseResourceResolver.resourceDataset;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ReferenceCollection;
import au.csiro.pathling.fhirpath.definition.ResourceTypeSet;
import au.csiro.pathling.fhirpath.execution.DataRoot.JoinRoot;
import au.csiro.pathling.fhirpath.execution.DataRoot.ResolveRoot;
import au.csiro.pathling.fhirpath.execution.DataRoot.ReverseResolveRoot;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import com.google.common.collect.Streams;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A FHIRPath ResourceResolver that can handle joins.
 */
@Value(staticConstructor = "of")
@Slf4j
public class JoinResolver {

  @Nonnull
  ResourceType subjectResource;

  @Nonnull
  FhirContext fhirContext;

  @Nonnull
  DataSource dataSource;

  @Nonnull
  Parser parser = new Parser();

  /**
   * Resolve the joins in the given {@link JoinSet} and return the resulting {@link Dataset}.
   */
  @Nonnull
  public Dataset<Row> resolveJoins(@Nonnull final JoinSet joinSet) {
    return resolveJoins(joinSet, resourceDataset(dataSource, subjectResource));
  }

  @Nonnull
  public Dataset<Row> resolveJoins(@Nonnull final JoinSet joinSet,
      @Nonnull final Dataset<Row> parentDataset) {
    // now just reduce current children
    return joinSet.getChildren().stream()
        .reduce(parentDataset, (dataset, subset) ->
            // the parent dataset for subjoin should be different
            computeJoin(dataset,
                resolveJoins(subset,
                    resourceDataset(dataSource, subset.getMaster().getResourceType())),
                (JoinRoot) subset.getMaster()), (dataset1, dataset2) -> dataset1);
  }


  @Nonnull
  static Column ns_map_concat(@Nonnull final Column left, @Nonnull final Column right) {
    return functions.when(left.isNull(), right)
        .when(right.isNull(), left)
        .otherwise(functions.map_concat(left, right));
  }

  // TODO: Move somewhere else

  @Nonnull
  public static Column collect_map(@Nonnull final Column mapColumn) {
    // TODO: try to implement this more efficiently and in a way 
    // that does not require:
    // .config("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
    return functions.reduce(
        functions.collect_list(mapColumn),
        functions.any_value(mapColumn),
        (acc, elem) -> functions.when(acc.isNull(), elem).otherwise(ns_map_concat(acc, elem))
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
      @Nullable final Dataset<Row> maybeChildDataset,
      @Nonnull final ResolveRoot joinRoot) {

    log.debug("Computing resolve join for: {}", joinRoot);

    final FhirpathEvaluator parentExecutor = createExecutor(joinRoot.getMaster().getResourceType(),
        dataSource);
    final ReferenceCollection referenceCollection = (ReferenceCollection)
        parentExecutor.evaluate(parser.parse(joinRoot.getMasterResourcePath()));

    final ResourceTypeSet referenceTypes = referenceCollection.getReferenceTypes();
    System.out.println(
        "Reference types: " + referenceTypes);

    if (!referenceTypes.contains(joinRoot.getResourceType())) {
      throw new IllegalArgumentException(
          "Reference type does not match. Expected: " + referenceTypes + " but got: "
              + joinRoot.getResourceType());
    }

    // unfortunatelly singularity cannot be determined from the reference alone as its parents 
    // may not be singular

    // TODO: optimize
    // for not we will just select referecne column on the parent dataset to check the representation

    final Dataset<Row> referenceDataset = parentDataset.select(
        referenceCollection.getColumnValue().alias("reference"));
    final boolean isSingular = !(referenceDataset.schema().apply(0)
        .dataType() instanceof ArrayType);

    final FhirpathEvaluator childExecutor = createExecutor(
        joinRoot.getForeignResourceType(),
        dataSource);

    final Dataset<Row> childDataset = maybeChildDataset == null
                                      ? childExecutor.createInitialDataset()
                                      : maybeChildDataset;

    logDataset("Child input", childDataset);
    // this shoukld point to the resource column
    final Collection childResource = childExecutor.createDefaultInputContext();

    // we essentially need to join the child result (with a map) to the parent dataset
    // using the resolved reference as the joining key

    final Stream<Column> passThroughColumns = Stream.of(childDataset.columns())
        .filter(c -> c.contains("@"))
        .map(functions::col);

    final Dataset<Row> childResult = withMapMerge(joinRoot.getValueTag(), tempColumn ->
        childDataset.select(
            Streams.concat(
                    Stream.of(
                        childDataset.col("key").alias(joinRoot.getChildKeyTag()),
                        functions.map_from_arrays(
                            functions.array(childDataset.col("key")),
                            // maybe need to be wrapped in another array
                            functions.array(childResource.getColumnValue())
                        ).alias(tempColumn)
                    ),
                    passThroughColumns)
                .toArray(Column[]::new))
    );

    logDataset("Child result", childResult);
    final Collection parentRefKey = referenceCollection.getKeyCollection(Optional.empty());
    
    if (isSingular) {
      logDataset("Parent input", parentDataset);
      // since the reference key is singular we can just join on the key
      final Dataset<Row> joinedDataset = joinWithMapMerge(parentDataset, childResult,
          parentRefKey.getColumnValue().equalTo(functions.col(joinRoot.getChildKeyTag()))
      ).drop(joinRoot.getChildKeyTag());
      logDataset("Joined result", joinedDataset);
      return joinedDataset;
    } else {

      final Dataset<Row> expandedParent = parentDataset.withColumn(joinRoot.getParentKeyTag(),
          functions.explode_outer(parentRefKey.getColumnValue()));

      logDataset("Expanded parent", expandedParent);

      final Dataset<Row> joinedDataset = joinWithMapMerge(expandedParent, childResult,
          expandedParent.col(joinRoot.getParentKeyTag())
              .equalTo(childResult.col(joinRoot.getChildKeyTag())))
          .drop(joinRoot.getChildKeyTag(), joinRoot.getParentKeyTag());

      logDataset("Joined result", joinedDataset);

      final Stream<Column> groupingColumns = Stream.of(joinedDataset.columns())
          .filter(c -> c.contains("@"))
          // TODO: replace with the map_combine function
          .map(c -> collect_map(functions.col(c)).alias(c));

      final Stream<Column> parentColumns = Stream.of(parentDataset.columns())
          .filter(c -> !"key".equals(c) && !"id".equals(c) && !c.contains("@"))
          // TODO: replace with the map_combine function
          .map(c -> functions.any_value(functions.col(c)).alias(c));

      final Column[] allPassColumns = Stream.concat(
          parentColumns,
          groupingColumns).toArray(Column[]::new);

      final Dataset<Row> resultDataset = joinedDataset.groupBy(joinedDataset.col("id"))
          .agg(
              functions.any_value(joinedDataset.col("key")).alias("key"),
              allPassColumns
          );

      logDataset("Re-grouped result", resultDataset);
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
  private static Dataset<Row> withMapMerge(@Nonnull final String columnName,
      @Nonnull final Function<String, Dataset<Row>> producer) {
    final String tempColumn = columnName + "_temp";
    return mergeMapColumns(producer.apply(tempColumn), columnName, tempColumn);
  }


  @Nonnull
  private static Dataset<Row> mergeMapColumns(@Nonnull final Dataset<Row> dataset,
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
      @Nullable final Dataset<Row> maybeChildDataset,
      @Nonnull final ReverseResolveRoot joinRoot) {

    log.debug("Computing reverse join for: {}", joinRoot);
    final FhirpathEvaluator childExecutor = createExecutor(joinRoot.getForeignResourceType(),
        dataSource);
    final FhirPath referencePath = parser.parse(joinRoot.getForeignKeyPath());
    final ReferenceCollection referenceResult = (ReferenceCollection) childExecutor.evaluate(
        referencePath);
    // check the type of the reference matches
    final ResourceTypeSet allowedReferenceTypes = referenceResult.getReferenceTypes();
    if (!allowedReferenceTypes.contains(joinRoot.getMaster().getResourceType())) {
      throw new IllegalArgumentException(
          "Reference type does not match. Expected: " + allowedReferenceTypes + " but got: "
              + joinRoot.getMaster().getResourceType());
    }

    final Collection childParentKeyResult = referenceResult.getKeyCollection(Optional.empty());
    final Collection childResource = childExecutor.createDefaultInputContext();
    final Dataset<Row> childInput = maybeChildDataset == null
                                    ? childExecutor.createInitialDataset()
                                    : maybeChildDataset;

    logDataset("Child input", childInput);
    final Column[] passThroughColumns = Stream.of(childInput.columns())
        .filter(c -> c.contains("@"))
        .map(c -> collect_map(functions.col(c)).alias(c))
        .toArray(Column[]::new);

    final Dataset<Row> childResult = withMapMerge(joinRoot.getValueTag(), tempColumn ->
        childInput
            .groupBy(childParentKeyResult.getColumnValue().alias(joinRoot.getChildKeyTag()))
            .agg(
                functions.map_from_entries(
                    // wrap the element into array to create the map
                    functions.array(
                        functions.struct(
                            functions.any_value(childParentKeyResult.getColumnValue()).alias("key"),
                            functions.collect_list(childResource.getColumnValue()).alias("value")
                        )
                    )
                ).alias(tempColumn),
                passThroughColumns
                // pass through for any existing columns that require
                // map aggregation
            )
    );

    logDataset("Child result", childResult);
    logDataset("Parent input", parentDataset);
    final Dataset<Row> joinedDataset = joinWithMapMerge(parentDataset, childResult,
        functions.col("key")
            .equalTo(childResult.col(joinRoot.getChildKeyTag())))
        .drop(joinRoot.getChildKeyTag());
    logDataset("Joined result", joinedDataset);
    return joinedDataset;
  }

  @Nonnull
  private FhirpathEvaluator createExecutor(@Nonnull final ResourceType subjectResourceType,
      @Nonnull final DataSource dataSource) {
    return SingleFhirpathEvaluator.of(subjectResourceType,
        fhirContext, StaticFunctionRegistry.getInstance(),
        Collections.emptyMap(), dataSource);
  }

  public static void logDataset(@Nonnull final String message,
      @Nonnull final Dataset<Row> dataset) {
    log.debug("{}: {}", message, List.of(dataset.columns()));
  }

}
