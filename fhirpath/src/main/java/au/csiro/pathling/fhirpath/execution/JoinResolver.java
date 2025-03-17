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
import au.csiro.pathling.fhirpath.execution.DataRoot.ResourceRoot;
import au.csiro.pathling.fhirpath.execution.DataRoot.ReverseResolveRoot;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.sql.SqlFunctions;
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
  public Dataset<Row> resolveJoins(@Nonnull final List<JoinSet> joinSet) {
    return resolveJoins(joinSet, resourceDataset(dataSource, subjectResource));
  }

  @Nonnull
  public Dataset<Row> resolveJoins(@Nonnull final List<JoinSet> joinSets,
      @Nonnull final Dataset<Row> parentDataset) {
    // we need to split the list into the required subject resource joinSet and 
    // other sets of foreign resources joins
    // all roots of join sets should be of type ResourceRoot

    final JoinSet subjectJoinsSet = joinSets.stream()
        .filter(js -> subjectResource.equals(js.getMasterResourceRoot().getResourceType()))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("No subject resource join set found"));

    final List<JoinSet> foreignJoinsSet = joinSets.stream()
        .filter(js -> !subjectResource.equals(js.getMasterResourceRoot().getResourceType()))
        .toList();

    return foreignJoinsSet.stream()
        .reduce(resolveJoinSet(subjectJoinsSet, parentDataset), this::resolveForeignJoinSet,
            (dataset1, dataset2) -> dataset1);
  }

  @Nonnull
  private Dataset<Row> resolveForeignJoinSet(@Nonnull final Dataset<Row> parentDataset,
      @Nonnull final JoinSet joinSet) {
    if (!joinSet.getChildren().isEmpty()) {
      throw new UnsupportedOperationException(
          "Not implemented - nested resolves for foreign resources");
    }
    log.warn("Cross join with foreign resource {} encountered. This can result in poor performance",
        joinSet.getMasterResourceRoot().getResourceType().toCode());
    // minimally add the foreign resources to the parent dataset
    // as array of structs
    final ResourceRoot dataRoot = joinSet.getMasterResourceRoot();
    final Dataset<Row> resourceDataset = resourceDataset(dataSource, dataRoot.getResourceType());

    // cross join with the parent dataset 
    // this is very inefficient and thus the warning
    // TODO: not sure if it's better to collect first and then join or the 
    //  other way around???
    logDataset("Foreign input", resourceDataset);
    // collect all the resources  to an array
    final Dataset<Row> groupedDataset = resourceDataset.groupBy()
        .agg(functions.collect_list(dataRoot.getTag()).alias(dataRoot.getTag()));
    logDataset("Grouped resources", groupedDataset);
    final Dataset<Row> joinedDataset = parentDataset.crossJoin(groupedDataset);
    logDataset("Joined dataset", joinedDataset);
    return joinedDataset;
  }

  @Nonnull
  private Dataset<Row> resolveJoinSet(@Nonnull final JoinSet joinSet,
      @Nonnull final Dataset<Row> parentDataset) {
    // now just reduce current children
    return joinSet.getChildren().stream()
        .reduce(parentDataset, (dataset, subset) ->
            // the parent dataset for subjoin should be different
            computeJoin(dataset,
                resolveJoinSet(subset,
                    resourceDataset(dataSource, subset.getMaster().getResourceType())),
                (JoinRoot) subset.getMaster()), (dataset1, dataset2) -> dataset1);
  }

  // TODO: Move somewhere else

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
    final ReferenceCollection childReferenceInParent = evaluateReference(
        joinRoot.getMasterResourcePath(), joinRoot.getResourceType(), parentExecutor);

    final boolean isSingularReference = isSingular(childReferenceInParent, parentDataset);

    log.debug("Child({}) reference({}) in parent({}) is singular: {} has types: {}",
        joinRoot.getResourceType(),
        joinRoot.getMasterResourcePath(),
        joinRoot.getMaster().getResourceType(),
        isSingularReference,
        childReferenceInParent.getReferenceTypes());

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

    final Stream<Column> passThroughForeignResourceColumns = mapForeignColumns(childDataset,
        Function.identity());
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
                    passThroughForeignResourceColumns)
                .toArray(Column[]::new))
    );

    logDataset("Child result", childResult);
    final Collection childKeyInParent = childReferenceInParent.getKeyCollection(Optional.empty());

    if (isSingularReference) {
      logDataset("Parent input", parentDataset);
      // since the reference key is singular we can just join on the key
      final Dataset<Row> joinedDataset = joinWithMapMerge(parentDataset, childResult,
          childKeyInParent.getColumnValue().equalTo(functions.col(joinRoot.getChildKeyTag()))
      ).drop(joinRoot.getChildKeyTag());
      logDataset("Joined result", joinedDataset);
      return joinedDataset;
    } else {

      final Dataset<Row> expandedParent = parentDataset.withColumn(joinRoot.getParentKeyTag(),
          functions.explode_outer(childKeyInParent.getColumnValue()));

      logDataset("Expanded parent", expandedParent);

      final Dataset<Row> joinedDataset = joinWithMapMerge(expandedParent, childResult,
          expandedParent.col(joinRoot.getParentKeyTag())
              .equalTo(childResult.col(joinRoot.getChildKeyTag())))
          .drop(joinRoot.getChildKeyTag(), joinRoot.getParentKeyTag());

      logDataset("Joined result", joinedDataset);

      final Stream<Column> aggForeignResourceColumns = mapForeignColumns(joinedDataset,
          SqlFunctions::collect_map);

      final Stream<Column> aggParentColumns = Stream.of(parentDataset.columns())
          .filter(c -> !"key".equals(c) && !"id".equals(c) && !c.contains("@"))
          .map(c -> functions.any_value(functions.col(c)).alias(c));

      final Column[] aggColumns = Stream.concat(
          aggParentColumns,
          aggForeignResourceColumns).toArray(Column[]::new);

      final Dataset<Row> resultDataset = joinedDataset.groupBy(joinedDataset.col("id"))
          .agg(
              functions.any_value(joinedDataset.col("key")).alias("key"),
              aggColumns
          );

      logDataset("Re-grouped result", resultDataset);
      return resultDataset;
    }
  }


  @Nonnull
  private Dataset<Row> computeReverseJoin(@Nonnull final Dataset<Row> parentDataset,
      @Nullable final Dataset<Row> maybeChildDataset,
      @Nonnull final ReverseResolveRoot joinRoot) {

    log.debug("Computing reverse join for: {}", joinRoot);
    // create executor for a child resource  (e.g. for Patient.reverseResolve(Condition.subject))
    // child resource is Condition
    final FhirpathEvaluator childExecutor = createExecutor(joinRoot.getForeignResourceType(),
        dataSource);
    // evaluate the child's reference to the parent resource (e.g. Condition.subject)
    // and check that the reference type matches the parent resource type (e.g. Patient)
    final ReferenceCollection parentReferenceInChild = evaluateReference(
        joinRoot.getForeignKeyPath(),
        joinRoot.getMaster().getResourceType(),
        childExecutor
    );
    log.debug("Parent({}) reference({}) in child({}) has types: {}",
        joinRoot.getMaster().getResourceType(),
        joinRoot.getForeignKeyPath(),
        joinRoot.getForeignResourceType(),
        parentReferenceInChild.getReferenceTypes());

    // get the joing key to the parent resource in the child resource
    final Collection parentKeyInChild = parentReferenceInChild.getKeyCollection(Optional.empty());

    final Collection childResource = childExecutor.createDefaultInputContext();
    final Dataset<Row> childInput = maybeChildDataset == null
                                    ? childExecutor.createInitialDataset()
                                    : maybeChildDataset;

    logDataset("Child input", childInput);

    //  aggregate existing foreign resource columns with `collect_map` function 
    final Column[] aggForeignResourceColumns = mapForeignColumns(childInput,
        SqlFunctions::collect_map).toArray(Column[]::new);

    final Dataset<Row> childResult = withMapMerge(joinRoot.getValueTag(), tempColumn ->
        childInput
            .groupBy(parentKeyInChild.getColumnValue().alias(joinRoot.getChildKeyTag()))
            .agg(
                functions.map_from_entries(
                    // wrap the element into array to create the map
                    functions.array(
                        functions.struct(
                            functions.any_value(parentKeyInChild.getColumnValue()).alias("key"),
                            functions.collect_list(childResource.getColumnValue()).alias("value")
                        )
                    )
                ).alias(tempColumn),
                aggForeignResourceColumns
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
  ReferenceCollection evaluateReference(@Nonnull final String referenceExpr,
      @Nonnull final ResourceType requiredType, @Nonnull final FhirpathEvaluator evaluator) {
    final FhirPath referencePath = parser.parse(referenceExpr);
    final ReferenceCollection referenceCollection = (ReferenceCollection) evaluator.evaluate(
        referencePath);
    // check the type of the reference matches
    final ResourceTypeSet allowedReferenceTypes = referenceCollection.getReferenceTypes();
    if (!allowedReferenceTypes.contains(requiredType)) {
      throw new IllegalArgumentException(
          "Reference type does not match. Expected: " + allowedReferenceTypes + " but got: "
              + requiredType);
    }
    return referenceCollection;
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
                      ? SqlFunctions.ns_map_concat(leftDataset.col(c), rightDataset.col(c)).alias(c)
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
              SqlFunctions.ns_map_concat(functions.col(finalColumn), functions.col(tempColumn)))
          .drop(tempColumn);
    } else {
      return dataset.withColumnRenamed(tempColumn, finalColumn);
    }
  }

  @Nonnull
  private static Stream<Column> mapForeignColumns(@Nonnull final Dataset<Row> dataset,
      @Nonnull Function<Column, Column> mapper) {
    return Stream.of(dataset.columns())
        .filter(c -> c.contains("@"))
        .map(c -> mapper.apply(functions.col(c)).alias(c));
  }

  boolean isSingular(@Nonnull final Collection collection, @Nonnull final Dataset<Row> dataset) {
    // unfortunatelly singularity cannot be determined from the reference alone as its parents 
    // may not be singular
    // TODO: optimize
    // for now we will just select the column on the parent dataset to check the representation
    final Dataset<Row> referenceDataset = dataset.select(
        collection.getColumnValue().alias("value"));
    return !(referenceDataset.schema().apply(0).dataType() instanceof ArrayType);
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
