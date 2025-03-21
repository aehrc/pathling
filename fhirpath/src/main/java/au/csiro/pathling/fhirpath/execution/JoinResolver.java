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

import static au.csiro.pathling.fhirpath.execution.BaseResourceResolver.getResourceDataset;
import static au.csiro.pathling.sql.SqlFunctions.ns_map_concat;
import static au.csiro.pathling.utilities.Streams.unsupportedCombiner;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ReferenceCollection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
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
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Resolves FHIR resource joins and creates datasets that include all necessary data for evaluating
 * resource references, forward resolves, and reverse resolves defined in {@link JoinSet}s.
 * <p>
 * This class is responsible for:
 * <ul>
 *   <li>Resolving references between FHIR resources</li>
 *   <li>Joining datasets based on these references</li>
 *   <li>Maintaining the correct structure for joined data</li>
 *   <li>Handling both singular and non-singular references</li>
 *   <li>Managing the merging of overlapping data during joins</li>
 * </ul>
 *
 * <h2>Data Representation</h2>
 *
 * <h3>Subject Resources</h3>
 * The subject resource (e.g., Patient) is represented as a struct column named with the resource type.
 * For example, a Patient resource would be in a column named "Patient" containing all fields like id, name, etc.
 *
 * <h3>Foreign Resources</h3>
 * Foreign resources (resources that are neither the subject nor directly related) are represented as
 * array columns of structs. For example, a Condition resource would be in a column named "Condition"
 * containing an array of all Condition resources.
 * <p>
 * <strong>Note:</strong> For each subject resource row, the foreign resources array contains ALL instances
 * of the foreign resource type, as there is no filtering based on the subject. This can lead to
 * poor performance with large datasets.
 *
 * <h3>Forward Resolve Joins</h3>
 * Forward resolve joins (e.g., Patient.managingOrganization.resolve()) are represented as map columns
 * with the naming convention "id@{resourceType}" where:
 * <ul>
 *   <li>The map key is the referenced resource ID</li>
 *   <li>The map value is the referenced resource as a struct</li>
 * </ul>
 * For example, a forward resolve to Organization would create a map column named "id@Organization".
 *
 * <h3>Reverse Resolve Joins</h3>
 * Reverse resolve joins (e.g., Patient.reverseResolve(Condition.subject)) are represented as map columns
 * with the naming convention "{resourceType}@{childReferencePath}" where:
 * <ul>
 *   <li>The map key is the master resource ID</li>
 *   <li>The map value is an array of child resources that reference the master</li>
 *   <li>childReferencePath is the path in the child that references the master (dots replaced with underscores)</li>
 * </ul>
 * For example, a reverse resolve from Patient to Condition.subject would create a map column named "Condition@subject".
 *
 * <h2>Map Column Merging</h2>
 * When joining datasets with overlapping map columns, the columns are merged using {@link SqlFunctions#ns_map_concat},
 * preserving data from both sources with the right side taking precedence for duplicate keys.
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
   * Resolves the joins in the given {@link JoinSet}s and returns a dataset containing the subject
   * resource and all joined resources.
   * <p>
   * This method retrieves the subject resource dataset and then resolves all joins defined in the
   * join sets.
   *
   * @param joinSet The join sets to resolve
   * @return A dataset containing the subject resource and all joined resources
   */
  @Nonnull
  public Dataset<Row> resolveJoins(@Nonnull final List<JoinSet> joinSet) {
    return resolveJoins(joinSet, getResourceDataset(dataSource, subjectResource));
  }

  /**
   * Resolves the joins in the given {@link JoinSet}s using the provided parent dataset.
   * <p>
   * This method:
   * <ol>
   *   <li>Splits the join sets into subject resource joins and foreign resource joins</li>
   *   <li>Resolves the subject resource joins first</li>
   *   <li>Then resolves each foreign resource join and merges the results</li>
   * </ol>
   *
   * @param joinSets The join sets to resolve
   * @param parentDataset The parent dataset to use as a starting point
   * @return A dataset containing the parent dataset with all joins resolved
   * @throws IllegalArgumentException if no subject resource join set is found
   */
  @Nonnull
  public Dataset<Row> resolveJoins(@Nonnull final List<JoinSet> joinSets,
      @Nonnull final Dataset<Row> parentDataset) {
    // Split the list into the required subject resource joinSet and 
    // other sets of foreign resources joins
    // All roots of join sets should be of type ResourceRoot

    final JoinSet subjectJoinsSet = joinSets.stream()
        .filter(js -> subjectResource.equals(js.getMasterResourceRoot().getResourceType()))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("No subject resource join set found"));

    final List<JoinSet> foreignJoinsSet = joinSets.stream()
        .filter(js -> !subjectResource.equals(js.getMasterResourceRoot().getResourceType()))
        .toList();

    return foreignJoinsSet.stream()
        .reduce(resolveJoinSet(subjectJoinsSet, parentDataset),
            this::resolveForeignJoinSet,
            unsupportedCombiner()
        );
  }

  /**
   * Resolves a foreign join set and merges it with the parent dataset.
   * <p>
   * This method handles joining with resources that are neither the subject nor directly related.
   * It performs a cross join between the parent dataset and the foreign resource dataset, which can
   * be inefficient for large datasets (hence the warning log).
   * <p>
   * The foreign resources are collected into an array and added as a column to the parent dataset.
   *
   * @param parentDataset The parent dataset to join with
   * @param joinSet The foreign join set to resolve
   * @return A dataset containing the parent dataset joined with the foreign resources
   * @throws UnsupportedOperationException if the join set has children (nested resolves)
   */
  @Nonnull
  private Dataset<Row> resolveForeignJoinSet(@Nonnull final Dataset<Row> parentDataset,
      @Nonnull final JoinSet joinSet) {
    if (!joinSet.getChildren().isEmpty()) {
      throw new UnsupportedOperationException(
          "Not implemented - nested resolves for foreign resources");
    }
    log.warn("Cross join with foreign resource {} encountered. This can result in poor performance",
        joinSet.getMasterResourceRoot().getResourceType().toCode());
    // Minimally add the foreign resources to the parent dataset
    // as array of structs
    final ResourceRoot dataRoot = joinSet.getMasterResourceRoot();
    final Dataset<Row> resourceDataset = getResourceDataset(dataSource, dataRoot.getResourceType());

    // Cross join with the parent dataset 
    // This is very inefficient and thus the warning
    logDataset("Foreign input", resourceDataset);
    // Collect all the resources to an array
    final Dataset<Row> groupedDataset = resourceDataset.groupBy()
        .agg(functions.collect_list(dataRoot.getTag()).alias(dataRoot.getTag()));
    logDataset("Grouped resources", groupedDataset);
    final Dataset<Row> joinedDataset = parentDataset.crossJoin(groupedDataset);
    logDataset("Joined dataset", joinedDataset);
    return joinedDataset;
  }

  /**
   * Resolves a join set by recursively resolving its children and computing joins.
   * <p>
   * This method:
   * <ol>
   *   <li>Takes the parent dataset as a starting point</li>
   *   <li>For each child in the join set, recursively resolves that child's join set</li>
   *   <li>Computes the join between the parent dataset and the child dataset</li>
   *   <li>Returns the final dataset with all joins resolved</li>
   * </ol>
   *
   * @param joinSet The join set to resolve
   * @param parentDataset The parent dataset to use as a starting point
   * @return A dataset with all joins in the join set resolved
   */
  @Nonnull
  private Dataset<Row> resolveJoinSet(@Nonnull final JoinSet joinSet,
      @Nonnull final Dataset<Row> parentDataset) {
    // Reduce current children by applying joins sequentially
    return joinSet.getChildren().stream()
        .reduce(parentDataset, (dataset, subset) ->
                // The parent dataset for subjoin should be different
                computeJoin(dataset,
                    resolveJoinSet(subset,
                        getResourceDataset(dataSource, subset.getMaster().getResourceType())),
                    (JoinRoot) subset.getMaster()),
            unsupportedCombiner());
  }

  /**
   * Computes a join between a parent dataset and a child dataset based on the join root type.
   * <p>
   * This method delegates to the appropriate join computation method based on whether the join root
   * is a reverse resolve root or a forward resolve root.
   *
   * @param parentDataset The parent dataset to join
   * @param maybeChildDataset The child dataset to join (may be null)
   * @param joinRoot The join root defining the join relationship
   * @return A dataset containing the joined data
   * @throws UnsupportedOperationException if the join root type is not supported
   */
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


  /**
   * Computes a forward resolve join between a parent dataset and a child dataset.
   * <p>
   * This method handles joining resources referenced by the subject resource (forward references).
   * For example, in Patient.managingOrganization.resolve(), this method joins Patient resources
   * with their referenced Organization resources.
   * <p>
   * The method:
   * <ol>
   *   <li>Evaluates the reference path in the parent resource</li>
   *   <li>Determines if the reference is singular or non-singular</li>
   *   <li>Creates a map of child resources indexed by their IDs</li>
   *   <li>Joins the parent dataset with the child dataset using the reference</li>
   *   <li>For non-singular references, expands the parent dataset, joins, and then re-groups</li>
   * </ol>
   *
   * @param parentDataset The parent dataset containing the references
   * @param maybeChildDataset The child dataset containing the referenced resources (may be null)
   * @param joinRoot The resolve root defining the join relationship
   * @return A dataset containing the joined data
   */
  @Nonnull
  private Dataset<Row> computeResolveJoin(@Nonnull final Dataset<Row> parentDataset,
      @Nullable final Dataset<Row> maybeChildDataset,
      @Nonnull final ResolveRoot joinRoot) {

    log.debug("Computing resolve join for: {}", joinRoot);

    // create the evaluator for parent resource
    final FhirpathEvaluator parentEvaluator = createEvaluator(
        joinRoot.getMaster().getResourceType());
    // evaluate the reference in the parent resource to the child resource 
    // (e.g. Patient.managingOrganization) validating the reference type
    final ReferenceCollection childReferenceInParent = evaluateReference(
        joinRoot.getMasterResourcePath(), joinRoot.getResourceType(), parentEvaluator);

    final boolean isSingularReference = childReferenceInParent.isSingular(parentDataset);

    log.debug("Child({}) reference({}) in parent({}) is singular: {} has types: {}",
        joinRoot.getResourceType(),
        joinRoot.getMasterResourcePath(),
        joinRoot.getMaster().getResourceType(),
        isSingularReference,
        childReferenceInParent.getReferenceTypes());

    // create the child evaluator for the child resource
    final FhirpathEvaluator childEvaluator = createEvaluator(joinRoot.getForeignResourceType());
    // this should point to the resource column
    final ResourceCollection childResource = childEvaluator.createDefaultInputContext();
    final Dataset<Row> childDataset = maybeChildDataset == null
                                      ? childEvaluator.createInitialDataset()
                                      : maybeChildDataset;

    logDataset("Child input", childDataset);

    // we essentially need to join the child result (with a map) to the parent dataset
    // using the resolved reference as the joining key

    // Create the child dataset (singular with respect to the child key) that includes all:
    // - the join key colum (the child id) to the reference in the parent named  `joinRoot.getChildKeyTag()`
    // - the map colum for this resolve join and if necessary merge with the exising one name joinRoot.getValueTag()
    // - all existing map (join) columns passed through
    final Dataset<Row> childResult = withMapMerge(joinRoot.getValueTag(), thisMapColumn ->
        childDataset.select(
            Streams.concat(
                Stream.of(
                    childResource.getKeyCollection().getColumnValue()
                        .alias(joinRoot.getChildKeyTag()),
                    createChildByChildIdMap(childResource).alias(thisMapColumn)
                ),
                mapJoinColumns(childDataset, Function.identity())
            ).toArray(Column[]::new)
        )
    );
    logDataset("Child result", childResult);

    // get the child join key from the reference in the parent
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

      // because the reference key is not singular we need to expand it first 
      // then join and re-group the result
      final Dataset<Row> expandedParent = parentDataset.withColumn(joinRoot.getParentKeyTag(),
          functions.explode_outer(childKeyInParent.getColumnValue()));

      logDataset("Expanded parent", expandedParent);

      // join with the expanded parent dataset using the child key in the parent reference 
      // and the child ke
      final Dataset<Row> joinedDataset = joinWithMapMerge(expandedParent, childResult,
          expandedParent.col(joinRoot.getParentKeyTag())
              .equalTo(childResult.col(joinRoot.getChildKeyTag())))
          .drop(joinRoot.getChildKeyTag(), joinRoot.getParentKeyTag());

      logDataset("Joined result", joinedDataset);

      // re-group the result to make it singular with respect to the parent key

      // aggregate all map (join) columns with `collect_map`
      final Stream<Column> aggJoinColumns = mapJoinColumns(joinedDataset,
          SqlFunctions::collect_map);
      // aggregate all other columns with `any_value()' except for the key and id columns
      // which are explicitly handled in the grouping query
      final Stream<Column> aggParentColumns = Stream.of(parentDataset.columns())
          .filter(c -> !"key".equals(c) && !"id".equals(c) && !JoinTag.isJoinTag(c))
          .map(c -> functions.any_value(functions.col(c)).alias(c));

      final Column[] aggColumns = Stream.concat(
          aggParentColumns,
          aggJoinColumns).toArray(Column[]::new);

      // re-group 
      final Dataset<Row> resultDataset = joinedDataset.groupBy(joinedDataset.col("id"))
          .agg(
              functions.any_value(joinedDataset.col("key")).alias("key"),
              aggColumns
          );

      logDataset("Re-grouped result", resultDataset);
      return resultDataset;
    }
  }

  /**
   * Computes a reverse resolve join between a parent dataset and a child dataset.
   * <p>
   * This method handles joining resources that reference the subject resource (backward
   * references). For example, in Patient.reverseResolve(Condition.subject), this method joins
   * Patient resources with Condition resources that reference them.
   * <p>
   * The method:
   * <ol>
   *   <li>Evaluates the reference path in the child resource that points to the parent</li>
   *   <li>Groups child resources by their reference to the parent</li>
   *   <li>Creates a map of child resource arrays indexed by parent resource IDs</li>
   *   <li>Joins the parent dataset with the grouped child dataset</li>
   * </ol>
   *
   * @param parentDataset The parent dataset being referenced
   * @param maybeChildDataset The child dataset containing the references (may be null)
   * @param joinRoot The reverse resolve root defining the join relationship
   * @return A dataset containing the joined data
   */
  @Nonnull
  private Dataset<Row> computeReverseJoin(@Nonnull final Dataset<Row> parentDataset,
      @Nullable final Dataset<Row> maybeChildDataset,
      @Nonnull final ReverseResolveRoot joinRoot) {

    log.debug("Computing reverse join for: {}", joinRoot);
    // create executor for a child resource  (e.g. for Patient.reverseResolve(Condition.subject))
    // child resource is Condition
    final FhirpathEvaluator childExecutor = createEvaluator(joinRoot.getForeignResourceType());
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

    // get the joining key to the parent resource in the child resource
    final Collection parentKeyInChild = parentReferenceInChild.getKeyCollection(Optional.empty());

    // create the child input context (child resource) and the child input dataset
    final Collection childResource = childExecutor.createDefaultInputContext();
    final Dataset<Row> childInput = maybeChildDataset == null
                                    ? childExecutor.createInitialDataset()
                                    : maybeChildDataset;

    logDataset("Child input", childInput);

    // Create the child dataset that includes all:
    // - all existing join columns aggregated with `collect_map`
    // - the map colum for this resolve join and if necessary merge with the exising one name joinRoot.getValueTag()
    // - the join key colum to the parent resource name joinRoot.getChildKeyTag()
    // The dataset is grouped by the parent key in the child resource and so its also singular with respect to the parent key
    final Dataset<Row> childResult = withMapMerge(joinRoot.getValueTag(), thisMapColumn ->
        childInput
            .groupBy(parentKeyInChild.getColumnValue().alias(joinRoot.getChildKeyTag()))
            .agg(
                createChildByParentIdMap(parentKeyInChild, childResource).alias(thisMapColumn),
                mapJoinColumns(childInput, SqlFunctions::collect_map).toArray(Column[]::new)
            )
    );
    logDataset("Child result", childResult);
    logDataset("Parent input", parentDataset);

    // join the parent dataset with the child result using the parent key in the child resource
    final Dataset<Row> joinedDataset = joinWithMapMerge(parentDataset, childResult,
        functions.col("key")
            .equalTo(childResult.col(joinRoot.getChildKeyTag())))
        .drop(joinRoot.getChildKeyTag());

    logDataset("Joined result", joinedDataset);
    return joinedDataset;
  }

  /**
   * Creates a map column where keys are child resource IDs and values are the child resources.
   * <p>
   * This method is used in forward resolve joins to create a map that associates each child
   * resource with its ID. The resulting map is used to look up referenced resources by their IDs
   * during the join operation.
   *
   * @param childResource The child resource collection
   * @return A column containing a map from child IDs to child resources
   */
  @Nonnull
  private static Column createChildByChildIdMap(ResourceCollection childResource) {
    return functions.map_from_arrays(
        functions.array(childResource.getKeyCollection().getColumnValue()),
        // maybe need to be wrapped in another array
        functions.array(childResource.getColumnValue())
    );
  }

  /**
   * Creates a map column where keys are parent resource IDs and values are arrays of child
   * resources.
   * <p>
   * This method is used in reverse resolve joins to create a map that associates each parent
   * resource ID with an array of child resources that reference it. The resulting map is used to
   * look up all child resources that reference a particular parent during the join operation.
   *
   * @param parentKeyInChild The collection containing parent keys in the child resources
   * @param childResource The child resource collection
   * @return A column containing a map from parent IDs to arrays of child resources
   */
  @Nonnull
  private static Column createChildByParentIdMap(@Nonnull final Collection parentKeyInChild,
      @Nonnull final Collection childResource) {
    return functions.map_from_entries(
        // wrap the element into array to create the map
        functions.array(
            functions.struct(
                functions.any_value(parentKeyInChild.getColumnValue()).alias("parentId"),
                functions.collect_list(childResource.getColumnValue()).alias("childResources")
            )
        )
    );
  }

  /**
   * Evaluates a reference expression and validates that it references the required resource type.
   * <p>
   * This method:
   * <ol>
   *   <li>Parses the reference expression into a FhirPath</li>
   *   <li>Evaluates the path to get a ReferenceCollection</li>
   *   <li>Validates that the reference types include the required type</li>
   * </ol>
   * <p>
   * This validation ensures that joins are only performed between compatible resource types.
   * For example, when joining Patient with Organization via managingOrganization, this method
   * checks that managingOrganization actually references Organization resources.
   *
   * @param referenceExpr The reference expression to evaluate (e.g., "managingOrganization")
   * @param requiredType The resource type that must be referenced (e.g., Organization)
   * @param evaluator The FhirpathEvaluator to use for evaluation
   * @return The evaluated reference collection
   * @throws IllegalArgumentException if the reference does not match the required type
   */
  @Nonnull
  private ReferenceCollection evaluateReference(@Nonnull final String referenceExpr,
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

  /**
   * Joins two datasets while properly merging map columns.
   * <p>
   * This method:
   * <ol>
   *   <li>Identifies common columns between the datasets</li>
   *   <li>Identifies common map columns (those containing '@')</li>
   *   <li>Creates a selection that:
   *     <ul>
   *       <li>Takes all columns from the left dataset</li>
   *       <li>Merges map columns that exist in both datasets</li>
   *       <li>Adds columns from the right dataset that don't exist in the left</li>
   *     </ul>
   *   </li>
   *   <li>Performs a left outer join and applies the selection</li>
   * </ol>
   *
   * @param leftDataset The left dataset in the join
   * @param rightDataset The right dataset in the join
   * @param on The join condition
   * @return A dataset containing the joined data with properly merged map columns
   */
  @Nonnull
  private static Dataset<Row> joinWithMapMerge(@Nonnull final Dataset<Row> leftDataset,
      @Nonnull final Dataset<Row> rightDataset,
      @Nonnull final Column on) {

    // Identify common columns and common map (join) columns between the two datasets 
    final Set<String> commonColumns = new HashSet<>(List.of(leftDataset.columns()));
    commonColumns.retainAll(Set.of(rightDataset.columns()));
    final Set<String> commonMapColumns = commonColumns.stream()
        .filter(JoinTag::isJoinTag)
        .collect(Collectors.toUnmodifiableSet());

    // Create the unique selection of columns for the join:
    // - Get columns from the left dataset 
    // - Merge map columns with their counterparts in the right dataset
    // - Append columns from the right dataset that are not in the left dataset
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


  /**
   * Creates a dataset with a map column that may be merged with an existing column.
   * <p>
   * This method:
   * <ol>
   *   <li>Creates a temporary column name</li>
   *   <li>Applies the producer function to create a dataset with the temporary column</li>
   *   <li>Merges the temporary column with any existing column of the final name</li>
   * </ol>
   * <p>
   * This is used to safely create or update map columns without overwriting existing data.
   *
   * @param columnName The final name of the column
   * @param producer A function that creates a dataset with a temporary column
   * @return A dataset with the merged map column
   */
  @Nonnull
  private static Dataset<Row> withMapMerge(@Nonnull final String columnName,
      @Nonnull final Function<String, Dataset<Row>> producer) {
    final String tempColumn = columnName + "_temp";
    return mergeMapColumns(producer.apply(tempColumn), columnName, tempColumn);
  }

  /**
   * Merges a temporary map column with an existing map column or renames it if no existing column
   * exists.
   * <p>
   * This method:
   * <ol>
   *   <li>Checks if the final column already exists in the dataset</li>
   *   <li>If it exists, merges the temporary column with the existing column using ns_map_concat</li>
   *   <li>If it doesn't exist, simply renames the temporary column to the final name</li>
   * </ol>
   *
   * @param dataset The dataset containing the columns
   * @param finalColumn The name of the final column
   * @param tempColumn The name of the temporary column to merge or rename
   * @return A dataset with the merged or renamed column
   */
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

  /**
   * Maps a function across all join columns in a dataset.
   * <p>
   * This method:
   * <ol>
   *   <li>Identifies all columns in the dataset that contain '@' (join columns)</li>
   *   <li>Applies the provided mapper function to each column</li>
   *   <li>Returns a stream of the mapped columns with their original names</li>
   * </ol>
   * <p>
   * This is used for operations that need to be applied to all join columns,
   * such as aggregation during regrouping.
   *
   * @param dataset The dataset containing the join columns
   * @param mapper The function to apply to each join column
   * @return A stream of mapped columns
   */
  @Nonnull
  private static Stream<Column> mapJoinColumns(@Nonnull final Dataset<Row> dataset,
      @Nonnull Function<Column, Column> mapper) {
    return Stream.of(dataset.columns())
        .filter(JoinTag::isJoinTag)
        .map(c -> mapper.apply(functions.col(c)).alias(c));
  }


  /**
   * Creates a FhirpathEvaluator for the specified resource type.
   * <p>
   * This method creates a new SingleFhirpathEvaluator configured for the given resource type. The
   * evaluator is used to evaluate FHIRPath expressions in the context of resources of that type.
   * <p>
   * For example, when evaluating "Patient.managingOrganization", an evaluator for Patient resources
   * is created to evaluate the expression.
   *
   * @param subjectResourceType The resource type to create an evaluator for
   * @return A new FhirpathEvaluator for the specified resource type
   */
  @Nonnull
  private FhirpathEvaluator createEvaluator(@Nonnull final ResourceType subjectResourceType) {
    return SingleFhirpathEvaluator.of(subjectResourceType,
        fhirContext, StaticFunctionRegistry.getInstance(),
        Collections.emptyMap(), dataSource);
  }

  /**
   * Logs debug information about a dataset.
   * <p>
   * This method logs the column names of a dataset with a descriptive message. It's used throughout
   * the class to provide debugging information about the datasets at various stages of processing.
   *
   * @param message A descriptive message about the dataset
   * @param dataset The dataset to log information about
   */
  private static void logDataset(@Nonnull final String message,
      @Nonnull final Dataset<Row> dataset) {
    log.debug("{}: {}", message, List.of(dataset.columns()));
  }

}
