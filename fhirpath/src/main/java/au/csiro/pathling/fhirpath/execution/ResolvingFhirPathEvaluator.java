package au.csiro.pathling.fhirpath.execution;


import au.csiro.pathling.encoders.ValueFunctions;
import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ReferenceCollection;
import au.csiro.pathling.fhirpath.execution.DataRoot.ResolveRoot;
import au.csiro.pathling.fhirpath.execution.DataRoot.ReverseResolveRoot;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.path.Paths.EvalFunction;
import au.csiro.pathling.fhirpath.path.Paths.Traversal;
import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.awt.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

@Value
public class ResolvingFhirPathEvaluator implements FhirPathEvaluator {

  @Nonnull
  ResourceType subjectResource;

  @Nonnull
  DataSource dataSource;


  @Nonnull
  Parser parser = new Parser();


  @Override
  @Nonnull
  public CollectionDataset evaluate(@Nonnull final String fhirpathExpression) {
    final FhirPath fhirPath = parser.parse(fhirpathExpression);

    final FhirPathExecutor subjectExecutor = createExecutor(subjectResource, dataSource);
    final Dataset<Row> initialDataset = subjectExecutor.createInitialDataset();
    initialDataset.show();
    final FhirpathResult purifiedResult = purify(dataSource, subjectResource, fhirPath,
        initialDataset, Optional.of(subjectExecutor.createDefaultInputContext()));
    System.out.println("Purified path: " + purifiedResult.getFhirPath());
    purifiedResult.getDataset().show();
    return subjectExecutor.evaluate(purifiedResult.getFhirPath(), purifiedResult.getDataset());
  }

  //
  // SECTION: Reverse Resolve
  //


  @Value()
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  static class GroupingContext {

    @Nonnull
    Column groupingColumn;
    @Nonnull
    String valueColumnAlias;

    @Nonnull
    List<String> passThroughColumns;

    @Nonnull
    String getColumnTag(@Nonnull final String column) {
      return column + "@" + Long.toHexString(System.identityHashCode(this));
    }


    @Nonnull
    static GroupingContext of(@Nonnull final Column groupingColumn,
        @Nonnull final String valueColumnAlias) {
      return new GroupingContext(groupingColumn, valueColumnAlias, List.of());
    }

    @Nonnull
    GroupingContext withPassThroughColumns(@Nonnull final List<String> passThroughColumns) {
      return new GroupingContext(groupingColumn, valueColumnAlias, passThroughColumns);
    }

    @Nonnull
    GroupingContext withPassThroughColumns(@Nonnull final Dataset<Row> dataset,
        String... excludeColumns) {
      final Set<String> excluded = Set.of(excludeColumns);
      return withPassThroughColumns(
          Stream.of(dataset.columns())
              .filter(c -> !excluded.contains(c))
              .toList());
    }
  }

  @Value(staticConstructor = "of")
  static class Aggregator {

    @Nonnull
    FhirPathExecutor executor;

    @Nonnull
    Function<Column, Column> aggregation;

    @Nonnull
    FhirPath suffix;
  }


  @Value(staticConstructor = "of")
  static class EvalResult {

    @Nonnull
    CollectionDataset result;
    @Nonnull
    Aggregator aggregator;


    @Nonnull
    Dataset<Row> getDataset() {
      return result.getDataset();
    }

    @Nonnull
    Collection getValue() {
      return result.getValue();
    }

    @Nonnull
    EvalResult withResult(@Nonnull final CollectionDataset result) {
      return EvalResult.of(result, aggregator);
    }

    @Nonnull
    public EvalResult aggregate(@Nonnull final Optional<GroupingContext> maybeGroupingContext) {
      return maybeGroupingContext.map(groupingContext -> {
            final CollectionDataset childAggResult = result;
            // TODO:  We should be able to combine the evaluation actually 
            // either by having a function that evaluates multiple expressions or 
            // having the dataset returne from the evaluator
            // for now lest just ignore the dataset and only use the value column

            final Dataset<Row> childDataset = childAggResult.materialize(
                groupingContext.getValueColumnAlias());
            final Collection aggCollection = childAggResult.getValue()
                .withColumn(aggregator.getAggregation()
                    .apply(functions.col(groupingContext.getValueColumnAlias())));

            final Dataset<Row> childResult = childDataset.groupBy(groupingContext.getGroupingColumn())
                .agg(
                    // TODO: maybe  use all the actual column list here
                    // but aliased with a unique ids
                    // functions.any_value(functions.col("id")).alias("id"),
                    // functions.any_value(functions.col("key")).alias("key"),
                    aggCollection.getColumnValue().alias(groupingContext.getValueColumnAlias()),
                    groupingContext.getPassThroughColumns().stream()
                        .map(c -> functions.first(functions.col(c)).alias(c))
                        .toArray(Column[]::new)
                );
            childResult.show();
            return EvalResult.of(
                CollectionDataset.of(childResult,
                    aggCollection.withColumn(functions.col(groupingContext.getValueColumnAlias()))),
                aggregator
            );
          })
          .orElseGet(() -> {
            // no groupping just apply suffix if any to the current result
            final Collection suffixedCollection = aggregator.getExecutor()
                .evaluate(getAggregator().getSuffix(),
                    getResult().getValue());
            // TODO: decide if what to do with the aggregator here (it technically should be empty) 
            // AS future application of aggregation should not be allowed
            return EvalResult.of(CollectionDataset.of(getDataset(), suffixedCollection),
                aggregator);
          });
    }
  }

  /**
   * Evaluate path with not reverseResolve function with a grouping context.
   */
  @Nonnull
  EvalResult evalPurePath(@Nonnull final DataSource dataSource,
      @Nonnull final ResourceType subjectResource,
      @Nonnull final FhirPath fhirPath,
      @Nonnull final Dataset<Row> subjectDataset) {

    // The problem here is that we might need to evaluate the column aggregation multiple times
    // for different grouping context and then 
    // to finally apply the suffix  (if any) when the empty (or root) context is reached
    // Not sure how if/how we can track the subjectResource here

    final FhirPathExecutor executor = createExecutor(subjectResource,
        dataSource);

    final Pair<FhirPath, FhirPath> aggAndSuffix = fhirPath.splitLeft(
        ResolvingFhirPathEvaluator::isAggregation);
    // check if the child ends with an aggregate 
    if (aggAndSuffix.getLeft().isNull()) {

      return EvalResult.of(
          executor.evaluate(aggAndSuffix.getRight(), subjectDataset),
          Aggregator.of(executor, c -> ValueFunctions.unnest(functions.collect_list(c)),
              FhirPath.nullPath())
      );
    } else {
      final FhirPath aggPath = aggAndSuffix.getLeft();
      final FhirPath aggSuffix = aggAndSuffix.getRight();
      System.out.println("Agg split: " + aggPath + " +  " + aggSuffix);
      return EvalResult.of(
          executor.evaluate(aggPath, subjectDataset),
          Aggregator.of(executor, getAggregation(aggPath.last()),
              getAggregationPath(aggPath.last()).andThen(aggSuffix))
      );
    }
  }

  @Nonnull
  static Function<Column, Column> getAggregation(@Nonnull final FhirPath path) {
    if (path instanceof Composite) {
      throw new IllegalArgumentException("Simple path expected");
    }
    if (path instanceof final EvalFunction evalFunction) {
      switch (evalFunction.getFunctionIdentifier()) {
        case "count", "sum" -> {
          return functions::sum;
        }
        case "first" -> {
          return functions::first;
        }
      }
    }
    return c -> ValueFunctions.unnest(functions.collect_list(c));
  }

  @Nonnull
  static FhirPath getAggregationPath(@Nonnull final FhirPath path) {
    if (path instanceof Composite) {
      throw new IllegalArgumentException("Simple path expected");
    }
    if (path instanceof final EvalFunction evalFunction) {
      switch (evalFunction.getFunctionIdentifier()) {
        case "count", "sum" -> {
          return new EvalFunction("sum", List.of());
        }
        case "first" -> {
          return new EvalFunction("first", List.of());
        }
      }
    }
    throw new IllegalArgumentException("Aggregate function expected");
  }


  static boolean isReverseResolve(final FhirPath path) {
    return asReverseResolve(path).isPresent();
  }

  static boolean isResolve(final FhirPath path) {
    return asResolve(path).isPresent();
  }

  @Nonnull
  static Optional<EvalFunction> asReverseResolve(final FhirPath path) {
    if (path instanceof EvalFunction evalFunction && evalFunction.getFunctionIdentifier()
        .equals("reverseResolve")) {
      return Optional.of(evalFunction);
    }
    return Optional.empty();
  }

  @Nonnull
  static Optional<EvalFunction> asResolve(final FhirPath path) {
    if (path instanceof EvalFunction evalFunction && evalFunction.getFunctionIdentifier()
        .equals("resolve")) {
      return Optional.of(evalFunction);
    }
    return Optional.empty();
  }

  private static boolean isAggregation(FhirPath fhirPath) {
    return fhirPath instanceof EvalFunction evalFunction && (evalFunction.getFunctionIdentifier()
        .equals("count") || evalFunction.getFunctionIdentifier().equals("first")
        || evalFunction.getFunctionIdentifier().equals("sum"));
  }

  // 
  // BEGIN: Purify implementation
  // 

  @Value(staticConstructor = "of")
  static class FhirpathResult {

    @Nonnull
    Dataset<Row> dataset;

    @Nonnull
    FhirPath fhirPath;
  }

  // This is mutable class
  // private constructor
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  @Getter
  static class FhirPathResultCollector {

    @Nonnull
    private Dataset<Row> dataset;
    @Nonnull
    private final List<FhirPath> paths;


    @Nonnull
    List<FhirPath> getPaths() {
      return Collections.unmodifiableList(paths);
    }

    @Nonnull
    FhirPathResultCollector add(@Nonnull final FhirpathResult result) {
      this.dataset = result.getDataset();
      this.paths.add(result.getFhirPath());
      return this;
    }

    @Nonnull
    static FhirPathResultCollector empty(@Nonnull final Dataset<Row> dataset) {
      return new FhirPathResultCollector(dataset, new ArrayList<>());
    }

    @Nonnull
    FhirPathResultCollector combine(@Nonnull final FhirPathResultCollector other) {
      throw new UnsupportedOperationException("Not implemented");
    }
  }

  @Value
  static class ConstPath implements FhirPath {

    @Nonnull
    Collection value;

    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      // TODO: add null checking here 
      // technically should check if the input is NULL and return NULL
      return value.mapColumn(c -> functions.when(input.getColumnValue().isNotNull(), c));
    }

    @Override
    @Nonnull
    public String toExpression() {
      return "const";
    }
  }

  @Nonnull
  FhirpathResult purify(@Nonnull final DataSource dataSource,
      @Nonnull final ResourceType subjectResource,
      @Nonnull final FhirPath fhirPath,
      @Nonnull final Dataset<Row> parentDataset,
      @Nonnull final Optional<Collection> maybeParentContext) {

    // TODO : remove the construction from here
    final FhirPathExecutor subjectExecutor = createExecutor(subjectResource, dataSource);

    final FhirPath headPath = fhirPath.first();

    if (headPath.isNull()) {
      return FhirpathResult.of(parentDataset, fhirPath);
    } else if (isReverseResolve(headPath)) {
      return purifyReverseResolve(dataSource,
          subjectResource, parentDataset,
          asReverseResolve(headPath).orElseThrow(),
          fhirPath.suffix());
    } else if (isResolve(headPath)) {
      return purifyResolve(dataSource,
          subjectResource, parentDataset,
          fhirPath.suffix(),
          // TODO: replace with a better exception
          maybeParentContext.orElseThrow()
      );
    } else {
      // ok how to purify an operator or anything else
      // well we will need to be able to mapChildren so that
      // they can be purified while at the same time the dataset is reduced
      final FhirPathResultCollector collector = headPath.children().sequential().reduce(
          FhirPathResultCollector.empty(parentDataset),
          (acc, childPath) -> {
            final FhirpathResult result = purify(dataSource, subjectResource, childPath,
                acc.getDataset(), maybeParentContext);
            return acc.add(result);
          }, FhirPathResultCollector::combine);

      final FhirPath pureHeadPath = headPath
          .withNewChildren(collector.getPaths());
      // and now purify the rest of the path
      final FhirpathResult pureSuffix = purify(dataSource, subjectResource, fhirPath.suffix(),
          collector.getDataset(),
          maybeParentContext.map(c -> subjectExecutor.evaluate(pureHeadPath, c)));

      return FhirpathResult.of(
          pureSuffix.getDataset(),
          pureHeadPath.andThen(pureSuffix.getFhirPath())
      );
    }
  }

  // TODO: remove parent path
  @Nonnull
  private FhirpathResult purifyReverseResolve(@Nonnull final DataSource dataSource,
      @Nonnull final ResourceType subjectResource, @Nonnull final Dataset<Row> parentDataset,
      @Nonnull final EvalFunction reverseResolve, @Nonnull final FhirPath childPath) {
    System.out.println(reverseResolve);
    final ReverseResolveRoot joinRoot = ExecutorUtils.fromPath(subjectResource,
        reverseResolve);
    System.out.println(joinRoot);
    // for now assume that the child is pure

    final FhirPathExecutor childExecutor = createExecutor(joinRoot.getForeignResourceType(),
        dataSource);

    // TODO: replace with the executor call

    final CollectionDataset childParentKeyResult =
        childExecutor.evaluate(joinRoot.getForeignKeyPath() + "." + "reference");

    // recursively purify child path
    final FhirpathResult pureChildResult = purify(dataSource, joinRoot.getForeignResourceType(),
        childPath, childParentKeyResult.getDataset(),
        Optional.of(childExecutor.createDefaultInputContext()));

    System.out.println(("Reverse - pure"));
    System.out.println(pureChildResult.getFhirPath());
    pureChildResult.getDataset().show();

    // The path is not pure so we can evaluate it
    final EvalResult childValueResult = evalPurePath(dataSource,
        joinRoot.getForeignResourceType(),
        pureChildResult.getFhirPath(), pureChildResult.getDataset())
        .aggregate(
            Optional.of(GroupingContext.of(
                childParentKeyResult.getValueColumn().alias(joinRoot.getChildKeyTag()),
                joinRoot.getValueTag()
            ))
        );

    // we need to apply the aggSuffix to the result of agg function
    // To be able to to this howerver we need access to the collection 
    // based on the result of the aggregation
    // and the execution context

    final Dataset<Row> childResult = childValueResult.getDataset();
    childResult.show();
    final Dataset<Row> joinedDataset = parentDataset.join(childResult,
            functions.col("key")
                .equalTo(childResult.col(joinRoot.getChildKeyTag())),
            "left_outer")
        .drop(joinRoot.getChildKeyTag());
    joinedDataset.show();

    return FhirpathResult.of(
        joinedDataset,
        new ConstPath(childValueResult.getValue().withColumn(
            functions.col(joinRoot.getValueTag())))
            .andThen(childValueResult.aggregator.getSuffix())
    );
  }


  @Nonnull
  private FhirpathResult purifyResolve(@Nonnull final DataSource dataSource,
      @Nonnull final ResourceType parentResource,
      @Nonnull final Dataset<Row> parentDataset,
      @Nonnull final FhirPath childPath,
      @Nonnull final Collection parentContext) {

    final FhirPathExecutor parentExecutor = createExecutor(parentResource,
        dataSource);

    // TODO: eval the reference and to get access to reference path 
    // and evaluate the key from it and also check for the type of the referenced resource
    // NOTE: I am not sure how to handle access to multiple reference types here, especially 
    // if ofType() is applied to an operator of the path 

    final ReferenceCollection referenceCollection = (ReferenceCollection) parentContext;
    System.out.println(
        "Reference types" + referenceCollection.getReferenceTypes().stream().toList());
    // TODO: this hardcoded Patient for subject referecnes (as the group is ommited
    // but should instead somehow alllow for lazy evaluation of ofType(...) for 
    // references with more than one type allowed (polymorphic references)

    final Collection childReferences = parentExecutor.evaluate(
        new Traversal("reference"), parentContext);

    System.out.println(
        "Reference types" + referenceCollection.getReferenceTypes().stream().toList());
    // TODO: this hardcoded Patient for subject referecnes (as the group is ommited
    // but should instead somehow alllow for lazy evaluation of ofType(...) for 
    // references with more than one type allowed (polymorphic references)
    final ResolveRoot resolveRoot = ResolveRoot.ofResource(parentResource,
        referenceCollection.getReferenceTypes().stream().filter(t -> t != ResourceType.GROUP)
            .toList().get(0),
        "_REF");

    System.out.println(
        "Resolve " + referenceCollection.isToOneReference() + " : " + resolveRoot + "->"
            + "_REF" + " : "
            + childPath.toExpression());

    if (referenceCollection.isToOneReference()) {

      // TODO: this should be replaced with call to evalPath() with not grouping context
      final FhirPathExecutor childExecutor = createExecutor(resolveRoot.getForeignResourceType(),
          dataSource);

      // recursively purify child path
      final FhirpathResult pureChildResult = purify(dataSource,
          resolveRoot.getForeignResourceType(),
          childPath, childExecutor.createInitialDataset(),
          Optional.of(childExecutor.createDefaultInputContext()));

      final CollectionDataset childResult = childExecutor.evaluate(pureChildResult.getFhirPath(),
          pureChildResult.getDataset());
      final Dataset<Row> childDataset = childResult.materialize(resolveRoot.getValueTag())
          .select(functions.col("key").alias(resolveRoot.getChildKeyTag()),
              functions.col(resolveRoot.getValueTag()));
      childDataset.show();

      // TODO: check if we need to materialize childReferences
      final Dataset<Row> joinedDataset = parentDataset
          .join(
              childDataset,
              childReferences.getColumnValue()
                  .equalTo(childDataset.col(resolveRoot.getChildKeyTag())),
              "left_outer");

      System.out.println("Joined dataset");
      joinedDataset.show();

      return FhirpathResult.of(
          joinedDataset.drop(resolveRoot.getChildKeyTag()),
          new ConstPath(childResult.getValue().withColumn(
              functions.col(resolveRoot.getValueTag())))
      );

    } else {

      final FhirPathExecutor childExecutor = createExecutor(resolveRoot.getForeignResourceType(),
          dataSource);

      // TODO: this should be replaced with call to evalPath() with not grouping context
      final EvalResult childResult = evalPurePath(dataSource,
          resolveRoot.getForeignResourceType(),
          childPath, childExecutor.createInitialDataset());

      final Dataset<Row> childDataset = childResult.getResult()
          .materialize(resolveRoot.getValueTag())
          .select(functions.col("key").alias(resolveRoot.getChildKeyTag()),
              functions.col(resolveRoot.getValueTag()));
      childDataset.show();

      final Dataset<Row> explodedParentDataset = parentDataset
          .withColumn(resolveRoot.getParentKeyTag(), functions.explode_outer(
              childReferences.getColumnValue()));

      final Dataset<Row> joinedDataset = explodedParentDataset
          .join(
              childDataset.drop("id", "key"),
              functions.col(resolveRoot.getParentKeyTag())
                  .equalTo(childDataset.col(resolveRoot.getChildKeyTag())),
              "left_outer");

      joinedDataset.show();

      final GroupingContext parentGroupingContext = GroupingContext.of(
          // TODO: we actually need to re-group based on the parent resource key (id)
          joinedDataset.col("key"),
          resolveRoot.getValueTag()
      ).withPassThroughColumns(joinedDataset, "key", resolveRoot.getValueTag());

      final EvalResult regroupedResult = childResult.withResult(
              CollectionDataset.of(joinedDataset,
                  childResult.getValue().withColumn(functions.col(resolveRoot.getValueTag()))))
          .aggregate(Optional.of(parentGroupingContext));

      final Dataset<Row> reGrouppedDataset = regroupedResult.getDataset();
      reGrouppedDataset.show();

      return FhirpathResult.of(
          reGrouppedDataset,
          new ConstPath(childResult.getValue().withColumn(
              functions.col(resolveRoot.getValueTag()))).andThen(
              regroupedResult.getAggregator().getSuffix())
      );
    }
  }


  @Nonnull
  private FhirPathExecutor createExecutor(final ResourceType subjectResourceType,
      final DataSource dataSource) {
    return new SingleFhirPathExecutor(subjectResourceType,
        FhirContext.forR4(), StaticFunctionRegistry.getInstance(),
        Collections.emptyMap(), dataSource);
  }

}
