package au.csiro.pathling.fhirpathe.execution;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ValueFunctions;
import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ReferenceCollection;
import au.csiro.pathling.fhirpath.execution.CollectionDataset;
import au.csiro.pathling.fhirpath.execution.DataRoot.ResolveRoot;
import au.csiro.pathling.fhirpath.execution.DataRoot.ReverseResolveRoot;
import au.csiro.pathling.fhirpath.execution.ExecutorUtils;
import au.csiro.pathling.fhirpath.execution.FhirPathExecutor;
import au.csiro.pathling.fhirpath.execution.SingleFhirPathExecutor;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.path.Paths.EvalFunction;
import au.csiro.pathling.fhirpath.path.Paths.Traversal;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.assertions.DatasetAssert;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import ca.uhn.fhir.context.FhirContext;
import java.awt.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.EpisodeOfCare;
import org.hl7.fhir.r4.model.EpisodeOfCare.EpisodeOfCareStatus;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Reference;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import scala.collection.mutable.WrappedArray;

/**
 * This is a test class to explore issues related to implementation of reverseResolve and resolve
 * functions.
 * <p>
 * This attemps to use 'purification approch' where elements that are not pure are replaced with
 * pure elements in a preprocessing step that constructs the input dataset.
 */
@SpringBootUnitTest
@Slf4j
class FhirpathPurifyTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirEncoders encoders;

  final Parser parser = new Parser();

  @Test
  void singleResourceTest() {
    final Patient patient = new Patient();
    patient.setId("1");
    patient.setGender(AdministrativeGender.FEMALE);
    patient.addName().setFamily("Kay").addGiven("Awee");
    patient.addName().setFamily("Kay").addGiven("Zosia");
    final ObjectDataSource dataSource = new ObjectDataSource(spark, encoders,
        List.of(patient));

    final Dataset<Row> result = evalExpression(dataSource, ResourceType.PATIENT,
        "where(gender='female').name.where(family.where($this='Kay').exists()).given.join(',')");
    result.show();
    System.out.println(result.queryExecution().executedPlan().toString());
    final Dataset<Row> expected = DatasetBuilder.of(spark)
        .withColumn("id", DataTypes.StringType)
        .withColumn("value", DataTypes.StringType)
        .withRow("1", "Awee,Zosia")
        .build();

    new DatasetAssert(result)
        .hasRowsUnordered(expected);
  }

  //
  // SECTION: Reverse Resolve
  //


  @Value(staticConstructor = "of")
  static class GroupingContext {

    @Nonnull
    Column groupingColumn;
    @Nonnull
    String valueColumnAlias;


    @Nonnull
    String getColumnTag(@Nonnull final String column) {
      return column + "@" + Long.toHexString(System.identityHashCode(this));
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
                    functions.any_value(functions.col("id")).alias("id"),
                    functions.any_value(functions.col("key")).alias("key"),
                    aggCollection.getColumnValue().alias(groupingContext.getValueColumnAlias()));
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
  EvalResult evalPurePath(@Nonnull final ObjectDataSource dataSource,
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
        FhirpathPurifyTest::isAggregation);
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
  EvalResult evalPurePath(@Nonnull final ObjectDataSource dataSource,
      @Nonnull final ResourceType subjectResource,
      @Nonnull final FhirPath fhirPath) {

    // The problem here is that we might need to evaluate the column aggregation multiple times
    // for different grouping context and then 
    // to finally apply the suffix  (if any) when the empty (or root) context is reached
    // Not sure how if/how we can track the subjectResource here

    final FhirPathExecutor executor = createExecutor(subjectResource,
        dataSource);

    final Pair<FhirPath, FhirPath> aggAndSuffix = fhirPath.splitLeft(
        FhirpathPurifyTest::isAggregation);
    // check if the child ends with an aggregate 
    if (aggAndSuffix.getLeft().isNull()) {

      return EvalResult.of(
          executor.evaluate(aggAndSuffix.getRight()),
          Aggregator.of(executor, c -> ValueFunctions.unnest(functions.collect_list(c)),
              FhirPath.nullPath())
      );
    } else {
      final FhirPath aggPath = aggAndSuffix.getLeft();
      final FhirPath aggSuffix = aggAndSuffix.getRight();
      System.out.println("Agg split: " + aggPath + " +  " + aggSuffix);
      return EvalResult.of(
          executor.evaluate(aggPath),
          Aggregator.of(executor, getAggregation(aggPath.last()), aggSuffix)
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

  static boolean isJoiningPath(final FhirPath path) {
    return isReverseResolve(path) || isResolve(path);
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

  @Nonnull
  EvalResult evalPath(@Nonnull final ObjectDataSource dataSource,
      @Nonnull final ResourceType subjectResource,
      @Nonnull final FhirPath fhirPath) {

    final Pair<FhirPath, FhirPath> parentAndJoninigPath = fhirPath.splitRight(
        FhirpathPurifyTest::isJoiningPath);

    final FhirPath parentPath = parentAndJoninigPath.getLeft();
    System.out.println("Parent path:" + parentPath + " -> " + parentPath.toExpression());
    final FhirPath joiningPath = parentAndJoninigPath.getRight();
    System.out.println("Joining path: " + joiningPath);

    if (joiningPath.isNull()) {
      return evalPurePath(dataSource, subjectResource, parentPath);
    } else if (isResolve(joiningPath.first())) {
      // well firstly I might not be able to determine the type of the join resource here 
      // unless the the explicit ofType() is used
      // sometimes the type can be inferred from the path itself
      // if a sinlular reference is used
      final EvalFunction resolve = asResolve(
          joiningPath.first()).orElseThrow();
      System.out.println(resolve);
      final FhirPath childPath = joiningPath.suffix();
      return evalResolve(dataSource, subjectResource, parentPath, childPath);
    } else {
      throw new IllegalArgumentException("Unsupported complex path: " + joiningPath);
    }
  }

  @Nonnull
  private EvalResult evalResolve(@Nonnull final ObjectDataSource dataSource,
      @Nonnull final ResourceType parentResource, @Nonnull final FhirPath parentPath,
      @Nonnull final FhirPath childPath) {

    final FhirPathExecutor parentExecutor = createExecutor(parentResource,
        dataSource);

    // TODO: eval the reference and to get access to reference path 
    // and evaluate the key from it and also check for the type of the referenced resource
    // NOTE: I am not sure how to handle access to multiple reference types here, especially 
    // if ofType() is applied to an operator of the path 
    final CollectionDataset parentResult = parentExecutor.evaluate(
        parentPath.andThen(new Traversal("reference")));

    final CollectionDataset referenceResult = parentExecutor.evaluate(
        parentPath);
    final ReferenceCollection referenceCollection = (ReferenceCollection) referenceResult.getValue();
    System.out.println(
        "Reference types" + referenceCollection.getReferenceTypes().stream().toList());
    // TODO: this hardcoded Patient for subject referecnes (as the group is ommited
    // but should instead somehow alllow for lazy evaluation of ofType(...) for 
    // references with more than one type allowed (polymorphic references)
    final ResolveRoot resolveRoot = ResolveRoot.ofResource(parentResource,
        referenceCollection.getReferenceTypes().stream().filter(t -> t != ResourceType.GROUP)
            .toList().get(0),
        parentPath.toExpression());

    System.out.println(
        "Resolve " + referenceCollection.isToOneReference() + " : " + resolveRoot + "->"
            + parentPath.toExpression() + " : "
            + childPath.toExpression());

    if (referenceCollection.isToOneReference()) {

      // TODO: this should be replaced with call to evalPath() with not grouping context
      final FhirPathExecutor childExecutor = createExecutor(resolveRoot.getForeignResourceType(),
          dataSource);

      final CollectionDataset childResult = childExecutor.evaluate(childPath);
      final Dataset<Row> childDataset = childResult.materialize(resolveRoot.getValueTag())
          .select(functions.col("key").alias(resolveRoot.getChildKeyTag()),
              functions.col(resolveRoot.getValueTag()));
      childDataset.show();

      final Dataset<Row> joinedDataset = parentResult.materialize(resolveRoot.getParentKeyTag())
          .join(
              childDataset,
              functions.col(resolveRoot.getParentKeyTag())
                  .equalTo(childDataset.col(resolveRoot.getChildKeyTag())),
              "left_outer");

      return EvalResult.of(
          CollectionDataset.of(joinedDataset,
              childResult.getValue().withColumn(functions.col(resolveRoot.getValueTag()))),
          Aggregator.of(childExecutor, c -> c,
              FhirPath.nullPath())
      );
    } else {

      final EvalResult childEvalResult = evalPath(dataSource,
          resolveRoot.getForeignResourceType(),
          childPath).aggregate(Optional.empty());

      final CollectionDataset childResult = childEvalResult.getResult();
      final Dataset<Row> childDataset = childResult.materialize(resolveRoot.getValueTag())
          .select(functions.col("key").alias(resolveRoot.getChildKeyTag()),
              functions.col(resolveRoot.getValueTag()));
      childDataset.show();

      final Dataset<Row> joinedDataset = parentResult.materialize(resolveRoot.getParentKeyTag(),
              functions::explode_outer)
          .join(
              childDataset,
              functions.col(resolveRoot.getParentKeyTag())
                  .equalTo(childDataset.col(resolveRoot.getChildKeyTag())),
              "left_outer");

      // and now aggregate by this context

      // this grouping context is based on the parent key
      final GroupingContext parentGroupingContext = GroupingContext.of(
          // TODO: we actually need to re-group based on the parent resource key (id)
          parentResult.getDataset().col("key"),
          resolveRoot.getValueTag()
      );

      return EvalResult.of(
          CollectionDataset.of(joinedDataset,
              childResult.getValue().withColumn(functions.col(resolveRoot.getValueTag()))),
          childEvalResult.getAggregator()
      ).aggregate(Optional.of(parentGroupingContext));
    }
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
  Dataset<Row> evalExpression(@Nonnull final ObjectDataSource dataSource,
      @Nonnull final ResourceType subjectResource,
      @Nonnull final String fhirExpression) {

    final FhirPath fhirPath = parser.parse(fhirExpression);

    final FhirPathExecutor subjectExecutor = createExecutor(subjectResource, dataSource);
    final Dataset<Row> initialDataset = subjectExecutor.createInitialDataset();
    initialDataset.show();
    final FhirpathResult purifiedResult = purify(dataSource, subjectResource, fhirPath,
        initialDataset);
    System.out.println("Purified path: " + purifiedResult.getFhirPath());
    purifiedResult.getDataset().show();
    return subjectExecutor.execute(purifiedResult.getFhirPath(), purifiedResult.getDataset());
  }

  @Nonnull
  FhirpathResult purify(@Nonnull final ObjectDataSource dataSource,
      @Nonnull final ResourceType subjectResource,
      @Nonnull final FhirPath fhirPath,
      @Nonnull final Dataset<Row> parentDataset) {

    final FhirPath headPath = fhirPath.first();

    if (headPath.isNull()) {
      return FhirpathResult.of(parentDataset, fhirPath);
    } else if (isReverseResolve(headPath)) {
      return purifyReverseResolve(dataSource,
          subjectResource, parentDataset,
          asReverseResolve(headPath).orElseThrow(),
          fhirPath.suffix());
    } else {
      // ok how to purify an operator or anything else
      // well we will need to be able to mapChildren so that
      // they can be purified while at the same time the dataset is reduced
      final FhirPathResultCollector collector = headPath.children().sequential().reduce(
          FhirPathResultCollector.empty(parentDataset),
          (acc, childPath) -> {
            final FhirpathResult result = purify(dataSource, subjectResource, childPath,
                acc.getDataset());
            return acc.add(result);
          }, FhirPathResultCollector::combine);

      // and now purify the rest of the path
      final FhirpathResult pureSuffix = purify(dataSource, subjectResource, fhirPath.suffix(),
          collector.getDataset());

      return FhirpathResult.of(
          pureSuffix.getDataset(),
          headPath.withNewChildren(collector.getPaths()).andThen(pureSuffix.getFhirPath())
      );
    }
  }

  // TODO: remove parent path
  @Nonnull
  private FhirpathResult purifyReverseResolve(@Nonnull final ObjectDataSource dataSource,
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
        childPath, childParentKeyResult.getDataset());

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

    final Dataset<Row> childResult = childValueResult.getDataset().drop("id", "key");
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
  
  @Test
  void simpleReverseResolveToSingularValue() {
    final ObjectDataSource dataSource = getPatientsWithConditions();
    final Dataset<Row> resultDataset = evalExpression(dataSource, ResourceType.PATIENT,
        "reverseResolve(Condition.subject).id");
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    resultDataset.show();
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", sql_array("x", "y")),
            RowFactory.create("2", sql_array("z")),
            RowFactory.create("3", null)
        );
  }

  @Test
  void simpleReverseResolveToManyValue() {
    final ObjectDataSource dataSource = getPatientsWithConditions();
    final Dataset<Row> resultDataset = evalExpression(dataSource, ResourceType.PATIENT,
        "reverseResolve(Condition.subject).code.coding.code");
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", sql_array("code-xx", "code-xy", "code-yx", "code-yy")),
            RowFactory.create("2", sql_array("code-zx", "code-zy", "code-zz")),
            RowFactory.create("3", null)
        );
  }

  @Test
  void simpleReverseResolveToLeafCountFunction() {
    final ObjectDataSource dataSource = getPatientsWithConditions();
    final Dataset<Row> resultDataset = evalExpression(dataSource, ResourceType.PATIENT,
        "reverseResolve(Condition.subject).code.coding.code.count()");

    // TODO: should be 0 in the last row

    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", 4),
            RowFactory.create("2", 3),
            RowFactory.create("3", 0)
        );
  }

  @Test
  void simpleReverseResolveToChildResourceCountFunction() {
    final ObjectDataSource dataSource = getPatientsWithConditions();
    final Dataset<Row> resultDataset = evalExpression(dataSource, ResourceType.PATIENT,
        "reverseResolve(Condition.subject).count()");

    // TODO: should be 0 in the last row

    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", 2),
            RowFactory.create("2", 1),
            RowFactory.create("3", 0)
        );
  }

  @Test
  void reverseResolveInWhereWithChildResourceCountFunction() {
    final ObjectDataSource dataSource = getPatientsWithConditions();
    final Dataset<Row> resultDataset = evalExpression(dataSource, ResourceType.PATIENT,
        "where(reverseResolve(Condition.subject).count() = 2).id");

    // TODO: should be 0 in the last row

    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", "1"),
            RowFactory.create("2", null),
            // TODO: count() should be 0
            RowFactory.create("3", null)
        );
  }

  @Test
  void simpleReverseResolveToLeafFirstFunction() {
    final ObjectDataSource dataSource = getPatientsWithConditions();
    final Dataset<Row> resultDataset = evalExpression(dataSource, ResourceType.PATIENT,
        "reverseResolve(Condition.subject).code.coding.code.first()");

    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", "code-xx"),
            RowFactory.create("2", "code-zx"),
            RowFactory.create("3", null)
        );
  }

  @Test
  void simpleReverseResolveToCountFirstFunction() {
    // TODO: Implement
    final ObjectDataSource dataSource = getPatientsWithConditions();
    final Dataset<Row> resultDataset = evalExpression(dataSource, ResourceType.PATIENT,
        "reverseResolve(Condition.subject).code.coding.code.count().first()");

    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", 4),
            RowFactory.create("2", 3),
            RowFactory.create("3", 0)
        );
  }

  @Test
  void simpleReverseResolveToFirstCountFunction() {
    // TODO: Implement
    final ObjectDataSource dataSource = getPatientsWithConditions();
    final Dataset<Row> resultDataset = evalExpression(dataSource, ResourceType.PATIENT,
        "reverseResolve(Condition.subject).code.coding.code.first().count()");

    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", 1),
            RowFactory.create("2", 1),
            RowFactory.create("3", 0)
        );
  }

  @Test
  void whereReverseResolveToSingularValue() {
    final ObjectDataSource dataSource = getPatientsWithConditions();
    final Dataset<Row> resultDataset = evalExpression(dataSource,
        ResourceType.PATIENT,
        "where(gender='female').reverseResolve(Condition.subject).id"
    );
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    resultDataset.show();
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", sql_array("x", "y")),
            RowFactory.create("2", null),
            RowFactory.create("3", null)
        );
  }

  @Test
  void multipleReverseResolveInOperator() {
    // TODO: Implement
    final ObjectDataSource dataSource = getPatientsWithConditions();
    final Dataset<Row> resultDataset = evalExpression(dataSource, ResourceType.PATIENT,
        "reverseResolve(Condition.subject).code.coding.count() + reverseResolve(Condition.subject).id.count()");

    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", 6),
            RowFactory.create("2", 4),
            RowFactory.create("3", 0)
        );
  }

  @Test
  void nestedReverseResolveToSingularValue() {
    final ObjectDataSource dataSource = getPatientsWithEncountersWithConditions();

    final Dataset<Row> resultDataset = evalExpression(dataSource,
        ResourceType.PATIENT,
        "reverseResolve(Encounter.subject).reverseResolve(Condition.encounter).id"
    );
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    resultDataset.show();
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", sql_array("1.1.1", "1.1.2", "1.1.3", "2.1.1")),
            RowFactory.create("2", sql_array("2.1.1", "2.1.2")),
            RowFactory.create("3", null)
        );
  }

  @Test
  void nestedReverseResolveToAggregation() {
    final ObjectDataSource dataSource = getPatientsWithEncountersWithConditions();

    final Dataset<Row> resultDataset = evalExpression(dataSource,
        ResourceType.PATIENT,
        "reverseResolve(Encounter.subject).reverseResolve(Condition.encounter).id.count()"
    );
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    resultDataset.show();

    // TODO: should be 0 in the last row

    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("1", 4),
            RowFactory.create("2", 2),
            RowFactory.create("3", 0)
        );
  }

  private @NotNull ObjectDataSource getPatientsWithEncountersWithConditions() {
    return new ObjectDataSource(spark, encoders,
        List.of(
            new Patient().setGender(AdministrativeGender.FEMALE).setId("Patient/1"),
            new Patient().setGender(AdministrativeGender.MALE).setId("Patient/2"),
            new Patient().setGender(AdministrativeGender.MALE).setId("Patient/3"),
            new Encounter().setSubject(new Reference("Patient/1")).setId("Encounter/1.1"),
            new Encounter().setSubject(new Reference("Patient/1")).setId("Encounter/1.2"),
            new Encounter().setSubject(new Reference("Patient/2")).setId("Encounter/2.1"),
            new Condition().setSubject(new Reference("Patient/1"))
                .setEncounter(new Reference("Encounter/1.1")).setId("Condition/1.1.1"),
            new Condition().setSubject(new Reference("Patient/1"))
                .setEncounter(new Reference("Encounter/1.1")).setId("Condition/1.1.2"),
            new Condition().setSubject(new Reference("Patient/1"))
                .setEncounter(new Reference("Encounter/1.1")).setId("Condition/1.1.3"),
            new Condition().setSubject(new Reference("Patient/1"))
                .setEncounter(new Reference("Encounter/1.2")).setId("Condition/2.1.1"),
            new Condition().setSubject(new Reference("Patient/2"))
                .setEncounter(new Reference("Encounter/2.1")).setId("Condition/2.1.1"),
            new Condition().setSubject(new Reference("Patient/2"))
                .setEncounter(new Reference("Encounter/2.1")).setId("Condition/2.1.2")
        ));
  }

  //
  // SECTION: Resolve
  //
  @Test
  void resolveManyToOneWithSimpleValue() {
    final ObjectDataSource dataSource = getPatientsWithConditions();

    final Dataset<Row> resultDataset = evalExpression(dataSource,
        ResourceType.CONDITION,
        "subject.resolve().gender"
    );
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    resultDataset.show();
    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("x", "female"),
            RowFactory.create("y", "female"),
            RowFactory.create("z", "male")
        );
  }

  @Test
  void resolveToManyWithSimpleValue() {
    // ON Encounter
    // final Dataset<Row> joinedResult = evalFhirPathMulti(ResourceType.ENCOUNTER,"episodeOfCare.resolve().status", dataSource);

    final ObjectDataSource dataSource = new ObjectDataSource(spark, encoders,
        List.of(
            new Encounter()
                .addEpisodeOfCare(new Reference("EpisodeOfCare/01_1"))
                .addEpisodeOfCare(new Reference("EpisodeOfCare/01_2"))
                .setId("Encounter/01"),
            new Encounter()
                .addEpisodeOfCare(new Reference("EpisodeOfCare/02_1"))
                .addEpisodeOfCare(new Reference("EpisodeOfCare/02_2"))
                .setId("Encounter/02"),
            new Encounter().setId("Encounter/03"),
            new EpisodeOfCare().setStatus(EpisodeOfCareStatus.ACTIVE).setId("EpisodeOfCare/01_1"),
            new EpisodeOfCare().setStatus(EpisodeOfCareStatus.FINISHED)
                .setId("EpisodeOfCare/01_2"),
            new EpisodeOfCare().setStatus(EpisodeOfCareStatus.ONHOLD).setId("EpisodeOfCare/02_1"),
            new EpisodeOfCare().setStatus(EpisodeOfCareStatus.PLANNED).setId("EpisodeOfCare/02_2")
        ));

    final Dataset<Row> resultDataset = evalExpression(dataSource,
        ResourceType.ENCOUNTER,
        "episodeOfCare.resolve().status"
    );
    System.out.println(resultDataset.queryExecution().executedPlan().toString());
    resultDataset.show();

    new DatasetAssert(resultDataset)
        .hasRowsUnordered(
            RowFactory.create("01", sql_array("active", "finished")),
            RowFactory.create("02", sql_array("onhold", "planned")),
            RowFactory.create("03", sql_array())
        );
  }

  @Nonnull
  private ObjectDataSource getPatientsWithConditions() {
    return new ObjectDataSource(spark, encoders,
        List.of(
            new Patient().setGender(AdministrativeGender.FEMALE).setId("Patient/1"),
            new Patient().setGender(AdministrativeGender.MALE).setId("Patient/2"),
            new Patient().setGender(AdministrativeGender.MALE).setId("Patient/3"),
            new Condition()
                .setSubject(new Reference("Patient/1"))
                .setCode(
                    new CodeableConcept()
                        .addCoding(new Coding().setCode("code-xx"))
                        .addCoding(new Coding().setCode("code-xy"))
                        .setText("Coding-x")
                )
                .setId("Condition/x"),
            new Condition()
                .setSubject(new Reference("Patient/1"))
                .setCode(
                    new CodeableConcept()
                        .addCoding(new Coding().setCode("code-yx"))
                        .addCoding(new Coding().setCode("code-yy"))
                        .setText("Coding-y")
                )
                .setId("Condition/y"),
            new Condition()
                .setSubject(new Reference("Patient/2"))
                .setCode(
                    new CodeableConcept()
                        .addCoding(new Coding().setCode("code-zx"))
                        .addCoding(new Coding().setCode("code-zy"))
                        .addCoding(new Coding().setCode("code-zz"))
                        .setText("Coding-z")
                )
                .setId("Condition/z")
        ));
  }

  @Nonnull
  private static <T> WrappedArray<T> sql_array(final T... values) {
    return WrappedArray.make(values);
  }

  @Nonnull
  private FhirPathExecutor createExecutor(final ResourceType subjectResourceType,
      final ObjectDataSource dataSource) {
    return new SingleFhirPathExecutor(subjectResourceType,
        FhirContext.forR4(), StaticFunctionRegistry.getInstance(),
        Collections.emptyMap(), dataSource);
  }
}
