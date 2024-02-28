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

package au.csiro.pathling.extract;

import static au.csiro.pathling.view.AggregationView.AGG_FUNCTIONS;

import au.csiro.pathling.aggregate.AggregateRequest;
import au.csiro.pathling.aggregate.AggregateResponse;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.path.Paths;
import au.csiro.pathling.fhirpath.path.Paths.ExtConsFhir;
import au.csiro.pathling.io.CacheableDatabase;
import au.csiro.pathling.query.QueryParser;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.helpers.TestHelpers;
import au.csiro.pathling.view.AbstractCompositeSelection;
import au.csiro.pathling.view.AggregationView;
import au.csiro.pathling.view.DatasetResult;
import au.csiro.pathling.view.ProjectionContext;
import au.csiro.pathling.view.ExecutionContext;
import au.csiro.pathling.view.ExtractView;
import au.csiro.pathling.view.ForEachOrNullSelection;
import au.csiro.pathling.view.FromSelection;
import au.csiro.pathling.view.PrimitiveSelection;
import au.csiro.pathling.view.Selection;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.common.collect.Streams;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

/**
 * @author John Grimes
 */
@SpringBootUnitTest
class ExtractAggregatePOCViewTest {

  @Autowired
  QueryConfiguration configuration;

  @Autowired
  FhirContext fhirContext;

  @Autowired
  SparkSession spark;

  @Autowired
  TerminologyServiceFactory terminologyServiceFactory;

  @Autowired
  IParser jsonParser;

  @Autowired
  FhirEncoders fhirEncoders;

  @MockBean
  CacheableDatabase dataSource;


  @Nonnull
  static Selection decompose(@Nonnull final List<FhirPath> paths) {
    return new FromSelection(new ExtConsFhir("%resource"), decomposeInternal(paths));
  }

  static boolean isTraversal(@Nonnull final FhirPath path) {
    return path instanceof Paths.Traversal || path.isNull();
  }

  static Stream<? extends Selection> decomposeSelection(@Nonnull final FhirPath parent,
      @Nonnull final List<FhirPath> children) {

    // TODO: do not create empty selections.
    return parent.isNull()
           ? Stream.of(new PrimitiveSelection(parent))
           : Stream.of(
               new ForEachOrNullSelection(parent, decomposeInternal(
                   children.stream().filter(ExtractAggregatePOCViewTest::isTraversal).collect(
                       Collectors.toUnmodifiableList()))),
               new FromSelection(parent, decomposeInternal(
                   children.stream().filter(c -> !isTraversal(c)).collect(
                       Collectors.toUnmodifiableList())))
           ).filter(s -> !s.getComponents().isEmpty());
  }

  static List<Selection> decomposeInternal(@Nonnull final List<FhirPath> paths) {
    final Map<FhirPath, List<FhirPath>> tailsByHeads = paths.stream()
        .collect(
            Collectors.groupingBy(FhirPath::first, LinkedHashMap::new,
                Collectors.mapping(
                    FhirPath::suffix,
                    Collectors.toList())));

    // This needs to be more sophisticated
    // 1. PathTraverslas and Nulls go into the ForSelection bucket
    // 2. Everyting else to to FromSelection bucket
    return tailsByHeads.entrySet().stream()
        .flatMap(e -> decomposeSelection(e.getKey(), e.getValue()))
        .collect(Collectors.toUnmodifiableList());
  }


  @Nonnull
  static Selection mergePathRule(@Nonnull final Selection selection) {
    // optimisation rules

    // composite selection with only one component can be joined to a 
    // simple selection with joined expressions

    if (selection instanceof AbstractCompositeSelection) {
      final AbstractCompositeSelection compositeSelection = (AbstractCompositeSelection) selection;
      if (compositeSelection.getComponents().size() == 1) {
        final Selection component = compositeSelection.getComponents().get(0);
        if (component instanceof PrimitiveSelection) {
          return new PrimitiveSelection(
              compositeSelection.getPath().andThen(((PrimitiveSelection) component).getPath()));
        }
      }
    }
    return selection;
  }

  @Nonnull
  static Selection optimise(@Nonnull final Selection selection) {
    // optimisation rules
    return selection.map(ExtractTest::mergePathRule);
  }


  @BeforeEach
  void setUp() {
    SharedMocks.resetAll();
    mockResource(ResourceType.PATIENT, ResourceType.CONDITION, ResourceType.OBSERVATION,
        ResourceType.MEDICATIONREQUEST);
  }


  ExecutionContext newContext() {
    return new ExecutionContext(spark, fhirContext, dataSource);
  }

  @Test
  void testExtractView() {

    dataSource.read(ResourceType.PATIENT).select("id", "name.family", "name.given")
        .show(false);

    final List<String> expressions = List.of(
        //"name.family",
        //"name.given",
        "id",
        "gender.first()",
        "name.use",
        "name.prefix",
        "name.family",
        "name.given.first()",
        "name.count()",
        "name.where(prefix = 'Ms.').count()",
        "maritalStatus.coding.code",
        "maritalStatus.coding.count()"
    );

    System.out.println("### Expressions: ###");
    expressions.forEach(System.out::println);
    final ExtractRequest extractRequest = ExtractRequest.fromUserInput(
        ResourceType.PATIENT,
        Optional.of(expressions),
        Optional.empty(),
        Optional.empty()
    );

    final QueryParser queryParser = new QueryParser(new Parser());
    final ExtractView extractView = queryParser.toView(extractRequest);
    System.out.println("## Extract view ##");
    extractView.printTree();
    final Dataset<Row> resultDataset = extractView.evaluate(newContext());
    resultDataset.show(false);
    System.out.println(resultDataset.logicalPlan());
    System.out.println(resultDataset.queryExecution().executedPlan());

    System.out.println(resultDataset.queryExecution().optimizedPlan());

  }


  @Test
  void testExtractWithResoutions() {

    final List<String> expressions = List.of(
        "id",
        "gender",
        "reverseResolve(Condition.subject).clinicalStatus"
    );

    System.out.println("### Expressions: ###");
    expressions.forEach(System.out::println);
    final ExtractRequest extractRequest = ExtractRequest.fromUserInput(
        ResourceType.PATIENT,
        Optional.of(expressions),
        Optional.empty(),
        Optional.empty()
    );

    final QueryParser queryParser = new QueryParser(new Parser());
    final ExtractView extractView = queryParser.toView(extractRequest);
    System.out.println("## Extract view ##");
    extractView.printTree();
    final Dataset<Row> resultDataset = extractView.evaluate(newContext());
    resultDataset.show(false);
    System.out.println(resultDataset.logicalPlan());
    System.out.println(resultDataset.queryExecution().executedPlan());
  }


  @Test
  void testExtractFGPDView() {
    final List<String> expressions = List.of(
        //"name.family",
        //"name.given",
        "id"
    );

    final List<String> filters = List.of(
        "reverseResolve(Observation.subject).where(code.subsumedBy(http://snomed.info/sct|424144002)).exists(valueQuantity >= 18 'a' and valueQuantity <= 75 'a')",
        "reverseResolve(MedicationRequest.subject).medicationCodeableConcept.subsumedBy(http://fhir.de/CodeSystem/bfarm/atc|A10B|2022).anyTrue()",
        "reverseResolve(MedicationRequest.subject).medicationCodeableConcept.subsumedBy(http://fhir.de/CodeSystem/bfarm/atc|A10A|2022).anyTrue()",
        "reverseResolve(Condition.subject).code.subsumedBy(http://fhir.de/CodeSystem/bfarm/icd-10-gm|E11).anyTrue()",
        "reverseResolve(Observation.subject).where(code.subsumedBy(http://loinc.org|4548-4)).exists(valueQuantity >= 6 '%' and valueQuantity <= 10 '%')",
        "reverseResolve(Condition.subject).code.subsumedBy(http://fhir.de/CodeSystem/bfarm/icd-10-gm|E10).allFalse()",
        "reverseResolve(Condition.subject).code.subsumedBy(http://fhir.de/CodeSystem/bfarm/icd-10-gm|N17).allFalse()"
    );

    System.out.println("### Expressions: ###");
    expressions.forEach(System.out::println);
    System.out.println("### Filters: ###");
    filters.forEach(System.out::println);

    final ExtractRequest extractRequest = ExtractRequest.fromUserInput(
        ResourceType.PATIENT,
        Optional.of(expressions),
        Optional.of(filters),
        Optional.empty()
    );

    final QueryParser queryParser = new QueryParser(new Parser());
    final ExtractView extractView = queryParser.toView(extractRequest);
    System.out.println("## Extract view ##");
    extractView.printTree();
    final Dataset<Row> resultDataset = extractView.evaluate(newContext());
    resultDataset.show(false);
    System.out.println(resultDataset.logicalPlan());
    System.out.println(resultDataset.queryExecution().executedPlan());

    System.out.println(resultDataset.queryExecution().optimizedPlan());

  }

  @Test
  void testAggregation() {

    final Parser parser = new Parser();
    final List<String> grouppingExpressions = List.of(
        //"name.family",
        //"name.given",
        "gender",
        "maritalStatus.coding.code.first()",
        "name.prefix.first()"
    );

    System.out.println("### Expressions: ###");
    grouppingExpressions.forEach(System.out::println);

    final List<FhirPath> groupingPaths = grouppingExpressions.stream()
        .map(parser::parse)
        .collect(Collectors.toUnmodifiableList());

    System.out.println("### Paths: ###");
    groupingPaths.forEach(System.out::println);
    final Selection groupingSelction = decompose(groupingPaths);
    System.out.println("## Raw view ##");
    groupingSelction.printTree();

    final ProjectionContext execContext = ProjectionContext.of(newContext(),
        ResourceType.PATIENT);

    final DatasetResult<Column> groupingResult = groupingSelction.evaluate(execContext).map(
        cr -> cr.getCollection().getColumn().getValue());
    final Dataset<Row> transDs = groupingResult.applyTransform(execContext.getDataset());

    transDs.show(false);

    transDs.groupBy(groupingResult.asStream().toArray(Column[]::new))
        .agg(functions.count(functions.col("id")))
        .show(false);

    final List<String> aggregations = List.of(
        //"name.family",
        //"name.given",
        "id.count()", // .count()
        "name.count().sum()" // .count()
    );

    final List<FhirPath> aggPaths = aggregations.stream()
        .map(parser::parse)
        .collect(Collectors.toUnmodifiableList());

    System.out.println("### Agg Paths ###");
    aggPaths.forEach(System.out::println);

    final List<FhirPath> aggFields = aggPaths.stream().map(FhirPath::prefix)
        .collect(Collectors.toUnmodifiableList());

    System.out.println("### Agg Fields ###");
    aggFields.forEach(System.out::println);

    final List<FhirPath> aggFunctions = aggPaths.stream().map(FhirPath::last)
        .collect(Collectors.toUnmodifiableList());

    System.out.println("### Agg Functions ###");
    aggFunctions.forEach(System.out::println);

    final Selection aggView = decompose(aggFields);
    final DatasetResult<Column> aggResult = aggView.evaluate(execContext).map(
        cr -> cr.getCollection().getColumn().getValue());

    System.out.println("### Pre-grouping ####");
    groupingResult.andThen(aggResult).select(execContext.getDataset(), Function.identity())
        .show(false);

    final Column[] groupingColumns = groupingResult.asStream().toArray(Column[]::new);

    final List<Column> aggColumnsValues = aggResult.asStream()
        .collect(Collectors.toUnmodifiableList());

    final Column[] aggColumns = IntStream.range(0, aggColumnsValues.size())
        .mapToObj(i -> AGG_FUNCTIONS.get(
                ((Paths.EvalFunction) aggFunctions.get(i)).getFunctionIdentifier())
            .apply(aggColumnsValues.get(i)))
        .toArray(Column[]::new);

    final Dataset<Row> finalResult = groupingResult.<Column>asTransform().andThen(aggResult)
        .applyTransform(execContext.getDataset())
        .groupBy(groupingColumns)
        .agg(aggColumns[0], Stream.of(aggColumns).skip(1).toArray(Column[]::new));

    finalResult.show(false);

    // System.out.println(resultDataset.logicalPlan());
    // System.out.println(resultDataset.queryExecution().executedPlan());
    // System.out.println(resultDataset.queryExecution().optimizedPlan());
  }

  @Test
  void testAggregationQuery() {

    final List<String> grouppingExpressions = List.of(
        "gender",
        "maritalStatus.coding.code.first()",
        "name.prefix.first()"
    );

    final List<String> aggregations = List.of(
        "id.count()",
        "name.count().sum()"
    );

    final AggregateRequest aggregateQuery = AggregateRequest.fromUserInput(
        ResourceType.PATIENT,
        Optional.of(aggregations),
        Optional.of(grouppingExpressions),
        Optional.empty());

    final QueryParser queryParser = new QueryParser(new Parser());
    final AggregationView aggregationView = queryParser.toView(aggregateQuery);

    System.out.println("## Aggregation view ##");
    aggregationView.printTree();

    final Dataset<Row> resultDataset = aggregationView.evaluate(newContext());
    resultDataset.show(false);
    System.out.println(resultDataset.logicalPlan());
    System.out.println(resultDataset.queryExecution().executedPlan());
    System.out.println(resultDataset.queryExecution().optimizedPlan());
  }


  @Test
  void testAggregationFull() {

    final Parser parser = new Parser();
    final List<String> grouppingExpressions = List.of(
        //"name.family",
        //"name.given",
        "gender",
        "maritalStatus.coding.code.first()",
        "name.prefix.count()"
    );

    System.out.println("### Expressions: ###");
    grouppingExpressions.forEach(System.out::println);

    final Paths.ExtConsFhir contextPath = new Paths.ExtConsFhir("%resource");

    final List<FhirPath> groupingPaths = grouppingExpressions.stream()
        .map(parser::parse)
        .map(contextPath::andThen)
        .collect(Collectors.toUnmodifiableList());

    System.out.println("### GroupBy Paths: ###");
    groupingPaths.forEach(System.out::println);

    final ProjectionContext execContext = ProjectionContext.of(newContext(),
        ResourceType.PATIENT);

    final List<Collection> groupByCollections = groupingPaths.stream()
        .map(p -> execContext.evalExpression(p).getValue())
        .collect(Collectors.toUnmodifiableList());

    System.out.println("### GroupBy collections: ###");
    groupByCollections.forEach(System.out::println);
    groupByCollections.forEach(c -> System.out.println(c.getFhirType()));

    final DatasetResult<Collection> datasetCollectionResult = groupByCollections.stream()
        .map(c -> (DatasetResult<Collection>) DatasetResult.pureOne(c))
        .reduce(DatasetResult.empty(), DatasetResult::andThen);

    System.out.println("### Grouping Collection Result: ###");
    System.out.println(datasetCollectionResult);

    final Dataset<Row> groupByDataset = datasetCollectionResult.select(execContext.getDataset(),
        c -> c.getColumn().getValue().alias(c.toString()));

    System.out.println("### GrouBy dataset  ###");
    groupByDataset.show(false);

    // collect and materialize the dataset

    final List<Collection> groupbyCollections = datasetCollectionResult.asStream()
        .collect(Collectors.toUnmodifiableList());

    final List<AggregateResponse.Grouping> groupings = groupByDataset.collectAsList().stream()
        .map(rowToGrouping(groupingPaths, groupbyCollections))
        .collect(Collectors.toUnmodifiableList());
    System.out.println("### Groupings  ###");
    groupings.forEach(System.out::println);

  }

  @Nonnull
  static Function<Row, AggregateResponse.Grouping> rowToGrouping(
      @Nonnull final List<FhirPath> groupByPaths,
      @Nonnull final List<Collection> groupByCollections) {
    return row -> {
      final List<Optional<Type>> labels = IntStream.range(0, groupByCollections.size())
          .mapToObj(
              i -> ((Materializable<Type>) groupByCollections.get(i)).getFhirValueFromRow(row, i))
          .collect(Collectors.toUnmodifiableList());

      final String drillDown = Streams.zip(groupByPaths.stream().map(FhirPath::toExpression),
              labels.stream(), Pair::of)
          .map(p -> p.getRight().map(v -> v + " in " + p.getLeft())
              .orElse(p.getLeft() + ".empty()"))
          .collect(Collectors.joining(" and "));

      return new AggregateResponse.Grouping(labels, Collections.emptyList(),
          Optional.of(drillDown));
    };
  }

  void mockResource(final ResourceType... resourceTypes) {
    TestHelpers.mockResource(dataSource, spark, resourceTypes);
  }


  @Test
  void testExtractViewWithReverseResolve() {

    final List<String> expressions = List.of(
        "id",
        "gender",
        "reverseResolve(Condition.subject).clinicalStatus",
        "reverseResolve(Observation.subject).id.count()"
    );

    System.out.println("### Expressions: ###");
    expressions.forEach(System.out::println);
    final ExtractRequest extractRequest = ExtractRequest.fromUserInput(
        ResourceType.PATIENT,
        Optional.of(expressions),
        Optional.empty(),
        Optional.empty()
    );

    final QueryParser queryParser = new QueryParser(new Parser());
    final ExtractView extractView = queryParser.toView(extractRequest);
    System.out.println("## Extract view ##");
    extractView.printTree();
    final Dataset<Row> resultDataset = extractView.evaluate(newContext());
    resultDataset.show(false);
    System.out.println(resultDataset.logicalPlan());
    System.out.println(resultDataset.queryExecution().executedPlan());
  }
}
