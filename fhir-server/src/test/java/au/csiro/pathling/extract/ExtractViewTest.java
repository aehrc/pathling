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

import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.path.Paths.ExtConsFhir;
import au.csiro.pathling.io.CacheableDatabase;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.helpers.TestHelpers;
import au.csiro.pathling.view.AbstractCompositeSelection;
import au.csiro.pathling.view.DatasetView;
import au.csiro.pathling.view.DefaultProjectionContext;
import au.csiro.pathling.view.ForEachSelection;
import au.csiro.pathling.view.FromSelection;
import au.csiro.pathling.view.PrimitiveSelection;
import au.csiro.pathling.view.Selection;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

/**
 * @author John Grimes
 */
@SpringBootUnitTest
class ExtractViewTest {

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
  static Selection decompose(@Nonnull final List<FhirPath<Collection>> paths) {
    return new FromSelection(new ExtConsFhir("%resource"), decomposeInternal(paths));
  }

  static List<Selection> decomposeInternal(@Nonnull final List<FhirPath<Collection>> paths) {
    final Map<FhirPath<Collection>, List<FhirPath<Collection>>> tailsByHeads = paths.stream()
        .collect(
            Collectors.groupingBy(FhirPath<Collection>::head, LinkedHashMap::new,
                Collectors.mapping(
                    FhirPath<Collection>::tail,
                    Collectors.toList())));

    return tailsByHeads.entrySet().stream()
        .map(e -> e.getKey().isNull()
                  ? new PrimitiveSelection(e.getKey())
                  : new ForEachSelection(e.getKey(), decomposeInternal(e.getValue())))
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
    mockResource(ResourceType.PATIENT, ResourceType.CONDITION, ResourceType.OBSERVATION);
  }


  DefaultProjectionContext newContext(@Nonnull final ResourceType resourceType) {
    final Dataset<Row> dataset = dataSource.read(resourceType);
    final ResourceCollection inputContext = ResourceCollection.build(fhirContext, dataset,
        resourceType);
    final EvaluationContext evaluationContext = new EvaluationContext(inputContext, inputContext,
        fhirContext, spark, dataset, StaticFunctionRegistry.getInstance(),
        Optional.of(terminologyServiceFactory), Optional.empty());
    return new DefaultProjectionContext(evaluationContext);
  }

  @Test
  void combineWithUnequalCardinalities() {
    final DefaultProjectionContext evalContext = newContext(ResourceType.PATIENT);

    final Parser parser = new Parser();
    final List<FhirPath<Collection>> paths = Stream.of(
            //"name.family",
            //"name.given",
            "id",
            "gender.first()",
            "name.use",
            "name.family",
            "name.given.first()"
        )
        .map(parser::parse)
        .collect(Collectors.toUnmodifiableList());

    paths.forEach(System.out::println);
    final Selection view = decompose(paths);
    System.out.println("## Raw view ##");
    view.printTree();
    final Selection optimizedView = optimise(view);
    System.out.println("## Optimised view ##");
    optimizedView.printTree();

    final DatasetView result = view.evaluate(evalContext);
    //final Dataset<Row> resultDataset = result.apply(evalContext.getDataset());
    final Dataset<Row> resultDataset = result.apply(
        evalContext.getEvaluationContext().getDataset());

    resultDataset.show(false);

    System.out.println(resultDataset.logicalPlan());
    System.out.println(resultDataset.queryExecution().executedPlan());
  }

  void mockResource(final ResourceType... resourceTypes) {
    TestHelpers.mockResource(dataSource, spark, resourceTypes);
  }

}
