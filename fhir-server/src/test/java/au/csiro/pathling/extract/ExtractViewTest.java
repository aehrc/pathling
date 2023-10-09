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
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.path.Paths.ExtConsFhir;
import au.csiro.pathling.io.CacheableDatabase;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.helpers.TestHelpers;
import au.csiro.pathling.view.AbstractCompositeSelection;
import au.csiro.pathling.view.ForEachSelection;
import au.csiro.pathling.view.FromSelection;
import au.csiro.pathling.view.PrimitiveSelection;
import au.csiro.pathling.view.Selection;
import au.csiro.pathling.view.View;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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


  View.Context newContext() {
    return new View.Context(spark, fhirContext, dataSource);
  }

  @Test
  void combineWithUnequalCardinalities() {

    final Parser parser = new Parser();
    final List<String> expressios = List.of(
        //"name.family",
        //"name.given",
        "id",
        "gender.first()",
        "name.use",
        "name.family",
        "name.given.first()"
    );

    System.out.println("### Expressions: ###");
    expressios.forEach(System.out::println);

    final List<FhirPath<Collection>> paths = expressios.stream()
        .map(parser::parse)
        .collect(Collectors.toUnmodifiableList());

    System.out.println("### Paths: ###");
    paths.forEach(System.out::println);
    final Selection view = decompose(paths);
    System.out.println("## Raw view ##");
    view.printTree();
    final Selection optimizedView = optimise(view);
    System.out.println("## Optimised view ##");
    optimizedView.printTree();

    final View extractView = new View(ResourceType.PATIENT, view);
    System.out.println("## Extract view ##");
    extractView.printTree();

    final Dataset<Row> resultDataset = extractView.evaluate(newContext());
    resultDataset.show(false);
    System.out.println(resultDataset.logicalPlan());
    System.out.println(resultDataset.queryExecution().executedPlan());

    System.out.println(resultDataset.queryExecution().optimizedPlan());

  }

  void mockResource(final ResourceType... resourceTypes) {
    TestHelpers.mockResource(dataSource, spark, resourceTypes);
  }

}
