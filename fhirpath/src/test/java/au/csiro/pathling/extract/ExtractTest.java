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

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.path.Paths.ExtConsFhir;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import au.csiro.pathling.view.AbstractCompositeSelection;
import au.csiro.pathling.view.ForEachSelection;
import au.csiro.pathling.view.FromSelection;
import au.csiro.pathling.view.PrimitiveSelection;
import au.csiro.pathling.view.Selection;
import org.junit.jupiter.api.Test;

public class ExtractTest {

  // there can a path
  // best to convert invovation path to the list of expressions.
  @Nonnull
  static Selection decompose(@Nonnull final List<FhirPath<Collection>> paths) {
    return new FromSelection(new ExtConsFhir("%resource"), decomposeInternal(paths));
  }

  static List<Selection> decomposeInternal(@Nonnull final List<FhirPath<Collection>> paths) {
    final Map<FhirPath<Collection>, List<FhirPath<Collection>>> tailsByHeads = paths.stream()
        .collect(
            Collectors.groupingBy(FhirPath<Collection>::head,
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


  @Test
  void testExpression() {

    final Parser parser = new Parser();

    final List<String> expressions = List.of(
        "name.first().family",
        "name.first().given.last()",
        "gender"
    );
    expressions.stream().map(parser::parse).forEach(System.out::println);

    final List<FhirPath<Collection>> paths = Stream.of(
            "name",
            "name.first().family",
            "name.first().given.last()",
            "gender",
            "name.where(use.first() = 'official').given"
        )
        .map(parser::parse)
        .collect(Collectors.toUnmodifiableList());

    paths.forEach(System.out::println);

    final Map<FhirPath<Collection>, List<FhirPath<Collection>>> xxx = paths.stream()
        .collect(
            Collectors.groupingBy(FhirPath<Collection>::head,
                Collectors.mapping(
                    FhirPath<Collection>::tail,
                    Collectors.toList())));

    xxx.forEach((k, v) -> System.out.println(k + " -> " + v));
    //
    // // final Map<String, List<String>> str = Stream.of("dsdsd", "reker", "rere")
    // //     .collect(Collectors.groupingBy(s -> s.substring(0, 1),
    // //         Collectors.flatMapping(s -> Optional.of(s.substring(1)).stream(),
    // //             Collectors.toUnmodifiableList())));
    // // str.entrySet().forEach(System.out::println);

    
    final Selection view = decompose(paths);
    System.out.println("## Raw view ##");
    view.printTree();
    final Selection optimizedView = optimise(view);
    System.out.println("## Optimised view ##");
    optimizedView.printTree();
  }
}
