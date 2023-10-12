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

package au.csiro.pathling.query;

import au.csiro.pathling.aggregate.AggregateRequest;
import au.csiro.pathling.extract.ExtractRequest;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.path.Paths;
import au.csiro.pathling.fhirpath.path.Paths.ExtConsFhir;
import au.csiro.pathling.view.AggregationView;
import au.csiro.pathling.view.ExtractView;
import au.csiro.pathling.view.ForEachOrNullSelection;
import au.csiro.pathling.view.FromSelection;
import au.csiro.pathling.view.PrimitiveSelection;
import au.csiro.pathling.view.Selection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;


@Value
@Slf4j
public class QueryParser {

  @Nonnull
  Parser parser;

  @Nonnull
  public ExtractView toView(@Nonnull final ExtractRequest request) {
    final List<FhirPath<Collection>> paths = request.getColumnsAsStrings().stream()
        .map(parser::parse)
        .collect(Collectors.toUnmodifiableList());
    log.debug("Parsed paths:\n{}",
        paths.stream().map(FhirPath::toString).collect(Collectors.joining("\n")));
    final Selection select = decompose(paths);
    return new ExtractView(ResourceType.PATIENT, select);
  }


  @Nonnull
  public AggregationView toView(@Nonnull final AggregateRequest request) {
    final List<FhirPath<Collection>> groupByPaths = request.getGroupings().stream()
        .map(parser::parse)
        .collect(Collectors.toUnmodifiableList());
    log.debug("Parsed groupBy paths:\n{}",
        groupByPaths.stream().map(FhirPath::toString).collect(Collectors.joining("\n")));
    final Selection groupBy = decompose(groupByPaths);

    final List<FhirPath<Collection>> aggPaths = request.getAggregations().stream()
        .map(parser::parse)
        .collect(Collectors.toUnmodifiableList());
    log.debug("Parsed aggregate paths:\n{}",
        aggPaths.stream().map(FhirPath::toString).collect(Collectors.joining("\n")));
    
    // TODO: validate that all aggPaths are aggregations
    final List<FhirPath<Collection>> aggFields = aggPaths.stream().map(FhirPath::prefix)
        .collect(Collectors.toUnmodifiableList());
    final List<FhirPath<Collection>> aggFunctions = aggPaths.stream().map(FhirPath::last)
        .collect(Collectors.toUnmodifiableList());

    // TODO: needs a better model for the aggregation decomposition
    return new AggregationView(ResourceType.PATIENT, groupBy, decomposeSimple(aggFields),
        aggFunctions);
  }

  @Nonnull
  public static Selection decomposeSimple(@Nonnull final List<FhirPath<Collection>> paths) {
    // TODO: this can be simplified if we can explode the leaves
    return new FromSelection(new ExtConsFhir("%resource"),
        paths.stream().map(p -> new ForEachOrNullSelection(p,
                List.of(new PrimitiveSelection(FhirPath.nullPath()))))
            .collect(Collectors.toUnmodifiableList()));
  }

  @Nonnull
  public static Selection decompose(@Nonnull final List<FhirPath<Collection>> paths) {
    return new FromSelection(new ExtConsFhir("%resource"), decomposeInternal(paths));
  }

  static boolean isTraversal(@Nonnull final FhirPath<Collection> path) {
    return path instanceof Paths.Traversal || path.isNull();
  }

  static Stream<? extends Selection> decomposeSelection(@Nonnull final FhirPath<Collection> parent,
      @Nonnull final List<FhirPath<Collection>> children) {

    // TODO: do not create empty selections.
    return parent.isNull()
           ? Stream.of(new PrimitiveSelection(parent))
           : Stream.of(
               new ForEachOrNullSelection(parent, decomposeInternal(
                   children.stream().filter(QueryParser::isTraversal).collect(
                       Collectors.toUnmodifiableList()))),
               new FromSelection(parent, decomposeInternal(
                   children.stream().filter(c -> !isTraversal(c)).collect(
                       Collectors.toUnmodifiableList())))
           ).filter(s -> !s.getComponents().isEmpty());
  }

  static List<Selection> decomposeInternal(@Nonnull final List<FhirPath<Collection>> paths) {
    final Map<FhirPath<Collection>, List<FhirPath<Collection>>> tailsByHeads = paths.stream()
        .collect(
            Collectors.groupingBy(FhirPath<Collection>::first, LinkedHashMap::new,
                Collectors.mapping(
                    FhirPath<Collection>::suffix,
                    Collectors.toList())));

    // This needs to be more sophisticated
    // 1. PathTraverslas and Nulls go into the ForSelection bucket
    // 2. Everyting else to to FromSelection bucket
    return tailsByHeads.entrySet().stream()
        .flatMap(e -> decomposeSelection(e.getKey(), e.getValue()))
        .collect(Collectors.toUnmodifiableList());
  }
}
