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
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;


@Value
@Slf4j
public class QueryParser {

  @Nonnull
  Parser parser;

  @Nonnull
  public ExtractView toView(@Nonnull final ExtractRequest request) {

    final List<PathWithAlias> columns = request.getColumns().stream()
        .map(e -> new PathWithAlias(parser.parse(e.getExpression()),
            Optional.ofNullable(e.getLabel())))
        .collect(Collectors.toUnmodifiableList());
    log.debug("Parsed paths:\n{}",
        columns.stream().map(PathWithAlias::toString).collect(Collectors.joining("\n")));

    final Selection select = decomposeAliased(columns);
    final List<FhirPath<Collection>> filter = request.getFilters().stream().map(parser::parse)
        .collect(Collectors.toUnmodifiableList());

    return new ExtractView(request.getSubjectResource(),
        select, decomposeFilter(filter)
    );
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
    return new AggregationView(request.getSubjectResource(),
        groupBy,
        decomposeSimple(aggFields),
        aggFunctions);
  }


  @Value
  public static class PathWithAlias {

    @Nonnull
    FhirPath<Collection> path;

    @Nonnull
    Optional<String> alias;

    @Nonnull
    public FhirPath<Collection> head() {
      return path.first();
    }

    @Nonnull
    public PathWithAlias tail() {
      return new PathWithAlias(path.suffix(), alias);
    }
  }

  @Nonnull
  public static Optional<Selection> decomposeFilter(
      @Nonnull final List<FhirPath<Collection>> paths) {
    // TODO: this should check/enforce the booleannes of the paths results
    return paths.isEmpty()
           ? Optional.empty()
           : Optional.of(new FromSelection(new ExtConsFhir("%resource"),
               paths.stream().map(PrimitiveSelection::new)
                   .collect(Collectors.toUnmodifiableList())));
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
    return decomposeAliased(paths.stream().map(p -> new PathWithAlias(p, Optional.empty()))
        .collect(Collectors.toUnmodifiableList()));
  }

  @Nonnull
  public static Selection decomposeAliased(@Nonnull final List<PathWithAlias> paths) {
    return new FromSelection(new ExtConsFhir("%resource"), decomposeInternal(paths));
  }

  static boolean isTraversal(@Nonnull final PathWithAlias pathWithAlias) {
    final FhirPath<Collection> path = pathWithAlias.getPath();
    return path instanceof Paths.Traversal || path.isNull();
  }

  static Stream<? extends Selection> decomposeSelection(@Nonnull final FhirPath<Collection> parent,
      @Nonnull final List<PathWithAlias> children) {

    // TODO: do not create empty selections.

    // HMM: with the primitive selection

    return parent.isNull()
           ? Stream.of(new PrimitiveSelection(parent, children.get(0).getAlias()))
           : Stream.of(
               new ForEachOrNullSelection(parent, decomposeInternal(
                   children.stream().filter(QueryParser::isTraversal).collect(
                       Collectors.toUnmodifiableList()))),
               new FromSelection(parent, decomposeInternal(
                   children.stream().filter(c -> !isTraversal(c)).collect(
                       Collectors.toUnmodifiableList())))
           ).filter(s -> !s.getComponents().isEmpty());
  }

  static List<Selection> decomposeInternal(@Nonnull final List<PathWithAlias> paths) {
    final Map<FhirPath<Collection>, List<PathWithAlias>> tailsByHeads = paths.stream()
        .collect(
            Collectors.groupingBy(PathWithAlias::head, LinkedHashMap::new,
                Collectors.mapping(
                    PathWithAlias::tail,
                    Collectors.toList())));
    return tailsByHeads.entrySet().stream()
        .flatMap(e -> decomposeSelection(e.getKey(), e.getValue()))
        .collect(Collectors.toUnmodifiableList());
  }
}
