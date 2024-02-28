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

package au.csiro.pathling.view;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.path.Paths;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

@Value
public class AggregationView {

  public static final Map<String, Function<Column, Column>> AGG_FUNCTIONS = ImmutableMap.of(
      "count", functions::count,
      "sum", functions::sum,
      "avg", functions::avg,
      "min", functions::min,
      "max", functions::max
  );

  ResourceType subjectResource;
  Selection groupBy;
  Selection select;
  List<FhirPath> aggregates;

  public Dataset<Row> evaluate(@Nonnull final ExecutionContext context) {
    final ProjectionContext projectionContext = ProjectionContext.of(context,
        subjectResource);

    final DatasetResult<Column> groupByResult = groupBy.evaluate(projectionContext)
        .map(cr -> cr.getCollection()
            .getColumn().getValue());
    final DatasetResult<Column> selectResult = select.evaluate(projectionContext)
        .map(cr -> cr.getCollection()
            .getColumn().getValue());

    final Column[] groupingColumns = groupByResult.asStream().toArray(Column[]::new);
    final Column[] selectColumns = selectResult.asStream().toArray(Column[]::new);

    // TODO: This probably can be better implemented as customized extractions on 
    // on the leaves of aggregation tree
    final Column[] aggColumns = IntStream.range(0, selectColumns.length)
        .mapToObj(
            i -> AGG_FUNCTIONS.get(((Paths.EvalFunction) aggregates.get(i)).getFunctionIdentifier())
                .apply(selectColumns[i]))
        .toArray(Column[]::new);

    return groupByResult.asTransform().andThen(selectResult)
        .applyTransform(projectionContext.getDataset())
        .groupBy(groupingColumns)
        .agg(aggColumns[0], Stream.of(aggColumns).skip(1).toArray(Column[]::new));
  }

  public void printTree() {
    System.out.println("groupBy:");
    groupBy.toTreeString()
        .forEach(s -> System.out.println("  " + s));
    System.out.println("select:");
    select.toTreeString()
        .forEach(s -> System.out.println("  " + s));
    System.out.println("aggregates:");
    aggregates.forEach(s -> System.out.println("  " + s));
  }
}
