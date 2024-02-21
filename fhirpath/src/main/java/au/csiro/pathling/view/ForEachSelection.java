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

import static au.csiro.pathling.utilities.Strings.randomAlias;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.ArrayOrSingularRepresentation;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.NullRepresentation;
import au.csiro.pathling.view.DatasetResult.One;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.hadoop.shaded.com.google.common.collect.Streams;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

@EqualsAndHashCode(callSuper = true)
@Value
public class ForEachSelection extends AbstractCompositeSelection {

  public ForEachSelection(final FhirPath parent, final List<Selection> components) {
    super(parent, components);
  }

  @Override
  public DatasetResult<CollectionResult> evaluate(@Nonnull final ProjectionContext context) {

    final ProjectionContext subContext = context.withInputContext(
        context.evalExpression(path).getPureValue());

    final Collection stubInputContext = subContext.getInputContext()
        .map(__ -> NullRepresentation.getInstance());

    // now we need to run the actual transformation on the column
    final ColumnRepresentation nestedResult = subContext.getInputContext().getColumn()
        .transform(
            c -> {
              // eval 
              final ProjectionContext elementCtx = context.withInputContext(
                  subContext.getInputContext().map(__ -> ArrayOrSingularRepresentation.of(c)));
              return functions.struct(
                  components.stream().flatMap(s -> s.evaluate(elementCtx).asStream()).map(
                      cr -> cr.getCollection().getCtx().getValue()
                          .alias(cr.getSelection().getTag())).toArray(Column[]::new)
              );
            }
        );

    final ProjectionContext stubSubContext = subContext.withInputContext(stubInputContext);
    DatasetResult<CollectionResult> myResult = components.stream()
        .map(s -> s.evaluate(stubSubContext))
        .reduce(DatasetResult.empty(), DatasetResult::andThen);

    // my transform 
    Function<Dataset<Row>, Dataset<Row>> explodeTransform = ds -> {
      final String myNestedAlias = randomAlias();
      return ds
          .withColumn(myNestedAlias,
              nestedResult.vectorize(functions::explode, Function.identity()).getValue())
          .filter(functions.col(myNestedAlias).isNotNull())
          .select(
              Streams.concat(
                  Stream.of(ds.columns()).map(ds::col),
                  myResult.asStream().map(cr -> functions.col(myNestedAlias)
                      .getField(cr.getSelection().getTag()).alias(cr.getSelection().getTag()))
              ).toArray(Column[]::new)
          );
    };

    return DatasetResult.<CollectionResult>fromTransform(explodeTransform).andThen(myResult)
        .map(cr -> new CollectionResult(
            cr.getCollection()
                .copyWith(ArrayOrSingularRepresentation.of(functions.col(cr.getSelection()
                    .getTag()))), cr.getSelection()));
  }

  @Nonnull
  @Override
  protected String getName() {
    return "forEach";
  }

  @Nonnull
  @Override
  protected ForEachSelection copy(@Nonnull final List<Selection> newComponents) {
    return new ForEachSelection(path, newComponents);
  }

  @Nonnull
  @Override
  protected One<ProjectionContext> subContext(@Nonnull final ProjectionContext context,
      @Nonnull final FhirPath parent) {
    return context.subContext(parent, true);
  }
}
