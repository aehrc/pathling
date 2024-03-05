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

import static au.csiro.pathling.extract.ExtractResultType.FLAT;
import static au.csiro.pathling.utilities.Functions.maybeCast;

import au.csiro.pathling.extract.ExtractResultType;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.views.ConstantDeclaration;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

@Value
@AllArgsConstructor
public class ExtractViewX {

  @Nonnull
  ResourceType subjectResource;
  @Nonnull
  List<ConstantDeclaration> constants;
  @Nonnull
  SelectionX selection;
  @Nonnull
  Optional<SelectionX> where;
  @Nonnull
  ExtractResultType resultType;

  public ExtractViewX(@Nonnull final ResourceType subjectResource,
      @Nonnull final List<ConstantDeclaration> constants, @Nonnull final SelectionX selection,
      final Optional<SelectionX> where) {
    this(subjectResource, constants, selection, where, ExtractResultType.UNCONSTRAINED);
  }

  public Dataset<Row> evaluate(@Nonnull final ExecutionContext context) {
    final ProjectionContext projectionContext = ProjectionContext.of(context,
        subjectResource, constants);

    final SelectionResult selectionResult = selection.evaluate(projectionContext);
    final Dataset<Row> filteredResult =
        evalFilter(projectionContext).map(
                filterCol -> projectionContext.getDataset().filter(filterCol))
            .orElse(projectionContext.getDataset());

    final Dataset<Row> explodedResult = filteredResult
        .select(functions.inline(selectionResult.getValue()));

    return explodedResult.select(selectionResult.getCollections().stream()
        .map(this::toColumn).toArray(Column[]::new));
  }

  public void printTree() {
    // System.out.println("select:");
    // selection.toTreeString()
    //     .forEach(s -> System.out.println("  " + s));
    // where.ifPresent(w -> {
    //   System.out.println("where:");
    //   w.toTreeString()
    //       .forEach(s -> System.out.println("  " + s));
    // });
  }


  @Nonnull
  private Column toColumn(@Nonnull final CollectionResult result) {
    final Collection collection = result.getCollection();
    final PrimitiveSelection info = result.getSelection();

    final Collection finalResult = FLAT.equals(resultType)
                                   ? Optional.of(collection)
                                       .flatMap(maybeCast(StringCoercible.class))
                                       .map(StringCoercible::asStringPath).orElseThrow()
                                   : collection;

    final Column columnResult = info.isAsCollection()
                                ? finalResult.getColumn().getValue()
                                : finalResult.asSingular().getColumn().getValue();
    return info.getAlias().map(columnResult::alias).orElse(columnResult);
  }


  @Nonnull
  private Optional<Column> evalFilter(@Nonnull final ProjectionContext context) {
    return where.flatMap(whereSelection -> {
      final List<CollectionResult> whereResult = whereSelection.evaluateFlat(context);
      final boolean isValidFilter = whereResult.stream()
          .allMatch(cr -> cr.getCollection() instanceof BooleanCollection);
      if (!isValidFilter) {
        throw new IllegalArgumentException("Filter must be a boolean expression");
      }
      return whereResult.stream()
          .map(cr -> cr.getCollection().asSingular().getColumn().getValue())
          .reduce(Column::and);
    });
  }
}
