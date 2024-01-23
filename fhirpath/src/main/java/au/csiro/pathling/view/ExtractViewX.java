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
import static au.csiro.pathling.utilities.Strings.randomAlias;

import au.csiro.pathling.extract.ExtractResultType;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
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
  SelectionX selection;
  @Nonnull
  Optional<SelectionX> where;
  @Nonnull
  ExtractResultType resultType;

  public ExtractViewX(@Nonnull final ResourceType subjectResource,
      @Nonnull final SelectionX selection, final Optional<SelectionX> where) {
    this(subjectResource, selection, where, ExtractResultType.UNCONSTRAINED);
  }

  public Dataset<Row> evaluate(@Nonnull final ExecutionContext context) {
    final DefaultProjectionContext projectionContext = DefaultProjectionContext.of(context,
        subjectResource);

    final SelectionResult selectionResult = selection.evaluate(projectionContext);
    final String resultAlias = randomAlias();
    final Dataset<Row> compactedResult =
        evalFilter(projectionContext).map(
                filterCol -> projectionContext.getDataset().filter(filterCol))
            .orElse(projectionContext.getDataset())
            .select(selectionResult.getValue().alias(resultAlias));
    
    // NOTE: this could technically be `inline(selectionResult.getValue())` but that 
    // that seem not to use code generation and fallback to eval() on expressions. 
    // This can in theory be slower, and for the time being eval() is not implemented on `struct_prod`
    // Another option would be `inline_outer(compatedResultColumn)` (and not filter) but this seems to 
    // product a plan where `selectionResult.getValue()` is evaluated twice 
    // once for the `inline()` and an additional time for a filter() generate by Catalyst.
    
     final Column compatedResultColumn = compactedResult.col(resultAlias);
     final Dataset<Row> explodedResult = compactedResult
        .filter(functions.size(compatedResultColumn).gt(0))
        .select(functions.inline_outer(compatedResultColumn));
     
    final Dataset<Row> result = explodedResult.select(selectionResult.getCollections().stream()
        .map(this::toColumn).toArray(Column[]::new));

    result.explain(true);
    return result;

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
                                ? finalResult.getColumnCtx().getValue()
                                : finalResult.asSingular().getColumnCtx().getValue();
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
          .map(cr -> cr.getCollection().asSingular().getColumnCtx().getValue())
          .reduce(Column::and);
    });
  }
}
