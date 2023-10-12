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

import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

@Value
@AllArgsConstructor
public class ExtractView {

  ResourceType subjectResource;
  Selection selection;
  Optional<Selection> where;

  public ExtractView(@Nonnull final ResourceType subjectResource,
      @Nonnull final Selection selection) {
    this(subjectResource, selection, Optional.empty());
  }

  public Dataset<Row> evaluate(@Nonnull final ViewContext context) {
    final DefaultProjectionContext projectionContext = DefaultProjectionContext.of(context,
        subjectResource);
    final DatasetView selectionResult = selection.evaluate(projectionContext);
    return where.map(projectionContext::evaluate)
        .map(DatasetView::toFilter)
        .map(selectionResult::andThen)
        .orElse(selectionResult)
        .select(projectionContext.getDataset());
  }

  public void printTree() {
    System.out.println("select:");
    selection.toTreeString()
        .forEach(s -> System.out.println("  " + s));
    where.ifPresent(w -> {
      System.out.println("where:");
      w.toTreeString()
          .forEach(s -> System.out.println("  " + s));
    });
  }
}
