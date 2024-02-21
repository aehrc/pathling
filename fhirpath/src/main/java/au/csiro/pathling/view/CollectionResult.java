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

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.ArrayOrSingularRepresentation;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

@Value
public class CollectionResult {

  @Nonnull
  Collection collection;

  @Nonnull
  PrimitiveSelection selection;

  @Nonnull
  public Column getTaggedColumn() {
    return collection.getColumnValue().alias(selection.getTag());
  }

  @Nonnull
  public CollectionResult toTagReference() {
    return new CollectionResult(
        collection.copyWith(ArrayOrSingularRepresentation.of(functions.col(selection.getTag()))),
        selection);

  }

}
