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
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.view.DatasetResult.One;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Value;

import static au.csiro.pathling.utilities.Strings.randomAlias;

@Value
@AllArgsConstructor
public class PrimitiveSelection implements Selection {

  @Nonnull
  FhirPath path;

  @Nonnull
  Optional<String> alias;

  boolean asCollection;

  @Nonnull
  String tag = randomAlias();

  public PrimitiveSelection(@Nonnull final FhirPath path) {
    this(path, Optional.empty());
  }

  public PrimitiveSelection(@Nonnull final FhirPath path,
      @Nonnull final Optional<String> alias) {
    this(path, alias, false);

  }

  @Nonnull
  public String getTag() {
    return alias.orElse("_" + tag);
  }
  
  @Override
  public DatasetResult<CollectionResult> evaluate(@Nonnull final ProjectionContext context) {
    final One<Collection> resultCollection = context.evalExpression(path);
    return resultCollection.map(collection -> new CollectionResult(collection, this));
  }


  public CollectionResult evaluateCollection(@Nonnull final ProjectionContext context) {
    final One<Collection> resultCollection = context.evalExpression(path);
    return new CollectionResult(resultCollection.getPureValue(), this);
  }
  
  @Override
  public Stream<String> toTreeString() {
    return Stream.of("select: " + path + " as " + alias + (asCollection
                                                           ? " (as collection)"
                                                           : ""));
  }

  @Nonnull
  @Override
  public Selection map(@Nonnull final Function<Selection, Selection> mapFunction) {
    return mapFunction.apply(this);
  }
}
