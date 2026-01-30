/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.collection.mixed;

import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.UnsupportedRepresentation;
import au.csiro.pathling.fhirpath.definition.ChoiceDefinition;
import au.csiro.pathling.fhirpath.function.ColumnTransform;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import lombok.Getter;

/**
 * Represents a polymorphic collection, which can be resolved to any one of a number of data types.
 *
 * @author John Grimes
 */
@Getter
public abstract class MixedCollection extends Collection {

  /** The description used for unsupported operation error messages. */
  @Nonnull private final String unsupportedDescription;

  /**
   * Creates a new MixedCollection with an unsupported description.
   *
   * @param unsupportedDescription the description for unsupported operations
   */
  protected MixedCollection(@Nonnull final String unsupportedDescription) {
    super(
        new UnsupportedRepresentation(unsupportedDescription),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
    this.unsupportedDescription = unsupportedDescription;
  }

  /** Creates a new MixedCollection with a default unsupported description. */
  @SuppressWarnings("unused")
  protected MixedCollection() {
    this("mixed collection (do you need to use ofType?)");
  }

  /**
   * Returns a new instance with the specified column and definition.
   *
   * @param parent The parent collection
   * @param definition The definition to use
   * @return A new instance of MixedCollection
   */
  @Nonnull
  public static MixedCollection buildElement(
      @Nonnull final Collection parent, @Nonnull final ChoiceDefinition definition) {
    return new ChoiceElementCollection(definition, parent);
  }

  @Nonnull
  @Override
  public Optional<Collection> traverse(@Nonnull final String elementName) {
    // This should technically result in unchecked traversal, but we don't currently have a way
    // of implementing this.
    // See: https://hl7.org/fhirpath/#paths-and-polymorphic-items
    throw new UnsupportedFhirPathFeatureError(
        "Direct traversal of polymorphic collections is not supported."
            + " Please use 'ofType()' to specify the type of element to traverse.");
  }

  /**
   * {@inheritDoc}
   *
   * <p>Mixed collections cannot be mapped because they have no concrete type to reconstruct. Use
   * {@code ofType()} to resolve the collection to a specific type first.
   *
   * @throws UnsupportedOperationException always, with a message indicating how to resolve the
   *     collection
   */
  @Nonnull
  @Override
  public Collection map(@Nonnull final ColumnTransform mapper) {
    throw new UnsupportedOperationException(
        "Representation of this path is not supported: " + unsupportedDescription);
  }
}
