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

package au.csiro.pathling.fhirpath.path;

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import lombok.Value;
import lombok.experimental.UtilityClass;

/** Helper FhirPath classes for the FHIRPath parser. */
@UtilityClass
public class ParserPaths {

  /**
   * Special path used to pass values between visitor in the FHIRPath parser.
   *
   * @param <T> the type of value contained in this path
   */
  public interface ValuePath<T> extends FhirPath {

    /**
     * Gets the value contained in this path.
     *
     * @return the value of this path
     */
    T getValue();

    /** {@inheritDoc} */
    @Override
    default Collection apply(
        @Nonnull final Collection input, @Nonnull final EvaluationContext context) {
      throw new UnsupportedOperationException("ValuePath cannot be evaluated directly");
    }

    @Override
    @Nonnull
    default String toExpression() {
      return getValue().toString();
    }
  }

  /** FHIRPath expression with a type specifier value. */
  @Value
  public static class TypeSpecifierPath implements ValuePath<TypeSpecifier> {

    TypeSpecifier value;
  }

  /** FHIRPath expression with a type namespace value. */
  @Value
  public static class TypeNamespacePath implements ValuePath<String> {

    String value;
  }
}
