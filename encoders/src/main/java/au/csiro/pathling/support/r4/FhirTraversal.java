/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2025 Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
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
 *
 */

package au.csiro.pathling.support.r4;

import jakarta.annotation.Nonnull;
import java.util.Objects;
import java.util.function.Consumer;
import org.hl7.fhir.r4.model.Base;

/**
 * A utility class for traversing FHIR resources.
 */
public class FhirTraversal {

  private FhirTraversal() {
  }

  private static void processChildrenRecursive(@Nonnull final Base object,
      @Nonnull final Consumer<Base> processor) {
    object.children().stream()
        .flatMap(p -> p.getValues().stream())
        .filter(Objects::nonNull)
        .forEach(child -> processRecursive(child, processor));
  }

  /**
   * Recursively applies the consumer to FHIR object and its non-empty children.
   *
   * @param object a FHIR object
   * @param processor the consumer to apply to all non-empty FHIR elements
   */
  public static void processRecursive(@Nonnull final Base object,
      @Nonnull final Consumer<Base> processor) {
    processor.accept(object);
    processChildrenRecursive(object, processor);
  }
}
