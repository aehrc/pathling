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

package au.csiro.pathling.definition.fhir;

import au.csiro.pathling.definition.ChoiceDefinition;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * The order of the variants of an open choice, such as {@code Extension.value[x]}, which can take
 * any of the types FHIR lists as open.
 *
 * <p>The definition library declares no types for an open choice. It fills the choice from a hash
 * map keyed by type class, and the comparator it then sorts with orders nothing but resources of an
 * earlier FHIR version, so the order it reports is that of identity hashes, which differ between
 * runs of one build. The order is therefore taken from the specification instead: the types in the
 * order the R4 definition of {@code Extension.value[x]} lists them, which is the order every open
 * choice in R4 lists them in, followed by any other type the definition library accepts, by name.
 */
final class OpenTypeOrder {

  /**
   * The open types, in the order the R4 definition of {@code Extension.value[x]} lists them ({@code
   * StructureDefinition-Extension.json} in the {@code hl7.fhir.r4.core} 4.0.1 package).
   */
  @Nonnull
  private static final List<String> R4_OPEN_TYPES =
      List.of(
          "base64Binary",
          "boolean",
          "canonical",
          "code",
          "date",
          "dateTime",
          "decimal",
          "id",
          "instant",
          "integer",
          "markdown",
          "oid",
          "positiveInt",
          "string",
          "time",
          "unsignedInt",
          "uri",
          "url",
          "uuid",
          "Address",
          "Age",
          "Annotation",
          "Attachment",
          "CodeableConcept",
          "Coding",
          "ContactPoint",
          "Count",
          "Distance",
          "Duration",
          "HumanName",
          "Identifier",
          "Money",
          "Period",
          "Quantity",
          "Range",
          "Ratio",
          "Reference",
          "SampledData",
          "Signature",
          "Timing",
          "ContactDetail",
          "Contributor",
          "DataRequirement",
          "Expression",
          "ParameterDefinition",
          "RelatedArtifact",
          "TriggerDefinition",
          "UsageContext",
          "Dosage",
          "Meta");

  private OpenTypeOrder() {}

  /**
   * Orders the names of the variants of an open choice: those of the open types first, in the order
   * the specification lists them, then the rest by name. A listed type the choice does not carry is
   * left out, so the result holds exactly the names given.
   *
   * @param choiceName the unqualified name of the choice, such as {@code value}
   * @param names the names of the variants the choice carries
   * @return the same names, in canonical order
   */
  @Nonnull
  static List<String> order(@Nonnull final String choiceName, @Nonnull final List<String> names) {
    final Set<String> present = Set.copyOf(names);
    final List<String> listed =
        R4_OPEN_TYPES.stream()
            .map(type -> ChoiceDefinition.columnName(choiceName, type))
            .filter(present::contains)
            .toList();
    final Set<String> ordered = Set.copyOf(listed);
    return Stream.concat(
            listed.stream(), names.stream().filter(name -> !ordered.contains(name)).sorted())
        .toList();
  }
}
