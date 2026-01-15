/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2026 Commonwealth Scientific
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

package au.csiro.pathling.encoders.datatypes;

import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.schema.ElementCtx;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeChildChoiceDefinition;
import ca.uhn.fhir.context.RuntimeChildExtension;
import ca.uhn.fhir.context.RuntimeCompositeDatatypeDefinition;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import scala.jdk.javaapi.CollectionConverters;

class R4DataTypeMappingsTest {

  private static final FhirContext fhirContext = FhirContext.forR4();

  // As defined in: https://www.hl7.org/fhir/datatypes.html#open
  private static final List<String> ALLOWED_R4_OPEN_ELEMENT_TYPES =
      Arrays.asList(
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

  private final DataTypeMappings dataTypeMappings = new R4DataTypeMappings();

  @Test
  void testCorrectChoicesOnOpenElementType() {
    final ElementCtx<Object, Object> ex = ElementCtx.forExtension(fhirContext);
    final RuntimeChildExtension extension = (RuntimeChildExtension) ex.childDefinition();
    final RuntimeCompositeDatatypeDefinition extensionChild =
        (RuntimeCompositeDatatypeDefinition) extension.getChildByName("extension");

    // This is how we can access the 'value' child od RuntimeChildExtension.
    final RuntimeChildChoiceDefinition openTypeElement =
        (RuntimeChildChoiceDefinition) extensionChild.getChildByName("value[x]");

    final Set<String> actualOpenElementUniqueChildNames =
        CollectionConverters.asJava(dataTypeMappings.getValidChoiceTypes(openTypeElement)).stream()
            .map(openTypeElement::getChildNameByDatatype)
            .collect(Collectors.toUnmodifiableSet());

    final Set<String> expectedOpenElementTypes =
        ALLOWED_R4_OPEN_ELEMENT_TYPES.stream()
            .map(typeName -> "value" + StringUtils.capitalize(typeName))
            .collect(Collectors.toUnmodifiableSet());

    assertEquals(expectedOpenElementTypes, actualOpenElementUniqueChildNames);
  }
}
