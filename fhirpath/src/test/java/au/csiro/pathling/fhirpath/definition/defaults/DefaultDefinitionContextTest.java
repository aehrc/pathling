/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.definition.defaults;

import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.fhirpath.definition.DefinitionContext;
import au.csiro.pathling.fhirpath.definition.ResourceDefinition;
import java.util.List;
import java.util.Optional;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Test;

class DefaultDefinitionContextTest {


  @Test
  void testDefinitions() {

    final ResourceDefinition rs = DefaultResourceDefinition.of(
        DefaultResourceTag.of("Test"),
        DefaultPrimitiveDefinition.single("name", FHIRDefinedType.STRING),
        DefaultPrimitiveDefinition.single("age", FHIRDefinedType.INTEGER),
        DefaultCompositeDefinition.backbone("address", List.of(
            DefaultPrimitiveDefinition.single("street", FHIRDefinedType.STRING),
            DefaultPrimitiveDefinition.single("city", FHIRDefinedType.STRING),
            DefaultPrimitiveDefinition.single("zip", FHIRDefinedType.STRING)
        ), 1)
    );

    assertEquals(
        Optional.of(DefaultPrimitiveDefinition.single("age", FHIRDefinedType.INTEGER)),
        rs.getChildElement("age")
    );

    final DefinitionContext ctx = DefaultDefinitionContext.of(rs);
    assertEquals(rs, ctx.findResourceDefinition("Test"));
  }
}
