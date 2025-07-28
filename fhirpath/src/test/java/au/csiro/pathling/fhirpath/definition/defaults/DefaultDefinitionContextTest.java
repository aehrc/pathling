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
