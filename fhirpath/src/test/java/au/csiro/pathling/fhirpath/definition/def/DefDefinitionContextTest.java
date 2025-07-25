package au.csiro.pathling.fhirpath.definition.def;

import au.csiro.pathling.fhirpath.definition.DefinitionContext;
import au.csiro.pathling.fhirpath.definition.ResourceDefinition;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DefDefinitionContextTest {


  @Test
  public void testDefinitions() {

    ResourceDefinition rs = DefResourceDefinition.of(
        DefResourceTag.of("Test"),
        DefPrimitiveDefinition.single("name", FHIRDefinedType.STRING),
        DefPrimitiveDefinition.single("age", FHIRDefinedType.INTEGER),
        DefCompositeDefinition.backbone("address", List.of(
            DefPrimitiveDefinition.single("street", FHIRDefinedType.STRING),
            DefPrimitiveDefinition.single("city", FHIRDefinedType.STRING),
            DefPrimitiveDefinition.single("zip", FHIRDefinedType.STRING)
        ), 1)
    );

    assertEquals(
        Optional.of(DefPrimitiveDefinition.single("age", FHIRDefinedType.INTEGER)),
        rs.getChildElement("age")
    );

    final DefinitionContext ctx = DefDefinitionContext.of(rs);
    assertEquals(rs, ctx.findResourceDefinition("Test"));
  }
}
