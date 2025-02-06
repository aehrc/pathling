package au.csiro.pathling.test.yaml;

import static org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType.INTEGER;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.fhirpath.definition.ChildDefinition;
import au.csiro.pathling.fhirpath.definition.def.DefCompositeDefinition;
import au.csiro.pathling.fhirpath.definition.def.DefPrimitiveDefinition;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class YamlSpecTest {


  @Test
  void testDefinitionOfListOfMaps() {

    final List<Object> testData = List.of(
        Map.of("a", 3),
        Map.of("a", 1, "b", 2)
    );

    final ChildDefinition defintion = YamlSupport.elementFromValues(
        "test", testData);

    assertEquals(
        DefCompositeDefinition.of("test",
            List.of(
                DefPrimitiveDefinition.of("a", INTEGER, 1),
                DefPrimitiveDefinition.of("b", INTEGER, 1)
            ), -1)
        , defintion);
  }
}
