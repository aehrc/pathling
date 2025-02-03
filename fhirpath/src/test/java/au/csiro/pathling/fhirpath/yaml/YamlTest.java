package au.csiro.pathling.fhirpath.yaml;

import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.definition.DefinitionContext;
import au.csiro.pathling.fhirpath.definition.ResourceDefinition;
import au.csiro.pathling.fhirpath.definition.def.DefCompositeDefinition;
import au.csiro.pathling.fhirpath.definition.def.DefDefinitionContext;
import au.csiro.pathling.fhirpath.definition.def.DefPrimitiveDefinition;
import au.csiro.pathling.fhirpath.definition.def.DefResourceDefinition;
import au.csiro.pathling.fhirpath.definition.def.DefResourceTag;
import au.csiro.pathling.fhirpath.execution.DefResourceResolver;
import au.csiro.pathling.fhirpath.execution.FhirpathEvaluator;
import au.csiro.pathling.fhirpath.execution.StdFhirpathEvaluator;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import au.csiro.pathling.fhirpath.parser.Parser;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Test;

public class YamlTest {

  @Test
  public void test() {

    ResourceDefinition rs = DefResourceDefinition.of(
        DefResourceTag.of("Test"),
        DefPrimitiveDefinition.single("name", FHIRDefinedType.STRING),
        DefPrimitiveDefinition.single("age", FHIRDefinedType.INTEGER),
        DefCompositeDefinition.of("address", List.of(
            DefPrimitiveDefinition.single("street", FHIRDefinedType.STRING),
            DefPrimitiveDefinition.single("city", FHIRDefinedType.STRING),
            DefPrimitiveDefinition.single("zip", FHIRDefinedType.STRING)
        ), 1)
    );

    assertEquals(
        Optional.of(DefPrimitiveDefinition.single("name", FHIRDefinedType.STRING)),
        rs.getChildElement("name")
    );

    final DefinitionContext ctx = DefDefinitionContext.of(rs);
    assertEquals(rs, ctx.findResourceDefinition("Test"));

    final FhirpathEvaluator evaluator = new StdFhirpathEvaluator(
        DefResourceResolver.of(
            DefResourceTag.of("Test"),
            ctx
        ),
        StaticFunctionRegistry.getInstance(),
        Map.of()
    );

    Parser parser = new Parser();
    final Collection result = evaluator.evaluate(
        parser.parse("address.city.count() + age"));

     String x = 
        """
        name: John
        age: 30
        address:
          street: 123 Main St
          city: Springfield
          zip: 12345
        """;
     
     // parser.parse(x);
     
    System.out.println(result);
  }

}
