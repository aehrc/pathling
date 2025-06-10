package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Resource;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;

/**
 * Tests for FHIRPath path traversal related operations.
 */
@Tag("UnitTest")
public class PathsDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testTraversal() {
    return builder()
        .withSubject(sb -> sb
            // Boolean values
            .bool("trueValue", true)
            .bool("falseValue", false)
        )
        .group("basic traversal")
        .testEmpty("unknown",
            "traversal to undefined property returns {}")
        .testTrue("trueValue",
            "correct traversal to a 'true' boolean property")
        .testFalse("falseValue",
            "correct traversal to a 'false' boolean property")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testTraversalOnFhirResource() {

    final Resource observation = new Observation()
        .setId("1");

    return builder()
        .withResource(observation)
        .group("basic traversal of FHIR resource")
        .testEmpty("unknown",
            "traversal to undefined property returns {}")
        .testEquals("1", "id",
            "correct traversal an ID property")
        .build();
  }
}
