package au.csiro.pathling.extract;

import static au.csiro.pathling.extract.Tree.node;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.parser.Parser;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ImplicitUnnesterTest {


  final Parser parser = new Parser();
  final ImplicitUnnester unnester = new ImplicitUnnester();

  @Test
  void testUnrelatedSimpleColumns() {
    final List<String> columns = List.of(
        "id",
        "gender"
    );
    final Tree<FhirPath> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            node("id"),
            node("gender")
        ),
        projection
    );
  }


  @Test
  void testNestedSimpleColumns() {
    final List<String> columns = List.of(
        "id",
        "name",
        "name.given",
        "name.family",
        "maritalStatus.coding.system",
        "maritalStatus.coding.code"
    );
    final Tree<FhirPath> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            node("id"),
            node("name",
                node("$this"),
                node("given"),
                node("family")
            ),
            node("maritalStatus.coding",
                node("system"),
                node("code")
            )
        ),
        projection
    );
  }

  @Test
  void testNestedSimpleAndAggregateColumns() {
    final List<String> columns = List.of(
        "id",
        "name.given",
        "name.family",
        "name.count()",
        "name.exists()"
    );
    final Tree<FhirPath> projection = toProjection(columns);
    // this should be 
    // id
    // name
    //   given
    //   family
    // name.count()
    // name.exists()
    assertTreeEquals(
        node("%resource",
            node("id"),
            node("name",
                node("given"),
                node("family")
            ),
            node("name.count()"),
            node("name.exists()")
        ),
        projection
    );
  }


  @Test
  void testNonBranchedPathsShouldNotBeUnnested() {
    final List<String> columns = List.of(
        "id",
        "name.given.count()"
    );
    final Tree<FhirPath> projection = toProjection(columns);
    // this should be 
    // id
    // name
    //   given
    //   family
    // name.count()
    // name.exists()
    assertTreeEquals(
        node("%resource",
            node("id"),
            node("name.given.count()")
        ),
        projection
    );
  }

  @Test
  void testMixedNestedAggAndValue() {
    final List<String> columns = List.of(
        "id",
        "name.family",
        "name.given.count()"
    );
    final Tree<FhirPath> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            node("id"),
            node("name",
                node("family"),
                node("given.count()")
            )
        ),
        projection
    );
  }


  @Test
  void testNestedSimpleAndNestedAggregateColumns() {
    final List<String> columns = List.of(
        "id",
        "name.given",
        "name.family",
        "name.count()",
        "name.count().count()",
        "name.count().exists()",
        "name.count().exists().first()"
    );
    final Tree<FhirPath> projection = toProjection(columns);
    // this should be 
    // id
    // name.count()
    //   $this
    //   count()
    //   exists()
    // name
    //   given
    //   family
  }

  @Test
  void testAggFolloweByUnesiting() {
    final List<String> columns = List.of(
        "id",
        "name.given",
        "name.first()",
        "name.first().given",
        "name.first().family",
        "name.first().count()",
        "name.exists()"
    );
    final Tree<FhirPath> projection = toProjection(columns);
    // this should be 
    // id
    // name.count()
    //   $this
    //   count()
    //   exists()
    // name
    //   given
    //   family
  }

  @Nonnull
  private Tree<FhirPath> toProjection(@Nonnull final List<String> columns) {
    final Tree<FhirPath> result = unnester.unnestPaths(
        columns.stream().map(parser::parse).toList());

    System.out.println(result.toTreeString(FhirPath::toExpression));
    return result;
  }

  static void assertTreeEquals(Tree<String> expected, Tree<FhirPath> actual) {
    assertEquals(expected.toTreeString(), actual.toTreeString(FhirPath::toExpression));
  }
}
