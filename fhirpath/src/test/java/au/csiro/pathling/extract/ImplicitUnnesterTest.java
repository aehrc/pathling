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
  void testSingleThis() {
    final List<String> columns = List.of(
        "$this"
    );
    final Tree<FhirPath> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            node("$this")
        ),
        projection
    );
  }
  
  @Test
  void testManyThis() {
    final List<String> columns = List.of(
        "$this"
    );
    final Tree<FhirPath> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            node("$this"),
            node("$this")
        ),
        projection
    );
  }
  
  @Test
  void testSingleAgg() {
    final List<String> columns = List.of(
        "count()"
    );
    final Tree<FhirPath> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            node("count()")
        ),
        projection
    );
  }
  
  @Test
  void testManySameAgg() {
    final List<String> columns = List.of(
        "count()",
        "count()"
    );
    final Tree<FhirPath> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            node("count()"),
            node("count()")
        ),
        projection
    );
  }


  @Test
  void testSingleProperty() {
    final List<String> columns = List.of(
        "id"
    );
    final Tree<FhirPath> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            node("id")
        ),
        projection
    );
  }


  @Test
  void testManySameProperties() {
    final List<String> columns = List.of(
        "id",
        "id"
    );
    final Tree<FhirPath> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            node("id",
                node("$this"),
                node("$this")
            )
        ),
        projection
    );
  }

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

  @Test
  void testUnnestSimpleOperatorWithCommonPrefix() {
    final List<String> columns = List.of(
        "id",
        "name.family",
        "name.given = 'John'"
    );
    final Tree<FhirPath> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            node("id"),
            node("name",
                node("family"),
                node("given.($this = 'John')")
            )
        ),
        projection
    );
  }

  @Test
  void testUnnestSimpleOperatorWithCommonPrefixInRightArgument() {
    final List<String> columns = List.of(
        "id",
        "name.family",
        "'John' = name.given"
    );
    final Tree<FhirPath> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            node("id"),
            node("name.family"),
            node("'John'.($this = name.given)")
        ),
        projection
    );
  }

  @Test
  void testUnnestSimpleOperatorWithFullCommonPrefix() {
    final List<String> columns = List.of(
        "id",
        "name.given",
        "name.given = 'John'"
    );
    final Tree<FhirPath> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            node("id"),
            node("name",
                node("family"),
                node("given.($this = 'John')")
            )
        ),
        projection
    );
  }


  @Test
  void testUnnestSimpleOperatorWithFullCommonPrefixXXX() {
    final List<String> columns = List.of(
        "id",
        "name.given.first()",
        "name.given = 'John'"
    );
    final Tree<FhirPath> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            node("id"),
            node("name",
                node("family"),
                node("given.($this = 'John')")
            )
        ),
        projection
    );
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
