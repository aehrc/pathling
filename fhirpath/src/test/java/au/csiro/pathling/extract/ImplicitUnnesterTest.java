package au.csiro.pathling.extract;

import static au.csiro.pathling.extract.Tree.node;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.extract.ImplicitUnnester.FhirPathWithTag;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.parser.Parser;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ImplicitUnnesterTest {


  final Parser parser = new Parser();
  final ImplicitUnnester unnester = new ImplicitUnnester();


  static Tree<String> leafWithThis(String value) {
    return node(value, node("$this"));
  }

  @Test
  void testEmptyList() {
    final List<String> columns = List.of();
    final Tree<FhirPathWithTag> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource"),
        projection
    );
  }


  @Test
  void testSingleThis() {
    final List<String> columns = List.of(
        "$this"
    );
    final Tree<FhirPathWithTag> projection = toProjection(columns);
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
        "$this",
        "$this"
    );
    final Tree<FhirPathWithTag> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            node("$this"),
            node("$this")
        ),
        projection
    );
  }

  @Test
  void testSingleProperty() {
    final List<String> columns = List.of(
        "name"
    );
    final Tree<FhirPathWithTag> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            leafWithThis("name")
        ),
        projection
    );
  }


  @Test
  void testManySameProperties() {
    final List<String> columns = List.of(
        "name",
        "name"
    );
    final Tree<FhirPathWithTag> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            node("name",
                node("$this"),
                node("$this")
            )
        ),
        projection
    );
  }


  @Test
  void testSingleTraversalPath() {
    final List<String> columns = List.of(
        "name.family"
    );
    final Tree<FhirPathWithTag> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            leafWithThis("name.family")
        ),
        projection
    );
  }


  @Test
  void testManySameTraversalPaths() {
    final List<String> columns = List.of(
        "name.family",
        "name.family"
    );
    final Tree<FhirPathWithTag> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            node("name.family",
                node("$this"),
                node("$this")
            )
        ),
        projection
    );
  }

  @Test
  void testSingleAgg() {
    final List<String> columns = List.of(
        "count()"
    );
    final Tree<FhirPathWithTag> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            leafWithThis("count()")
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
    final Tree<FhirPathWithTag> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            node("count()",
                node("$this"),
                node("$this")
            )
        ),
        projection
    );
  }


  @Test
  void testCommonAggregateWithProperty() {
    final List<String> columns = List.of(
        "first().id",
        "first().gender()"
    );
    final Tree<FhirPathWithTag> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            node("first()",
                leafWithThis("id"),
                leafWithThis("gender()")
            )
        ),
        projection
    );
  }

  @Test
  void testCommonPropertyWithAggregate() {
    final List<String> columns = List.of(
        "name.count()",
        "name.exists()"
    );
    final Tree<FhirPathWithTag> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            leafWithThis("name.count()"),
            leafWithThis("name.exists()")

        ),
        projection
    );
  }


  @Test
  void testCommonAggregateWithAggregate() {
    final List<String> columns = List.of(
        "first().count()",
        "first().exists()"
    );
    final Tree<FhirPathWithTag> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            leafWithThis("first().count()"),
            leafWithThis("first().exists()")
        ),
        projection
    );
  }


  @Test
  void testSingleOperator() {
    final List<String> columns = List.of(
        "$this = 'John'"
    );
    final Tree<FhirPathWithTag> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            leafWithThis("$this = 'John'")
        ),
        projection
    );
  }


  @Test
  void testManySameOperators() {
    final List<String> columns = List.of(
        "$this = 'John'",
        "$this = 'John'"
    );
    final Tree<FhirPathWithTag> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            node("$this = 'John'",
                node("$this"),
                node("$this")
            )
        ),
        projection
    );
  }

  @Test
  void testThisWithThisOperator() {
    final List<String> columns = List.of(
        "$this",
        "$this = 'John'"
    );
    final Tree<FhirPathWithTag> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            node("$this"),
            leafWithThis("$this = 'John'")
        ),
        projection
    );
  }

  @Test
  void testPropertyWithPropertyOperator() {
    final List<String> columns = List.of(
        "name",
        "name = 'John'"
    );
    final Tree<FhirPathWithTag> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            node("name",
                node("$this"),
                leafWithThis("$this = 'John'")
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
    final Tree<FhirPathWithTag> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            leafWithThis("id"),
            leafWithThis("gender")
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
    final Tree<FhirPathWithTag> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            leafWithThis("id"),
            node("name",
                node("$this"),
                leafWithThis("given"),
                leafWithThis("family")
            ),
            node("maritalStatus.coding",
                leafWithThis("system"),
                leafWithThis("code")
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
    final Tree<FhirPathWithTag> projection = toProjection(columns);
    // this should be 
    // id
    // name
    //   given
    //   family
    // name.count()
    // name.exists()
    assertTreeEquals(
        node("%resource",
            leafWithThis("id"),
            node("name",
                leafWithThis("given"),
                leafWithThis("family")
            ),
            leafWithThis("name.count()"),
            leafWithThis("name.exists()")
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
    final Tree<FhirPathWithTag> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            leafWithThis("id"),
            leafWithThis("name.given.count()")
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
    final Tree<FhirPathWithTag> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            leafWithThis("id"),
            node("name",
                leafWithThis("family"),
                leafWithThis("given.count()")
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
        "name.family.count()",
        "name.family.count().count()",
        "name.family.count().exists()",
        "name.family.count().exists().first()"
    );
    final Tree<FhirPathWithTag> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            leafWithThis("id"),
            node("name",
                leafWithThis("given"),
                leafWithThis("family"),
                // we never unnest aggregates
                leafWithThis("family.count()"),
                leafWithThis("family.count().count()"),
                leafWithThis("family.count().exists()"),
                leafWithThis("family.count().exists().first()")
            )
        ),
        projection
    );
  }

  @Test
  void testUnnestSimpleOperatorWithCommonPrefix() {
    final List<String> columns = List.of(
        "id",
        "name.family",
        "name.given = 'John'"
    );
    final Tree<FhirPathWithTag> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            leafWithThis("id"),
            node("name",
                leafWithThis("family"),
                leafWithThis("given.($this = 'John')")
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
    final Tree<FhirPathWithTag> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            leafWithThis("id"),
            leafWithThis("name.family"),
            leafWithThis("'John'.($this = name.given)")
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
    final Tree<FhirPathWithTag> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            leafWithThis("id"),
            node("name.given",
                node("$this"),
                leafWithThis("$this = 'John'")
            )
        ),
        projection
    );
  }


  @Test
  void testUnnestSimpleOperatorWithFullCommonPrefixAndAggregation() {
    final List<String> columns = List.of(
        "id",
        "name.given.first()",
        "name.given = 'John'"
    );
    final Tree<FhirPathWithTag> projection = toProjection(columns);
    assertTreeEquals(
        node("%resource",
            leafWithThis("id"),
            node("name",
                leafWithThis("given.($this = 'John')"),
                // agg nodes go after the unnested nodes
                leafWithThis("given.first()")
            )
        ),
        projection
    );
  }


  @Nonnull
  private Tree<FhirPathWithTag> toProjection(@Nonnull final List<String> columns) {
    final Tree<FhirPathWithTag> result = unnester.unnestPaths(
        columns.stream()
            .map(parser::parse)
            .map(FhirPathWithTag::of)
            .toList()
    );

    System.out.println(result.toTreeString(FhirPathWithTag::toExpression));
    return result;
  }

  static void assertTreeEquals(Tree<String> expected, Tree<FhirPathWithTag> actual) {
    assertEquals(expected.toTreeString(), actual.toTreeString(FhirPathWithTag::toExpression));
  }
}
