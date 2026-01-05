/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.views;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.Collectors;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link FhirView} static factory methods and builder patterns.
 *
 * @author John Grimes
 */
class FhirViewBuilderUnitTest {

  @Test
  void testBuilder() {
    // Test the static builder() method.
    final FhirViewBuilder builder = FhirView.builder();
    assertNotNull(builder);

    final FhirView view =
        builder.resource("Patient").select(FhirView.select(FhirView.column("id", "id"))).build();

    assertEquals("Patient", view.getResource());
  }

  @Test
  void testOfResourceString() {
    // Test ofResource with a String parameter.
    final FhirView view =
        FhirView.ofResource("Observation")
            .select(FhirView.select(FhirView.column("id", "id")))
            .build();

    assertEquals("Observation", view.getResource());
  }

  @Test
  void testOfResourceResourceType() {
    // Test ofResource with a ResourceType parameter.
    final FhirView view =
        FhirView.ofResource(ResourceType.CONDITION)
            .select(FhirView.select(FhirView.column("id", "id")))
            .build();

    assertEquals("Condition", view.getResource());
  }

  @Test
  void testColumnsFactory() {
    // Test the columns() factory method.
    final Column col1 = FhirView.column("name", "name.given");
    final Column col2 = FhirView.column("family", "name.family");
    final SelectClause select = FhirView.columns(col1, col2);

    assertNotNull(select);
    assertEquals(2, select.getColumn().size());
  }

  @Test
  void testColumnFactory() {
    // Test the column() factory method.
    final Column column = FhirView.column("birthDate", "birthDate");

    assertEquals("birthDate", column.getName());
    assertEquals("birthDate", column.getPath());
  }

  @Test
  void testSelectWithColumns() {
    // Test select() with Column varargs.
    final SelectClause select =
        FhirView.select(FhirView.column("id", "id"), FhirView.column("name", "name.text"));

    assertNotNull(select);
    assertEquals(2, select.getColumn().size());
  }

  @Test
  void testSelectWithSelectClauses() {
    // Test select() with SelectClause varargs.
    final SelectClause inner = FhirView.select(FhirView.column("value", "value"));
    final SelectClause outer = FhirView.select(inner);

    assertNotNull(outer);
    assertEquals(1, outer.getSelect().size());
  }

  @Test
  void testForEachWithColumns() {
    // Test forEach() with columns.
    final SelectClause select =
        FhirView.forEach(
            "name", FhirView.column("given", "given"), FhirView.column("family", "family"));

    assertNotNull(select);
    assertEquals("name", select.getForEach());
    assertEquals(2, select.getColumn().size());
  }

  @Test
  void testForEachWithSelectClauses() {
    // Test forEach() with nested SelectClauses.
    final SelectClause inner = FhirView.select(FhirView.column("text", "text"));
    final SelectClause select = FhirView.forEach("name", inner);

    assertNotNull(select);
    assertEquals("name", select.getForEach());
    assertEquals(1, select.getSelect().size());
  }

  @Test
  void testForEachOrNullWithColumns() {
    // Test forEachOrNull() with columns.
    final SelectClause select =
        FhirView.forEachOrNull(
            "identifier", FhirView.column("system", "system"), FhirView.column("value", "value"));

    assertNotNull(select);
    assertEquals("identifier", select.getForEachOrNull());
    assertEquals(2, select.getColumn().size());
  }

  @Test
  void testForEachOrNullWithSelectClauses() {
    // Test forEachOrNull() with nested SelectClauses.
    final SelectClause inner = FhirView.select(FhirView.column("use", "use"));
    final SelectClause select = FhirView.forEachOrNull("telecom", inner);

    assertNotNull(select);
    assertEquals("telecom", select.getForEachOrNull());
    assertEquals(1, select.getSelect().size());
  }

  @Test
  void testRepeatWithColumns() {
    // Test repeat() with columns.
    final SelectClause select =
        FhirView.repeat(
            List.of("property.part", "component.property.part"),
            FhirView.column("code", "code"),
            FhirView.column("value", "value"));

    assertNotNull(select);
    assertNotNull(select.getRepeat());
    assertEquals(2, select.getRepeat().size());
    assertEquals(2, select.getColumn().size());
  }

  @Test
  void testRepeatWithSelectClauses() {
    // Test repeat() with nested SelectClauses.
    final SelectClause inner = FhirView.select(FhirView.column("code", "code"));
    final SelectClause select = FhirView.repeat(List.of("link.other"), inner);

    assertNotNull(select);
    assertNotNull(select.getRepeat());
    assertEquals(1, select.getRepeat().size());
    assertEquals(1, select.getSelect().size());
  }

  @Test
  void testUnionAll() {
    // Test unionAll() factory method.
    final SelectClause select1 = FhirView.select(FhirView.column("id", "id"));
    final SelectClause select2 = FhirView.select(FhirView.column("id", "id"));
    final SelectClause union = FhirView.unionAll(select1, select2);

    assertNotNull(union);
    assertNotNull(union.getUnionAll());
    assertEquals(2, union.getUnionAll().size());
  }

  @Test
  void testGetAllColumns() {
    // Test getAllColumns() method.
    final FhirView view =
        FhirView.ofResource("Patient")
            .name("patient_view")
            .select(
                FhirView.forEach(
                    "name",
                    FhirView.column("given", "given.first()"),
                    FhirView.column("family", "family")))
            .select(FhirView.select(FhirView.column("id", "id")))
            .build();

    final List<Column> allColumns = view.getAllColumns().collect(Collectors.toList());
    assertEquals(3, allColumns.size());

    // Verify column names.
    final List<String> columnNames =
        allColumns.stream().map(Column::getName).collect(Collectors.toList());
    assertTrue(columnNames.contains("given"));
    assertTrue(columnNames.contains("family"));
    assertTrue(columnNames.contains("id"));
  }

  // Additional tests for FhirViewBuilder coverage

  @Test
  void testBuilderConstantWithDeclaration() {
    // Test constant(ConstantDeclaration...) method.
    final ConstantDeclaration constant =
        new ConstantDeclaration("myConst", new org.hl7.fhir.r4.model.StringType("value"));
    final FhirView view =
        FhirView.builder()
            .resource("Patient")
            .constant(constant)
            .select(FhirView.select(FhirView.column("id", "id")))
            .build();

    assertEquals(1, view.getConstant().size());
    assertEquals("myConst", view.getConstant().get(0).getName());
  }

  @Test
  void testBuilderConstantWithNameAndValue() {
    // Test constant(String, IBase) method.
    final FhirView view =
        FhirView.builder()
            .resource("Patient")
            .constant("maxAge", new org.hl7.fhir.r4.model.IntegerType(100))
            .select(FhirView.select(FhirView.column("id", "id")))
            .build();

    assertEquals(1, view.getConstant().size());
    assertEquals("maxAge", view.getConstant().get(0).getName());
  }

  @Test
  void testBuilderWhereWithClause() {
    // Test where(WhereClause...) method.
    final WhereClause whereClause = new WhereClause("active = true", null);
    final FhirView view =
        FhirView.builder()
            .resource("Patient")
            .select(FhirView.select(FhirView.column("id", "id")))
            .where(whereClause)
            .build();

    assertNotNull(view.getWhere());
    assertEquals(1, view.getWhere().size());
  }

  @Test
  void testBuilderWhereWithStrings() {
    // Test where(String...) method by calling it with multiple where() calls.
    final FhirView view =
        FhirView.builder()
            .resource("Patient")
            .select(FhirView.select(FhirView.column("id", "id")))
            .where("active = true")
            .where("deceased = false")
            .build();

    assertNotNull(view.getWhere());
    assertEquals(2, view.getWhere().size());
  }

  @Test
  void testBuilderWhereWithConditionAndDescription() {
    // Test where(String, String) method.
    final FhirView view =
        FhirView.builder()
            .resource("Patient")
            .select(FhirView.select(FhirView.column("id", "id")))
            .where("active = true", "Only include active patients")
            .build();

    assertNotNull(view.getWhere());
    assertEquals(1, view.getWhere().size());
    assertEquals("Only include active patients", view.getWhere().get(0).getDescription());
  }

  @Test
  void testBuilderWithoutResourceThrows() {
    // Test that build() throws when resource is not set.
    final FhirViewBuilder builder =
        FhirView.builder().select(FhirView.select(FhirView.column("id", "id")));

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void testBuilderName() {
    // Test the name() method.
    final FhirView view =
        FhirView.builder()
            .name("my_patient_view")
            .resource("Patient")
            .select(FhirView.select(FhirView.column("id", "id")))
            .build();

    assertEquals("my_patient_view", view.getName());
  }
}
