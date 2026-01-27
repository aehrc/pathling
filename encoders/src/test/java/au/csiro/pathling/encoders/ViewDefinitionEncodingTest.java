/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2026 Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
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
 *
 */

package au.csiro.pathling.encoders;

import static au.csiro.pathling.encoders.SchemaConverterTest.OPEN_TYPES;
import static au.csiro.pathling.test.SchemaAsserts.assertFieldNotPresent;
import static au.csiro.pathling.test.SchemaAsserts.assertFieldPresent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.encoders.ViewDefinitionResource.ColumnComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.ConstantComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.SelectComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.TagComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.WhereComponent;
import au.csiro.pathling.encoders.datatypes.R4DataTypeMappings;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.IntegerType;
import org.json.JSONException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Tests for encoding, decoding, and schema building for the ViewDefinition custom resource.
 *
 * @author John Grimes
 */
class ViewDefinitionEncodingTest {

  private static SparkSession spark;
  private static FhirContext fhirContext;
  private static FhirEncoders fhirEncodersL0;
  private static FhirEncoders fhirEncodersL2;
  private static SchemaConverter schemaConverterL0;
  private static SchemaConverter schemaConverterL2;

  @BeforeAll
  static void setUp() {
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("ViewDefinitionEncodingTest")
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.driver.host", "localhost")
            .config("spark.ui.enabled", "false")
            .getOrCreate();

    // Create a FhirContext and register the custom ViewDefinition resource type.
    fhirContext = FhirContext.forR4();
    fhirContext.registerCustomType(ViewDefinitionResource.class);

    // Create encoders with the custom FhirContext at different nesting levels.
    // Note: Extensions must be enabled for schema matching between SchemaConverter and
    // FhirEncoders.
    fhirEncodersL0 = new FhirEncoders(fhirContext, new R4DataTypeMappings(), 0, OPEN_TYPES, true);
    fhirEncodersL2 = new FhirEncoders(fhirContext, new R4DataTypeMappings(), 2, OPEN_TYPES, true);

    // Create schema converters at different nesting levels.
    schemaConverterL0 =
        new SchemaConverter(
            fhirContext,
            new R4DataTypeMappings(),
            EncoderConfig.apply(0, CollectionConverters.asScala(OPEN_TYPES).toSet(), true));
    schemaConverterL2 =
        new SchemaConverter(
            fhirContext,
            new R4DataTypeMappings(),
            EncoderConfig.apply(2, CollectionConverters.asScala(OPEN_TYPES).toSet(), true));
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  // ========== SCHEMA TESTS ==========

  @Test
  void testSchemaHasBasicFields() {
    final StructType schema = schemaConverterL0.resourceSchema(ViewDefinitionResource.class);

    // Verify top-level fields exist.
    assertTrue(schema.getFieldIndex("id").isDefined());
    assertTrue(schema.getFieldIndex("name").isDefined());
    assertTrue(schema.getFieldIndex("resource").isDefined());
    assertTrue(schema.getFieldIndex("status").isDefined());
    assertTrue(schema.getFieldIndex("select").isDefined());
    assertTrue(schema.getFieldIndex("where").isDefined());
    assertTrue(schema.getFieldIndex("constant").isDefined());
  }

  @Test
  void testSchemaHandlesRecursiveSelectComponent() {
    // Level 0: should NOT have nested select.select or unionAll.
    final StructType schemaL0 = schemaConverterL0.resourceSchema(ViewDefinitionResource.class);
    final DataType selectArrayL0 = schemaL0.fields()[schemaL0.fieldIndex("select")].dataType();
    final StructType selectTypeL0 = (StructType) ((ArrayType) selectArrayL0).elementType();
    assertFieldNotPresent("select", selectTypeL0);
    assertFieldNotPresent("unionAll", selectTypeL0);

    // Level 2: SHOULD have nested select.select and unionAll.
    final StructType schemaL2 = schemaConverterL2.resourceSchema(ViewDefinitionResource.class);
    final DataType selectArrayL2 = schemaL2.fields()[schemaL2.fieldIndex("select")].dataType();
    final StructType selectTypeL2 = (StructType) ((ArrayType) selectArrayL2).elementType();
    assertFieldPresent("select", selectTypeL2);
    assertFieldPresent("unionAll", selectTypeL2);
  }

  @Test
  void testSchemaConverterMatchesEncoderL0() {
    final StructType schemaFromConverter =
        schemaConverterL0.resourceSchema(ViewDefinitionResource.class);
    final ExpressionEncoder<ViewDefinitionResource> encoder =
        fhirEncodersL0.of(ViewDefinitionResource.class);

    assertEquals(schemaFromConverter.treeString(), encoder.schema().treeString());
  }

  @Test
  void testSchemaConverterMatchesEncoderL2() {
    final StructType schemaFromConverter =
        schemaConverterL2.resourceSchema(ViewDefinitionResource.class);
    final ExpressionEncoder<ViewDefinitionResource> encoder =
        fhirEncodersL2.of(ViewDefinitionResource.class);

    assertEquals(schemaFromConverter.treeString(), encoder.schema().treeString());
  }

  @Test
  void testChoiceTypeConstantValue() {
    final StructType schema = schemaConverterL0.resourceSchema(ViewDefinitionResource.class);

    // Navigate to constant array element type.
    final DataType constantArray = schema.fields()[schema.fieldIndex("constant")].dataType();
    final StructType constantType = (StructType) ((ArrayType) constantArray).elementType();

    // Should have valueString and valueBoolean (choice type expansion).
    assertFieldPresent("valueString", constantType);
    assertFieldPresent("valueBoolean", constantType);
  }

  @Test
  void testSelectComponentFields() {
    final StructType schema = schemaConverterL0.resourceSchema(ViewDefinitionResource.class);
    final DataType selectArray = schema.fields()[schema.fieldIndex("select")].dataType();
    final StructType selectType = (StructType) ((ArrayType) selectArray).elementType();

    // Verify SelectComponent fields.
    assertFieldPresent("column", selectType);
    assertFieldPresent("forEach", selectType);
    assertFieldPresent("forEachOrNull", selectType);
  }

  @Test
  void testColumnComponentFields() {
    final StructType schema = schemaConverterL0.resourceSchema(ViewDefinitionResource.class);
    final DataType selectArray = schema.fields()[schema.fieldIndex("select")].dataType();
    final StructType selectType = (StructType) ((ArrayType) selectArray).elementType();
    final DataType columnArray = selectType.fields()[selectType.fieldIndex("column")].dataType();
    final StructType columnType = (StructType) ((ArrayType) columnArray).elementType();

    // Verify ColumnComponent fields.
    assertFieldPresent("name", columnType);
    assertFieldPresent("path", columnType);
    assertFieldPresent("description", columnType);
    assertFieldPresent("collection", columnType);
    assertFieldPresent("type", columnType);
    assertFieldPresent("tag", columnType);
  }

  @Test
  void testWhereComponentFields() {
    final StructType schema = schemaConverterL0.resourceSchema(ViewDefinitionResource.class);
    final DataType whereArray = schema.fields()[schema.fieldIndex("where")].dataType();
    final StructType whereType = (StructType) ((ArrayType) whereArray).elementType();

    // Verify WhereComponent fields.
    assertFieldPresent("path", whereType);
    assertFieldPresent("description", whereType);
  }

  @Test
  void testTagComponentFields() {
    final StructType schema = schemaConverterL0.resourceSchema(ViewDefinitionResource.class);
    final DataType selectArray = schema.fields()[schema.fieldIndex("select")].dataType();
    final StructType selectType = (StructType) ((ArrayType) selectArray).elementType();
    final DataType columnArray = selectType.fields()[selectType.fieldIndex("column")].dataType();
    final StructType columnType = (StructType) ((ArrayType) columnArray).elementType();
    final DataType tagArray = columnType.fields()[columnType.fieldIndex("tag")].dataType();
    final StructType tagType = (StructType) ((ArrayType) tagArray).elementType();

    // Verify TagComponent fields.
    assertFieldPresent("name", tagType);
    assertFieldPresent("value", tagType);
  }

  @Test
  void testPrimitiveFieldTypes() {
    final StructType schema = schemaConverterL0.resourceSchema(ViewDefinitionResource.class);

    // Verify primitive field types.
    assertInstanceOf(StringType.class, schema.fields()[schema.fieldIndex("name")].dataType());
    assertInstanceOf(StringType.class, schema.fields()[schema.fieldIndex("resource")].dataType());
    assertInstanceOf(StringType.class, schema.fields()[schema.fieldIndex("status")].dataType());
  }

  // ========== ENCODING/DECODING TESTS ==========

  @Test
  void testEncodeDecodeSimpleViewDefinition() {
    final ViewDefinitionResource original = createSimpleViewDefinition();

    final ExpressionEncoder<ViewDefinitionResource> encoder =
        fhirEncodersL0.of(ViewDefinitionResource.class);
    final ExpressionEncoder<ViewDefinitionResource> resolvedEncoder =
        EncoderUtils.defaultResolveAndBind(encoder);

    final InternalRow serializedRow = resolvedEncoder.createSerializer().apply(original);
    final ViewDefinitionResource decoded =
        resolvedEncoder.createDeserializer().apply(serializedRow);

    assertTrue(original.equalsDeep(decoded));
  }

  @Test
  void testEncodeDecodeWithNestedSelectL2() {
    // Create view with nesting level 2.
    final ViewDefinitionResource original = createNestedViewDefinition(2);

    // Use L2 encoder to preserve nesting.
    final ExpressionEncoder<ViewDefinitionResource> encoder =
        fhirEncodersL2.of(ViewDefinitionResource.class);
    final ExpressionEncoder<ViewDefinitionResource> resolvedEncoder =
        EncoderUtils.defaultResolveAndBind(encoder);

    final InternalRow serializedRow = resolvedEncoder.createSerializer().apply(original);
    final ViewDefinitionResource decoded =
        resolvedEncoder.createDeserializer().apply(serializedRow);

    assertTrue(original.equalsDeep(decoded));
  }

  @Test
  void testNestedSelectPrunedAtNestingLevelL0() {
    // Create view with nesting level 2.
    final ViewDefinitionResource original = createNestedViewDefinition(2);

    // Encode with L0 encoder - should prune all nested selects.
    final ExpressionEncoder<ViewDefinitionResource> encoderL0 =
        fhirEncodersL0.of(ViewDefinitionResource.class);
    final ExpressionEncoder<ViewDefinitionResource> resolvedEncoderL0 =
        EncoderUtils.defaultResolveAndBind(encoderL0);

    final InternalRow serializedRow = resolvedEncoderL0.createSerializer().apply(original);
    final ViewDefinitionResource decoded =
        resolvedEncoderL0.createDeserializer().apply(serializedRow);

    // Nested select should be empty (pruned).
    assertTrue(decoded.getSelect().get(0).getSelect().isEmpty());
  }

  @Test
  void testEncodeDecodeWithConstantStringValue() {
    final ViewDefinitionResource original = createSimpleViewDefinition();
    original.setId("const-string-test");

    final ConstantComponent constant = new ConstantComponent();
    constant.setName(new org.hl7.fhir.r4.model.StringType("myConst"));
    constant.setValue(new org.hl7.fhir.r4.model.StringType("test-value"));
    original.getConstant().add(constant);

    final ExpressionEncoder<ViewDefinitionResource> encoder =
        fhirEncodersL0.of(ViewDefinitionResource.class);
    final ExpressionEncoder<ViewDefinitionResource> resolvedEncoder =
        EncoderUtils.defaultResolveAndBind(encoder);

    final InternalRow serializedRow = resolvedEncoder.createSerializer().apply(original);
    final ViewDefinitionResource decoded =
        resolvedEncoder.createDeserializer().apply(serializedRow);

    assertInstanceOf(
        org.hl7.fhir.r4.model.StringType.class, decoded.getConstant().get(0).getValue());
    assertEquals(
        "test-value",
        ((org.hl7.fhir.r4.model.StringType) decoded.getConstant().get(0).getValue()).getValue());
  }

  @Test
  void testEncodeDecodeWithConstantBooleanValue() {
    final ViewDefinitionResource original = createSimpleViewDefinition();
    original.setId("const-bool-test");

    final ConstantComponent constant = new ConstantComponent();
    constant.setName(new org.hl7.fhir.r4.model.StringType("myFlag"));
    constant.setValue(new BooleanType(true));
    original.getConstant().add(constant);

    final ExpressionEncoder<ViewDefinitionResource> encoder =
        fhirEncodersL0.of(ViewDefinitionResource.class);
    final ExpressionEncoder<ViewDefinitionResource> resolvedEncoder =
        EncoderUtils.defaultResolveAndBind(encoder);

    final InternalRow serializedRow = resolvedEncoder.createSerializer().apply(original);
    final ViewDefinitionResource decoded =
        resolvedEncoder.createDeserializer().apply(serializedRow);

    assertInstanceOf(BooleanType.class, decoded.getConstant().get(0).getValue());
    assertTrue(((BooleanType) decoded.getConstant().get(0).getValue()).getValue());
  }

  @Test
  void testEncodeDecodeWithWhereClause() {
    final ViewDefinitionResource original = createViewDefinitionWithWhere();

    final ExpressionEncoder<ViewDefinitionResource> encoder =
        fhirEncodersL0.of(ViewDefinitionResource.class);
    final ExpressionEncoder<ViewDefinitionResource> resolvedEncoder =
        EncoderUtils.defaultResolveAndBind(encoder);

    final InternalRow serializedRow = resolvedEncoder.createSerializer().apply(original);
    final ViewDefinitionResource decoded =
        resolvedEncoder.createDeserializer().apply(serializedRow);

    assertTrue(original.equalsDeep(decoded));
    assertEquals("active = true", decoded.getWhere().get(0).getPath().getValue());
  }

  @Test
  void testEncodeDecodeWithTags() {
    final ViewDefinitionResource original = createViewDefinitionWithTags();

    final ExpressionEncoder<ViewDefinitionResource> encoder =
        fhirEncodersL0.of(ViewDefinitionResource.class);
    final ExpressionEncoder<ViewDefinitionResource> resolvedEncoder =
        EncoderUtils.defaultResolveAndBind(encoder);

    final InternalRow serializedRow = resolvedEncoder.createSerializer().apply(original);
    final ViewDefinitionResource decoded =
        resolvedEncoder.createDeserializer().apply(serializedRow);

    assertTrue(original.equalsDeep(decoded));
    final TagComponent decodedTag = decoded.getSelect().get(0).getColumn().get(0).getTag().get(0);
    assertEquals("sensitivity", decodedTag.getName().getValue());
    assertEquals("PHI", decodedTag.getValue().getValue());
  }

  @Test
  void testDatasetOperations() {
    final ViewDefinitionResource view1 = createSimpleViewDefinition();
    view1.setId("view-1");
    final ViewDefinitionResource view2 = createSimpleViewDefinition();
    view2.setId("view-2");

    final Dataset<ViewDefinitionResource> dataset =
        spark.createDataset(List.of(view1, view2), fhirEncodersL0.of(ViewDefinitionResource.class));

    assertEquals(2, dataset.count());

    // Test querying specific fields.
    final List<Row> rows = dataset.select("id", "name", "resource").collectAsList();
    assertEquals(2, rows.size());
    assertEquals("view-1", rows.get(0).getString(0));
    assertEquals("view-2", rows.get(1).getString(0));
  }

  @Test
  void testCanEncodeDecodeMinimalViewDefinition() {
    final ViewDefinitionResource original = new ViewDefinitionResource();
    original.setId("minimal-view");
    original.setResource(new CodeType("Patient"));

    // Minimum required: at least one select with one column.
    final SelectComponent select = new SelectComponent();
    final ColumnComponent column = new ColumnComponent();
    column.setName(new org.hl7.fhir.r4.model.StringType("id"));
    column.setPath(new org.hl7.fhir.r4.model.StringType("id"));
    select.getColumn().add(column);
    original.getSelect().add(select);

    final ExpressionEncoder<ViewDefinitionResource> encoder =
        fhirEncodersL0.of(ViewDefinitionResource.class);
    final ExpressionEncoder<ViewDefinitionResource> resolvedEncoder =
        EncoderUtils.defaultResolveAndBind(encoder);

    final InternalRow serializedRow = resolvedEncoder.createSerializer().apply(original);
    final ViewDefinitionResource decoded =
        resolvedEncoder.createDeserializer().apply(serializedRow);

    assertTrue(original.equalsDeep(decoded));
  }

  @Test
  void testEncodeDecodeWithForEachFields() {
    final ViewDefinitionResource original = createSimpleViewDefinition();
    original.setId("foreach-test");

    // Add forEach and forEachOrNull to the select.
    final SelectComponent select = original.getSelect().get(0);
    select.setForEach(new org.hl7.fhir.r4.model.StringType("name"));
    select.setForEachOrNull(new org.hl7.fhir.r4.model.StringType("address"));

    final ExpressionEncoder<ViewDefinitionResource> encoder =
        fhirEncodersL0.of(ViewDefinitionResource.class);
    final ExpressionEncoder<ViewDefinitionResource> resolvedEncoder =
        EncoderUtils.defaultResolveAndBind(encoder);

    final InternalRow serializedRow = resolvedEncoder.createSerializer().apply(original);
    final ViewDefinitionResource decoded =
        resolvedEncoder.createDeserializer().apply(serializedRow);

    assertTrue(original.equalsDeep(decoded));
    assertEquals("name", decoded.getSelect().get(0).getForEach().getValue());
    assertEquals("address", decoded.getSelect().get(0).getForEachOrNull().getValue());
  }

  @Test
  void testEncodeDecodeWithUnionAll() {
    final ViewDefinitionResource original = createSimpleViewDefinition();
    original.setId("union-all-test");

    // Add unionAll to the select at L2 nesting.
    final SelectComponent select = original.getSelect().get(0);
    final SelectComponent unionSelect = new SelectComponent();
    final ColumnComponent unionColumn = new ColumnComponent();
    unionColumn.setName(new org.hl7.fhir.r4.model.StringType("union_id"));
    unionColumn.setPath(new org.hl7.fhir.r4.model.StringType("id"));
    unionSelect.getColumn().add(unionColumn);
    select.getUnionAll().add(unionSelect);

    // Use L2 encoder to preserve unionAll.
    final ExpressionEncoder<ViewDefinitionResource> encoder =
        fhirEncodersL2.of(ViewDefinitionResource.class);
    final ExpressionEncoder<ViewDefinitionResource> resolvedEncoder =
        EncoderUtils.defaultResolveAndBind(encoder);

    final InternalRow serializedRow = resolvedEncoder.createSerializer().apply(original);
    final ViewDefinitionResource decoded =
        resolvedEncoder.createDeserializer().apply(serializedRow);

    assertTrue(original.equalsDeep(decoded));
    assertEquals(1, decoded.getSelect().get(0).getUnionAll().size());
    assertEquals(
        "union_id",
        decoded.getSelect().get(0).getUnionAll().get(0).getColumn().get(0).getName().getValue());
  }

  @Test
  void testEncodeDecodeWithMultipleColumns() {
    final ViewDefinitionResource original = new ViewDefinitionResource();
    original.setId("multi-column-view");
    original.setResource(new CodeType("Patient"));
    original.setName(new org.hl7.fhir.r4.model.StringType("MultiColumnView"));

    final SelectComponent select = new SelectComponent();

    // Add multiple columns with various properties.
    final ColumnComponent col1 = new ColumnComponent();
    col1.setName(new org.hl7.fhir.r4.model.StringType("patient_id"));
    col1.setPath(new org.hl7.fhir.r4.model.StringType("id"));
    col1.setDescription(new org.hl7.fhir.r4.model.StringType("Patient identifier"));
    select.getColumn().add(col1);

    final ColumnComponent col2 = new ColumnComponent();
    col2.setName(new org.hl7.fhir.r4.model.StringType("names"));
    col2.setPath(new org.hl7.fhir.r4.model.StringType("name.given"));
    col2.setCollection(new BooleanType(true));
    col2.setType(new org.hl7.fhir.r4.model.StringType("string"));
    select.getColumn().add(col2);

    original.getSelect().add(select);

    final ExpressionEncoder<ViewDefinitionResource> encoder =
        fhirEncodersL0.of(ViewDefinitionResource.class);
    final ExpressionEncoder<ViewDefinitionResource> resolvedEncoder =
        EncoderUtils.defaultResolveAndBind(encoder);

    final InternalRow serializedRow = resolvedEncoder.createSerializer().apply(original);
    final ViewDefinitionResource decoded =
        resolvedEncoder.createDeserializer().apply(serializedRow);

    assertTrue(original.equalsDeep(decoded));
    assertEquals(2, decoded.getSelect().get(0).getColumn().size());
    assertTrue(decoded.getSelect().get(0).getColumn().get(1).getCollection().getValue());
  }

  /**
   * Round-trip test for external ViewDefinition resources loaded from JSON files. This test
   * verifies that ViewDefinition resources can be parsed from JSON, encoded to Spark InternalRow,
   * decoded back to ViewDefinitionResource, and serialised to JSON that matches the original.
   */
  @Test
  void testRoundTripExternalViewDefinitions() throws IOException, JSONException {
    // List all JSON files in the viewdefinitions directory.
    final Path resourceDir = Path.of("src/test/resources/viewdefinitions");
    final List<Path> viewDefFiles;
    try (var stream = Files.list(resourceDir)) {
      viewDefFiles = stream.filter(p -> p.toString().endsWith(".json")).sorted().toList();
    }

    // Parse each ViewDefinition and verify round-trip encoding.
    final IParser parser = fhirContext.newJsonParser();
    final ExpressionEncoder<ViewDefinitionResource> encoder =
        fhirEncodersL2.of(ViewDefinitionResource.class);
    final ExpressionEncoder<ViewDefinitionResource> resolvedEncoder =
        EncoderUtils.defaultResolveAndBind(encoder);

    for (final Path file : viewDefFiles) {
      final String originalJson = Files.readString(file);
      final ViewDefinitionResource original =
          parser.parseResource(ViewDefinitionResource.class, originalJson);

      final InternalRow serializedRow = resolvedEncoder.createSerializer().apply(original);
      final ViewDefinitionResource decoded =
          resolvedEncoder.createDeserializer().apply(serializedRow);

      // Compare using JSONAssert for detailed diff on failure.
      final String decodedJson = parser.encodeResourceToString(decoded);
      JSONAssert.assertEquals(
          "Round-trip failed for: " + file.getFileName(),
          originalJson,
          decodedJson,
          JSONCompareMode.STRICT_ORDER);
    }
  }

  // ========== TEST DATA FACTORY METHODS ==========

  static ViewDefinitionResource createSimpleViewDefinition() {
    final ViewDefinitionResource view = new ViewDefinitionResource();
    view.setId("simple-view");
    view.setName(new org.hl7.fhir.r4.model.StringType("SimplePatientView"));
    view.setResource(new CodeType("Patient"));
    view.setStatus(new CodeType("active"));

    // Add a simple select with one column.
    final SelectComponent select = new SelectComponent();
    final ColumnComponent column = new ColumnComponent();
    column.setName(new org.hl7.fhir.r4.model.StringType("patient_id"));
    column.setPath(new org.hl7.fhir.r4.model.StringType("id"));
    select.getColumn().add(column);
    view.getSelect().add(select);

    return view;
  }

  static ViewDefinitionResource createNestedViewDefinition(final int nestingLevel) {
    final ViewDefinitionResource view = new ViewDefinitionResource();
    view.setId("nested-view-" + nestingLevel);
    view.setName(new org.hl7.fhir.r4.model.StringType("NestedView"));
    view.setResource(new CodeType("Observation"));
    view.setStatus(new CodeType("draft"));

    // Create nested select structure.
    final SelectComponent rootSelect = createNestedSelect(nestingLevel, "root");
    view.getSelect().add(rootSelect);

    return view;
  }

  private static SelectComponent createNestedSelect(final int level, final String prefix) {
    final SelectComponent select = new SelectComponent();

    final ColumnComponent column = new ColumnComponent();
    column.setName(new org.hl7.fhir.r4.model.StringType("col_" + prefix + "_" + level));
    column.setPath(new org.hl7.fhir.r4.model.StringType("id"));
    select.getColumn().add(column);

    if (level > 0) {
      select.getSelect().add(createNestedSelect(level - 1, prefix));
    }

    return select;
  }

  static ViewDefinitionResource createViewDefinitionWithWhere() {
    final ViewDefinitionResource view = createSimpleViewDefinition();
    view.setId("view-with-where");

    final WhereComponent where = new WhereComponent();
    where.setPath(new org.hl7.fhir.r4.model.StringType("active = true"));
    where.setDescription(new org.hl7.fhir.r4.model.StringType("Only active patients"));
    view.getWhere().add(where);

    return view;
  }

  static ViewDefinitionResource createViewDefinitionWithTags() {
    final ViewDefinitionResource view = createSimpleViewDefinition();
    view.setId("view-with-tags");

    // Add column with tags.
    final ColumnComponent column = view.getSelect().get(0).getColumn().get(0);
    final TagComponent tag = new TagComponent();
    tag.setName(new org.hl7.fhir.r4.model.StringType("sensitivity"));
    tag.setValue(new org.hl7.fhir.r4.model.StringType("PHI"));
    column.getTag().add(tag);

    return view;
  }

  // ========== COPY METHOD TESTS ==========

  @Test
  void testCopyViewDefinitionResource() {
    // Create a fully populated ViewDefinition.
    final ViewDefinitionResource original = createSimpleViewDefinition();
    original.getFhirVersion().add(new CodeType("4.0.1"));
    original.getWhere().add(new WhereComponent());
    original.getWhere().get(0).setPath(new org.hl7.fhir.r4.model.StringType("active = true"));

    final ConstantComponent constant = new ConstantComponent();
    constant.setName(new org.hl7.fhir.r4.model.StringType("maxAge"));
    constant.setValue(new IntegerType(65));
    original.getConstant().add(constant);

    // Copy and verify deep equality.
    final ViewDefinitionResource copy = (ViewDefinitionResource) original.copy();

    assertTrue(original.equalsDeep(copy));
    assertEquals(original.getName().getValue(), copy.getName().getValue());
    assertEquals(original.getResource().getValue(), copy.getResource().getValue());
    assertEquals(original.getFhirVersion().size(), copy.getFhirVersion().size());
    assertEquals(original.getSelect().size(), copy.getSelect().size());
    assertEquals(original.getWhere().size(), copy.getWhere().size());
    assertEquals(original.getConstant().size(), copy.getConstant().size());
  }

  @Test
  void testCopySelectComponent() {
    // Create a SelectComponent with nested selects and unionAll.
    final SelectComponent original = new SelectComponent();
    original.setForEach(new org.hl7.fhir.r4.model.StringType("name"));
    original.setForEachOrNull(new org.hl7.fhir.r4.model.StringType("address"));

    final ColumnComponent column = new ColumnComponent();
    column.setName(new org.hl7.fhir.r4.model.StringType("test"));
    column.setPath(new org.hl7.fhir.r4.model.StringType("id"));
    original.getColumn().add(column);

    final SelectComponent nestedSelect = new SelectComponent();
    nestedSelect.setForEach(new org.hl7.fhir.r4.model.StringType("telecom"));
    original.getSelect().add(nestedSelect);

    final SelectComponent unionSelect = new SelectComponent();
    unionSelect.setForEach(new org.hl7.fhir.r4.model.StringType("contact"));
    original.getUnionAll().add(unionSelect);

    // Copy and verify.
    final SelectComponent copy = original.copy();

    assertTrue(original.equalsDeep(copy));
    assertEquals(original.getForEach().getValue(), copy.getForEach().getValue());
    assertEquals(original.getForEachOrNull().getValue(), copy.getForEachOrNull().getValue());
    assertEquals(original.getColumn().size(), copy.getColumn().size());
    assertEquals(original.getSelect().size(), copy.getSelect().size());
    assertEquals(original.getUnionAll().size(), copy.getUnionAll().size());
  }

  @Test
  void testCopyColumnComponent() {
    final ColumnComponent original = new ColumnComponent();
    original.setName(new org.hl7.fhir.r4.model.StringType("patientId"));
    original.setPath(new org.hl7.fhir.r4.model.StringType("id"));
    original.setDescription(new org.hl7.fhir.r4.model.StringType("The patient identifier"));
    original.setCollection(new BooleanType(false));
    original.setType(new org.hl7.fhir.r4.model.StringType("string"));

    final TagComponent tag = new TagComponent();
    tag.setName(new org.hl7.fhir.r4.model.StringType("phi"));
    tag.setValue(new org.hl7.fhir.r4.model.StringType("true"));
    original.getTag().add(tag);

    final ColumnComponent copy = original.copy();

    assertTrue(original.equalsDeep(copy));
    assertEquals(original.getName().getValue(), copy.getName().getValue());
    assertEquals(original.getPath().getValue(), copy.getPath().getValue());
    assertEquals(original.getDescription().getValue(), copy.getDescription().getValue());
    assertEquals(original.getCollection().getValue(), copy.getCollection().getValue());
    assertEquals(original.getType().getValue(), copy.getType().getValue());
    assertEquals(original.getTag().size(), copy.getTag().size());
  }

  @Test
  void testCopyWhereComponent() {
    final WhereComponent original = new WhereComponent();
    original.setPath(new org.hl7.fhir.r4.model.StringType("active = true"));
    original.setDescription(new org.hl7.fhir.r4.model.StringType("Filter active patients"));

    final WhereComponent copy = original.copy();

    assertTrue(original.equalsDeep(copy));
    assertEquals(original.getPath().getValue(), copy.getPath().getValue());
    assertEquals(original.getDescription().getValue(), copy.getDescription().getValue());
  }

  @Test
  void testCopyConstantComponent() {
    final ConstantComponent original = new ConstantComponent();
    original.setName(new org.hl7.fhir.r4.model.StringType("threshold"));
    original.setValue(new IntegerType(100));

    final ConstantComponent copy = original.copy();

    assertTrue(original.equalsDeep(copy));
    assertEquals(original.getName().getValue(), copy.getName().getValue());
    assertInstanceOf(IntegerType.class, copy.getValue());
    assertEquals(
        ((IntegerType) original.getValue()).getValue(), ((IntegerType) copy.getValue()).getValue());
  }

  @Test
  void testCopyTagComponent() {
    final TagComponent original = new TagComponent();
    original.setName(new org.hl7.fhir.r4.model.StringType("category"));
    original.setValue(new org.hl7.fhir.r4.model.StringType("vital-signs"));

    final TagComponent copy = original.copy();

    assertTrue(original.equalsDeep(copy));
    assertEquals(original.getName().getValue(), copy.getName().getValue());
    assertEquals(original.getValue().getValue(), copy.getValue().getValue());
  }

  // ========== ISEMPTY METHOD TESTS ==========

  @Test
  void testIsEmptyViewDefinitionResource() {
    final ViewDefinitionResource empty = new ViewDefinitionResource();
    assertTrue(empty.isEmpty());

    final ViewDefinitionResource withName = new ViewDefinitionResource();
    withName.setName(new org.hl7.fhir.r4.model.StringType("test"));
    assertFalse(withName.isEmpty());
  }

  @Test
  void testIsEmptySelectComponent() {
    final SelectComponent empty = new SelectComponent();
    assertTrue(empty.isEmpty());

    final SelectComponent withColumn = new SelectComponent();
    final ColumnComponent column = new ColumnComponent();
    column.setName(new org.hl7.fhir.r4.model.StringType("test"));
    withColumn.getColumn().add(column);
    assertFalse(withColumn.isEmpty());
  }

  @Test
  void testIsEmptyColumnComponent() {
    final ColumnComponent empty = new ColumnComponent();
    assertTrue(empty.isEmpty());

    final ColumnComponent withName = new ColumnComponent();
    withName.setName(new org.hl7.fhir.r4.model.StringType("test"));
    assertFalse(withName.isEmpty());
  }

  @Test
  void testIsEmptyWhereComponent() {
    final WhereComponent empty = new WhereComponent();
    assertTrue(empty.isEmpty());

    final WhereComponent withPath = new WhereComponent();
    withPath.setPath(new org.hl7.fhir.r4.model.StringType("active = true"));
    assertFalse(withPath.isEmpty());
  }

  @Test
  void testIsEmptyConstantComponent() {
    final ConstantComponent empty = new ConstantComponent();
    assertTrue(empty.isEmpty());

    final ConstantComponent withName = new ConstantComponent();
    withName.setName(new org.hl7.fhir.r4.model.StringType("test"));
    assertFalse(withName.isEmpty());
  }

  @Test
  void testIsEmptyTagComponent() {
    final TagComponent empty = new TagComponent();
    assertTrue(empty.isEmpty());

    final TagComponent withName = new TagComponent();
    withName.setName(new org.hl7.fhir.r4.model.StringType("test"));
    assertFalse(withName.isEmpty());
  }

  // ========== RESOURCE TYPE TESTS ==========

  @Test
  void testGetResourceType() {
    final ViewDefinitionResource view = new ViewDefinitionResource();
    // Custom resource types return null.
    assertNull(view.getResourceType());
  }

  @Test
  void testFhirType() {
    final ViewDefinitionResource view = new ViewDefinitionResource();
    assertEquals("ViewDefinition", view.fhirType());
  }
}
