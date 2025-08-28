/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.sql.udf;

import static au.csiro.pathling.fhirpath.encoding.CodingSchema.toLiteralColumn;
import static au.csiro.pathling.sql.Terminology.designation;
import static au.csiro.pathling.sql.Terminology.display;
import static au.csiro.pathling.sql.Terminology.member_of;
import static au.csiro.pathling.sql.Terminology.subsumed_by;
import static au.csiro.pathling.sql.Terminology.subsumes;
import static au.csiro.pathling.sql.Terminology.translate;
import static au.csiro.pathling.test.helpers.TestHelpers.LOINC_URL;
import static au.csiro.pathling.test.helpers.TestHelpers.SNOMED_URL;
import static org.apache.spark.sql.functions.lit;
import static org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence.RELATEDTO;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.encoding.CodingSchema;
import au.csiro.pathling.sql.Terminology;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyService.Translation;
import au.csiro.pathling.test.AbstractTerminologyTestBase;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.assertions.DatasetAssert;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers.LookupExpectations;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

@SpringBootUnitTest
public class TerminologyUdfTest extends AbstractTerminologyTestBase {

  @Autowired
  private SparkSession spark;

  @Autowired
  TerminologyService terminologyService;

  @BeforeEach
  void setUp() {
    SharedMocks.resetAll();
  }

  private static final Coding CODING_1 = new Coding(LOINC_URL, "10337-4",
      "Procollagen type I [Mass/volume] in Serum");
  private static final Coding CODING_2 = new Coding(LOINC_URL, "10428-1",
      "Varicella zoster virus immune globulin given [Volume]");
  private static final Coding CODING_3 = new Coding(LOINC_URL, "10555-1", null);
  private static final Coding CODING_4 = new Coding(LOINC_URL, "10665-8",
      "Fungus colony count [#/volume] in Unspecified specimen by Environmental culture");
  private static final Coding CODING_5 = new Coding(SNOMED_URL, "416399002",
      "Procollagen type I amino-terminal propeptide level");

  private static final String CODING_1_VALUE_SET_URI = "uuid:ValueSet_coding1";
  private static final String CODING_2_VALUE_SET_URI = "uuid:ValueSet_coding2";

  private static final Coding USE_A = new Coding("uuid:use", "useA", "Use A");
  private static final Coding USE_B = new Coding("uuid:use", "useB", "Use B");


  private static final String LANGUAGE_X = "languageX";
  private static final String LANGUAGE_Y = "languageY";

  // helper functions
  private DatasetBuilder codingDatasetBuilder() {
    return DatasetBuilder.of(spark)
        .withIdColumn("id")
        .withColumn("code", CodingSchema.DATA_TYPE);
  }

  private void setupValidateCodingExpectations() {
    TerminologyServiceHelpers.setupValidate(terminologyService)
        .withValueSet(CODING_1_VALUE_SET_URI, CODING_1)
        .withValueSet(CODING_2_VALUE_SET_URI, CODING_2);
  }

  private void setupTranslateExpectations() {
    TerminologyServiceHelpers.setupTranslate(terminologyService)
        .withTranslations(CODING_1, "someUrl",
            Translation.of(RELATEDTO, CODING_5),
            Translation.of(RELATEDTO, CODING_4)
        );
  }

  private void setupSubsumesExpectations() {
    TerminologyServiceHelpers.setupSubsumes(terminologyService)
        .withSubsumes(CODING_1, CODING_2)
        .withSubsumes(CODING_3, CODING_4);
  }


  private LookupExpectations setupDisplayExpectations() {

    return TerminologyServiceHelpers.setupLookup(terminologyService)
        .withDisplay(CODING_1)
        .withDisplay(CODING_2);
  }

  private void setupDesignationExpectations() {
    TerminologyServiceHelpers.setupLookup(terminologyService)
        .withDesignation(CODING_1, USE_A, LANGUAGE_X, "designation_A_X")
        .withDesignation(CODING_1, USE_A, LANGUAGE_Y, "designation_A_Y")
        .withDesignation(CODING_2, USE_B, LANGUAGE_X, "designation_B_X.1", "designation_B_X.2")
        .withDesignation(CODING_2, USE_B, LANGUAGE_Y, "designation_B_Y")
        .done();
  }

  @Test
  public void testValidateCoding() {
    setupValidateCodingExpectations();
    final Dataset<Row> df = codingDatasetBuilder()
        .withRow("id-1", CodingSchema.encode(CODING_1))
        .withRow("id-2", CodingSchema.encode(CODING_2))
        .withRow("id-3", null)
        .build();

    final Dataset<Row> result1 = df.select(
        member_of(df.col("code"), CODING_1_VALUE_SET_URI));

    final Dataset<Row> result2 = df.select(
        member_of(df.col("code"), CODING_2_VALUE_SET_URI));

    DatasetAssert.of(result1).hasRows(
        RowFactory.create(true),
        RowFactory.create(false),
        RowFactory.create((Boolean) null)
    );

    DatasetAssert.of(result2).hasRows(
        RowFactory.create(false),
        RowFactory.create(true),
        RowFactory.create((Boolean) null)
    );
  }

  @Test
  public void testValidateCodingNullDataset() {
    setupValidateCodingExpectations();
    final Dataset<Row> df = codingDatasetBuilder()
        .withRow("id-1", CodingSchema.encode(CODING_1))
        .withRow("id-2", CodingSchema.encode(CODING_2))
        .withRow("id-3", null)
        .build();

    final Dataset<Row> result = df.select(member_of(df.col("code"), lit(null)));

    DatasetAssert.of(result).hasRows(
        RowFactory.create((Boolean) null),
        RowFactory.create((Boolean) null),
        RowFactory.create((Boolean) null)
    );
  }

  @Test
  public void testValidateCodingArray() {
    setupValidateCodingExpectations();
    final Dataset<Row> ds = DatasetBuilder.of(spark)
        .withIdColumn("id")
        .withColumn("codings", DataTypes.createArrayType(CodingSchema.DATA_TYPE))
        .withRow("id-1", null)
        .withRow("id-2", Collections.emptyList())
        .withRow("id-3",
            Collections.singletonList(CodingSchema.encode(CODING_1)))
        .withRow("id-4",
            Collections.singletonList(CodingSchema.encode(CODING_2)))
        .withRow("id-5",
            Collections.singletonList(CodingSchema.encode(CODING_3)))
        .withRow("id-6",
            Collections.singletonList(null))
        .withRow("id-7",
            Arrays.asList(CodingSchema.encode(CODING_1), CodingSchema.encode(CODING_5)))
        .withRow("id-8",
            Arrays.asList(CodingSchema.encode(CODING_2), CodingSchema.encode(CODING_5)))
        .withRow("id-9",
            Arrays.asList(CodingSchema.encode(CODING_3), CodingSchema.encode(CODING_5)))
        .withRow("id-a",
            Arrays.asList(CodingSchema.encode(CODING_3), CodingSchema.encode(CODING_5)))
        .withRow("id-b", Arrays.asList(CodingSchema.encode(CODING_1), null))
        .withRow("id-c", Arrays.asList(CodingSchema.encode(CODING_2), null))
        .withRow("id-d", Arrays.asList(CodingSchema.encode(CODING_3), null))
        .withRow("id-e", Arrays.asList(null, null))
        .build();

    final Dataset<Row> result1 = ds.select(ds.col("id"),
        member_of(ds.col("codings"), CODING_1_VALUE_SET_URI));

    final Dataset<Row> result2 = ds.select(ds.col("id"),
        member_of(ds.col("codings"), CODING_2_VALUE_SET_URI));

    final Dataset<Row> expectedResult1 = DatasetBuilder.of(spark)
        .withIdColumn("id")
        .withColumn("result", DataTypes.BooleanType)
        .withRow("id-1", null)
        .withRow("id-2", false)
        .withRow("id-3", true)
        .withRow("id-4", false)
        .withRow("id-5", false)
        .withRow("id-6", false)
        .withRow("id-7", true)
        .withRow("id-8", false)
        .withRow("id-9", false)
        .withRow("id-a", false)
        .withRow("id-b", true)
        .withRow("id-c", false)
        .withRow("id-d", false)
        .withRow("id-e", false)
        .build();

    DatasetAssert.of(result1).hasRows(expectedResult1);

    final Dataset<Row> expectedResult2 = DatasetBuilder.of(spark)
        .withIdColumn("id")
        .withColumn("result", DataTypes.BooleanType)
        .withRow("id-1", null)
        .withRow("id-2", false)
        .withRow("id-3", false)
        .withRow("id-4", true)
        .withRow("id-5", false)
        .withRow("id-6", false)
        .withRow("id-7", false)
        .withRow("id-8", true)
        .withRow("id-9", false)
        .withRow("id-a", false)
        .withRow("id-b", false)
        .withRow("id-c", true)
        .withRow("id-d", false)
        .withRow("id-e", false)
        .build();

    DatasetAssert.of(result2).hasRows(expectedResult2);
  }

  @Test
  public void validateCodingArrayWithNullDataset() {
    setupValidateCodingExpectations();
    final Dataset<Row> ds = DatasetBuilder.of(spark)
        .withIdColumn("id")
        .withColumn("codings", DataTypes.createArrayType(CodingSchema.DATA_TYPE))
        .withRow("id-1", null)
        .withRow("id-2", Collections.emptyList())
        .withRow("id-3",
            Collections.singletonList(CodingSchema.encode(CODING_1)))
        .withRow("id-4",
            Arrays.asList(CodingSchema.encode(CODING_1), CodingSchema.encode(CODING_5)))
        .build();

    final Dataset<Row> result = ds.select(member_of(ds.col("codings"), lit(null)));

    DatasetAssert.of(result).hasRows(
        RowFactory.create((Boolean) null),
        RowFactory.create((Boolean) null),
        RowFactory.create((Boolean) null),
        RowFactory.create((Boolean) null)
    );
  }


  @Test
  public void testTranslateCoding() {
    setupTranslateExpectations();

    final Dataset<Row> ds = codingDatasetBuilder()
        .withRow("uc-null", null)
        .withRow("uc-coding_1", CodingSchema.encode(CODING_1))
        .withRow("uc-coding_2", CodingSchema.encode(CODING_2))
        .build();

    final Dataset<Row> result = ds.select(ds.col("id"),
        translate(
            ds.col("code"),
            "someUrl",
            false,
            List.of(RELATEDTO)));
    final Dataset<Row> expectedResult = DatasetBuilder.of(spark).withIdColumn("id")
        .withColumn("result", TranslateUdf.RETURN_TYPE)
        .withRow("uc-null", null)
        .withRow("uc-coding_1", CodingSchema.encodeListToArray(List.of(CODING_5, CODING_4)))
        .withRow("uc-coding_2", new Row[]{})
        .build();
    DatasetAssert.of(result).hasRows(expectedResult);
  }


  @Test
  public void testTranslateCodingArray() {
    setupTranslateExpectations();

    final Dataset<Row> ds = DatasetBuilder.of(spark)
        .withIdColumn("id")
        .withColumn("codings", DataTypes.createArrayType(CodingSchema.DATA_TYPE))
        .withRow("uc-null", null)
        .withRow("uc-empty", new Row[]{})
        .withRow("uc-coding_1", new Row[]{CodingSchema.encode(CODING_1)})
        .withRow("uc-coding_2", new Row[]{CodingSchema.encode(CODING_2)})
        .withRow("uc-coding_1+coding_1",
            CodingSchema.encodeListToArray(List.of(CODING_1, CODING_1)))
        .build();

    final Dataset<Row> result = ds.select(ds.col("id"),
        translate(
            ds.col("codings"),
            "someUrl",
            false,
            List.of(RELATEDTO), null));
    final Dataset<Row> expectedResult = DatasetBuilder.of(spark).withIdColumn("id")
        .withColumn("result", TranslateUdf.RETURN_TYPE)
        .withRow("uc-null", null)
        .withRow("uc-empty", new Row[]{})
        .withRow("uc-coding_1", CodingSchema.encodeListToArray(List.of(CODING_5, CODING_4)))
        .withRow("uc-coding_2", new Row[]{})
        .withRow("uc-coding_1+coding_1",
            CodingSchema.encodeListToArray(List.of(CODING_5, CODING_4)))
        .build();
    DatasetAssert.of(result).hasRows(expectedResult);
  }

  @Nullable
  private static Row toRow(@Nonnull final Coding coding) {
    return CodingSchema.encode(coding);
  }

  @Nonnull
  private static List<Row> toArray(@Nonnull final Coding... codings) {
    return Stream.of(codings).map(CodingSchema::encode).toList();
  }

  @Test
  public void testSubsumes() {

    setupSubsumesExpectations();

    final Dataset<Row> ds = DatasetBuilder.of(spark)
        .withIdColumn("id")
        .withColumn("codingA", CodingSchema.DATA_TYPE)
        .withColumn("codingsB", DataTypes.createArrayType(CodingSchema.DATA_TYPE))
        .withRow("uc-null-empty", null, Collections.emptyList())
        .withRow("uc-any-null", toRow(CODING_1), null)
        .withRow("uc-invalid", toRow(INVALID_CODING_0),
            toArray(INVALID_CODING_1, INVALID_CODING_2, null))
        .withRow("uc-subsumes", toRow(CODING_1), toArray(CODING_2, CODING_3))
        .withRow("uc-subsumed_by", toRow(CODING_4), toArray(CODING_5, CODING_3))
        .build();

    final Dataset<Row> result = ds.select(ds.col("id"),
        subsumes(ds.col("codingA"), ds.col("codingsB")));

    final Dataset<Row> expectedResult = DatasetBuilder.of(spark).withIdColumn("id")
        .withColumn("result", DataTypes.BooleanType)
        .withRow("uc-null-empty", null)
        .withRow("uc-any-null", null)
        .withRow("uc-invalid", false)
        .withRow("uc-subsumes", true)
        .withRow("uc-subsumed_by", false)
        .build();
    DatasetAssert.of(result).hasRows(expectedResult);

    final Dataset<Row> resultSwapped = ds.select(ds.col("id"),
        subsumes(ds.col("codingsB"), ds.col("codingA")));

    final Dataset<Row> expectedResultSwapped = DatasetBuilder.of(spark).withIdColumn("id")
        .withColumn("result", DataTypes.BooleanType)
        .withRow("uc-null-empty", null)
        .withRow("uc-any-null", null)
        .withRow("uc-invalid", false)
        .withRow("uc-subsumes", false)
        .withRow("uc-subsumed_by", true)
        .build();
    DatasetAssert.of(resultSwapped).hasRows(expectedResultSwapped);
  }

  @Test
  public void testSubsumedBy() {

    setupSubsumesExpectations();

    final Dataset<Row> ds = DatasetBuilder.of(spark)
        .withIdColumn("id")
        .withColumn("codingA", CodingSchema.DATA_TYPE)
        .withColumn("codingsB", DataTypes.createArrayType(CodingSchema.DATA_TYPE))
        .withRow("uc-null-empty", null, Collections.emptyList())
        .withRow("uc-any-null", toRow(CODING_1), null)
        .withRow("uc-invalid", toRow(INVALID_CODING_0),
            toArray(INVALID_CODING_1, INVALID_CODING_2, null))
        .withRow("uc-subsumes", toRow(CODING_1), toArray(CODING_2, CODING_3))
        .withRow("uc-subsumed_by", toRow(CODING_4), toArray(CODING_5, CODING_3))
        .build();

    final Dataset<Row> result = ds.select(ds.col("id"),
        subsumed_by(ds.col("codingA"), ds.col("codingsB")));

    final Dataset<Row> expectedResult = DatasetBuilder.of(spark).withIdColumn("id")
        .withColumn("result", DataTypes.BooleanType)
        .withRow("uc-null-empty", null)
        .withRow("uc-any-null", null)
        .withRow("uc-invalid", false)
        .withRow("uc-subsumes", false)
        .withRow("uc-subsumed_by", true)
        .build();
    DatasetAssert.of(result).hasRows(expectedResult);

    final Dataset<Row> resultSwapped = ds.select(ds.col("id"),
        subsumed_by(ds.col("codingsB"), ds.col("codingA")));

    final Dataset<Row> expectedResultSwapped = DatasetBuilder.of(spark).withIdColumn("id")
        .withColumn("result", DataTypes.BooleanType)
        .withRow("uc-null-empty", null)
        .withRow("uc-any-null", null)
        .withRow("uc-invalid", false)
        .withRow("uc-subsumes", true)
        .withRow("uc-subsumed_by", false)
        .build();
    DatasetAssert.of(resultSwapped).hasRows(expectedResultSwapped);
  }

  @Test
  public void testDisplay() {
    setupDisplayExpectations();

    final Dataset<Row> ds = DatasetBuilder.of(spark)
        .withIdColumn("id")
        .withColumn("coding", CodingSchema.DATA_TYPE)
        .withRow("uc-null", null)
        .withRow("uc-invalid", toRow(INVALID_CODING_0))
        .withRow("uc-codingA", toRow(CODING_1))
        .withRow("uc-codingB", toRow(CODING_2))
        .build();

    final Dataset<Row> result = ds.select(ds.col("id"),
        display(ds.col("coding")));

    final Dataset<Row> expectedResult = DatasetBuilder.of(spark).withIdColumn("id")
        .withColumn("result", DataTypes.StringType)
        .withRow("uc-null", null)
        .withRow("uc-invalid", null)
        .withRow("uc-codingA", CODING_1.getDisplay())
        .withRow("uc-codingB", CODING_2.getDisplay())
        .build();
    DatasetAssert.of(result).hasRows(expectedResult);
  }

  @Test
  public void testDisplayWithLanguage() {
    setupDisplayExpectations()
        .withDisplay(CODING_1, "display 1 (de)", "de")
        .withDisplay(CODING_2, "display 2 (de)", "de");

    final Dataset<Row> ds = DatasetBuilder.of(spark)
        .withIdColumn("id")
        .withColumn("coding", CodingSchema.DATA_TYPE)
        .withRow("uc-null", null)
        .withRow("uc-invalid", toRow(INVALID_CODING_0))
        .withRow("uc-codingA", toRow(CODING_1))
        .withRow("uc-codingB", toRow(CODING_2))
        .build();

    final Dataset<Row> result = ds.select(ds.col("id"),
        display(ds.col("coding"), "de"));

    final Dataset<Row> expectedResult = DatasetBuilder.of(spark).withIdColumn("id")
        .withColumn("result", DataTypes.StringType)
        .withRow("uc-null", null)
        .withRow("uc-invalid", null)
        .withRow("uc-codingA", "display 1 (de)")
        .withRow("uc-codingB", "display 2 (de)")
        .build();
    DatasetAssert.of(result).hasRows(expectedResult);
  }

  @ParameterizedTest
  @MethodSource("propertyParameters")
  public void testProperty(final String propertyType, final DataType resultDataType,
      final List<Type> propertyAFhirValues, final List<Type> propertyBFhirValues,
      final List<Object> propertyASqlValues, final List<Object> propertyBSqlValues) {
    TerminologyServiceHelpers.setupLookup(terminologyService)
        .withProperty(CODING_1, "property_a", null, propertyAFhirValues)
        .withProperty(CODING_2, "property_b", null, propertyBFhirValues);

    final Dataset<Row> ds = DatasetBuilder.of(spark)
        .withIdColumn("id")
        .withColumn("coding", CodingSchema.DATA_TYPE)
        .withRow("uc-null", null)
        .withRow("uc-invalid", toRow(INVALID_CODING_0))
        .withRow("uc-codingA", toRow(CODING_1))
        .withRow("uc-codingB", toRow(CODING_2))
        .build();

    final Dataset<Row> resultA = ds.select(ds.col("id"),
        Terminology.property_of(ds.col("coding"), "property_a", propertyType).alias("values"));

    final Dataset<Row> expectedResultA = DatasetBuilder.of(spark).withIdColumn("id")
        .withColumn("result", DataTypes.createArrayType(resultDataType))
        .withRow("uc-null", null)
        .withRow("uc-invalid", null)
        .withRow("uc-codingA", propertyASqlValues.toArray())
        .withRow("uc-codingB", new Object[]{})
        .build();

    DatasetAssert.of(resultA)
        .hasRows(expectedResultA);

    // Test the FHIRDefineType version as well
    final Dataset<Row> resultB = ds.select(ds.col("id"),
        Terminology.property_of(ds.col("coding"), "property_b",
            FHIRDefinedType.fromCode(propertyType)).alias("values"));

    final Dataset<Row> expectedResultB = DatasetBuilder.of(spark).withIdColumn("id")
        .withColumn("result", DataTypes.createArrayType(resultDataType))
        .withRow("uc-null", null)
        .withRow("uc-invalid", null)
        .withRow("uc-codingA", new Object[]{})
        .withRow("uc-codingB", propertyBSqlValues.toArray())
        .build();

    DatasetAssert.of(resultB)
        .hasRows(expectedResultB);
  }

  @Test
  public void testPropertyWithDefaultType() {
    TerminologyServiceHelpers.setupLookup(terminologyService)
        .withProperty(CODING_1, "property_a", null, List.of(new StringType("value_a")));

    final Dataset<Row> ds = DatasetBuilder.of(spark)
        .withIdColumn("id")
        .withColumn("coding", CodingSchema.DATA_TYPE)
        .withRow("uc-null", null)
        .withRow("uc-codingA", toRow(CODING_1))
        .withRow("uc-codingB", toRow(CODING_2))
        .build();

    final Dataset<Row> resultA = ds.select(ds.col("id"),
        Terminology.property_of(ds.col("coding"), "property_a").alias("values"));

    final Dataset<Row> expectedResultA = DatasetBuilder.of(spark).withIdColumn("id")
        .withColumn("result", DataTypes.createArrayType(DataTypes.StringType))
        .withRow("uc-null", null)
        .withRow("uc-codingA", Collections.singletonList("value_a"))
        .withRow("uc-codingB", Collections.emptyList())
        .build();

    DatasetAssert.of(resultA)
        .hasRows(expectedResultA);
  }

  @Test
  public void testPropertyWithLanguage() {
    TerminologyServiceHelpers.setupLookup(terminologyService)
        .withProperty(CODING_1, "property_a", "de", List.of(new StringType("value_a_de")))
        .withProperty(CODING_2, "property_b", "fr", List.of(new StringType("value_b_fr")));

    final Dataset<Row> ds = DatasetBuilder.of(spark)
        .withIdColumn("id")
        .withColumn("coding", CodingSchema.DATA_TYPE)
        .withRow("uc-null", null)
        .withRow("uc-codingA", toRow(CODING_1))
        .withRow("uc-codingB", toRow(CODING_2))
        .build();

    final Dataset<Row> resultA = ds.select(ds.col("id"),
        Terminology.property_of(ds.col("coding"), "property_a", FHIRDefinedType.STRING, "de")
            .alias("values"));

    final Dataset<Row> expectedResultA = DatasetBuilder.of(spark).withIdColumn("id")
        .withColumn("result", DataTypes.createArrayType(DataTypes.StringType))
        .withRow("uc-null", null)
        .withRow("uc-codingA", Collections.singletonList("value_a_de"))
        .withRow("uc-codingB", Collections.emptyList())
        .build();

    DatasetAssert.of(resultA)
        .hasRows(expectedResultA);

    final Dataset<Row> resultB = ds.select(ds.col("id"),
        Terminology.property_of(ds.col("coding"), "property_b", "string", "fr")
            .alias("values"));

    final Dataset<Row> expectedResultB = DatasetBuilder.of(spark).withIdColumn("id")
        .withColumn("result", DataTypes.createArrayType(DataTypes.StringType))
        .withRow("uc-null", null)
        .withRow("uc-codingA", Collections.emptyList())
        .withRow("uc-codingB", Collections.singletonList("value_b_fr"))
        .build();

    DatasetAssert.of(resultB)
        .hasRows(expectedResultB);
  }


  @Test
  void testInputErrorForPropertyOfUnknownType() {
    final Column nullColumn = lit(null);
    assertEquals(
        "Type: 'instant' is not supported for 'property' udf",
        assertThrows(InvalidUserInputError.class,
            () -> Terminology.property_of(nullColumn, "display",
                FHIRDefinedType.INSTANT)).getMessage()
    );
    assertEquals(
        "Type: 'Quantity' is not supported for 'property' udf",
        assertThrows(InvalidUserInputError.class,
            () -> Terminology.property_of(nullColumn, "display", "Quantity")).getMessage()
    );

    assertEquals(
        "Unknown FHIRDefinedType code 'NotAFhirType'",
        assertThrows(InvalidUserInputError.class,
            () -> Terminology.property_of(nullColumn, "display", "NotAFhirType")).getMessage()
    );
  }


  @Nonnull
  private Dataset<Row> callWithDesignationTestData(@Nonnull final Function<Column, Column> call) {
    setupDesignationExpectations();

    final Dataset<Row> ds = DatasetBuilder.of(spark)
        .withIdColumn("id")
        .withColumn("coding", CodingSchema.DATA_TYPE)
        .withRow("uc-null", null)
        .withRow("uc-invalid", toRow(INVALID_CODING_0))
        .withRow("uc-codingA", toRow(CODING_1))
        .withRow("uc-codingB", toRow(CODING_2))
        .build();

    return ds.select(ds.col("id"),
        call.apply(ds.col("coding")).alias("values"));
  }

  @Test
  public void testDesignationWithLanguage() {

    final Dataset<Row> resultA = callWithDesignationTestData(
        coding -> designation(coding, USE_A, LANGUAGE_X));

    final Dataset<Row> expectedResultA = DatasetBuilder.of(spark).withIdColumn("id")
        .withColumn("result", DataTypes.createArrayType(DataTypes.StringType))
        .withRow("uc-null", null)
        .withRow("uc-invalid", Collections.emptyList())
        .withRow("uc-codingA", Collections.singletonList("designation_A_X"))
        .withRow("uc-codingB", Collections.emptyList())
        .build();

    DatasetAssert.of(resultA)
        .hasRows(expectedResultA);
  }


  @Test
  public void testDesignationWithNullLanguageAndUseColumn() {

    final Dataset<Row> resultA = callWithDesignationTestData(
        coding -> designation(coding,
            toLiteralColumn(USE_B),
            null));

    final Dataset<Row> expectedResultA = DatasetBuilder.of(spark).withIdColumn("id")
        .withColumn("result", DataTypes.createArrayType(DataTypes.StringType))
        .withRow("uc-null", null)
        .withRow("uc-invalid", Collections.emptyList())
        .withRow("uc-codingA", Collections.emptyList())
        .withRow("uc-codingB", List.of("designation_B_X.1", "designation_B_X.2", "designation_B_Y"))
        .build();

    DatasetAssert.of(resultA)
        .hasRows(expectedResultA);
  }


  @Test
  public void testDesignationWithNoLanguageAndAlternativeDisplayName() {

    final Dataset<Row> resultA = callWithDesignationTestData(
        coding -> designation(coding, USE_A.copy().setDisplay("Alternative Display A")));

    final Dataset<Row> expectedResultA = DatasetBuilder.of(spark).withIdColumn("id")
        .withColumn("result", DataTypes.createArrayType(DataTypes.StringType))
        .withRow("uc-null", null)
        .withRow("uc-invalid", Collections.emptyList())
        .withRow("uc-codingA", List.of("designation_A_X", "designation_A_Y"))
        .withRow("uc-codingB", Collections.emptyList())
        .build();

    DatasetAssert.of(resultA)
        .hasRows(expectedResultA);
  }

  @Test
  public void testDesignationWithNoUse() {

    //noinspection Convert2MethodRef
    final Dataset<Row> resultA = callWithDesignationTestData(Terminology::designation);

    final Dataset<Row> expectedResultA = DatasetBuilder.of(spark).withIdColumn("id")
        .withColumn("result", DataTypes.createArrayType(DataTypes.StringType))
        .withRow("uc-null", null)
        .withRow("uc-invalid", Collections.emptyList())
        .withRow("uc-codingA", List.of("designation_A_X", "designation_A_Y"))
        .withRow("uc-codingB", List.of("designation_B_X.1", "designation_B_X.2", "designation_B_Y"))
        .build();

    DatasetAssert.of(resultA)
        .hasRows(expectedResultA);
  }

}
