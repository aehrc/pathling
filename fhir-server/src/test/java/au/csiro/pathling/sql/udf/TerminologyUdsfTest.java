package au.csiro.pathling.sql.udf;

import static au.csiro.pathling.sql.Terminology.member_of;
import static au.csiro.pathling.sql.Terminology.subsumed_by;
import static au.csiro.pathling.sql.Terminology.subsumes;
import static au.csiro.pathling.sql.Terminology.translate;
import static au.csiro.pathling.test.helpers.TestHelpers.LOINC_URL;
import static au.csiro.pathling.test.helpers.TestHelpers.SNOMED_URL;
import static org.apache.spark.sql.functions.lit;
import static org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence.RELATEDTO;

import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import au.csiro.pathling.terminology.TerminologyService2;
import au.csiro.pathling.terminology.TerminologyService2.Translation;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.assertions.DatasetAssert;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import ca.uhn.fhir.parser.IParser;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@Tag("UnitTest")
@SpringBootTest
public class TerminologyUdsfTest {

  @Autowired
  private SparkSession spark;

  @Autowired
  TerminologyService2 terminologyService;

  @Autowired
  IParser jsonParser;

  @BeforeEach
  void setUp() {
    SharedMocks.resetAll();
  }

  private final static Coding CODING_1 = new Coding(LOINC_URL, "10337-4",
      "Procollagen type I [Mass/volume] in Serum");
  private final static Coding CODING_2 = new Coding(LOINC_URL, "10428-1",
      "Varicella zoster virus immune globulin given [Volume]");
  private final static Coding CODING_3 = new Coding(LOINC_URL, "10555-1", null);
  private final static Coding CODING_4 = new Coding(LOINC_URL, "10665-8",
      "Fungus colony count [#/volume] in Unspecified specimen by Environmental culture");
  private final static Coding CODING_5 = new Coding(SNOMED_URL, "416399002",
      "Procollagen type I amino-terminal propeptide level");

  private final static String CODING_1_VALUE_SET_URI = "uiid:ValueSet_coding1";
  private final static String CODING_2_VALUE_SET_URI = "uiid:ValueSet_coding2";


  private static final Coding INVALID_CODING_0 = new Coding(null, null, "");
  private static final Coding INVALID_CODING_1 = new Coding("uiid:system", null, "");
  private static final Coding INVALID_CODING_2 = new Coding(null, "someCode", "");


  // helper functions
  private DatasetBuilder codingDatasetBuilder() {
    return DatasetBuilder.of(spark)
        .withIdColumn("id")
        .withColumn("code", CodingEncoding.DATA_TYPE);
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


  @Test
  public void testValidateCoding() {
    setupValidateCodingExpectations();
    final Dataset<Row> df = codingDatasetBuilder()
        .withRow("id-1", CodingEncoding.encode(CODING_1))
        .withRow("id-2", CodingEncoding.encode(CODING_2))
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
        .withRow("id-1", CodingEncoding.encode(CODING_1))
        .withRow("id-2", CodingEncoding.encode(CODING_2))
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
        .withColumn("codings", DataTypes.createArrayType(CodingEncoding.DATA_TYPE))
        .withRow("id-1", null)
        .withRow("id-2", Collections.emptyList())
        .withRow("id-3",
            Collections.singletonList(CodingEncoding.encode(CODING_1)))
        .withRow("id-4",
            Collections.singletonList(CodingEncoding.encode(CODING_2)))
        .withRow("id-5",
            Collections.singletonList(CodingEncoding.encode(CODING_3)))
        .withRow("id-6",
            Collections.singletonList(null))
        .withRow("id-7",
            Arrays.asList(CodingEncoding.encode(CODING_1), CodingEncoding.encode(CODING_5)))
        .withRow("id-8",
            Arrays.asList(CodingEncoding.encode(CODING_2), CodingEncoding.encode(CODING_5)))
        .withRow("id-9",
            Arrays.asList(CodingEncoding.encode(CODING_3), CodingEncoding.encode(CODING_5)))
        .withRow("id-a",
            Arrays.asList(CodingEncoding.encode(CODING_3), CodingEncoding.encode(CODING_5)))
        .withRow("id-b", Arrays.asList(CodingEncoding.encode(CODING_1), null))
        .withRow("id-c", Arrays.asList(CodingEncoding.encode(CODING_2), null))
        .withRow("id-d", Arrays.asList(CodingEncoding.encode(CODING_3), null))
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
        .withColumn("codings", DataTypes.createArrayType(CodingEncoding.DATA_TYPE))
        .withRow("id-1", null)
        .withRow("id-2", Collections.emptyList())
        .withRow("id-3",
            Collections.singletonList(CodingEncoding.encode(CODING_1)))
        .withRow("id-4",
            Arrays.asList(CodingEncoding.encode(CODING_1), CodingEncoding.encode(CODING_5)))
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
        .withRow("uc-coding_1", CodingEncoding.encode(CODING_1))
        .withRow("uc-coding_2", CodingEncoding.encode(CODING_2))
        .build();

    final Dataset<Row> result = ds.select(ds.col("id"),
        translate(
            ds.col("code"),
            "someUrl",
            false,
            "relatedto"));
    final Dataset<Row> expectedResult = DatasetBuilder.of(spark).withIdColumn("id")
        .withColumn("result", TranslateUdf.RETURN_TYPE)
        .withRow("uc-null", null)
        .withRow("uc-coding_1", CodingEncoding.encodeList(Arrays.asList(CODING_5, CODING_4)))
        .withRow("uc-coding_2", Collections.emptyList())
        .build();
    DatasetAssert.of(result).hasRows(expectedResult);
  }


  @Test
  public void testTranslateCodingArray() {
    setupTranslateExpectations();

    final Dataset<Row> ds = DatasetBuilder.of(spark)
        .withIdColumn("id")
        .withColumn("codings", DataTypes.createArrayType(CodingEncoding.DATA_TYPE))
        .withRow("uc-null", null)
        .withRow("uc-empty", Collections.emptyList())
        .withRow("uc-coding_1", Collections.singletonList(CodingEncoding.encode(CODING_1)))
        .withRow("uc-coding_2", Collections.singletonList(CodingEncoding.encode(CODING_2)))
        .withRow("uc-coding_1+coding_1", Arrays.asList(CodingEncoding.encode(CODING_1),
            CodingEncoding.encode(CODING_1)))
        .build();

    final Dataset<Row> result = ds.select(ds.col("id"),
        translate(
            ds.col("codings"),
            "someUrl",
            false,
            "relatedto"));
    final Dataset<Row> expectedResult = DatasetBuilder.of(spark).withIdColumn("id")
        .withColumn("result", TranslateUdf.RETURN_TYPE)
        .withRow("uc-null", null)
        .withRow("uc-empty", Collections.emptyList())
        .withRow("uc-coding_1", CodingEncoding.encodeList(Arrays.asList(CODING_5, CODING_4)))
        .withRow("uc-coding_2", Collections.emptyList())
        .withRow("uc-coding_1+coding_1",
            CodingEncoding.encodeList(Arrays.asList(CODING_5, CODING_4)))
        .build();
    DatasetAssert.of(result).hasRows(expectedResult);
  }

  @Nullable
  private static Row toRow(@Nonnull Coding coding) {
    return CodingEncoding.encode(coding);
  }

  @Nonnull
  private static List<Row> toArray(@Nonnull Coding... codings) {
    return Stream.of(codings).map(CodingEncoding::encode).collect(Collectors.toList());
  }

  @Test
  public void testSubsumes() {

    setupSubsumesExpectations();

    final Dataset<Row> ds = DatasetBuilder.of(spark)
        .withIdColumn("id")
        .withColumn("codingA", CodingEncoding.DATA_TYPE)
        .withColumn("codingsB", DataTypes.createArrayType(CodingEncoding.DATA_TYPE))
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
        .withColumn("codingA", CodingEncoding.DATA_TYPE)
        .withColumn("codingsB", DataTypes.createArrayType(CodingEncoding.DATA_TYPE))
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
}
 