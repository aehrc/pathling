/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.parser;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CD_SNOMED_284551006;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CD_SNOMED_403190006;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.setOfSimpleFrom;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.QueryHelpers;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.TimingExtension;
import au.csiro.pathling.test.assertions.FhirPathAssertion;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author Piotr Szul
 */
@Disabled
@SpringBootTest
@Tag("UnitTest")
@ExtendWith(TimingExtension.class)
public class ProfTest {

  @Autowired
  private SparkSession spark;

  @Autowired
  private FhirContext fhirContext;

  @Autowired
  private TerminologyService terminologyService;

  @Autowired
  private TerminologyServiceFactory terminologyServiceFactory;

  @Autowired
  private IParser jsonParser;

  private Parser parser;
  private ResourceReader mockReader;

  @BeforeEach
  public void setUp() throws IOException {
    SharedMocks.resetAll();
    mockReader = mock(ResourceReader.class);
    mockResourceReader(ResourceType.PATIENT, ResourceType.CONDITION, ResourceType.ENCOUNTER,
        ResourceType.PROCEDURE, ResourceType.MEDICATIONREQUEST, ResourceType.OBSERVATION,
        ResourceType.DIAGNOSTICREPORT, ResourceType.ORGANIZATION);

    final ResourcePath subjectResource = ResourcePath
        .build(fhirContext, mockReader, ResourceType.PATIENT, ResourceType.PATIENT.toCode(), true);

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .terminologyClientFactory(terminologyServiceFactory)
        .resourceReader(mockReader)
        .inputContext(subjectResource)
        .build();
    parser = new Parser(parserContext);
  }

  private void mockResourceReader(final ResourceType... resourceTypes)
      throws MalformedURLException {
    for (final ResourceType resourceType : resourceTypes) {
      final File parquetFile =
          new File("src/test/resources/test-data/parquet/" + resourceType.toCode() + ".parquet");
      final URL parquetUrl = parquetFile.getAbsoluteFile().toURI().toURL();
      assertNotNull(parquetUrl);
      final Dataset<Row> dataset = spark.read().parquet(parquetUrl.toString());
      when(mockReader.read(resourceType)).thenReturn(dataset);
      when(mockReader.getAvailableResourceTypes())
          .thenReturn(new HashSet<>(Arrays.asList(resourceTypes)));
    }
  }

  @SuppressWarnings("rawtypes")
  private FhirPathAssertion assertThatResultOf(final String expression) {
    return assertThat(parser.parse(expression));
  }

  @SuppressWarnings({"rawtypes", "SameParameterValue"})
  @Nonnull
  private FhirPathAssertion assertThatResultOf(@Nonnull final ResourceType resourceType,
      @Nonnull final String expression) {
    final ResourcePath subjectResource = ResourcePath
        .build(fhirContext, mockReader, resourceType, resourceType.toCode(), true);

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .terminologyClientFactory(terminologyServiceFactory)
        .resourceReader(mockReader)
        .inputContext(subjectResource)
        .build();
    final Parser resourceParser = new Parser(parserContext);
    return assertThat(resourceParser.parse(expression));
  }


  @Test
  public void testProjection() {

    DatasetBuilder x = new DatasetBuilder(spark).withIdColumn();
    IntStream.range(0, 1000).mapToObj(String::valueOf).map(s -> "f_" + s)
        .forEach(s -> x.withColumn(s, DataTypes.StringType));
    Dataset d = x.build();
    //d.printSchema();

    Dataset ad = QueryHelpers.aliasAllColumns(d).getDataset();

    for (int i = 0; i < 100; i++) {
      //DatasetWithColumnMap result = QueryHelpers.createColumns(ad, functions.lit("xx"), functions.lit("yy"));
      ad.withColumns(
          scala.collection.JavaConverters.asScalaBuffer(Arrays.asList("xx", "yy")).toSeq(),
          scala.collection.JavaConverters
              .asScalaBuffer(Arrays.asList(functions.lit("xx"), functions.lit("yy"))).toSeq());
    }
  }


  @Test
  public void testExplode() {

    DatasetBuilder x = new DatasetBuilder(spark).withIdColumn();
    x.withIdColumn()
        .withColumn("test", DataTypes.createArrayType(DataTypes.StringType));

    Dataset d = x.build();
    final Column rid = functions.col(d.columns()[0]);
    d.printSchema();

    Column c = d.col("test");
    Column pe = functions.posexplode_outer(c).as(new String[]{"x1", "x2"});

    System.out.println(pe.expr());
    Column id = functions.array(functions.col("x1"), rid).alias("y1");

    Expression ne = id.expr();
    System.out.println(ne);
    System.out.println(ne.children());

    Column val = functions.col("x2").alias("y3");


    d.select(rid, pe).select(id, val).printSchema();
    d.select(rid, pe, id, val).select(id, val).collectAsList();

  }


  @Test
  public void testLazy() {
    Dataset<Long> ds = spark.range(100);
    final Column colId = functions.col("id");

    ds
        .withColumn("id", functions.col("id").plus("10"))
        .select(functions.col("id"), colId.plus(functions.lit(1)))
        .collectAsList().forEach(System.out::println);
  }


  @Test
  public void testObservations() {

    System.out
        .println(spark.conf().get(org.apache.spark.sql.internal.SQLConf.CODEGEN_FACTORY_MODE()));

    when(terminologyService.intersect(any(), any()))
        .thenReturn(setOfSimpleFrom(CD_SNOMED_403190006, CD_SNOMED_284551006));

    final String expression = "reverseResolve(Observation.subject).valueQuantity.value";
    final long startTime = System.currentTimeMillis();
    final FhirPathAssertion as = assertThatResultOf(expression);
    final long parserTime = System.currentTimeMillis();

    as.getFhirPath().getDataset().explain();

    System.out.format("Parsing time: %s\n", parserTime - startTime);
    as.selectOrderedResult()
        .debugAllRows();
    final long exectTime = System.currentTimeMillis();
    System.out.format("Exec time: %s\n", exectTime - parserTime);
    //    "iif(reverseResolve(Condition.subject).where($this.code.memberOf('http://snomed.info/sct?fhir_vs=refset/32570521000036109')).empty().not(), 2, 0)"
    //        + "+ iif(reverseResolve(Condition.subject).where($this.code.memberOf('http://snomed.info/sct?fhir_vs=refset/32570521000036109')).empty().not(), 2, 0)"

  }


  @Test
  public void testWhereWithMemberOf1() {

    System.out
        .println(spark.conf().get(org.apache.spark.sql.internal.SQLConf.CODEGEN_FACTORY_MODE()));

    when(terminologyService.intersect(any(), any()))
        .thenReturn(setOfSimpleFrom(CD_SNOMED_403190006, CD_SNOMED_284551006));

    final String expression = IntStream.range(0, 1).mapToObj(i -> String.format(
        "iif(reverseResolve(Condition.subject).where($this.code.memberOf('http://snomed.info/sct?fhir_vs=refset/%010d')).empty().not(), 2, 0)",
        i)).collect(
        Collectors.joining("\n+"));
    final long startTime = System.currentTimeMillis();
    final FhirPathAssertion as = assertThatResultOf(expression);
    final long parserTime = System.currentTimeMillis();

    as.getFhirPath().getDataset().explain();

    System.out.format("Parsing time: %s\n", parserTime - startTime);
    as.selectOrderedResult()
        .debugAllRows();
    final long exectTime = System.currentTimeMillis();
    System.out.format("Exec time: %s\n", exectTime - parserTime);
    //    "iif(reverseResolve(Condition.subject).where($this.code.memberOf('http://snomed.info/sct?fhir_vs=refset/32570521000036109')).empty().not(), 2, 0)"
    //        + "+ iif(reverseResolve(Condition.subject).where($this.code.memberOf('http://snomed.info/sct?fhir_vs=refset/32570521000036109')).empty().not(), 2, 0)"

  }

  @Test
  public void testWhereWithMemberOf10() {

    System.out
        .println(spark.conf().get(org.apache.spark.sql.internal.SQLConf.CODEGEN_FACTORY_MODE()));

    when(terminologyService.intersect(any(), any()))
        .thenReturn(setOfSimpleFrom(CD_SNOMED_403190006, CD_SNOMED_284551006));

    final String expression = IntStream.range(0, 10).mapToObj(i -> String.format(
        "iif(reverseResolve(Condition.subject).where($this.code.memberOf('http://snomed.info/sct?fhir_vs=refset/%010d')).empty().not(), 2, 0)",
        i)).collect(
        Collectors.joining("\n+"));
    final long startTime = System.currentTimeMillis();
    final FhirPathAssertion as = assertThatResultOf(expression);
    final long parserTime = System.currentTimeMillis();
    System.out.format("Parsing time: %s\n", parserTime - startTime);
    as.selectOrderedResult()
        .debugAllRows();
    final long exectTime = System.currentTimeMillis();
    System.out.format("Exec time: %s\n", exectTime - parserTime);
    //    "iif(reverseResolve(Condition.subject).where($this.code.memberOf('http://snomed.info/sct?fhir_vs=refset/32570521000036109')).empty().not(), 2, 0)"
    //        + "+ iif(reverseResolve(Condition.subject).where($this.code.memberOf('http://snomed.info/sct?fhir_vs=refset/32570521000036109')).empty().not(), 2, 0)"

  }

  @Test
  public void testWhereWithMemberOf20() {
    when(terminologyService.intersect(any(), any()))
        .thenReturn(setOfSimpleFrom(CD_SNOMED_403190006, CD_SNOMED_284551006));

    final String expression = IntStream.range(0, 20).mapToObj(i -> String.format(
        "iif(reverseResolve(Condition.subject).where($this.code.memberOf('http://snomed.info/sct?fhir_vs=refset/%010d')).empty().not(), 2, 0)",
        i)).collect(
        Collectors.joining("\n+"));
    final long startTime = System.currentTimeMillis();
    final FhirPathAssertion as = assertThatResultOf(expression);
    final long parserTime = System.currentTimeMillis();
    System.out.format("Parsing time: %s\n", parserTime - startTime);
    as.selectOrderedResult()
        .debugAllRows();
    final long exectTime = System.currentTimeMillis();
    System.out.format("Exec time: %s\n", exectTime - parserTime);
    //    "iif(reverseResolve(Condition.subject).where($this.code.memberOf('http://snomed.info/sct?fhir_vs=refset/32570521000036109')).empty().not(), 2, 0)"
    //        + "+ iif(reverseResolve(Condition.subject).where($this.code.memberOf('http://snomed.info/sct?fhir_vs=refset/32570521000036109')).empty().not(), 2, 0)"

  }


  @Test
  public void testWhereWithMemberOf30() {
    when(terminologyService.intersect(any(), any()))
        .thenReturn(setOfSimpleFrom(CD_SNOMED_403190006, CD_SNOMED_284551006));

    final String expression = IntStream.range(0, 30).mapToObj(i -> String.format(
        "iif(reverseResolve(Condition.subject).where($this.code.memberOf('http://snomed.info/sct?fhir_vs=refset/%010d')).empty().not(), 2, 0)",
        i)).collect(
        Collectors.joining("\n+"));
    final long startTime = System.currentTimeMillis();
    final FhirPathAssertion as = assertThatResultOf(expression);
    final long parserTime = System.currentTimeMillis();
    System.out.format("Parsing time: %s\n", parserTime - startTime);
    as.selectOrderedResult()
        .debugAllRows();
    final long exectTime = System.currentTimeMillis();
    System.out.format("Exec time: %s\n", exectTime - parserTime);
    //    "iif(reverseResolve(Condition.subject).where($this.code.memberOf('http://snomed.info/sct?fhir_vs=refset/32570521000036109')).empty().not(), 2, 0)"
    //        + "+ iif(reverseResolve(Condition.subject).where($this.code.memberOf('http://snomed.info/sct?fhir_vs=refset/32570521000036109')).empty().not(), 2, 0)"

  }

  @Test
  public void testWhereWithMemberOf40() {
    when(terminologyService.intersect(any(), any()))
        .thenReturn(setOfSimpleFrom(CD_SNOMED_403190006, CD_SNOMED_284551006));

    final String expression = IntStream.range(0, 40).mapToObj(i -> String.format(
        "iif(reverseResolve(Condition.subject).where($this.code.memberOf('http://snomed.info/sct?fhir_vs=refset/%010d')).empty().not(), 2, 0)",
        i)).collect(
        Collectors.joining("\n+"));
    System.out.format("Starting parsing\n");
    final long startTime = System.currentTimeMillis();
    final FhirPathAssertion as = assertThatResultOf(expression);
    final long parserTime = System.currentTimeMillis();
    System.out.format("Parsing time: %s\n", parserTime - startTime);
    as.selectOrderedResult()
        .debugAllRows();
    final long exectTime = System.currentTimeMillis();
    System.out.format("Exec time: %s\n", exectTime - parserTime);
    //    "iif(reverseResolve(Condition.subject).where($this.code.memberOf('http://snomed.info/sct?fhir_vs=refset/32570521000036109')).empty().not(), 2, 0)"
    //        + "+ iif(reverseResolve(Condition.subject).where($this.code.memberOf('http://snomed.info/sct?fhir_vs=refset/32570521000036109')).empty().not(), 2, 0)"

  }


  @Test
  public void testWhereWithMemberOf50() {
    when(terminologyService.intersect(any(), any()))
        .thenReturn(setOfSimpleFrom(CD_SNOMED_403190006, CD_SNOMED_284551006));

    final String expression = IntStream.range(0, 50).mapToObj(i -> String.format(
        "iif(reverseResolve(Condition.subject).where($this.code.memberOf('http://snomed.info/sct?fhir_vs=refset/%010d')).empty().not(), 2, 0)",
        i)).collect(
        Collectors.joining("\n+"));
    final long startTime = System.currentTimeMillis();
    final FhirPathAssertion as = assertThatResultOf(expression);
    final long parserTime = System.currentTimeMillis();
    System.out.format("Parsing time: %s\n", parserTime - startTime);
    as.selectOrderedResult()
        .debugAllRows();
    final long exectTime = System.currentTimeMillis();
    System.out.format("Exec time: %s\n", exectTime - parserTime);
    //    "iif(reverseResolve(Condition.subject).where($this.code.memberOf('http://snomed.info/sct?fhir_vs=refset/32570521000036109')).empty().not(), 2, 0)"
    //        + "+ iif(reverseResolve(Condition.subject).where($this.code.memberOf('http://snomed.info/sct?fhir_vs=refset/32570521000036109')).empty().not(), 2, 0)"

  }

}
