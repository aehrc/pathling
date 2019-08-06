/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import static au.csiro.clinsight.TestConfiguration.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import au.csiro.clinsight.fhir.AnalyticsServerConfiguration;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalog.Catalog;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.eclipse.jetty.server.Server;
import org.hl7.fhir.dstu3.model.ConceptMap;
import org.hl7.fhir.dstu3.model.ConceptMap.ConceptMapGroupComponent;
import org.hl7.fhir.dstu3.model.ConceptMap.SourceElementComponent;
import org.hl7.fhir.dstu3.model.ConceptMap.TargetElementComponent;
import org.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

/**
 * @author John Grimes
 */
public class SubsumesTest {

  private static final String QUERY_URL = FHIR_SERVER_URL + "/$aggregate-query";
  private Server server;
  private TerminologyClient mockTerminologyClient;
  private SparkSession mockSpark;
  private CloseableHttpClient httpClient;

  @Before
  public void setUp() throws Exception {
    mockTerminologyClient = mock(TerminologyClient.class);
    mockSpark = mock(SparkSession.class);
    mockDefinitionRetrieval(mockTerminologyClient);

    Catalog mockCatalog = mock(Catalog.class);
    when(mockSpark.catalog()).thenReturn(mockCatalog);
    when(mockCatalog.tableExists(any(), any())).thenReturn(false);

    AnalyticsServerConfiguration configuration = new AnalyticsServerConfiguration();
    configuration.setTerminologyClient(mockTerminologyClient);
    configuration.setSparkSession(mockSpark);
    configuration.setExplainQueries(false);

    server = startFhirServer(configuration);
    httpClient = HttpClients.createDefault();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void subsumes() throws IOException, JSONException {
    String inParams = "{\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"subjectResource\",\n"
        + "      \"valueUri\": \"http://hl7.org/fhir/StructureDefinition/Patient\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.count()\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of patients\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.gender\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Gender\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"filter\",\n"
        + "      \"valueString\": \"%resource.reverseResolve(Condition.subject).code.subsumes(http://snomed.info/sct|9859006)\"\n"
        + "    }\n"
        + "  ],\n"
        + "  \"resourceType\": \"Parameters\"\n"
        + "}";

    String expectedResponse = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueCode\": \"female\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"result\",\n"
        + "          \"valueUnsignedInt\": 145\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueCode\": \"male\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"result\",\n"
        + "          \"valueUnsignedInt\": 49\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedSql1 = "SELECT DISTINCT b.system, b.code "
        + "FROM patient "
        + "LEFT JOIN condition a ON patient.id = a.subject.reference "
        + "LATERAL VIEW OUTER EXPLODE(a.code.coding) b AS b "
        + "WHERE b.system IS NOT NULL AND b.code IS NOT NULL";

    String expectedSql2 =
        "SELECT patient.gender AS `Gender`, "
            + "COUNT(DISTINCT patient.id) AS `Number of patients` "
            + "FROM patient "
            + "LEFT JOIN ("
            + "SELECT patient.id, "
            + "MAX(c.equivalence IN ('subsumes', 'equal')) AS result "
            + "FROM patient "
            + "LEFT JOIN condition a ON patient.id = a.subject.reference "
            + "LEFT JOIN ("
            + "SELECT condition.id, b.* "
            + "FROM condition "
            + "LATERAL VIEW OUTER EXPLODE(condition.code.coding) b AS b"
            + ") e ON a.id = e.id "
            + "LEFT JOIN closure_046e290 c "
            + "ON e.b.system = c.targetSystem AND e.b.code = c.targetCode "
            + "AND 'http://snomed.info/sct' = c.sourceSystem AND '9859006' = c.sourceCode "
            + "GROUP BY 1"
            + ") d ON patient.id = d.id "
            + "WHERE d.result "
            + "GROUP BY 1 "
            + "ORDER BY 1, 2";

    StructField[] fields1 = {
        new StructField("system", DataTypes.StringType, true, null),
        new StructField("code", DataTypes.StringType, true, null)
    };
    StructType structType1 = new StructType(fields1);
    List<Row> fakeResult1 = new ArrayList<>(Arrays.asList(
        new GenericRowWithSchema(
            new Object[]{"https://csiro.au/fhir/CodeSystem/foods", "zucchini"}, structType1),
        new GenericRowWithSchema(
            new Object[]{"https://csiro.au/fhir/CodeSystem/foods", "fruit"}, structType1),
        new GenericRowWithSchema(
            new Object[]{"https://csiro.au/fhir/CodeSystem/foods", "vegetable"}, structType1)));

    Dataset mockDataset1 = createMockDataset();
    when(mockSpark.sql(expectedSql1)).thenReturn(mockDataset1);
    when(mockDataset1.select(any(Column.class))).thenReturn(mockDataset1);
    when(mockDataset1.distinct()).thenReturn(mockDataset1);
    when(mockDataset1.col(any())).thenReturn(mock(Column.class));
    when(mockDataset1.filter(any(String.class))).thenReturn(mockDataset1);
    when(mockDataset1.collectAsList()).thenReturn(fakeResult1);

    StructField[] fields2 = {
        new StructField("Gender", DataTypes.StringType, true, null),
        new StructField("Number of patients", DataTypes.LongType, true, null)
    };
    StructType structType2 = new StructType(fields2);
    List<Row> fakeResult2 = new ArrayList<>(Arrays.asList(
        new GenericRowWithSchema(new Object[]{"female", 145L}, structType2),
        new GenericRowWithSchema(new Object[]{"male", 49L}, structType2)
    ));

    Dataset mockDataset2 = createMockDataset();
    when(mockSpark.createDataset(any(List.class), any(Encoder.class))).thenReturn(mockDataset2);

    Dataset mockDataset3 = createMockDataset();
    when(mockSpark.sql(expectedSql2)).thenReturn(mockDataset3);
    when(mockDataset3.collectAsList()).thenReturn(fakeResult2);

    ConceptMap fakeConceptMap = new ConceptMap();
    ConceptMapGroupComponent group = new ConceptMapGroupComponent();
    group.setSource("https://csiro.au/fhir/CodeSystem/foods");
    group.setTarget("https://csiro.au/fhir/CodeSystem/foods");
    fakeConceptMap.getGroup().add(group);

    SourceElementComponent element = new SourceElementComponent();
    element.setCode("zucchini");
    TargetElementComponent target = new TargetElementComponent();
    target.setCode("vegetable");
    element.getTarget().add(target);
    group.getElement().add(element);

    element = new SourceElementComponent();
    element.setCode("zucchini");
    target = new TargetElementComponent();
    target.setCode("fruit");
    element.getTarget().add(target);
    group.getElement().add(element);

    when(mockTerminologyClient.closure(any(), any(), any())).thenReturn(fakeConceptMap);

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
      StringWriter writer = new StringWriter();
      IOUtils.copy(response.getEntity().getContent(), writer, StandardCharsets.UTF_8);
      JSONAssert.assertEquals(expectedResponse, writer.toString(), true);
    }

    verify(mockDataset2).createOrReplaceTempView("closure_046e290");
    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql1);
    verify(mockSpark).sql(expectedSql2);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void subsumedBy() throws IOException {
    String inParams = "{\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"subjectResource\",\n"
        + "      \"valueUri\": \"http://hl7.org/fhir/StructureDefinition/Patient\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.count()\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of patients\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.gender\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Gender\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"filter\",\n"
        + "      \"valueString\": \"%resource.reverseResolve(Condition.subject).code.subsumedBy(http://snomed.info/sct|44054006)\"\n"
        + "    }\n"
        + "  ],\n"
        + "  \"resourceType\": \"Parameters\"\n"
        + "}";

    String expectedSql1 = "SELECT DISTINCT b.system, b.code "
        + "FROM patient "
        + "LEFT JOIN condition a ON patient.id = a.subject.reference "
        + "LATERAL VIEW OUTER EXPLODE(a.code.coding) b AS b "
        + "WHERE b.system IS NOT NULL AND b.code IS NOT NULL";

    String expectedSql2 =
        "SELECT patient.gender AS `Gender`, "
            + "COUNT(DISTINCT patient.id) AS `Number of patients` "
            + "FROM patient "
            + "LEFT JOIN ("
            + "SELECT patient.id, MAX(c.equivalence IN ('subsumes', 'equal')) AS result "
            + "FROM patient "
            + "LEFT JOIN condition a ON patient.id = a.subject.reference "
            + "LEFT JOIN ("
            + "SELECT condition.id, b.* FROM condition "
            + "LATERAL VIEW OUTER EXPLODE(condition.code.coding) b AS b"
            + ") e ON a.id = e.id "
            + "LEFT JOIN closure_29e9307 c "
            + "ON 'http://snomed.info/sct' = c.targetSystem AND '44054006' = c.targetCode "
            + "AND e.b.system = c.sourceSystem AND e.b.code = c.sourceCode "
            + "GROUP BY 1"
            + ") d ON patient.id = d.id "
            + "WHERE d.result "
            + "GROUP BY 1 "
            + "ORDER BY 1, 2";

    Dataset mockDataset1 = createMockDataset();
    when(mockSpark.sql(any())).thenReturn(mockDataset1);
    when(mockDataset1.select(any(Column.class))).thenReturn(mockDataset1);
    when(mockDataset1.distinct()).thenReturn(mockDataset1);
    when(mockDataset1.col(any())).thenReturn(mock(Column.class));
    when(mockDataset1.filter(any(String.class))).thenReturn(mockDataset1);
    when(mockDataset1.collectAsList()).thenReturn(new ArrayList());

    ConceptMap fakeConceptMap = new ConceptMap();
    when(mockTerminologyClient.closure(any(), any(), any())).thenReturn(fakeConceptMap);

    Dataset mockDataset2 = createMockDataset();
    when(mockSpark.createDataset(any(List.class), any(Encoder.class))).thenReturn(mockDataset2);

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    httpClient.execute(httpPost);

    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql1);
    verify(mockSpark).sql(expectedSql2);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void kidgenExample() throws IOException {
    String inParams = "{\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"subjectResource\",\n"
        + "      \"valueUri\": \"http://hl7.org/fhir/StructureDefinition/Patient\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.count()\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of patients\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.reverseResolve(Encounter.subject).where($this.type contains https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type-0|referral).reverseResolve(Condition.context).verificationStatus\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Referral verification status\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.reverseResolve(Encounter.subject).where($this.type contains https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type-0|pre-investigation).reverseResolve(Condition.context).verificationStatus\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Pre-investigation verification status\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.reverseResolve(Encounter.subject).where($this.type contains https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type-0|pre-investigation).reverseResolve(Condition.context).code.subsumedBy(%resource.reverseResolve(Encounter.subject).where($this.type contains https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type-0|referral).reverseResolve(Condition.context).code)\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Pre-investigation diagnosis more specific than referral diagnosis?\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.reverseResolve(Encounter.subject).where($this.type contains https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type-0|post-investigation).reverseResolve(Condition.context).verificationStatus\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Post-investigation verification status\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.reverseResolve(Encounter.subject).where($this.type contains https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type-0|post-investigation).reverseResolve(Condition.context).code.subsumedBy(%resource.reverseResolve(Encounter.subject).where($this.type contains https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type-0|pre-investigation).reverseResolve(Condition.context).code)\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Post-investigation diagnosis more specific than pre-investigation diagnosis?\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ],\n"
        + "  \"resourceType\": \"Parameters\"\n"
        + "}";

    String expectedSql1 = "SELECT DISTINCT y.system, y.code "
        + "FROM patient "
        + "LEFT JOIN ("
        + "SELECT patient.id, "
        + "IFNULL(MAX(u.system = 'https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type-0' AND u.code = 'referral'), FALSE) AS result "
        + "FROM patient "
        + "LEFT JOIN encounter s ON patient.id = s.subject.reference "
        + "LATERAL VIEW OUTER EXPLODE(s.type) t AS t "
        + "LATERAL VIEW OUTER EXPLODE(t.coding) u AS u "
        + "GROUP BY 1"
        + ") v ON patient.id = v.id "
        + "LEFT JOIN encounter w ON patient.id = w.subject.reference AND v.result "
        + "LEFT JOIN condition x ON w.id = x.context.reference "
        + "LATERAL VIEW OUTER EXPLODE(x.code.coding) y AS y "
        + "WHERE y.system IS NOT NULL AND y.code IS NOT NULL "
        + "UNION "
        + "SELECT DISTINCT z.system, z.code "
        + "FROM patient "
        + "LEFT JOIN ("
        + "SELECT patient.id, IFNULL(MAX(o.system = 'https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type-0' AND o.code = 'pre-investigation'), FALSE) AS result "
        + "FROM patient "
        + "LEFT JOIN encounter m ON patient.id = m.subject.reference "
        + "LATERAL VIEW OUTER EXPLODE(m.type) n AS n "
        + "LATERAL VIEW OUTER EXPLODE(n.coding) o AS o "
        + "GROUP BY 1"
        + ") p ON patient.id = p.id "
        + "LEFT JOIN encounter q ON patient.id = q.subject.reference AND p.result "
        + "LEFT JOIN condition r ON q.id = r.context.reference "
        + "LATERAL VIEW OUTER EXPLODE(r.code.coding) z AS z "
        + "WHERE z.system IS NOT NULL AND z.code IS NOT NULL";

    String expectedSql2 = "SELECT DISTINCT aw.system, aw.code "
        + "FROM patient "
        + "LEFT JOIN ("
        + "SELECT patient.id, "
        + "IFNULL(MAX(as.system = 'https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type-0' AND as.code = 'pre-investigation'), FALSE) AS result "
        + "FROM patient "
        + "LEFT JOIN encounter aq ON patient.id = aq.subject.reference "
        + "LATERAL VIEW OUTER EXPLODE(aq.type) ar AS ar "
        + "LATERAL VIEW OUTER EXPLODE(ar.coding) as AS as "
        + "GROUP BY 1"
        + ") at ON patient.id = at.id "
        + "LEFT JOIN encounter au ON patient.id = au.subject.reference AND at.result "
        + "LEFT JOIN condition av ON au.id = av.context.reference "
        + "LATERAL VIEW OUTER EXPLODE(av.code.coding) aw AS aw "
        + "WHERE aw.system IS NOT NULL AND aw.code IS NOT NULL "
        + "UNION "
        + "SELECT DISTINCT ax.system, ax.code "
        + "FROM patient "
        + "LEFT JOIN ("
        + "SELECT patient.id, "
        + "IFNULL(MAX(am.system = 'https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type-0' AND am.code = 'post-investigation'), FALSE) AS result "
        + "FROM patient "
        + "LEFT JOIN encounter ak ON patient.id = ak.subject.reference "
        + "LATERAL VIEW OUTER EXPLODE(ak.type) al AS al "
        + "LATERAL VIEW OUTER EXPLODE(al.coding) am AS am "
        + "GROUP BY 1"
        + ") an ON patient.id = an.id "
        + "LEFT JOIN encounter ao ON patient.id = ao.subject.reference AND an.result "
        + "LEFT JOIN condition ap ON ao.id = ap.context.reference "
        + "LATERAL VIEW OUTER EXPLODE(ap.code.coding) ax AS ax "
        + "WHERE ax.system IS NOT NULL AND ax.code IS NOT NULL";

    String expectedSql3 =
        "SELECT f.verificationStatus AS `Referral verification status`, "
            + "l.verificationStatus AS `Pre-investigation verification status`, "
            + "ab.result AS `Pre-investigation diagnosis more specific than referral diagnosis?`, "
            + "aj.verificationStatus AS `Post-investigation verification status`, "
            + "az.result AS `Post-investigation diagnosis more specific than pre-investigation diagnosis?`, "
            + "COUNT(DISTINCT patient.id) AS `Number of patients` "
            + "FROM patient "
            + "LEFT JOIN ("
            + "SELECT patient.id, "
            + "IFNULL(MAX(c.system = 'https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type-0' AND c.code = 'referral'), FALSE) AS result "
            + "FROM patient "
            + "LEFT JOIN encounter a ON patient.id = a.subject.reference "
            + "LATERAL VIEW OUTER EXPLODE(a.type) b AS b "
            + "LATERAL VIEW OUTER EXPLODE(b.coding) c AS c GROUP BY 1"
            + ") d ON patient.id = d.id "
            + "LEFT JOIN encounter e ON patient.id = e.subject.reference AND d.result "
            + "LEFT JOIN condition f ON e.id = f.context.reference "
            + "LEFT JOIN ("
            + "SELECT patient.id, "
            + "IFNULL(MAX(i.system = 'https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type-0' AND i.code = 'pre-investigation'), FALSE) AS result "
            + "FROM patient "
            + "LEFT JOIN encounter g ON patient.id = g.subject.reference "
            + "LATERAL VIEW OUTER EXPLODE(g.type) h AS h "
            + "LATERAL VIEW OUTER EXPLODE(h.coding) i AS i "
            + "GROUP BY 1"
            + ") j ON patient.id = j.id "
            + "LEFT JOIN encounter k ON patient.id = k.subject.reference AND j.result "
            + "LEFT JOIN condition l ON k.id = l.context.reference "
            + "LEFT JOIN ("
            + "SELECT patient.id, "
            + "MAX(aa.equivalence IN ('subsumes', 'equal')) AS result "
            + "FROM patient "
            + "LEFT JOIN ("
            + "SELECT patient.id, "
            + "IFNULL(MAX(u.system = 'https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type-0' AND u.code = 'referral'), FALSE) AS result "
            + "FROM patient "
            + "LEFT JOIN encounter s ON patient.id = s.subject.reference "
            + "LATERAL VIEW OUTER EXPLODE(s.type) t AS t "
            + "LATERAL VIEW OUTER EXPLODE(t.coding) u AS u "
            + "GROUP BY 1"
            + ") v ON patient.id = v.id "
            + "LEFT JOIN encounter w ON patient.id = w.subject.reference AND v.result "
            + "LEFT JOIN condition x ON w.id = x.context.reference "
            + "LEFT JOIN ("
            + "SELECT patient.id, "
            + "IFNULL(MAX(o.system = 'https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type-0' AND o.code = 'pre-investigation'), FALSE) AS result "
            + "FROM patient "
            + "LEFT JOIN encounter m ON patient.id = m.subject.reference "
            + "LATERAL VIEW OUTER EXPLODE(m.type) n AS n "
            + "LATERAL VIEW OUTER EXPLODE(n.coding) o AS o "
            + "GROUP BY 1"
            + ") p ON patient.id = p.id "
            + "LEFT JOIN encounter q ON patient.id = q.subject.reference AND p.result "
            + "LEFT JOIN condition r ON q.id = r.context.reference "
            + "LEFT JOIN (SELECT condition.id, z.* FROM condition "
            + "LATERAL VIEW OUTER EXPLODE(condition.code.coding) z AS z"
            + ") ac ON r.id = ac.id "
            + "LEFT JOIN ("
            + "SELECT condition.id, y.* "
            + "FROM condition "
            + "LATERAL VIEW OUTER EXPLODE(condition.code.coding) y AS y"
            + ") ad ON x.id = ad.id "
            + "LEFT JOIN closure_0375bab aa "
            + "ON ad.y.system = aa.targetSystem AND ad.y.code = aa.targetCode "
            + "AND ac.z.system = aa.sourceSystem AND ac.z.code = aa.sourceCode "
            + "GROUP BY 1"
            + ") ab ON patient.id = ab.id "
            + "LEFT JOIN ("
            + "SELECT patient.id, "
            + "IFNULL(MAX(ag.system = 'https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type-0' AND ag.code = 'post-investigation'), FALSE) AS result "
            + "FROM patient "
            + "LEFT JOIN encounter ae ON patient.id = ae.subject.reference "
            + "LATERAL VIEW OUTER EXPLODE(ae.type) af AS af "
            + "LATERAL VIEW OUTER EXPLODE(af.coding) ag AS ag "
            + "GROUP BY 1"
            + ") ah ON patient.id = ah.id "
            + "LEFT JOIN encounter ai ON patient.id = ai.subject.reference AND ah.result "
            + "LEFT JOIN condition aj ON ai.id = aj.context.reference "
            + "LEFT JOIN ("
            + "SELECT patient.id, "
            + "MAX(ay.equivalence IN ('subsumes', 'equal')) AS result "
            + "FROM patient "
            + "LEFT JOIN ("
            + "SELECT patient.id, "
            + "IFNULL(MAX(as.system = 'https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type-0' AND as.code = 'pre-investigation'), FALSE) AS result "
            + "FROM patient "
            + "LEFT JOIN encounter aq ON patient.id = aq.subject.reference "
            + "LATERAL VIEW OUTER EXPLODE(aq.type) ar AS ar "
            + "LATERAL VIEW OUTER EXPLODE(ar.coding) as AS as "
            + "GROUP BY 1"
            + ") at ON patient.id = at.id "
            + "LEFT JOIN encounter au ON patient.id = au.subject.reference AND at.result "
            + "LEFT JOIN condition av ON au.id = av.context.reference "
            + "LEFT JOIN ("
            + "SELECT patient.id, "
            + "IFNULL(MAX(am.system = 'https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type-0' AND am.code = 'post-investigation'), FALSE) AS result "
            + "FROM patient "
            + "LEFT JOIN encounter ak ON patient.id = ak.subject.reference "
            + "LATERAL VIEW OUTER EXPLODE(ak.type) al AS al "
            + "LATERAL VIEW OUTER EXPLODE(al.coding) am AS am "
            + "GROUP BY 1"
            + ") an ON patient.id = an.id "
            + "LEFT JOIN encounter ao ON patient.id = ao.subject.reference AND an.result "
            + "LEFT JOIN condition ap ON ao.id = ap.context.reference "
            + "LEFT JOIN ("
            + "SELECT condition.id, ax.* "
            + "FROM condition "
            + "LATERAL VIEW OUTER EXPLODE(condition.code.coding) ax AS ax"
            + ") ba ON ap.id = ba.id "
            + "LEFT JOIN ("
            + "SELECT condition.id, aw.* "
            + "FROM condition "
            + "LATERAL VIEW OUTER EXPLODE(condition.code.coding) aw AS aw"
            + ") bb ON av.id = bb.id "
            + "LEFT JOIN closure_1256445 ay "
            + "ON bb.aw.system = ay.targetSystem AND bb.aw.code = ay.targetCode "
            + "AND ba.ax.system = ay.sourceSystem AND ba.ax.code = ay.sourceCode "
            + "GROUP BY 1"
            + ") az ON patient.id = az.id "
            + "GROUP BY 1, 2, 3, 4, 5 "
            + "ORDER BY 1, 2, 3, 4, 5, 6";

    Dataset mockDataset1 = createMockDataset();
    when(mockSpark.sql(any())).thenReturn(mockDataset1);
    when(mockDataset1.select(any(Column.class))).thenReturn(mockDataset1);
    when(mockDataset1.distinct()).thenReturn(mockDataset1);
    when(mockDataset1.col(any())).thenReturn(mock(Column.class));
    when(mockDataset1.filter(any(String.class))).thenReturn(mockDataset1);
    when(mockDataset1.collectAsList()).thenReturn(new ArrayList());

    ConceptMap fakeConceptMap = new ConceptMap();
    when(mockTerminologyClient.closure(any(), any(), any())).thenReturn(fakeConceptMap);

    Dataset mockDataset2 = createMockDataset();
    when(mockSpark.createDataset(any(List.class), any(Encoder.class))).thenReturn(mockDataset2);

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    httpClient.execute(httpPost);

    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql1);
    verify(mockSpark).sql(expectedSql2);
    verify(mockSpark).sql(expectedSql3);
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
    httpClient.close();
  }

}
