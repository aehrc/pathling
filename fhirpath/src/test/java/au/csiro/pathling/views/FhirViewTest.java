package au.csiro.pathling.views;

import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.assertions.DatasetAssert;
import ca.uhn.fhir.context.FhirContext;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

@SpringBootUnitTest
class FhirViewTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  @Autowired
  FhirEncoders fhirEncoders;

  @MockBean
  TerminologyServiceFactory terminologyServiceFactory;

  FhirViewExecutor executor;

  static final Path TEST_DATA_PATH = Path.of(
      "src/test/resources/test-data/views").toAbsolutePath().normalize();

  @BeforeEach
  void setUp() {
    final QueryConfiguration queryConfiguration = QueryConfiguration.builder().build();
    final DataSource dataSource = Database.forFileSystem(spark, fhirEncoders,
        TEST_DATA_PATH.toUri().toString(), true);
    executor = new FhirViewExecutor(queryConfiguration, fhirContext, spark, dataSource,
        Optional.of(terminologyServiceFactory));
  }

  // Test 1:
  // Select ID, gender and birth date for each patient.
  // {
  //   "resource": "Patient",
  //   "columns": [
  //     {
  //       "name": "id",
  //       "expr": "id"
  //     },
  //     {
  //       "name": "gender",
  //       "expr": "gender"
  //     },
  //     {
  //       "name": "birth_date",
  //       "expr": "birthDate"
  //     }
  //   ]
  // }
  @Test
  void test1() {
    final FhirView view = new FhirView(ResourceType.PATIENT,
        List.of(
            new NamedExpression("id", "id"),
            new NamedExpression("gender", "gender"),
            new NamedExpression("birthDate", "birth_date")
        ), List.of(), List.of());
    final Dataset<Row> result = executor.buildQuery(view);

    // Expected result:
    // | id | gender | birth_date |
    // |----|--------|------------|
    // | 1  | female | 1959-09-27 |
    // | 2  | male   | 1983-09-06 |
    DatasetAssert.of(result).hasRows(
        RowFactory.create("1", "female", "1959-09-27"),
        RowFactory.create("2", "male", "1983-09-06")
    );
  }

  // Test 2:
  // Select ID, name use and family name for each patient.
  // {
  //   "resource": "Patient",
  //   "vars": [
  //     {
  //       "name": "name",
  //       "expr": "name",
  //       "whenMany": "unnest"
  //     }
  //   ],
  //   "columns": [
  //     {
  //       "name": "id",
  //       "expr": "id"
  //     },
  //     {
  //       "name": "name_use",
  //       "expr": "%name.use"
  //     },
  //     {
  //       "name": "family_name",
  //       "expr": "%name.family"
  //     }
  //   ]
  // }
  @Test
  void test2() {
    final FhirView view = new FhirView(ResourceType.PATIENT,
        List.of(
            new NamedExpression("id", "id"),
            new NamedExpression("%name.use", "name_use"),
            new NamedExpression("%name.family", "family_name")
        ),
        List.of(
            new VariableExpression("name", "name", WhenMany.UNNEST)
        ),
        List.of());
    final Dataset<Row> result = executor.buildQuery(view);

    // Expected result:
    // | id | name_use | family_name |
    // |----|----------|-------------|
    // | 1  | maiden   | Wuckert     |
    // | 1  | official | Oberbrunner |
    // | 2  | nickname | Cleveland   |
    // | 2  | official | Towne       |
    DatasetAssert.of(result).hasRowsUnordered(
        RowFactory.create("1", "maiden", "Wuckert"),
        RowFactory.create("1", "official", "Oberbrunner"),
        RowFactory.create("2", "nickname", "Cleveland"),
        RowFactory.create("2", "official", "Towne")
    );
  }

  // Test 3:
  // Select ID, family name and given name for each patient.
  // {
  //   "resource": "Patient",
  //   "vars": [
  //     {
  //       "name": "name",
  //       "expr": "name",
  //       "whenMany": "unnest"
  //     },
  //     {
  //       "name": "givenName",
  //       "expr": "name.given",
  //       "whenMany": "unnest"
  //     }
  //   ],
  //   "columns": [
  //     {
  //       "name": "id",
  //       "expr": "id"
  //     },
  //     {
  //       "name": "family_name",
  //       "expr": "%name.family"
  //     },
  //     {
  //       "name": "given_name",
  //       "expr": "%givenName"
  //     }
  //   ]
  // }
  @Test
  void test3() {
    final FhirView view = new FhirView(ResourceType.PATIENT,
        List.of(
            new NamedExpression("id", "id"),
            new NamedExpression("%name.family", "family_name"),
            new NamedExpression("%givenName", "given_name")
        ),
        List.of(
            new VariableExpression("name", "name", WhenMany.UNNEST),
            new VariableExpression("name.given", "givenName", WhenMany.UNNEST)
        ),
        List.of());
    final Dataset<Row> result = executor.buildQuery(view);

    // Expected result:
    // | id | family_name | given_name |
    // |----|-------------|------------|
    // | 1  | Wuckert     | Karina     |
    // | 1  | Oberbrunner | Karina     |
    // | 2  | Towne       | Guy        |
    // | 2  | Cleveland   | Maponos    |
    // | 2  | Cleveland   | Wilburg    |
    DatasetAssert.of(result).hasRowsUnordered(
        RowFactory.create("1", "Wuckert", "Karina"),
        RowFactory.create("1", "Oberbrunner", "Karina"),
        RowFactory.create("2", "Towne", "Guy"),
        RowFactory.create("2", "Cleveland", "Maponos"),
        RowFactory.create("2", "Cleveland", "Wilburg")
    );
  }

  // Test 4:
  // Select ID, name prefix, family name, marital status system and marital status code for each 
  // patient.
  // {
  //   "resource": "Patient",
  //   "vars": [
  //     {
  //       "name": "name",
  //       "expr": "name",
  //       "whenMany": "unnest"
  //     },
  //     {
  //       "name": "namePrefix",
  //       "expr": "name.prefix",
  //       "whenMany": "unnest"
  //     },
  //     {
  //       "name": "maritalStatus",
  //       "expr": "maritalStatus.coding",
  //       "whenMany": "unnest"
  //     }
  //   ],
  //   "columns": [
  //     {
  //       "name": "id",
  //       "expr": "id"
  //     },
  //     {
  //       "name": "name_prefix",
  //       "expr": "%namePrefix"
  //     },
  //     {
  //       "name": "family_name",
  //       "expr": "%name.family"
  //     },
  //     {
  //       "name": "marital_status_system",
  //       "expr": "%maritalStatus.system"
  //     },
  //     {
  //       "name": "marital_status_code",
  //       "expr": "%maritalStatus.code"
  //     }
  //   ]
  // }
  @Test
  void test4() {
    final FhirView view = new FhirView(ResourceType.PATIENT,
        List.of(
            new NamedExpression("id", "id"),
            new NamedExpression("%namePrefix", "name_prefix"),
            new NamedExpression("%name.family", "family_name"),
            new NamedExpression("%maritalStatus.system", "marital_status_system"),
            new NamedExpression("%maritalStatus.code", "marital_status_code")
        ),
        List.of(
            new VariableExpression("name", "name", WhenMany.UNNEST),
            new VariableExpression("name.prefix", "namePrefix", WhenMany.UNNEST),
            new VariableExpression("maritalStatus.coding", "maritalStatus", WhenMany.UNNEST)
        ),
        List.of());
    final Dataset<Row> result = executor.buildQuery(view);

    // Expected result:
    // | id | name_prefix | family_name | marital_status_system                                  | marital_status_code |
    // |----|-------------|-------------|--------------------------------------------------------|---------------------|
    // | 1  | Miss.       | Wuckert     | http://terminology.hl7.org/CodeSystem/v3-MaritalStatus | M                   |
    // | 1  | Miss.       | Wuckert     | http://snomed.info/sct                                 | 87915002            |
    // | 1  | Mrs.        | Oberbrunner | http://terminology.hl7.org/CodeSystem/v3-MaritalStatus | M                   |
    // | 1  | Mrs.        | Oberbrunner | http://snomed.info/sct                                 | 87915002            |
    // | 2  | Mr.         | Towne       | NULL                                                   | NULL                |
    // | 2  | Prof.       | Cleveland   | NULL                                                   | NULL                |
    DatasetAssert.of(result).hasRowsUnordered(
        RowFactory.create("1", "Miss.", "Wuckert",
            "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus", "M"),
        RowFactory.create("1", "Miss.", "Wuckert", "http://snomed.info/sct", "87915002"),
        RowFactory.create("1", "Mrs.", "Oberbrunner",
            "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus", "M"),
        RowFactory.create("1", "Mrs.", "Oberbrunner", "http://snomed.info/sct", "87915002"),
        RowFactory.create("2", "Mr.", "Towne", null, null),
        RowFactory.create("2", "Prof.", "Cleveland", null, null)
    );
  }

}
