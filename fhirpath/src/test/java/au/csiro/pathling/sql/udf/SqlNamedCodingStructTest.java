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

package au.csiro.pathling.sql.udf;

import static au.csiro.pathling.sql.Terminology.display;
import static au.csiro.pathling.sql.Terminology.member_of;
import static au.csiro.pathling.test.helpers.TestHelpers.LOINC_URL;
import static org.apache.spark.sql.functions.expr;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.assertions.DatasetAssert;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Coding;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests the terminology functions in Spark against a Coding built with the SQL {@code named_struct}
 * function, as the R API's {@code tx_to_coding} builds it.
 *
 * <p>That function names the fields of the Coding, and leaves its id, display and userSelected as
 * untyped nulls, as the Python API's {@code to_coding} does.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
class SqlNamedCodingStructTest {

  private static final String VALUE_SET_URL = "uuid:vs";

  private static final Coding CODING_1 = new Coding(LOINC_URL, "10337-4", null);

  private static final Coding CODING_2 = new Coding(LOINC_URL, "10428-1", null);

  @Autowired private SparkSession spark;

  @Autowired private TerminologyService terminologyService;

  @BeforeEach
  void setUp() {
    SharedMocks.resetAll();
  }

  @Test
  void memberOfAcceptsANamedStructCoding() {
    TerminologyServiceHelpers.setupValidate(terminologyService)
        .withValueSet(VALUE_SET_URL, CODING_1);
    final Dataset<Row> codings = withNamedStructCoding();

    final Dataset<Row> result = codings.select(member_of(codings.col("coding"), VALUE_SET_URL));

    DatasetAssert.of(result)
        .hasRows(
            RowFactory.create(true), RowFactory.create(false), RowFactory.create((Boolean) null));
  }

  @Test
  void displayAcceptsANamedStructCoding() {
    TerminologyServiceHelpers.setupLookup(terminologyService)
        .withDisplay(CODING_1, "Display 1")
        .withDisplay(CODING_2, "Display 2");
    final Dataset<Row> codings = withNamedStructCoding();

    final Dataset<Row> result = codings.select(display(codings.col("coding")));

    DatasetAssert.of(result)
        .hasRows(
            RowFactory.create("Display 1"),
            RowFactory.create("Display 2"),
            RowFactory.create((String) null));
  }

  /**
   * Builds a Coding column from a code column with the SQL that {@code tx_to_coding} translates to,
   * and checks that its fields carry the Coding field names.
   */
  private Dataset<Row> withNamedStructCoding() {
    final Dataset<Row> codes =
        DatasetBuilder.of(spark)
            .withIdColumn("id")
            .withColumn("code", DataTypes.StringType)
            .withRow("id-1", CODING_1.getCode())
            .withRow("id-2", CODING_2.getCode())
            .withRow("id-3", null)
            .build();
    final Dataset<Row> result =
        codes.withColumn(
            "coding",
            expr(
                "CASE WHEN code IS NOT NULL THEN named_struct('id', NULL, 'system', string('"
                    + LOINC_URL
                    + "'), 'version', string(NULL), 'code', string(code), 'display', NULL,"
                    + " 'userSelected', NULL) ELSE NULL END"));
    final StructType codingType = (StructType) result.schema().apply("coding").dataType();
    assertArrayEquals(
        new String[] {"id", "system", "version", "code", "display", "userSelected"},
        codingType.fieldNames());
    return result;
  }
}
