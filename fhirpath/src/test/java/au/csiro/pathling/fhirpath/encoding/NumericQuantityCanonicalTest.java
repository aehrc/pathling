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

package au.csiro.pathling.fhirpath.encoding;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;
import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.fhirpath.unit.UcumUnit;
import au.csiro.pathling.schema.annotation.kinds.QuantityCanonicalKind;
import au.csiro.pathling.test.SpringBootUnitTest;
import java.math.BigDecimal;
import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests that the canonical form of a number encoded as a quantity with unit "1", which is built
 * without UCUM, is the one UCUM computes from the same value.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
class NumericQuantityCanonicalTest {

  @Autowired SparkSession spark;

  @Test
  void canonicalFormIsTheOneUcumComputes() {
    final Dataset<Row> numbers =
        spark
            .createDataset(
                List.of(
                    "5",
                    "1.5",
                    "1.25",
                    "1.250000",
                    "0",
                    "-0.5",
                    "0.000001",
                    "100",
                    "-12345678901234567890123456.123456",
                    "99999999999999999999999999.999999",
                    // Beyond the twenty-six integer digits of the decimal type, so there is no
                    // value.
                    "123456789012345678901234567.5"),
                Encoders.STRING())
            .select(col("value").try_cast("decimal(38,10)").alias("number"));

    final Column quantity = QuantityEncoding.encodeNumeric(col("number"));
    final Column computed =
        QuantityCanonicalKind.INSTANCE
            .compute(
                struct(
                    quantity.getField(QuantityEncoding.VALUE_COLUMN).alias("value"),
                    quantity.getField(QuantityEncoding.SYSTEM_COLUMN).alias("system"),
                    quantity.getField(QuantityEncoding.CODE_COLUMN).alias("code")))
            .getField(QuantityEncoding.CANONICAL_EXACT_COLUMN);
    final List<Row> rows =
        numbers
            .select(
                col("number"),
                QuantityEncoding.canonical(quantity).alias("built"),
                computed.alias("computed"))
            .collectAsList();

    assertThat(rows).hasSize(11);
    rows.forEach(
        row ->
            assertThat(row.<Row>getAs("built"))
                .as("canonical form of %s", row.<BigDecimal>getAs("number"))
                .isEqualTo(row.<Row>getAs("computed")));
  }

  @Test
  void canonicalFormHasTheDigitsOfTheValue() {
    final Row canonical =
        spark
            .range(1)
            .select(QuantityEncoding.canonical(QuantityEncoding.encodeNumeric(lit(1.25))))
            .first()
            .getStruct(0);

    assertThat(canonical.getStruct(0).<BigDecimal>getAs(0)).isEqualTo(new BigDecimal("125"));
    assertThat(canonical.getStruct(0).getInt(1)).isEqualTo(2);
    assertThat(canonical.getString(1)).isEqualTo(UcumUnit.ONE.code());
  }
}
