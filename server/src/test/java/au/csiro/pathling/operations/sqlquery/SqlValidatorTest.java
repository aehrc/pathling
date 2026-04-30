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

package au.csiro.pathling.operations.sqlquery;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import au.csiro.pathling.test.SpringBootUnitTest;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/** Unit tests for {@link SqlValidator}. */
@Import(SqlValidator.class)
@SpringBootUnitTest
class SqlValidatorTest {

  @Autowired private SqlValidator sqlValidator;

  // -------------------------------------------------------------------------
  // Valid SQL queries — should not throw.
  // -------------------------------------------------------------------------

  @Test
  void acceptsSimpleSelect() {
    assertThatCode(() -> sqlValidator.validate("SELECT 1")).doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithAlias() {
    assertThatCode(() -> sqlValidator.validate("SELECT 1 AS value")).doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectFromTable() {
    assertThatCode(() -> sqlValidator.validate("SELECT * FROM my_view")).doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithWhere() {
    assertThatCode(() -> sqlValidator.validate("SELECT a, b FROM t WHERE a > 10"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithJoin() {
    assertThatCode(
            () -> sqlValidator.validate("SELECT a.id, b.name FROM a JOIN b ON a.id = b.a_id"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithAggregation() {
    assertThatCode(() -> sqlValidator.validate("SELECT count(*), sum(x) FROM t GROUP BY y"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithSubquery() {
    assertThatCode(() -> sqlValidator.validate("SELECT * FROM (SELECT 1 AS x) sub"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithCte() {
    assertThatCode(() -> sqlValidator.validate("WITH cte AS (SELECT 1 AS x) SELECT * FROM cte"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithOrderByAndLimit() {
    assertThatCode(() -> sqlValidator.validate("SELECT * FROM t ORDER BY id LIMIT 10"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithCaseWhen() {
    assertThatCode(
            () ->
                sqlValidator.validate(
                    "SELECT CASE WHEN x > 0 THEN 'positive' ELSE 'negative' END FROM t"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithStringFunctions() {
    assertThatCode(
            () -> sqlValidator.validate("SELECT upper(name), lower(name), length(name) FROM t"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithMathFunctions() {
    assertThatCode(() -> sqlValidator.validate("SELECT abs(-1), sqrt(4), round(3.14, 1)"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithDateFunctions() {
    assertThatCode(() -> sqlValidator.validate("SELECT current_date(), current_timestamp()"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithUnion() {
    assertThatCode(() -> sqlValidator.validate("SELECT 1 AS x UNION ALL SELECT 2 AS x"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithWindowFunction() {
    assertThatCode(
            () -> sqlValidator.validate("SELECT id, row_number() OVER (ORDER BY id) AS rn FROM t"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithDistinct() {
    assertThatCode(() -> sqlValidator.validate("SELECT DISTINCT name FROM t"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithInClause() {
    assertThatCode(() -> sqlValidator.validate("SELECT * FROM t WHERE id IN (1, 2, 3)"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithCast() {
    assertThatCode(() -> sqlValidator.validate("SELECT CAST(x AS STRING) FROM t"))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsSelectWithCoalesce() {
    assertThatCode(() -> sqlValidator.validate("SELECT coalesce(a, b, 'default') FROM t"))
        .doesNotThrowAnyException();
  }

  // -------------------------------------------------------------------------
  // Invalid SQL — should reject DDL and DML.
  // -------------------------------------------------------------------------

  @Test
  void rejectsDropTable() {
    assertThatThrownBy(() -> sqlValidator.validate("DROP TABLE my_table"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed");
  }

  @Test
  void rejectsCreateTable() {
    assertThatThrownBy(() -> sqlValidator.validate("CREATE TABLE my_table (id INT)"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed");
  }

  @Test
  void rejectsInsertInto() {
    assertThatThrownBy(() -> sqlValidator.validate("INSERT INTO my_table VALUES (1, 'a')"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed");
  }

  @Test
  void rejectsDeleteFrom() {
    assertThatThrownBy(() -> sqlValidator.validate("DELETE FROM my_table WHERE id = 1"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed");
  }

  @Test
  void rejectsUpdateStatement() {
    assertThatThrownBy(() -> sqlValidator.validate("UPDATE my_table SET name = 'x' WHERE id = 1"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed");
  }

  @Test
  void rejectsCreateView() {
    assertThatThrownBy(() -> sqlValidator.validate("CREATE VIEW my_view AS SELECT 1"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed");
  }

  // -------------------------------------------------------------------------
  // Invalid SQL — should reject dangerous functions.
  // -------------------------------------------------------------------------

  @Test
  void rejectsReflectFunction() {
    assertThatThrownBy(
            () -> sqlValidator.validate("SELECT reflect('java.lang.Runtime', 'getRuntime') FROM t"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed function");
  }

  @Test
  void rejectsJavaMethodFunction() {
    assertThatThrownBy(
            () -> sqlValidator.validate("SELECT java_method('java.lang.Math', 'random') FROM t"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("disallowed function");
  }

  // -------------------------------------------------------------------------
  // Invalid SQL syntax.
  // -------------------------------------------------------------------------

  @Test
  void rejectsInvalidSqlSyntax() {
    assertThatThrownBy(() -> sqlValidator.validate("NOT VALID SQL AT ALL"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Invalid SQL syntax");
  }

  // -------------------------------------------------------------------------
  // Regression corpus — known-bad queries the validator must reject.
  // -------------------------------------------------------------------------

  /**
   * Documents the queries that must always be rejected. New entries here lock current rejection
   * behaviour into the test suite and double as a security-model checklist for future readers.
   */
  @org.junit.jupiter.params.ParameterizedTest(name = "rejects: {0}")
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {
        // Hive metastore commands.
        "LOAD DATA INPATH '/tmp/x' INTO TABLE my_table",
        "MSCK REPAIR TABLE my_table",
        // Session configuration.
        "SET spark.sql.shuffle.partitions = 200",
        "SET",
        "RESET",
        // INSERT variants.
        "INSERT OVERWRITE TABLE my_table SELECT 1",
        "INSERT INTO my_table SELECT 1",
        // Data manipulation.
        "TRUNCATE TABLE my_table",
        "ALTER TABLE my_table ADD COLUMNS (x INT)",
        "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET"
            + " target.x = source.x",
        // Catalog management.
        "CREATE DATABASE my_db",
        "DROP DATABASE my_db",
        "USE my_db",
        // Cache control.
        "CACHE TABLE my_table",
        "UNCACHE TABLE my_table",
        "REFRESH TABLE my_table",
        // Discovery / SHOW.
        "SHOW TABLES",
        "DESCRIBE my_table",
        "EXPLAIN SELECT 1",
        // Reflection-style functions.
        "SELECT reflect('java.lang.Runtime', 'getRuntime')",
        "SELECT java_method('java.lang.Math', 'random')"
      })
  void rejectsKnownDangerousQueries(final String sql) {
    assertThatThrownBy(() -> sqlValidator.validate(sql))
        .as("validator must reject: %s", sql)
        .isInstanceOf(InvalidRequestException.class);
  }
}
