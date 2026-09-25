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
 */
package au.csiro.pathling.sql

import au.csiro.pathling.encoders.AnsiTestSupport
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.junit.jupiter.api.{AfterAll, BeforeAll, TestInstance}

import java.nio.file.{Files, Path}
import java.util.Comparator

/**
 * A Spark session for the query-time expression tests, with helpers to read data back from Parquet
 * so that plans run over a real file scan rather than a local relation the optimiser can fold.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
trait SparkSessionSupport {

  protected var spark: SparkSession = _

  private var tempDir: Path = _

  @BeforeAll
  def setUpSpark(): Unit = {
    spark = AnsiTestSupport.configureAnsiMode(
      SparkSession.builder()
        .master("local[2]")
        .appName(getClass.getSimpleName)
        .config("spark.driver.bindAddress", "localhost")
        .config("spark.driver.host", "localhost")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate())
    tempDir = Files.createTempDirectory(getClass.getSimpleName)
  }

  @AfterAll
  def tearDownSpark(): Unit = {
    spark.stop()
    Files.walk(tempDir).sorted(Comparator.reverseOrder[Path]()).forEach(p => Files.delete(p))
  }

  /**
   * Writes the result of a SQL query to Parquet and reads it back.
   *
   * @param name the name of the directory to write to, unique within the test class
   * @param sql  the query producing the data
   * @return the data, read from Parquet
   */
  protected def parquet(name: String, sql: String): DataFrame = {
    val path = tempDir.resolve(name).toString
    if (!Files.exists(tempDir.resolve(name))) {
      spark.sql(sql).coalesce(1).write.parquet(path)
    }
    spark.read.parquet(path)
  }

  /** Collects the rows of a data frame as strings, sorted so that the comparison is stable. */
  protected def rows(df: DataFrame): Seq[String] =
    df.collect().toSeq.map(rowString).sorted

  /** Collects the rows of a data frame as strings, in the order the plan produces them. */
  protected def orderedRows(df: DataFrame): Seq[String] =
    df.collect().toSeq.map(rowString)

  protected def column(expression: Expression): Column = ExpressionColumns.column(expression)

  protected def expression(column: Column): Expression = ExpressionColumns.expression(column)

  private def rowString(row: Row): String = row.toString
}
