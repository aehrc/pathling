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

package au.csiro.pathling.views.ansi;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.views.ansi.generated.AnsiSqlDataTypeLexer;
import au.csiro.pathling.views.ansi.generated.AnsiSqlDataTypeParser;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.spark.sql.types.DataType;

/** Parser for ANSI SQL type hints that produces Spark SQL DataTypes. */
@Slf4j
public class AnsiSqlTypeParser {

  private AnsiSqlTypeParser() {}

  /**
   * Parse an ANSI SQL type string and convert it to a Spark SQL DataType.
   *
   * @param typeString the ANSI SQL type string to parse
   * @return an Optional containing the corresponding Spark SQL DataType, or empty if parsing failed
   */
  @Nonnull
  public static DataType parseType(@Nonnull final String typeString) throws InvalidUserInputError {
    // Set up the lexer and parser
    final CharStream input = CharStreams.fromString(typeString);
    final AnsiSqlDataTypeLexer lexer = new AnsiSqlDataTypeLexer(input);
    final CommonTokenStream tokens = new CommonTokenStream(lexer);
    final AnsiSqlDataTypeParser parser = new AnsiSqlDataTypeParser(tokens);

    // Configure error handling
    lexer.removeErrorListeners();
    lexer.addErrorListener(new AnsiSqlErrorListener());
    parser.removeErrorListeners();
    parser.addErrorListener(new AnsiSqlErrorListener());

    // Visit the parse tree to convert to Spark SQL DataType
    return new AnsiSqlToSparkDataTypeVisitor().visit(parser.sqlType());
  }
}
