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

/**
 * Parser for ANSI SQL type hints that produces Spark SQL DataTypes.
 */
@Slf4j
public class AnsiSqlTypeParser {

  private AnsiSqlTypeParser() {
  }

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
