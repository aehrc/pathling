package au.csiro.pathling.views.ansi;

import au.csiro.pathling.errors.InvalidUserInputError;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.apache.spark.sql.types.DataType;

/**
 * Parser for ANSI SQL type hints that produces Spark SQL DataTypes.
 */
@Slf4j
public class AnsiSqlTypeParser {

  static class ErrorListener extends BaseErrorListener {

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line,
        int charPositionInLine, String msg, RecognitionException e) {
      throw new InvalidUserInputError("Error parsing ANSI SQL type: " + msg);
    }
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
    final TypesOfAnsiSqlLexer lexer = new TypesOfAnsiSqlLexer(input);
    final CommonTokenStream tokens = new CommonTokenStream(lexer);
    final TypesOfAnsiSqlParser parser = new TypesOfAnsiSqlParser(tokens);

    // Configure error handling
    lexer.removeErrorListeners();
    lexer.addErrorListener(new ErrorListener());
    parser.removeErrorListeners();
    parser.addErrorListener(new ErrorListener());

    // Visit the parse tree to convert to Spark SQL DataType
    return new ToDataTypeVisitor().visit(parser.sqlType());
  }

}
