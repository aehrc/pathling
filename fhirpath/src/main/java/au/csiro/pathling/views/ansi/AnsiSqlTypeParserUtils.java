package au.csiro.pathling.views.ansi;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.spark.sql.types.*;
import javax.annotation.Nonnull;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Parser for ANSI SQL type hints that produces Spark SQL DataTypes.
 */
@Slf4j
public class AnsiSqlTypeParserUtils {

  /**
   * Parse an ANSI SQL type string and convert it to a Spark SQL DataType.
   *
   * @param typeString the ANSI SQL type string to parse
   * @return an Optional containing the corresponding Spark SQL DataType, or empty if parsing failed
   */
  @Nonnull
  public static Optional<DataType> parse(@Nonnull final String typeString) {
    try {
      // Set up the lexer and parser
      final CharStream input = CharStreams.fromString(typeString.toUpperCase());
      final AnsiSqlTypeLexer lexer = new AnsiSqlTypeLexer(input);
      final CommonTokenStream tokens = new CommonTokenStream(lexer);
      final AnsiSqlTypeParser parser = new AnsiSqlTypeParser(tokens);

      // Configure error handling
      lexer.removeErrorListeners();
      lexer.addErrorListener(new BaseErrorListener() {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line,
            int charPositionInLine, String msg, RecognitionException e) {
          throw new IllegalArgumentException("Error parsing ANSI SQL type: " + msg);
        }
      });

      parser.removeErrorListeners();
      parser.addErrorListener(new BaseErrorListener() {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line,
            int charPositionInLine, String msg, RecognitionException e) {
          throw new IllegalArgumentException("Error parsing ANSI SQL type: " + msg);
        }
      });

      // Parse the input
      final ParseTree tree = parser.sqlType();

      // Visit the parse tree to convert to Spark SQL DataType
      final AnsiSqlTypeVisitor<DataType> visitor = new ToDataTypeVisitor();
      return Optional.of(visitor.visit(tree));
    } catch (Exception e) {
      log.warn("Failed to parse ANSI SQL type: {}", typeString, e);
      return Optional.empty();
    }
  }
}
