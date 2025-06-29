package au.csiro.pathling.test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import au.csiro.pathling.fhirpath.parser.Parser;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test class for bulk parsing of FHIRPath expressions from search parameters. This test reads
 * expressions from a text file and verifies that each expression can be parsed without errors.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
@Disabled
class BulkExpressionParsingTest {

  private static final String SEARCH_PARAMS_FILE = "/search-params.txt";

  /**
   * Provides a stream of FHIRPath expressions from the search parameters file.
   *
   * @return Stream of FHIRPath expressions
   * @throws IOException if the file cannot be read
   */
  @Nonnull
  private static Stream<String> expressionProvider() throws IOException {
    try (final InputStream inputStream = BulkExpressionParsingTest.class.getResourceAsStream(
        SEARCH_PARAMS_FILE)) {
      assertNotNull(inputStream);
      try (final BufferedReader reader = new BufferedReader(
          new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {

        if (inputStream == null) {
          throw new IOException("Cannot find resource: " + SEARCH_PARAMS_FILE);
        }

        return reader.lines()
            .filter(line -> !line.trim().isEmpty()) // Filter out empty lines.
            .toList() // Collect to list to avoid stream being closed.
            .stream();
      }
    }
  }

  /**
   * Tests that each FHIRPath expression can be parsed without throwing an exception.
   *
   * @param expression the FHIRPath expression to test
   */
  @ParameterizedTest
  @MethodSource("expressionProvider")
  void testExpressionParsing(final String expression) {
    final Parser parser = new Parser();

    // The test passes if no exception is thrown during parsing.
    assertDoesNotThrow(() -> parser.parse(expression),
        "Expression should parse without errors: " + expression);
  }
}
