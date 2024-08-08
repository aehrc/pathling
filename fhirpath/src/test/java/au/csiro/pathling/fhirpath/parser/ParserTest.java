package au.csiro.pathling.fhirpath.parser;

import static au.csiro.pathling.test.TestResources.getResourceAsString;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.FhirPathGsonTypeAdapterFactory;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

@Slf4j
class ParserTest {

  private static Stream<TestParameters> parameters() {
    return Stream.of(
        new TestParameters("first-official-name", "name.where(use = 'official').first()",
            "fhirpath/first-official-name.json")
    );
  }

  @ParameterizedTest
  @MethodSource("parameters")
  void test(final TestParameters parameters) throws JSONException {
    final Parser parser = new Parser();
    final Gson gson = new GsonBuilder()
        .registerTypeAdapterFactory(new FhirPathGsonTypeAdapterFactory())
        .disableHtmlEscaping()
        .create();
    final FhirPath result = parser.parse(parameters.fhirPath());

    final String actual = result.toJson(gson);
    final String expected = getResourceAsString(parameters.parsedFile());
    JSONAssert.assertEquals(expected, actual, JSONCompareMode.STRICT);
  }

  record TestParameters(String name, String fhirPath, String parsedFile) {

    @Override
    public String toString() {
      return parsedFile;
    }

  }

}
