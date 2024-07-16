package au.csiro.pathling.fhirpath.parser;

import au.csiro.pathling.fhirpath.FhirPath;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class ParserTest {

  @Test
  void test() {
    final Parser parser = new Parser();
    final FhirPath result = parser.parse("name.where(use = 'official').first()");
    log.debug(result.toString());
  }
  
}
