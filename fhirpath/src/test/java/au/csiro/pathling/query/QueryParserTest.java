package au.csiro.pathling.query;

import au.csiro.pathling.extract.ExtractRequest;
import au.csiro.pathling.extract.ExtractResultType;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.view.ExtractView;
import java.util.List;
import java.util.Optional;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Test;

class QueryParserTest {

  @Test
  void toView() {
    final QueryParser queryParser = new QueryParser(new Parser());
    final ExtractRequest query = ExtractRequest.fromUserInput(ResourceType.PATIENT,
        Optional.of(List.of("id", "id.count()", "name.given", "name.family", "telecom.value")),
        Optional.empty(), Optional.empty());
    final ExtractResultType resultType = ExtractResultType.UNCONSTRAINED;

    final ExtractView extractView = queryParser.toView(query, resultType);
    extractView.printTree();
  }
}
