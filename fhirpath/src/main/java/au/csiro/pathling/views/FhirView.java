package au.csiro.pathling.views;

import java.util.List;
import javax.annotation.Nonnull;
import lombok.Data;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

@Data
public class FhirView {

  @Nonnull
  ResourceType resource;

  @Nonnull
  List<NamedExpression> columns;

  @Nonnull
  List<NamedExpression> variables;

  @Nonnull
  List<String> filters;

}
