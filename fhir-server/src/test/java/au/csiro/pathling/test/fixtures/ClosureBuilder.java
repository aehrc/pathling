package au.csiro.pathling.test.fixtures;

import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.terminology.Relation;
import au.csiro.pathling.terminology.Mapping;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Coding;

public class ClosureBuilder {

  private final List<Mapping> mappings = new ArrayList<>();

  @Nonnull
  public Relation build() {

    return (mappings.isEmpty())
           ? Relation.empty()
           : Relation.fromMappings(mappings);
  }

  public ClosureBuilder add(@Nonnull final Coding from, @Nonnull Coding to) {
    mappings.add(Mapping.of(new SimpleCoding(from), new SimpleCoding(to)));
    return this;
  }

  public static ClosureBuilder empty() {
    return new ClosureBuilder();
  }
}
