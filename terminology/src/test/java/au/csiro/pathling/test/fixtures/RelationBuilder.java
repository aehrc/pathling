/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.fixtures;

import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.terminology.Relation;
import au.csiro.pathling.terminology.Relation.Entry;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Coding;

public class RelationBuilder {

  private final List<Entry> entries = new ArrayList<>();

  @Nonnull
  public Relation build() {

    return (entries.isEmpty())
           ? Relation.equality()
           : Relation.fromMappings(entries);
  }

  @Nonnull
  public RelationBuilder add(@Nonnull final Coding src, @Nonnull final Coding... targets) {
    Stream.of(targets).forEach(to ->
        entries.add(Entry.of(new SimpleCoding(src), new SimpleCoding(to))));
    return this;
  }

  @Nonnull
  public static RelationBuilder empty() {
    return new RelationBuilder();
  }
}
