/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
