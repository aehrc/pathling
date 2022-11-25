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

package au.csiro.pathling.terminology;

import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.terminology.Relation.Entry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.ConceptMap.ConceptMapGroupComponent;
import org.hl7.fhir.r4.model.ConceptMap.SourceElementComponent;
import org.hl7.fhir.r4.model.ConceptMap.TargetElementComponent;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;

/**
 * Input/output mappings for {@code $closure} operation.
 *
 * @author Piotr Szul
 * @see <a href="https://www.hl7.org/fhir/terminology-service.html#closure">Maintaining a Closure
 * Table</a>
 * @see <a href="https://www.hl7.org/fhir/conceptmap-operation-closure.html">Operation $closure on
 * ConceptMap</a>
 */
@Slf4j
@Deprecated
public final class ClosureMapping extends BaseMapping {

  private ClosureMapping() {
  }


  private static void appendSubsumesMapping(@Nonnull final Collection<Entry> entries,
      @Nonnull final SimpleCoding source, @Nonnull final SimpleCoding target,
      @Nonnull final ConceptMapEquivalence equivalence) {

    // According to the specification the only valid equivalences in the
    // response are: equal, specializes, subsumes and unmatched.
    switch (equivalence) {
      case SUBSUMES:
        entries.add(Entry.of(target, source));
        break;
      case SPECIALIZES:
        entries.add(Entry.of(source, target));
        break;
      case EQUAL:
        entries.add(Entry.of(source, target));
        entries.add(Entry.of(target, source));
        break;
      case UNMATCHED:
        break;
      default:
        log.warn("Ignoring unexpected equivalence: " + equivalence + " source: " + source
            + " target: " + target);
        break;
    }
  }

  @Nonnull
  private static List<Entry> conceptMapToMappings(@Nonnull final ConceptMap conceptMap) {
    final List<Entry> entries = new ArrayList<>();
    if (conceptMap.hasGroup()) {
      final List<ConceptMapGroupComponent> groups = conceptMap.getGroup();
      for (final ConceptMapGroupComponent group : groups) {
        final List<SourceElementComponent> elements = group.getElement();
        for (final SourceElementComponent source : elements) {
          for (final TargetElementComponent target : source.getTarget()) {
            appendSubsumesMapping(entries,
                new SimpleCoding(group.getSource(), source.getCode(), group.getSourceVersion()),
                new SimpleCoding(group.getTarget(), target.getCode(), group.getTargetVersion()),
                target.getEquivalence());
          }
        }
      }
    }
    return entries;
  }

  /**
   * Construct the relation from a concept map using th {@code SUBSUMES}, {@code SPECIALIZES} and
   * {@code EQUAL} equivalences.
   *
   * @param conceptMap the concept map to convert to a relation.
   * @return the relation instance.
   */
  @Nonnull
  public static Relation relationFromConceptMap(@Nonnull final ConceptMap conceptMap) {
    return Relation.fromMappings(conceptMapToMappings(conceptMap));
  }
}
