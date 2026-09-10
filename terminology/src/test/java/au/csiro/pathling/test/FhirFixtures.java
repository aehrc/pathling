/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.test;

import jakarta.annotation.Nonnull;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Constants and paths for the synthetic FHIR terminology fixtures under {@code
 * terminology/src/test/resources/fhir-fixtures/}. The well-known URLs and codes mirror those
 * documented in that directory's README.
 *
 * @author John Grimes
 */
public final class FhirFixtures {

  private FhirFixtures() {
    // Constants holder.
  }

  /** The canonical URL of the fixture code system. */
  public static final String ANIMAL_SPECIES = "http://example.org/fhir/CodeSystem/animal-species";

  /** The version of the fixture code system. */
  public static final String VERSION = "1.0.0";

  /** The canonical URL of the fixture concept map's target code system. */
  public static final String ANIMAL_CATEGORY = "http://example.org/fhir/CodeSystem/animal-category";

  // Concept codes.
  public static final String ORGANISM = "organism";
  public static final String ANIMAL = "animal";
  public static final String MAMMAL = "mammal";
  public static final String DOG = "dog";
  public static final String CAT = "cat";
  public static final String WHALE = "whale";
  public static final String BIRD = "bird";
  public static final String SPARROW = "sparrow";
  public static final String PENGUIN = "penguin";

  // Value set canonical URLs.
  public static final String VS_MAMMALS_ENUMERATED =
      "http://example.org/fhir/ValueSet/mammals-enumerated";
  public static final String VS_MAMMALS_ISA = "http://example.org/fhir/ValueSet/mammals-isa";
  public static final String VS_ANIMALS_EXCEPT_WHALE =
      "http://example.org/fhir/ValueSet/animals-except-whale";
  public static final String VS_LAND_DWELLERS = "http://example.org/fhir/ValueSet/land-dwellers";
  public static final String VS_NESTED_MAMMALS = "http://example.org/fhir/ValueSet/nested-mammals";
  public static final String VS_EXPANSION_ONLY = "http://example.org/fhir/ValueSet/expansion-only";
  public static final String VS_PETS = "http://example.org/fhir/ValueSet/pets";

  /** The canonical URL of the fixture concept map. */
  public static final String CONCEPT_MAP = "http://example.org/fhir/ConceptMap/species-to-category";

  @Nonnull
  private static Path resource(@Nonnull final String path) {
    final URL url = FhirFixtures.class.getResource(path);
    if (url == null) {
      throw new IllegalStateException("FHIR fixture not found on classpath: " + path);
    }
    return Paths.get(url.getPath());
  }

  /**
   * Returns the directory of individual FHIR resource JSON files.
   *
   * @return the path to the {@code json} fixture directory
   */
  @Nonnull
  public static Path jsonDirectory() {
    return resource("/fhir-fixtures/json");
  }

  /**
   * Returns the path to the code system resource file.
   *
   * @return the path to {@code codesystem-animal-species.json}
   */
  @Nonnull
  public static Path codeSystemFile() {
    return resource("/fhir-fixtures/json/codesystem-animal-species.json");
  }

  /**
   * Returns the path to the packaged FHIR NPM archive.
   *
   * @return the path to {@code animals.tgz}
   */
  @Nonnull
  public static Path packageArchive() {
    return resource("/fhir-fixtures/package/animals.tgz");
  }
}
