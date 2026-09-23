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

package au.csiro.pathling.terminology.local;

import au.csiro.pathling.config.LocalTerminologyConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import au.csiro.pathling.terminology.store.FhirTerminologyImporter;
import au.csiro.pathling.terminology.store.SnomedRf2Importer;
import au.csiro.pathling.test.Rf2Mini;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.ValueSet;

/**
 * Builds, once for the whole test JVM, a store holding the rf2-mini SNOMED CT release together with
 * imported FHIR resources whose canonical URLs and versions exercise concept map resolution and the
 * LOCAL mode pin check: the worked example concept map as versions {@code 2025} (edited) and {@code
 * 2026}, a map with two versions whose order cannot be determined, a map whose target carries
 * {@code dependsOn}, and a value set at a canonical URL that carries a query.
 *
 * @author John Grimes
 */
final class ConceptMapTerminologyFixture {

  /** The canonical URL of the worked example concept map. */
  static final String SCT_TO_ICD10 = "http://example.org/ConceptMap/sct-to-icd10";

  /** The canonical URL of a map imported under two versions whose order cannot be determined. */
  static final String AMBIGUOUS = "http://example.org/ConceptMap/ambiguous";

  /** The canonical URL of a map whose target carries {@code dependsOn}. */
  static final String DEPENDS_ON = "http://example.org/ConceptMap/depends-on";

  /** The canonical URL, carrying a query, of a value set imported as version {@code 2026}. */
  static final String VALUE_SET_WITH_QUERY = "http://example.org/ValueSet/x?edition=au";

  /**
   * The target code that the edited {@code 2025} version of the worked example maps diabetes to.
   */
  static final String EDITED_2025_TARGET = "E11";

  private static final String FIXTURE_RESOURCE = "/conceptmap/sct-to-icd10.ConceptMap.json";

  private static String storagePath;
  private static ConceptMap conceptMap2026;
  private static ConceptMap conceptMap2025;

  private ConceptMapTerminologyFixture() {
    // Static holder.
  }

  static synchronized void ensure() {
    if (storagePath != null) {
      return;
    }
    final SparkSession spark =
        SparkSession.builder()
            .appName("ConceptMapTerminologyFixture")
            .master("local[2]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.driver.host", "localhost")
            .config("spark.ui.enabled", "false")
            .getOrCreate();
    final IParser parser = FhirContext.forR4().newJsonParser();
    conceptMap2026 = load(parser);
    conceptMap2025 = load(parser);
    conceptMap2025.setVersion("2025");
    conceptMap2025
        .getGroupFirstRep()
        .getElement()
        .get(1)
        .getTargetFirstRep()
        .setCode(EDITED_2025_TARGET);
    final Path path;
    final Path resources;
    try {
      final Path root = Files.createTempDirectory("concept-map-store");
      path = root.resolve("store");
      resources = Files.createDirectory(root.resolve("resources"));
      write(parser, resources, "sct-to-icd10-2026.json", conceptMap2026);
      write(parser, resources, "sct-to-icd10-2025.json", conceptMap2025);
      write(parser, resources, "ambiguous-a.json", ambiguous("1.0.0+build1"));
      write(parser, resources, "ambiguous-b.json", ambiguous("1.0.0+build2"));
      write(parser, resources, "depends-on.json", dependsOn());
      write(parser, resources, "value-set-with-query.json", valueSetWithQuery());
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    new SnomedRf2Importer(spark, path.toString())
        .importFrom(Rf2Mini.baseRelease().toString(), null);
    new FhirTerminologyImporter(spark, path.toString())
        .importFrom(resources.toString(), false, null);
    storagePath = path.toString();
  }

  @Nonnull
  private static ConceptMap load(@Nonnull final IParser parser) {
    try (final InputStream stream =
        ConceptMapTerminologyFixture.class.getResourceAsStream(FIXTURE_RESOURCE)) {
      if (stream == null) {
        throw new IllegalStateException("Fixture not found: " + FIXTURE_RESOURCE);
      }
      return (ConceptMap)
          parser.parseResource(new String(stream.readAllBytes(), StandardCharsets.UTF_8));
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void write(
      @Nonnull final IParser parser,
      @Nonnull final Path directory,
      @Nonnull final String fileName,
      @Nonnull final Resource resource)
      throws IOException {
    Files.writeString(directory.resolve(fileName), parser.encodeResourceToString(resource));
  }

  @Nonnull
  private static ConceptMap ambiguous(@Nonnull final String version) {
    final ConceptMap conceptMap = new ConceptMap();
    conceptMap.setUrl(AMBIGUOUS);
    conceptMap.setVersion(version);
    conceptMap
        .addGroup()
        .setSource(Rf2Mini.SNOMED_URI)
        .addElement()
        .setCode(Rf2Mini.DIABETES)
        .addTarget()
        .setCode(Rf2Mini.TYPE1_DIABETES)
        .setEquivalence(ConceptMapEquivalence.EQUIVALENT);
    return conceptMap;
  }

  @Nonnull
  private static ConceptMap dependsOn() {
    final ConceptMap conceptMap = new ConceptMap();
    conceptMap.setUrl(DEPENDS_ON);
    conceptMap.setVersion("1");
    conceptMap
        .addGroup()
        .setSource(Rf2Mini.SNOMED_URI)
        .addElement()
        .setCode(Rf2Mini.DIABETES)
        .addTarget()
        .setCode(Rf2Mini.TYPE1_DIABETES)
        .setEquivalence(ConceptMapEquivalence.EQUIVALENT)
        .addDependsOn()
        .setProperty("laterality")
        .setValue("left");
    return conceptMap;
  }

  @Nonnull
  private static ValueSet valueSetWithQuery() {
    final ValueSet valueSet = new ValueSet();
    valueSet.setUrl(VALUE_SET_WITH_QUERY);
    valueSet.setVersion("2026");
    final ValueSet.ConceptSetComponent include = valueSet.getCompose().addInclude();
    include.setSystem(Rf2Mini.SNOMED_URI);
    include.addConcept().setCode(Rf2Mini.DIABETES);
    include.addConcept().setCode(Rf2Mini.TYPE1_DIABETES);
    return valueSet;
  }

  @Nonnull
  static String storagePath() {
    ensure();
    return storagePath;
  }

  /** Returns the worked example concept map exactly as the fixture file carries it. */
  @Nonnull
  static ConceptMap conceptMap2026() {
    ensure();
    return conceptMap2026;
  }

  /** Returns the edited {@code 2025} version of the worked example concept map. */
  @Nonnull
  static ConceptMap conceptMap2025() {
    ensure();
    return conceptMap2025;
  }

  @Nonnull
  static LocalTerminologyService service() {
    ensure();
    final TerminologyConfiguration configuration =
        TerminologyConfiguration.builder()
            .mode(TerminologyMode.LOCAL)
            .local(LocalTerminologyConfiguration.builder().storagePath(storagePath).build())
            .build();
    return new LocalTerminologyService(configuration, Map.of());
  }
}
