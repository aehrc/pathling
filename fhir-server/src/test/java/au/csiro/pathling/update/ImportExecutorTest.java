/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.update;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.helpers.TestHelpers;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.net.URL;
import java.util.Collections;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.UrlType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.util.FileSystemUtils;

/**
 * @author John Grimes
 */
@SpringBootTest
@Tag("UnitTest")
@ActiveProfiles({"core"})
@TestPropertySource(
    properties = {
        "pathling.import.allowableSources=file:/"
    })
class ImportExecutorTest {

  @TempDir
  static File testRootDir;
  static File warehouseDir;

  @DynamicPropertySource
  @SuppressWarnings("unused")
  static void registerProperties(final DynamicPropertyRegistry registry) {
    warehouseDir = new File(testRootDir, "default");
    assertTrue(warehouseDir.mkdir());
    registry.add("pathling.storage.warehouseUrl",
        () -> testRootDir.toURI());
  }

  @BeforeEach
  void setUp() {
    FileSystemUtils.deleteRecursively(warehouseDir);
    assertTrue(warehouseDir.mkdir());
    resourceReader.updateAvailableResourceTypes();
    resourceReader.invalidateCache();
    assertEquals(Collections.emptySet(), resourceReader.getAvailableResourceTypes());
  }

  @Autowired
  private ResourceReader resourceReader;

  @Autowired
  private ImportExecutor importExecutor;

  @Nonnull
  private Parameters buildImportParameters(URL jsonURL, ResourceType resourceType) {
    Parameters parameters = new Parameters();
    ParametersParameterComponent sourceParam = parameters.addParameter().setName("source");
    sourceParam.addPart().setName("resourceType").setValue(new CodeType(resourceType.toCode()));
    sourceParam.addPart().setName("url").setValue(new UrlType(jsonURL.toExternalForm()));
    return parameters;
  }

  @Test
  public void testImportJsonFile() {
    final URL jsonURL = TestHelpers.getResourceAsUrl("import/Patient.ndjson");
    importExecutor.execute(buildImportParameters(jsonURL, ResourceType.PATIENT));
    assertEquals(ImmutableSet.of(ResourceType.PATIENT), resourceReader.getAvailableResourceTypes());
    assertEquals(9, resourceReader.read(ResourceType.PATIENT).count());
  }

  @Test
  public void testImportJsonFileWithBlankLines() {
    final URL jsonURL = TestHelpers.getResourceAsUrl("import/Patient_with_eol.ndjson");
    importExecutor.execute(buildImportParameters(jsonURL, ResourceType.PATIENT));
    assertEquals(ImmutableSet.of(ResourceType.PATIENT), resourceReader.getAvailableResourceTypes());
    assertEquals(9, resourceReader.read(ResourceType.PATIENT).count());
  }
}