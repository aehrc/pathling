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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

import au.csiro.pathling.config.LocalTerminologyConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link LocalTerminologyServiceFactory} and the {@link LocalTerminologyService}: the
 * factory builds a local service, is serialisable so it can be shipped to executors, and memoises
 * the service per JVM. The terminology operations themselves are exercised by the service-level and
 * end-to-end tests.
 *
 * @author John Grimes
 */
class LocalTerminologyServiceFactoryTest {

  private TerminologyConfiguration configuration;

  @BeforeEach
  void setUp() {
    // Each test builds from a clean per-JVM singleton.
    LocalTerminologyServiceFactory.reset();
    configuration =
        TerminologyConfiguration.builder()
            .mode(TerminologyMode.LOCAL)
            .local(LocalTerminologyConfiguration.builder().storagePath("/data/tx-store").build())
            .build();
  }

  @AfterEach
  void tearDown() {
    LocalTerminologyServiceFactory.reset();
  }

  @Test
  void buildReturnsLocalService() {
    final LocalTerminologyServiceFactory factory =
        new LocalTerminologyServiceFactory(configuration, Map.of());

    assertInstanceOf(LocalTerminologyService.class, factory.build());
  }

  @Test
  void getConfigurationReturnsConfiguration() {
    final LocalTerminologyServiceFactory factory =
        new LocalTerminologyServiceFactory(configuration, Map.of());

    assertEquals(configuration, factory.getConfiguration());
  }

  @Test
  void buildIsMemoisedPerJvm() {
    final LocalTerminologyServiceFactory factory =
        new LocalTerminologyServiceFactory(configuration, Map.of());

    assertSame(factory.build(), factory.build());
  }

  @Test
  void factoryIsSerialisable() throws IOException, ClassNotFoundException {
    final LocalTerminologyServiceFactory factory =
        new LocalTerminologyServiceFactory(configuration, Map.of("fs.defaultFS", "file:///"));

    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(factory);
    }
    final LocalTerminologyServiceFactory restored;
    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      restored = (LocalTerminologyServiceFactory) in.readObject();
    }

    assertEquals(TerminologyMode.LOCAL, restored.getConfiguration().getMode());
    assertEquals("/data/tx-store", restored.getConfiguration().getLocal().getStoragePath());
    assertEquals(Map.of("fs.defaultFS", "file:///"), restored.hadoopConfiguration());
  }
}
