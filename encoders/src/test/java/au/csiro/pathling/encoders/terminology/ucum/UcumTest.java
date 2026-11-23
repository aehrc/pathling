/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2026 Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
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

package au.csiro.pathling.encoders.terminology.ucum;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.fhnaumann.configuration.ConfigurationRegistry;
import io.github.fhnaumann.model.UCUMDefinition.DefinedUnit;
import io.github.fhnaumann.util.UCUMRegistry;
import io.github.fhnaumann.util.VersionSpecificUCUMRegistry;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Tests the creation of the shared UCUM service.
 *
 * @author John Grimes
 */
class UcumTest {

  @Test
  void everyDefinedUnitIsResolvedWhenTheServiceIsCreated() throws ReflectiveOperationException {
    // Spark runs many tasks in one JVM, and ucumate otherwise resolves the definition of a unit on
    // first use into a shared map without synchronisation. Initialising the class, which creates
    // the service without converting anything, must leave that map complete, so that conversions
    // only ever read it.
    Class.forName(Ucum.class.getName());

    final VersionSpecificUCUMRegistry registry =
        UCUMRegistry.getInstance()
            .getVersionSpecificUCUMRegistry(ConfigurationRegistry.get().getUCUMVersionAsEnum());
    final Field field =
        VersionSpecificUCUMRegistry.class.getDeclaredField("definedUnitSourceDefinitions");
    field.setAccessible(true);
    final Map<?, ?> resolved = (Map<?, ?>) field.get(registry);
    final Set<String> unresolved =
        registry.getDefinedUnits().stream()
            .filter(unit -> !resolved.containsKey(unit))
            .map(DefinedUnit::code)
            .collect(Collectors.toSet());
    assertEquals(Set.of(), unresolved);
  }
}
