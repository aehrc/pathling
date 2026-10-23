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

package au.csiro.pathling.projection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.path.Paths.Traversal;
import java.util.Optional;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Test;

class ProjectedColumnTest {

  @Test
  void throwsInvalidUserInputErrorWhenDeclaredTypeDoesNotMatch() {
    final Collection collection = mock(Collection.class);
    when(collection.getFhirType()).thenReturn(Optional.of(FHIRDefinedType.CANONICAL));
    final RequestedColumn requestedColumn =
        new RequestedColumn(
            new Traversal("$this"),
            "profile",
            false,
            Optional.of(FHIRDefinedType.STRING),
            Optional.empty());
    final ProjectedColumn projectedColumn = new ProjectedColumn(collection, requestedColumn);

    final InvalidUserInputError error =
        assertThrows(InvalidUserInputError.class, projectedColumn::getValue);
    assertEquals(
        "Column 'profile' with path '$this' has type canonical, "
            + "but the column declares type string",
        error.getMessage());
  }
}