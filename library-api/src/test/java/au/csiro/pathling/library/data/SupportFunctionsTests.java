/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.library.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.library.PathlingContext;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

public class SupportFunctionsTests {
  
  @Test
  public void testBasenameToResourceTypeMapper() {
    assertEquals(List.of("Patient"), SupportFunctions.basenameToResource("Patient.json"));
    assertEquals(List.of("Condition"),
        SupportFunctions.basenameToResource("/foo.xxx/bar/Condition.xml"));
    assertEquals(List.of("Observation"),
        SupportFunctions.basenameToResource("file:///foo.yyy/bar/Observation"));
  }

  @Test
  public void testIdentityTransformer() {
    final Dataset<Row> mockDataset = mock(Dataset.class);
    assertEquals(mockDataset, SupportFunctions.identityTransformer(mockDataset, "Patient"));
  }

  @Test
  public void testTextEncodingTransformer() {
    final Dataset<Row> mockDataset = mock(Dataset.class);
    final Dataset<Row> mockResultDataset = mock(Dataset.class);
    final PathlingContext mockPathlingContext = mock(PathlingContext.class);

    when(mockPathlingContext.encode(mockDataset, "Patient", "text/plain"))
        .thenReturn(mockResultDataset);

    assertEquals(mockResultDataset,
        SupportFunctions.textEncodingTransformer(mockPathlingContext, "text/plain")
            .apply(mockDataset, "Patient"));
    verify(mockPathlingContext).encode(mockDataset, "Patient", "text/plain");
    verifyNoMoreInteractions(mockPathlingContext);
  }

}
