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

package au.csiro.pathling.export.utils;

import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ExecutorServiceResourceTest {
  
  @Mock
  ExecutorService executorService;

  @Test
  void testClosesExecutorServiceNicely() throws InterruptedException {

    System.out.println(executorService);
    final ExecutorServiceResource resource = ExecutorServiceResource.of(executorService, 2000);
    when(executorService.awaitTermination(Mockito.eq(2000L),Mockito.eq(TimeUnit.MILLISECONDS))).thenReturn(true);
    resource.close();
    Mockito.verify(executorService).shutdownNow();
  }
}
