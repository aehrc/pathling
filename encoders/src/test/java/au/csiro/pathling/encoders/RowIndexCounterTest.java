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

package au.csiro.pathling.encoders;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.junit.jupiter.api.Test;

/** Tests for {@link RowIndexCounter}. */
class RowIndexCounterTest {

  @Test
  void getReturnsZeroInitially() {
    final RowIndexCounter counter = new RowIndexCounter();
    assertEquals(0, counter.get());
  }

  @Test
  void incrementAdvancesCounter() {
    final RowIndexCounter counter = new RowIndexCounter();
    counter.increment();
    assertEquals(1, counter.get());
    counter.increment();
    assertEquals(2, counter.get());
  }

  @Test
  void getIsIdempotentBetweenIncrements() {
    final RowIndexCounter counter = new RowIndexCounter();
    counter.increment();
    assertEquals(1, counter.get());
    assertEquals(1, counter.get());
  }

  @Test
  void resetSetsCounterToZero() {
    final RowIndexCounter counter = new RowIndexCounter();
    counter.increment();
    counter.increment();
    assertEquals(2, counter.get());
    counter.reset();
    assertEquals(0, counter.get());
  }

  @Test
  void threadLocalIsolation() throws Exception {
    final RowIndexCounter counter = new RowIndexCounter();
    counter.increment();
    counter.increment();

    // A different thread should see its own independent counter starting at zero.
    final int[] otherThreadValue = new int[1];
    final Thread thread = new Thread(() -> otherThreadValue[0] = counter.get());
    thread.start();
    thread.join();

    assertEquals(0, otherThreadValue[0]);
    assertEquals(2, counter.get());
  }

  @Test
  void serializationRestoresCounter() throws Exception {
    final RowIndexCounter counter = new RowIndexCounter();
    counter.increment();

    // Serialize and deserialize.
    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (final ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(counter);
    }
    final ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    final RowIndexCounter deserialized;
    try (final ObjectInputStream ois = new ObjectInputStream(bis)) {
      deserialized = (RowIndexCounter) ois.readObject();
    }

    // Deserialized counter should start fresh at zero.
    assertEquals(0, deserialized.get());
    deserialized.increment();
    assertEquals(1, deserialized.get());
  }
}
