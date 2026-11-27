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

package au.csiro.pathling.fhirpath.encoding;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Coding;
import org.junit.jupiter.api.Test;

/**
 * Tests the caching of the decoders that read a Coding by field name (T100n).
 *
 * @author Piotr Szul
 */
class CodingDecoderTest {

  @Test
  void schemasWithTheSameFieldNamesShareOneDecoder() {
    // The positions of a Coding's fields depend on their names alone, so structures that differ
    // only in the types beneath their fields, their nullability or their metadata are one entry.
    final Metadata metadata = new MetadataBuilder().putString("comment", "x").build();
    final StructType withStringExtension =
        new StructType()
            .add(CodingSchema.SYSTEM_FIELD, DataTypes.StringType)
            .add(CodingSchema.CODE_FIELD, DataTypes.StringType)
            .add("extension", DataTypes.createArrayType(DataTypes.StringType));
    final StructType withStructExtension =
        new StructType()
            .add(CodingSchema.SYSTEM_FIELD, DataTypes.StringType, false)
            .add(CodingSchema.CODE_FIELD, DataTypes.StringType, true, metadata)
            .add(
                "extension",
                DataTypes.createArrayType(new StructType().add("url", DataTypes.StringType)));

    assertSame(
        CodingDecoder.forSchema(withStringExtension), CodingDecoder.forSchema(withStructExtension));
  }

  @Test
  void decodersUsedConcurrentlyByThreadsReadTheirOwnSchemas() throws Exception {
    // Each thread alternates between two schemas that put the fields in opposite orders, so a
    // decoder taken for the wrong schema would swap the system and the code.
    final StructType systemFirst =
        new StructType()
            .add(CodingSchema.SYSTEM_FIELD, DataTypes.StringType)
            .add(CodingSchema.CODE_FIELD, DataTypes.StringType);
    final StructType codeFirst =
        new StructType()
            .add(CodingSchema.CODE_FIELD, DataTypes.StringType)
            .add(CodingSchema.SYSTEM_FIELD, DataTypes.StringType);
    final ExecutorService executor = Executors.newFixedThreadPool(4);
    try {
      final List<Future<Integer>> results =
          IntStream.range(0, 8)
              .mapToObj(
                  thread ->
                      executor.submit(
                          () -> {
                            int wrong = 0;
                            for (int i = 0; i < 20_000; i++) {
                              final boolean first = (i + thread) % 2 == 0;
                              final Coding coding =
                                  first
                                      ? decode(systemFirst, "urn:s", "c")
                                      : decode(codeFirst, "c", "urn:s");
                              if (!"urn:s".equals(coding.getSystem())
                                  || !"c".equals(coding.getCode())) {
                                wrong++;
                              }
                            }
                            return wrong;
                          }))
              .toList();
      for (final Future<Integer> result : results) {
        assertEquals(0, result.get());
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Nonnull
  private static Coding decode(
      @Nonnull final StructType schema, @Nonnull final String first, @Nonnull final String second) {
    final Row row = new GenericRowWithSchema(new Object[] {first, second}, schema);
    return CodingDecoder.decodeRow(row);
  }
}
