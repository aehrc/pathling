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
package au.csiro.pathling.encoders;

import static au.csiro.pathling.encoders.utils.SchemaMisalignment.narrowEncoders;
import static au.csiro.pathling.encoders.utils.SchemaMisalignment.wideEncoders;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Date;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Period;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests that data carrying fields the running encoder does not know about decodes to the subset the
 * encoder can represent.
 *
 * <p>This class stands alone rather than joining {@link MisalignedSchemaDecodingTest}. Before the
 * fix, reading a wider dataset with a narrower encoder read a length out of a misaligned row and
 * then copied that many bytes. Both tests here failed with {@code java.lang.InternalError: a fault
 * occurred in an unsafe memory access operation}, and during the investigation of #2697 the same
 * fault took the JVM down outright with {@code SIGBUS BUS_ADRALN} in {@code
 * StubRoutines::forward_copy_longs}, reported by surefire as exit code 134. A fork that dies
 * reports every other test in its class as crashed rather than reporting the failure, so the
 * crashing case is kept on its own to keep a failure here diagnosable.
 *
 * @author John Grimes
 */
class NarrowEncoderDecodingTest {

  private static final String STRING_EXTENSION_URL = "http://example.org/string-extension";
  private static final String PERIOD_EXTENSION_URL = "http://example.org/period-extension";

  private static SparkSession spark;

  /** Set up Spark. */
  @BeforeAll
  static void setUp() {
    spark =
        AnsiTestSupport.configureAnsiMode(
            SparkSession.builder()
                .master("local[*]")
                .appName("testing")
                .config("spark.driver.bindAddress", "localhost")
                .config("spark.driver.host", "localhost")
                .config("spark.ui.enabled", "false")
                .getOrCreate());
  }

  /** Tear down Spark. */
  @AfterAll
  static void tearDown() {
    spark.stop();
  }

  /**
   * A dataset encoded with an encoder including {@code Period} and {@code Quantity} decodes with an
   * encoder excluding both: the other extension values are correct and the process survives (US2.1,
   * SC-002).
   */
  @Test
  void widerDataDecodesWithNarrowerEncoder() {
    // Arrange: encode a Patient carrying a string extension and a Period extension with the wide
    // encoder. The Period field sits before the string field in the extension struct, so the
    // narrow encoder reads the string value at the Period value's offset.
    final Dataset<Row> widelyEncoded = widePatient();

    // Act: decode with the narrow encoder, which has no field for the Period value.
    final Patient decoded = widelyEncoded.as(narrowEncoders().of(Patient.class)).head();

    // Assert: the extension the narrow encoder can represent holds the value that was written.
    assertEquals(
        "the written value",
        ((StringType) decoded.getExtensionByUrl(STRING_EXTENSION_URL).getValue()).getValue());
  }

  /**
   * An extension whose only value is of an excluded type is absent from the decoded resource,
   * rather than present with a wrong value (US2.2).
   */
  @Test
  void extensionOfAnExcludedTypeIsAbsentRatherThanWrong() {
    // Arrange: the same widely-encoded Patient, whose Period extension the narrow encoder cannot
    // represent at all.
    final Dataset<Row> widelyEncoded = widePatient();

    // Act.
    final Patient decoded = widelyEncoded.as(narrowEncoders().of(Patient.class)).head();

    // Assert: the extension is either absent, or present with no value; what it must not be is
    // present carrying a value read out of another field.
    final Extension periodExtension = decoded.getExtensionByUrl(PERIOD_EXTENSION_URL);
    if (periodExtension != null) {
      assertNull(periodExtension.getValue());
    }
  }

  /** Returns a Patient carrying a string extension and a Period extension, encoded widely. */
  private static Dataset<Row> widePatient() {
    final Patient patient = new Patient();
    patient.setId("wide-patient");
    patient.addExtension(new Extension(STRING_EXTENSION_URL, new StringType("the written value")));
    patient.addExtension(new Extension(PERIOD_EXTENSION_URL, new Period().setStart(new Date(0L))));
    return spark.createDataset(List.of(patient), wideEncoders().of(Patient.class)).toDF();
  }
}
