/*
 * Copyright 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.sql;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SyntheticFieldUtils}.
 *
 * @author John Grimes
 */
class SyntheticFieldUtilsTest {

  @Test
  void fieldStartingWithUnderscoreIsSynthetic() {
    // Fields prefixed with underscore are internal metadata fields (e.g. _fid,
    // _value_canonicalized, _code_canonicalized).
    assertTrue(SyntheticFieldUtils.isSyntheticField("_fid"));
    assertTrue(SyntheticFieldUtils.isSyntheticField("_value_canonicalized"));
    assertTrue(SyntheticFieldUtils.isSyntheticField("_code_canonicalized"));
    assertTrue(SyntheticFieldUtils.isSyntheticField("_anything"));
  }

  @Test
  void fieldEndingWithScaleIsSynthetic() {
    // Fields ending with _scale store BigDecimal scale for decimal representation.
    assertTrue(SyntheticFieldUtils.isSyntheticField("value_scale"));
    assertTrue(SyntheticFieldUtils.isSyntheticField("something_scale"));
  }

  @Test
  void normalFieldNamesAreNotSynthetic() {
    // Regular FHIR field names should not be identified as synthetic.
    assertFalse(SyntheticFieldUtils.isSyntheticField("value"));
    assertFalse(SyntheticFieldUtils.isSyntheticField("code"));
    assertFalse(SyntheticFieldUtils.isSyntheticField("system"));
    assertFalse(SyntheticFieldUtils.isSyntheticField("display"));
    assertFalse(SyntheticFieldUtils.isSyntheticField("unit"));
    assertFalse(SyntheticFieldUtils.isSyntheticField("family"));
    assertFalse(SyntheticFieldUtils.isSyntheticField("given"));
  }

  @Test
  void edgeCases() {
    // A single underscore is synthetic (starts with _).
    assertTrue(SyntheticFieldUtils.isSyntheticField("_"));

    // "scale" alone does not end with _scale.
    assertFalse(SyntheticFieldUtils.isSyntheticField("scale"));

    // "_scale" starts with _ so is synthetic.
    assertTrue(SyntheticFieldUtils.isSyntheticField("_scale"));
  }
}
