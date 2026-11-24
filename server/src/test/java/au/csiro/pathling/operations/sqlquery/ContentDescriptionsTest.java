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

package au.csiro.pathling.operations.sqlquery;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.views.FhirView;
import jakarta.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.Map;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ContentDescriptions}, which encodes the values placed in an async job cache
 * key so that none of them can reproduce the key's delimiters (aehrc/pathling#2768).
 *
 * @author John Grimes
 */
class ContentDescriptionsTest {

  // ---- escape ----

  @Test
  void escapeEncodesEveryDelimiterTheCacheKeysUse() {
    // None of the structural characters written by the key renderers may survive encoding.
    assertThat(ContentDescriptions.escape(":=,{}[]|\n\u0000 ")).doesNotContainPattern("[^%0-9A-F]");
  }

  @Test
  void escapeKeepsLettersDigitsUnreservedPunctuationAndNonAsciiCharacters() {
    assertThat(ContentDescriptions.escape("Az09-._~é")).isEqualTo("Az09-._~é");
  }

  @Test
  void escapeEncodesThePercentSignSoThatEncodingIsInjective() {
    // Were "%" left as it is, a value already holding "%3A" would encode the same as ":".
    assertThat(ContentDescriptions.escape("%3A")).isEqualTo("%253A");
    assertThat(ContentDescriptions.escape(":")).isEqualTo("%3A");
  }

  @Test
  void escapeKeepsLoneSurrogatesDistinct() {
    // An encoder that went through UTF-8 would replace an unpaired surrogate with "?", making it
    // indistinguishable from a literal question mark.
    assertThat(ContentDescriptions.escape("\uD800")).isNotEqualTo(ContentDescriptions.escape("?"));
  }

  // ---- describeMap ----

  @Test
  void describeMapEncodesKeysAndValues() {
    assertThat(ContentDescriptions.describeMap(Map.of("a=b", "https://x.org/c,d")))
        .isEqualTo("{a%3Db=https%3A%2F%2Fx.org%2Fc%2Cd}");
  }

  @Test
  void describeMapDoesNotDependOnIterationOrder() {
    final Map<String, Object> ascending = new LinkedHashMap<>();
    ascending.put("a", 1);
    ascending.put("b", 2);
    final Map<String, Object> descending = new LinkedHashMap<>();
    descending.put("b", 2);
    descending.put("a", 1);

    assertThat(ContentDescriptions.describeMap(descending))
        .isEqualTo(ContentDescriptions.describeMap(ascending))
        .isEqualTo("{a=1,b=2}");
  }

  @Test
  void describeMapRendersAnEmptyMapAsEmptyBraces() {
    assertThat(ContentDescriptions.describeMap(Map.of())).isEqualTo("{}");
  }

  // ---- describeView ----

  @Test
  void describeViewIsIdenticalForSeparatelyBuiltCopiesOfOneView() {
    assertThat(ContentDescriptions.describeView(viewWithConstant(new StringType("1"))))
        .isEqualTo(ContentDescriptions.describeView(viewWithConstant(new StringType("1"))));
  }

  @Test
  void describeViewDistinguishesConstantsOfDifferentTypes() {
    // The JSON rendering carries the constant's type in its value key, so a string "1" and an
    // integer 1 are told apart.
    assertThat(ContentDescriptions.describeView(viewWithConstant(new StringType("1"))))
        .isNotEqualTo(ContentDescriptions.describeView(viewWithConstant(new IntegerType(1))));
  }

  @Nonnull
  private static FhirView viewWithConstant(@Nonnull final IBase value) {
    return FhirView.ofResource("Patient")
        .constant("c", value)
        .select(FhirView.columns(FhirView.column("id", "id")))
        .build();
  }
}
