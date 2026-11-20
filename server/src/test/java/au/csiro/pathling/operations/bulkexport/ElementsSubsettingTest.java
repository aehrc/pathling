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

package au.csiro.pathling.operations.bulkexport;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.Set;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for the JSON-level subsetting in {@link ElementsSubsetting}.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
class ElementsSubsettingTest {

  // Requested elements are kept along with the resource type, and everything else is dropped.
  @Test
  void keepsOnlyRequestedElements() {
    final JsonObject result =
        subset(
            """
            {"resourceType":"Patient","id":"p1","gender":"male","birthDate":"1990-01-01",
             "name":[{"family":"Smith"}]}
            """,
            Set.of("id", "gender"));

    assertThat(result.keySet()).containsExactlyInAnyOrder("resourceType", "id", "gender", "meta");
  }

  // The id and extensions of a kept primitive element travel with it, and those of a dropped one
  // are dropped with it.
  @Test
  void keepsPrimitiveMetadataWithItsElement() {
    final JsonObject result =
        subset(
            """
            {"resourceType":"Patient","id":"p1",
             "birthDate":"1990-01-01","_birthDate":{"extension":[{"url":"http://e","valueString":"x"}]},
             "gender":"male","_gender":{"id":"g"}}
            """,
            Set.of("id", "birthDate"));

    assertThat(result.keySet())
        .containsExactlyInAnyOrder("resourceType", "id", "birthDate", "_birthDate", "meta");
  }

  // The SUBSETTED tag is added beside any tags the resource already carries.
  @Test
  void addsSubsettedTagBesideExistingTags() {
    final JsonObject result =
        subset(
            """
            {"resourceType":"Patient","id":"p1",
             "meta":{"versionId":"2","tag":[{"system":"http://example.org","code":"other"}]}}
            """,
            Set.of("id", "meta"));

    final JsonObject meta = result.getAsJsonObject("meta");
    assertThat(meta.get("versionId").getAsString()).isEqualTo("2");
    final JsonArray tags = meta.getAsJsonArray("tag");
    assertThat(tags).hasSize(2);
    assertThat(tags.get(1).getAsJsonObject().get("code").getAsString())
        .isEqualTo(ElementsSubsetting.SUBSETTED_CODE);
  }

  // A resource already tagged as subsetted is not tagged twice.
  @Test
  void doesNotDuplicateSubsettedTag() {
    final JsonObject result =
        subset(
            """
            {"resourceType":"Patient","id":"p1","meta":{"tag":[
              {"system":"http://terminology.hl7.org/CodeSystem/v3-ObservationValue",
               "code":"SUBSETTED"}]}}
            """,
            Set.of("id", "meta"));

    assertThat(result.getAsJsonObject("meta").getAsJsonArray("tag")).hasSize(1);
  }

  private static JsonObject subset(final String document, final Set<String> elements) {
    return JsonParser.parseString(ElementsSubsetting.subset(document, elements)).getAsJsonObject();
  }
}
