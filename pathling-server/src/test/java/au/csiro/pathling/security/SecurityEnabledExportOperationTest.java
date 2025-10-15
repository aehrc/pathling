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

package au.csiro.pathling.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatNoException;

import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.export.ExportProvider;
import au.csiro.pathling.export.ExportRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * @see <a
 * href="https://docs.spring.io/spring-security/site/docs/5.2.x/reference/html/test.html">Spring
 * Security - Testing</a>
 * @see <a
 * href="https://stackoverflow.com/questions/58289509/in-spring-boot-test-how-do-i-map-a-temporary-folder-to-a-configuration-property">In
 * Spring Boot Test, how do I map a temporary folder to a configuration property?</a>
 */
@TestPropertySource(properties = {"pathling.auth.enabled=true",
    "pathling.auth.issuer=https://pathling.acme.com/fhir"})
@MockBean(OidcConfiguration.class)
@MockBean(JwtDecoder.class)
@MockBean(JwtAuthenticationConverter.class)
class SecurityEnabledExportOperationTest extends SecurityTestForOperations<ExportRequest> {

  private static final String PATHLING_EXPORT_MSG = ERROR_MSG.apply("export");

  @TempDir
  private static Path tempDir;
  @Autowired
  private ApplicationContext applicationContext;


  @DynamicPropertySource
  static void configureProperties(DynamicPropertyRegistry registry) {
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + tempDir.toAbsolutePath());
  }
  
  @AfterEach
  void cleanup() throws IOException {
    FileUtils.cleanDirectory(tempDir.toFile());
  }

  @Test
  @WithMockJwt(username = "admin", authorities = {"pathling:export"})
  void test_forbidden_if_export_with_authority_no_read_authority() {
    exportProvider = setup_scenario(tempDir, "Patient");
    assertThatNoException().isThrownBy(() -> {
      JsonNode manifest = perform_export();
      ArrayNode output = (ArrayNode) manifest.get("output");
      assertThat(output)
          .isEmpty();
    });
  }

  @Test
  @WithMockJwt(username = "admin", authorities = {"pathling:export"})
  void test_forbidden_if_job_with_different_owner() {
    exportProvider = setup_scenario(tempDir, "Patient");
    // Test Scenario:
    // Another user 'other-user' has submitted a job. The 'admin' user with 'pathling:export'
    // authority attempts to read the job submitted by 'other-user' (it should fail).
    assertThatException().isThrownBy(() -> perform_export("other-user"))
        .isExactlyInstanceOf(AccessDeniedError.class)
        .withMessage("The requested result is not owned by the current user 'admin'.");
  }

  @Test
  @WithMockJwt(username = "admin", authorities = {"pathling:export", "pathling:read"})
  void test_pass_if_export_with_authority_and_read_authority() {
    exportProvider = setup_scenario(tempDir, "Patient");
    assertThatNoException().isThrownBy(() -> {
      JsonNode manifest = perform_export();
      ArrayNode output = (ArrayNode) manifest.get("output");
      assertThat(output)
          .hasSize(1)
          .extracting(node -> node.get("url").asText())
          .singleElement().asString()
          .contains("Patient");
    });
  }
  
  @Test
  @WithMockJwt(username = "admin", authorities = {"pathling:export", "pathling:read:Patient"})
  void test_silently_filter_out_if_export_with_authority_and_partial_read_authority() {
    exportProvider = setup_scenario(tempDir, "Patient", "Encounter");
    assertThatNoException().isThrownBy(() -> {
      JsonNode manifest = perform_export();
      ArrayNode output = (ArrayNode) manifest.get("output");
      assertThat(output)
          .hasSize(1)
          .extracting(node -> node.get("url").asText())
          .singleElement().asString()
          .doesNotContain("Encounter")
          .contains("Patient");
    });
  }

  @Test
  @WithMockJwt(username = "admin", authorities = {"pathling:export", "pathling:read:Patient"})
  void test_lenient_silently_filter_out_if_export_with_authority_and_partial_read_authority() {
    exportProvider = setup_scenario(tempDir, "Patient", "Encounter");
    assertThatNoException().isThrownBy(() -> {
      JsonNode manifest = perform_lenient_export();
      ArrayNode output = (ArrayNode) manifest.get("output");
      assertThat(output)
          .hasSize(1)
          .extracting(node -> node.get("url").asText())
          .singleElement().asString()
          .doesNotContain("Encounter")
          .contains("Patient");
    });
  }

  @Test
  @DisplayName("User only has read:Patient but explicitly requests Patient,Encounter (lenient=false) -> deny access")
  @WithMockJwt(username = "admin", authorities = {"pathling:export", "pathling:read:Patient"})
  void test_forbidden_if_export_with_type_param_with_authority_and_partial_read_authority() {
    exportProvider = setup_scenario(tempDir, "Patient", "Encounter");
    assertThatException().isThrownBy(() -> perform_export(ADMIN_USER, List.of("Patient", "Encounter"), false))
        .isExactlyInstanceOf(AccessDeniedError.class)
        .withMessage(ERROR_MSG.apply("read:Encounter"));
  }

  @Test
  @DisplayName("User only has read:Patient but explicitly requests Patient,Encounter (lenient=true) -> silently filter out")
  @WithMockJwt(username = "admin", authorities = {"pathling:export", "pathling:read:Patient"})
  void test_lenient_silently_filter_out_if_export_with_type_param_with_authority_and_partial_read_authority() {
    exportProvider = setup_scenario(tempDir, "Patient", "Encounter");
    assertThatNoException().isThrownBy(() -> {
      JsonNode manifest = perform_export(ADMIN_USER, List.of("Patient", "Encounter"), true);
      ArrayNode output = (ArrayNode) manifest.get("output");
      assertThat(output)
          .hasSize(1)
          .extracting(node -> node.get("url").asText())
          .singleElement().asString()
          .doesNotContain("Encounter")
          .contains("Patient");
    });
  }
  
  @Test
  @WithMockJwt(username = "admin")
  void test_forbidden_if_export_without_authority() {
    ExportProvider beanExportProvider = applicationContext.getBean(ExportProvider.class);
    assertThatException()
        .isThrownBy(() -> perform_export(beanExportProvider, ADMIN_USER, List.of(), false))
        .isExactlyInstanceOf(AccessDeniedError.class)
        .withMessage(PATHLING_EXPORT_MSG);
  }
  
  @Test
  @WithMockJwt(username = "admin", authorities = {"pathling:export", "pathling:read:Patient"})
  void test_forbidden_download_ndjson_without_authority() {
    exportProvider = setup_scenario(tempDir, "Patient");
    JsonNode manifest = perform_export();
    String url = manifest.get("output").get(0).get("url").asText();
    Map<String, String> queryParams = UriComponentsBuilder.fromUriString(url).build().getQueryParams().toSingleValueMap();

    switchToUser("newUser");
    
    assertThatException().isThrownBy(() -> perform_export_result(queryParams.get("job"), queryParams.get("file"), null))
        .isExactlyInstanceOf(AccessDeniedError.class)
        .withMessage(PATHLING_EXPORT_MSG);
  }
  
  @Test
  @WithMockJwt(username = "admin", authorities = {"pathling:export", "pathling:read:Patient"})
  void test_forbidden_download_ndjson_if_job_with_different_owner() {
    exportProvider = setup_scenario(tempDir, "Patient");
    JsonNode manifest = perform_export();
    String url = manifest.get("output").get(0).get("url").asText();
    Map<String, String> queryParams = UriComponentsBuilder.fromUriString(url).build().getQueryParams().toSingleValueMap();
    
    assertThatException().isThrownBy(() -> perform_export_result(queryParams.get("job"), queryParams.get("file"), "other-user"))
        .isExactlyInstanceOf(AccessDeniedError.class)
        .withMessage("The requested result is not owned by the current user 'admin'.");
  }
  
  @Test
  @WithMockJwt(username = "admin", authorities = {"pathling:export", "pathling:read:Patient"})
  void test_pass_if_download_ndjson_with_same_auth() {
    exportProvider = setup_scenario(tempDir, "Patient");
    JsonNode manifest = perform_export();
    String url = manifest.get("output").get(0).get("url").asText();
    Map<String, String> queryParams = UriComponentsBuilder.fromUriString(url).build().getQueryParams().toSingleValueMap();
    
    assertThatNoException().isThrownBy(() -> perform_export_result(queryParams.get("job"), queryParams.get("file"), "admin"));
  }

}
