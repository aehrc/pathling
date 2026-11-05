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

package au.csiro.pathling.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.annotation.Nonnull;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.BindException;
import org.springframework.boot.context.properties.bind.BindHandler;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.context.properties.bind.validation.ValidationBindHandler;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.validation.beanvalidation.SpringValidatorAdapter;

/**
 * Unit tests for {@link SqlQueryConfiguration}, covering the {@code maxDependencyDepth}, {@code
 * valueSetMaxMembers} and {@code conceptMapMaxMappings} defaults and their {@code @Min(1)}
 * validation, guarding against the reintroduction of a server-side row cap or query timeout, and
 * the Bean Validation rules for {@code externalTables}.
 *
 * @author John Grimes
 */
class SqlQueryConfigurationTest {

  private Validator validator;

  @BeforeEach
  void setUp() {
    validator = Validation.buildDefaultValidatorFactory().getValidator();
  }

  @Test
  void exposesOnlyTheDependencyDepthExternalTableAndTerminologySizeSettings() {
    // The dependency-depth limit and the value set and concept map size caps are the only resource
    // limits this configuration carries. A row cap silently truncates results and a wall-clock
    // timeout aborts legitimate long-running work, so neither may return: this test fails if a new
    // setting is added here.
    final Set<String> declaredSettings =
        Arrays.stream(SqlQueryConfiguration.class.getDeclaredFields())
            .filter(field -> !field.isSynthetic())
            .filter(field -> !Modifier.isStatic(field.getModifiers()))
            .map(Field::getName)
            .collect(Collectors.toSet());

    assertThat(declaredSettings)
        .containsExactlyInAnyOrder(
            "maxDependencyDepth", "externalTables", "valueSetMaxMembers", "conceptMapMaxMappings");
  }

  @Test
  void defaultMaxDependencyDepthIsTen() {
    // The default must be a generous-but-bounded value so that real, shallow view graphs are never
    // rejected while pathological fan-out is still capped.
    assertThat(new SqlQueryConfiguration().getMaxDependencyDepth()).isEqualTo(10);
  }

  @Test
  void acceptsPositiveMaxDependencyDepth() {
    final SqlQueryConfiguration config = new SqlQueryConfiguration();
    config.setMaxDependencyDepth(1);

    final Set<ConstraintViolation<SqlQueryConfiguration>> violations = validator.validate(config);

    assertThat(violations).isEmpty();
  }

  @Test
  void rejectsZeroMaxDependencyDepth() {
    // A depth of zero would forbid even a single dependency, which is nonsensical for a feature
    // whose purpose is dependency resolution.
    final SqlQueryConfiguration config = new SqlQueryConfiguration();
    config.setMaxDependencyDepth(0);

    final Set<ConstraintViolation<SqlQueryConfiguration>> violations = validator.validate(config);

    assertThat(violations).isNotEmpty();
  }

  @Test
  void rejectsNegativeMaxDependencyDepth() {
    final SqlQueryConfiguration config = new SqlQueryConfiguration();
    config.setMaxDependencyDepth(-5);

    final Set<ConstraintViolation<SqlQueryConfiguration>> violations = validator.validate(config);

    assertThat(violations).isNotEmpty();
  }

  // -------------------------------------------------------------------------
  // Value set membership cap
  // -------------------------------------------------------------------------

  @Test
  void defaultValueSetMaxMembersIsOneHundredThousand() {
    // The cap bounds the memory a single value set relation may take; the default admits every
    // realistic clinical value set while still rejecting an unbounded expansion.
    assertThat(new SqlQueryConfiguration().getValueSetMaxMembers()).isEqualTo(100_000);
  }

  @Test
  void acceptsValueSetMaxMembersOfOne() {
    final SqlQueryConfiguration config = new SqlQueryConfiguration();
    config.setValueSetMaxMembers(1);

    assertThat(validator.validate(config)).isEmpty();
  }

  @Test
  void rejectsZeroValueSetMaxMembers() {
    // A cap of zero would reject every non-empty value set, defeating the feature entirely.
    final SqlQueryConfiguration config = new SqlQueryConfiguration();
    config.setValueSetMaxMembers(0);

    final Set<ConstraintViolation<SqlQueryConfiguration>> violations = validator.validate(config);

    assertThat(violations).hasSize(1);
    assertThat(violations.iterator().next().getPropertyPath()).hasToString("valueSetMaxMembers");
  }

  // -------------------------------------------------------------------------
  // Concept map size cap
  // -------------------------------------------------------------------------

  @Test
  void defaultConceptMapMaxMappingsIsOneHundredThousand() {
    // The cap bounds the memory a single concept map relation may take; the default admits every
    // realistic clinical concept map while still rejecting an unbounded one.
    assertThat(new SqlQueryConfiguration().getConceptMapMaxMappings()).isEqualTo(100_000);
  }

  @Test
  void acceptsConceptMapMaxMappingsOfOne() {
    final SqlQueryConfiguration config = new SqlQueryConfiguration();
    config.setConceptMapMaxMappings(1);

    assertThat(validator.validate(config)).isEmpty();
  }

  @Test
  void rejectsZeroConceptMapMaxMappings() {
    // A cap of zero would reject every non-empty concept map, defeating the feature entirely.
    final SqlQueryConfiguration config = new SqlQueryConfiguration();
    config.setConceptMapMaxMappings(0);

    final Set<ConstraintViolation<SqlQueryConfiguration>> violations = validator.validate(config);

    assertThat(violations).hasSize(1);
    assertThat(violations.iterator().next().getPropertyPath()).hasToString("conceptMapMaxMappings");
  }

  // -------------------------------------------------------------------------
  // External tables
  // -------------------------------------------------------------------------

  @Test
  void externalTablesDefaultToEmptyAndAreValid() {
    // An absent property must be indistinguishable from "no tables", so the default is an empty
    // list rather than null.
    final SqlQueryConfiguration config = new SqlQueryConfiguration();

    assertThat(config.getExternalTables()).isEmpty();
    assertThat(validator.validate(config)).isEmpty();
  }

  @Test
  void formatDefaultsToDelta() {
    assertThat(new ExternalTableConfiguration().getFormat()).isEqualTo("delta");
  }

  @Test
  void acceptsValidDeltaAndParquetEntries() {
    final SqlQueryConfiguration config = new SqlQueryConfiguration();
    config.setExternalTables(
        List.of(
            externalTable("https://example.org/data/refsets", "s3a://bucket/refsets", "delta"),
            externalTable(
                "https://example.org/data/postcodes", "file:///data/postcodes", "parquet")));

    assertThat(validator.validate(config)).isEmpty();
  }

  @Test
  void rejectsBlankUrl() {
    assertSingleEntryViolation(externalTable("  ", "s3a://bucket/refsets", "delta"), "url");
  }

  @Test
  void rejectsUrlContainingPipe() {
    // CanonicalReference.parse splits "url|version" on the pipe, so such a URL could never be
    // referenced from a Library.
    assertSingleEntryViolation(
        externalTable("https://example.org/data/refsets|1", "s3a://bucket/refsets", "delta"),
        "url");
  }

  @Test
  void rejectsBlankPath() {
    assertSingleEntryViolation(
        externalTable("https://example.org/data/refsets", "", "delta"), "path");
  }

  @Test
  void rejectsUnsupportedFormat() {
    assertSingleEntryViolation(
        externalTable("https://example.org/data/refsets", "s3a://bucket/refsets", "csv"), "format");
  }

  @Test
  void rejectsDuplicateUrls() {
    final SqlQueryConfiguration config = new SqlQueryConfiguration();
    config.setExternalTables(
        List.of(
            externalTable("https://example.org/data/refsets", "s3a://bucket/a", "delta"),
            externalTable("https://example.org/data/refsets", "s3a://bucket/b", "parquet")));

    final Set<ConstraintViolation<SqlQueryConfiguration>> violations = validator.validate(config);

    assertThat(violations).hasSize(1);
    assertThat(violations.iterator().next().getMessage())
        .isEqualTo("externalTables must not contain duplicate urls");
  }

  @Test
  void bindingFailsForUnsupportedFormat() {
    // Binding is the mechanism Spring Boot uses to populate configuration properties at startup, so
    // a rejected value here is what prevents the server from starting.
    final MapConfigurationPropertySource source =
        new MapConfigurationPropertySource(
            Map.of(
                "pathling.sqlQuery.externalTables.0.url", "https://example.org/data/refsets",
                "pathling.sqlQuery.externalTables.0.path", "s3a://bucket/refsets",
                "pathling.sqlQuery.externalTables.0.format", "csv"));
    final BindHandler handler =
        new ValidationBindHandler(
            new SpringValidatorAdapter(Validation.buildDefaultValidatorFactory().getValidator()));

    // The property source keeps the camel-case form used in the property files; the bind name
    // itself must be canonical (kebab-case), as Spring Boot normalises it at startup.
    assertThatThrownBy(
            () ->
                new Binder(source)
                    .bind("pathling.sql-query", Bindable.of(SqlQueryConfiguration.class), handler))
        .isInstanceOf(BindException.class)
        .rootCause()
        .isInstanceOf(BindValidationException.class)
        .hasMessageContaining("external-tables[0]")
        .hasMessageContaining("on field 'format'")
        .hasMessageContaining("pathling.sqlQuery.externalTables.0.format")
        .hasMessageContaining("csv");
  }

  private void assertSingleEntryViolation(
      @Nonnull final ExternalTableConfiguration entry, @Nonnull final String field) {
    final SqlQueryConfiguration config = new SqlQueryConfiguration();
    config.setExternalTables(List.of(entry));

    final Set<ConstraintViolation<SqlQueryConfiguration>> violations = validator.validate(config);

    assertThat(violations).hasSize(1);
    assertThat(violations.iterator().next().getPropertyPath())
        .hasToString("externalTables[0]." + field);
  }

  @Nonnull
  private static ExternalTableConfiguration externalTable(
      @Nonnull final String url, @Nonnull final String path, @Nonnull final String format) {
    final ExternalTableConfiguration entry = new ExternalTableConfiguration();
    entry.setUrl(url);
    entry.setPath(path);
    entry.setFormat(format);
    return entry;
  }
}
