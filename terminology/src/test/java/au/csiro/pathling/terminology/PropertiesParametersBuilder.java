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

package au.csiro.pathling.terminology;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.Type;

@SuppressWarnings({"unused", "SameParameterValue"})
class PropertiesParametersBuilder {

  private final Parameters parameters = new Parameters();
  private Parameters.ParametersParameterComponent currentGroup = null;

  PropertiesParametersBuilder withStdProperty(@Nonnull final String code,
      @Nonnull final Type value) {
    parameters.addParameter(code, value);
    return this;
  }

  PropertiesParametersBuilder withStdProperty(@Nonnull final String code,
      @Nonnull final String value) {
    parameters.addParameter(code, value);
    return this;
  }

  PropertiesParametersBuilder withStdProperties(@Nonnull final Coding coding) {
    return this
        .withStdProperty("display", coding.getDisplay())
        .withStdProperty("code", new CodeType(coding.getCode()))
        .withStdProperty("name", "My Test Coding System");
  }

  PropertiesParametersBuilder withProperty(@Nonnull final String code,
      @Nonnull final Type value) {
    final Parameters.ParametersParameterComponent component = parameters.addParameter();
    component.setName("property");
    component.addPart().setName("code").setValue(new CodeType(code));
    component.addPart().setName("value").setValue(value);
    return this;
  }

  PropertiesParametersBuilder withProperty(@Nonnull final String code,
      @Nonnull final String stringValue) {
    return withProperty(code, new StringType(stringValue));
  }

  PropertiesParametersBuilder withPropertyGroup(@Nonnull final String code) {
    final Parameters.ParametersParameterComponent component = parameters.addParameter();
    component.setName("property");
    component.addPart().setName("code").setValue(new CodeType(code));
    currentGroup = component;
    return this;
  }

  PropertiesParametersBuilder withSubProperty(@Nonnull final String code,
      @Nonnull final Type value) {
    final ParametersParameterComponent subcomponent = currentGroup.addPart()
        .setName("subproperty");
    subcomponent.addPart().setName("code").setValue(new CodeType(code));
    subcomponent.addPart().setName("value").setValue(value);
    return this;
  }

  PropertiesParametersBuilder withSubProperty(@Nonnull final String code,
      @Nonnull final String stringValue) {
    return withSubProperty(code, new StringType(stringValue));
  }

  @Nonnull
  PropertiesParametersBuilder withDesignation(@Nonnull final String value,
      @Nonnull final Coding use, @Nonnull final String languageCode) {
    return withDesignation(value, Optional.of(use), Optional.of(languageCode));
  }

  @Nonnull
  PropertiesParametersBuilder withDesignation(@Nonnull final String value,
      @Nonnull final Optional<Coding> maybeUse, @Nonnull final Optional<String> maybeLanguageCode) {
    final Parameters.ParametersParameterComponent component = parameters
        .addParameter().setName("designation");

    component.addPart().setName("value").setValue(new StringType(value));
    maybeUse.ifPresent(use -> component.addPart().setName("use").setValue(use));
    maybeLanguageCode.ifPresent(languageCode ->
        component.addPart().setName("language").setValue(new CodeType(languageCode)));
    return this;
  }

  @Nonnull
  Parameters build() {
    return parameters;
  }

  @Nonnull
  static PropertiesParametersBuilder standardProperties(@Nonnull final Coding coding) {
    return new PropertiesParametersBuilder().withStdProperties(coding);
  }
}
