package au.csiro.pathling.terminology;

import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;

import javax.annotation.Nonnull;

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

  PropertiesParametersBuilder withDesignation(@Nonnull final String languageCode,
      @Nonnull final Coding use,
      @Nonnull final String value) {
    final Parameters.ParametersParameterComponent component = parameters.addParameter();
    component.setName("designation");
    component.addPart().setName("language").setValue(new CodeType(languageCode));
    component.addPart().setName("use").setValue(use);
    component.addPart().setName("value").setValue(new StringType(value));
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
