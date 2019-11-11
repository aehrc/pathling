/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Objects;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.UriType;
import org.mockito.ArgumentMatcher;
import org.mockito.ArgumentMatchers;

/**
 * @author John Grimes
 */
public abstract class TestUtilities {

  private static final FhirContext fhirContext = FhirContext.forR4();
  private static final IParser jsonParser = fhirContext.newJsonParser();

  public static FhirContext getFhirContext() {
    return fhirContext;
  }

  public static IParser getJsonParser() {
    return jsonParser;
  }

  public static InputStream getResourceAsStream(String name) {
    InputStream expectedStream = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream(name);
    assertThat(expectedStream).isNotNull();
    return expectedStream;
  }

  public static String getResourceAsString(String name) throws IOException {
    InputStream expectedStream = getResourceAsStream(name);
    StringWriter writer = new StringWriter();
    IOUtils.copy(expectedStream, writer, UTF_8);
    return writer.toString();
  }

  public static StructType codingStructType() {
    Metadata metadata = new MetadataBuilder().build();
    StructField system = new StructField("system", DataTypes.StringType, true, metadata);
    StructField version = new StructField("version", DataTypes.StringType, true, metadata);
    StructField code = new StructField("code", DataTypes.StringType, true, metadata);
    StructField display = new StructField("display", DataTypes.StringType, true, metadata);
    StructField userSelected = new StructField("userSelected", DataTypes.BooleanType, true,
        metadata);
    return new StructType(new StructField[]{system, version, code, display, userSelected});
  }

  public static StructType codeableConceptStructType() {
    Metadata metadata = new MetadataBuilder().build();
    StructField coding = new StructField("coding", DataTypes.createArrayType(codingStructType()),
        true, metadata);
    StructField text = new StructField("text", DataTypes.StringType, true, metadata);
    return new StructType(new StructField[]{coding, text});
  }

  public static Row rowFromCoding(Coding coding) {
    return RowFactory
        .create(coding.getSystem(), coding.getVersion(), coding.getCode(), coding.getDisplay(),
            coding.getUserSelected());
  }

  public static Row rowFromCodeableConcept(CodeableConcept codeableConcept) {
    Object[] codings = codeableConcept.getCoding().stream().map(TestUtilities::rowFromCoding)
        .toArray();
    return RowFactory.create(codings, codeableConcept.getText());
  }

  public static Coding matchesCoding(Coding expected) {
    return ArgumentMatchers.argThat(expected::equalsDeep);
  }

  public static CodeableConcept matchesCodeableConcept(CodeableConcept expected) {
    return ArgumentMatchers.argThat(expected::equalsDeep);
  }

  public static UriType matchesUri(String expected) {
    return ArgumentMatchers.argThat(actual -> new UriType(expected).equalsDeep(actual));
  }

  public static class CodingMatcher implements ArgumentMatcher<Coding> {

    private Coding coding;

    public CodingMatcher(Coding coding) {
      this.coding = coding;
    }

    @Override
    public boolean matches(Coding argument) {
      if (argument == null) {
        return false;
      }
      return coding.getUserSelected() == argument.getUserSelected() &&
          Objects.equals(coding.getSystem(), argument.getSystem()) &&
          Objects.equals(coding.getVersion(), argument.getVersion()) &&
          Objects.equals(coding.getCode(), argument.getCode()) &&
          Objects.equals(coding.getDisplay(), argument.getDisplay());
    }

  }

  public static class UriMatcher implements ArgumentMatcher<UriType> {

    private UriType uriType;

    public UriMatcher(UriType uriType) {
      this.uriType = uriType;
    }

    @Override
    public boolean matches(UriType argument) {
      if (argument == null) {
        return false;
      }
      return Objects.equals(argument.asStringValue(), uriType.asStringValue());
    }
  }


}
