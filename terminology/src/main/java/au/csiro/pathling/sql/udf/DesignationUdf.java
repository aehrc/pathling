package au.csiro.pathling.sql.udf;

import static au.csiro.pathling.fhirpath.CodingHelpers.codingEquals;
import static au.csiro.pathling.fhirpath.encoding.CodingEncoding.decode;
import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.isValidCoding;
import static java.util.Objects.isNull;

import au.csiro.pathling.terminology.TerminologyService2;
import au.csiro.pathling.terminology.TerminologyService2.Designation;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;

/**
 * The implementation of the 'designation()' udf.
 */
@Slf4j
public class DesignationUdf implements SqlFunction,
    SqlFunction3<Row, Row, String, String[]> {

  private static final long serialVersionUID = -4123584679085357391L;

  public static final String FUNCTION_NAME = "designation";
  public static final DataType RETURN_TYPE = DataTypes.createArrayType(DataTypes.StringType);
  public static final String DESIGNATION_PROPERTY_CODE = Designation.PROPERTY_CODE;

  private static final String[] EMPTY_RESULT = new String[0];

  @Nonnull
  private final TerminologyServiceFactory terminologyServiceFactory;

  DesignationUdf(@Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    this.terminologyServiceFactory = terminologyServiceFactory;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public DataType getReturnType() {
    return RETURN_TYPE;
  }

  @Nullable
  protected String[] doCall(@Nullable final Coding coding,
      @Nullable final Coding use, @Nullable final String language) {
    if (coding == null || use == null) {
      return null;
    }
    if (!isValidCoding(coding) || !isValidCoding(use)) {
      return EMPTY_RESULT;
    }
    final TerminologyService2 terminologyService = terminologyServiceFactory.buildService2();
    return terminologyService.lookup(coding, DESIGNATION_PROPERTY_CODE).stream()
        .filter(Designation.class::isInstance)
        .map(Designation.class::cast)
        .filter(designation -> isNull(language) || language.equals(designation.getLanguage()))
        .filter(designation -> codingEquals(use, designation.getUse()))
        .map(Designation::getValue)
        .toArray(String[]::new);
  }

  @Nullable
  @Override
  public String[] call(@Nullable final Row codingRow, @Nullable final Row use,
      @Nullable final String language) {
    return doCall(decode(codingRow), decode(use), language);
  }
}
