package au.csiro.pathling.sql.udf;

import static au.csiro.pathling.fhirpath.encoding.CodingEncoding.decode;
import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.isValidCoding;
import static java.util.Objects.nonNull;

import au.csiro.pathling.terminology.TerminologyService2;
import au.csiro.pathling.terminology.TerminologyService2.Property;
import au.csiro.pathling.terminology.TerminologyService2.PropertyOrDesignation;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;

/**
 * The implementation of the 'display()' udf.
 */
@Slf4j
public class DisplayUdf implements SqlFunction,
    SqlFunction1<Row, String> {

  private static final long serialVersionUID = 7605853352299165569L;
  
  public static final String DISPLAY_PROPERTY_CODE = "display";
  public static final String FUNCTION_NAME = "display";
  public static final DataType RETURN_TYPE = DataTypes.StringType;

  @Nonnull
  private final TerminologyServiceFactory terminologyServiceFactory;

  DisplayUdf(@Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
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
  protected String doCall(@Nullable final Coding coding) {
    if (coding == null || !isValidCoding(coding)) {
      return null;
    }
    final TerminologyService2 terminologyService = terminologyServiceFactory.buildService2();
    final List<PropertyOrDesignation> result = terminologyService.lookup(
        coding, DISPLAY_PROPERTY_CODE, null);

    final Optional<Property> maybeDisplayName = result.stream()
        .filter(s -> s instanceof Property)
        .map(s -> (Property) s)
        .filter(p -> DISPLAY_PROPERTY_CODE.equals(p.getCode()))
        .findFirst();

    return maybeDisplayName.map(Property::getValueAsString).orElse(null);
  }

  @Nullable
  @Override
  public String call(@Nullable final Row codingRow) {
    return doCall(nonNull(codingRow)
                  ? decode(codingRow)
                  : null);
  }
}
