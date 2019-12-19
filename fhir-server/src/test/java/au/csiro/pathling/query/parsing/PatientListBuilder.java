package au.csiro.pathling.query.parsing;

import au.csiro.pathling.test.RowListBuilder;
import java.util.Arrays;
import java.util.List;


/**
 * @author Piotr Szul
 */

public class PatientListBuilder extends RowListBuilder {

  public static final String PATIENT_ID_121503c8 = "Patient/121503c8-9564-4b48-9086-a22df717948e";
  public static final String PATIENT_ID_2b36c1e2 = "Patient/2b36c1e2-bbe1-45ae-8124-4adad2677702";
  public static final String PATIENT_ID_7001ad9c = "Patient/7001ad9c-34d2-4eb5-8165-5fdc2147f469";
  public static final String PATIENT_ID_8ee183e2 = "Patient/8ee183e2-b3c0-4151-be94-b945d6aa8c6d";
  public static final String PATIENT_ID_9360820c = "Patient/9360820c-8602-4335-8b50-c88d627a0c20";
  public static final String PATIENT_ID_a7eb2ce7 = "Patient/a7eb2ce7-1075-426c-addd-957b861b0e55";
  public static final String PATIENT_ID_bbd33563 = "Patient/bbd33563-70d9-4f6d-a79a-dd1fc55f5ad9";
  public static final String PATIENT_ID_beff242e = "Patient/beff242e-580b-47c0-9844-c1a68c36c5bf";
  public static final String PATIENT_ID_e62e52ae = "Patient/e62e52ae-2d75-4070-a0ae-3cc78d35ed08";

  public static final List<String> PATIENT_ALL_IDS = Arrays.asList(PATIENT_ID_121503c8,
      PATIENT_ID_2b36c1e2, PATIENT_ID_7001ad9c, PATIENT_ID_8ee183e2, PATIENT_ID_9360820c,
      PATIENT_ID_a7eb2ce7, PATIENT_ID_bbd33563, PATIENT_ID_beff242e, PATIENT_ID_e62e52ae);


  public static RowListBuilder allPatientsWithValue(Object value) {
    return allWithValue(value, PATIENT_ALL_IDS);
  }
}
