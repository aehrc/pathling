package au.csiro.pathling.test;

import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Coding;
import scala.collection.mutable.WrappedArray;

public abstract class TerminologyTest {

  public static final String SYSTEM_A = "uuid:systemA";
  public static final String CODE_A = "codeA";
  public static final String SYSTEM_B = "uuid:systemB";
  public static final String CODE_B = "codeB";
  public static final String SYSTEM_C = "uuid:systemC";
  public static final String CODE_C = "codeC";
  public static final String SYSTEM_D = "uuid:systemD";
  public static final String CODE_D = "codeD";

  public static final Coding CODING_AA = new Coding(SYSTEM_A, CODE_A, "displayAA");
  public static final Coding CODING_AB = new Coding(SYSTEM_A, CODE_B, "displayAB");

  public static final String VERSION_1 = "version1";
  public static final String VERSION_2 = "version2";

  public static final Coding CODING_AA_VERSION1 = new Coding(SYSTEM_A, CODE_A,
      "displayAA").setVersion(
      VERSION_1);
  public static final Coding CODING_AB_VERSION1 = new Coding(SYSTEM_A, CODE_B,
      "displayAB").setVersion(
      VERSION_1);
  public static final Coding CODING_AB_VERSION2 = new Coding(SYSTEM_A, CODE_B,
      "displayAB").setVersion(
      VERSION_2);

  public static final Coding CODING_BA = new Coding(SYSTEM_B, CODE_A, "displayBA");
  public static final Coding CODING_BB = new Coding(SYSTEM_B, CODE_B, "displayBB");
  public static final Coding CODING_BB_VERSION1 = new Coding(SYSTEM_B, CODE_B,
      "displayB").setVersion(
      VERSION_1);

  public static final Coding CODING_A = CODING_AA;
  public static final Coding CODING_B = CODING_BB;
  public static final Coding CODING_C = new Coding(SYSTEM_C, CODE_C, "displayCC");
  public static final Coding CODING_D = new Coding(SYSTEM_D, CODE_D, "displayDD");

  public static final Coding INVALID_CODING_0 = new Coding(null, null, "");
  public static final Coding INVALID_CODING_1 = new Coding("uiid:system", null, "");
  public static final Coding INVALID_CODING_2 = new Coding(null, "someCode", "");


  @Nonnull
  public static Row[] asArray(@Nonnull final Coding... codings) {
    return Stream.of(codings).map(CodingEncoding::encode).toArray(Row[]::new);
  }

  @Nonnull
  public static WrappedArray<Row> encodeMany(Coding... codings) {
    return WrappedArray.make(Stream.of(codings).map(CodingEncoding::encode).toArray(Row[]::new));
  }
}
