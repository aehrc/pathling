package au.csiro.pathling.sql.udf;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import au.csiro.pathling.terminology.TerminologyService2;
import au.csiro.pathling.terminology.TerminologyService2.Translation;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Coding;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.mutable.WrappedArray;

import javax.annotation.Nonnull;
import java.util.stream.Stream;

import static au.csiro.pathling.fhirpath.encoding.CodingEncoding.encode;
import static org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence.EQUIVALENT;
import static org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence.NARROWER;
import static org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence.RELATEDTO;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class TranslateOfUdfTest {
  
  public static final String SYSTEM_A = "uuid:systemA";
  public static final String CODE_A = "codeA";
  private static final String SYSTEM_B = "uuid:systemB";
  private static final String CODE_B = "codeB";

  private static final Coding CODING_AA = new Coding(SYSTEM_A, CODE_A, "displayAA");
  private static final Coding CODING_AB = new Coding(SYSTEM_A, CODE_B, "displayAB");

  public static final String VERSION_1 = "version1";
  public static final String VERSION_2 = "version2";

  private static final Coding CODING_AA_VERSION1 = new Coding(SYSTEM_A, CODE_A,
      "displayAA").setVersion(
      VERSION_1);
  private static final Coding CODING_AB_VERSION1 = new Coding(SYSTEM_A, CODE_B,
      "displayAB").setVersion(
      VERSION_1);
  private static final Coding CODING_AB_VERSION2 = new Coding(SYSTEM_A, CODE_B,
      "displayAB").setVersion(
      VERSION_2);

  private static final Coding CODING_BA = new Coding(SYSTEM_B, CODE_A, "displayBA");
  private static final Coding CODING_BB = new Coding(SYSTEM_B, CODE_B, "displayBB");
  private static final Coding CODING_BB_VERSION1 = new Coding(SYSTEM_B, CODE_B,
      "displayB").setVersion(
      VERSION_1);

  private static final String CONCEPT_MAP_A = "uuid:caA";
  private static final String CONCEPT_MAP_B = "uuid:caB";


  private static final Coding INVALID_CODING_0 = new Coding(null, null, "");
  private static final Coding INVALID_CODING_1 = new Coding("uiid:system", null, "");
  private static final Coding INVALID_CODING_2 = new Coding(null, "someCode", "");

  private static final Row[] NO_TRANSLATIONS = new Row[]{};

  private TranslateUdf translateUdf;
  private TerminologyService2 terminologyService2;


  @Nonnull
  private static Row[] asArray(@Nonnull final Coding... codings) {
    return Stream.of(codings).map(CodingEncoding::encode).toArray(Row[]::new);
  }

  @Nonnull
  public static WrappedArray<Row> encodeMany(Coding... codings) {
    return WrappedArray.make(Stream.of(codings).map(CodingEncoding::encode).toArray(Row[]::new));
  }

  @BeforeEach
  void setUp() {
    terminologyService2 = mock(TerminologyService2.class);
    final TerminologyServiceFactory terminologyServiceFactory = mock(
        TerminologyServiceFactory.class);
    when(terminologyServiceFactory.buildService2()).thenReturn(terminologyService2);
    translateUdf = new TranslateUdf(terminologyServiceFactory);

  }

  @Test
  void testNullCodings() {
    assertNull(translateUdf.call(null, CONCEPT_MAP_A,
        true, null, null));
  }

  @Test
  void testTranslatesCodingWithDefaults() {

    TerminologyServiceHelpers.setupTranslate(terminologyService2)
        .withTranslations(CODING_AA, CONCEPT_MAP_A,
            Translation.of(EQUIVALENT, CODING_BB),
            Translation.of(RELATEDTO, CODING_AB));

    assertArrayEquals(asArray(CODING_BB),
        translateUdf.call(encode(CODING_AA), CONCEPT_MAP_A, false, null, null));
  }

  @Test
  void testTranslatesCodingsUniqueResults() {

    TerminologyServiceHelpers.setupTranslate(terminologyService2)
        .withTranslations(CODING_AA_VERSION1, CONCEPT_MAP_B, true, SYSTEM_B,
            Translation.of(EQUIVALENT, CODING_AA),
            Translation.of(NARROWER, CODING_BB),
            Translation.of(RELATEDTO, CODING_AB))
        .withTranslations(CODING_AB_VERSION1, CONCEPT_MAP_B, true, SYSTEM_B,
            Translation.of(EQUIVALENT, CODING_AB),
            Translation.of(NARROWER, CODING_BB),
            Translation.of(RELATEDTO, CODING_BA));

    assertArrayEquals(asArray(CODING_BB, CODING_AB, CODING_BA),
        translateUdf.call(encodeMany(null, INVALID_CODING_1, INVALID_CODING_0, INVALID_CODING_2,
                CODING_AA_VERSION1, CODING_AB_VERSION1), CONCEPT_MAP_B, true,
            "narrower, relatedto", SYSTEM_B));
  }

  @Test
  void testInvalidAndNullCodings() {
    assertArrayEquals(NO_TRANSLATIONS,
        translateUdf.call(encodeMany(INVALID_CODING_0, INVALID_CODING_1, INVALID_CODING_2, null),
            "uuid:url", true, null, null));
    verifyNoMoreInteractions(terminologyService2);
  }

  @Test
  void testThrowsInputErrorWhenInvalidEquivalence() {
    final InvalidUserInputError ex = assertThrows(InvalidUserInputError.class,
        () -> translateUdf.call(encode(CODING_AA), CONCEPT_MAP_B, true, "invalid", null));
    assertEquals("Unknown ConceptMapEquivalence code 'invalid'", ex.getMessage());
    verifyNoMoreInteractions(terminologyService2);
  }

}
