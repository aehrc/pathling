package au.csiro.pathling.test.helpers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.r4.model.Base;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

public class FhirDeepMatcher<T extends Base> implements ArgumentMatcher<T> {

  @Nonnull
  private final T expected;

  private FhirDeepMatcher(@Nonnull final T expected) {
    this.expected = expected;
  }

  @Override
  public boolean matches(@Nullable final T actual) {
    return expected.equalsDeep(actual);
  }

  public static <T extends Base> T deepEq(@Nonnull final T expected) {
    return Mockito.argThat(new FhirDeepMatcher<>(expected));
  }

}
