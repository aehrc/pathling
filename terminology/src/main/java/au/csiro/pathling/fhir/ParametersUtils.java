package au.csiro.pathling.fhir;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.beanutils.BeanUtils;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome;

/**
 *
 */
public final class ParametersUtils {

  private ParametersUtils() {
    // Utility class
  }
  
  private static void setProperty(@Nonnull final Object bean, @Nonnull final String name,
      @Nullable final Object value) {
    try {
      BeanUtils.setProperty(bean, name, value);
    } catch (final IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Converts ParametersParameterComponent object to a java bean class initializing its properties
   * from the part properties.
   *
   * @param component the ParametersParameterComponent element.
   * @param supplier the supplier for the bean class.
   * @param <T> the type of the bean.
   * @return the new java bean of type T initialize from the ParametersParameterComponent element.
   */
  @Nonnull
  public static <T> T partsToBean(@Nonnull final ParametersParameterComponent component,
      @Nonnull final Supplier<T> supplier) {
    final T result = supplier.get();
    component.getPart().forEach(p -> setProperty(result, p.getName(), p.getValue()));
    return result;
  }

  public static boolean toBoolean(final @Nonnull Parameters parameters) {
    return parameters.getParameterBool("result");
  }

  @Nonnull
  public static ConceptSubsumptionOutcome toSubsumptionOutcome(
      final @Nonnull Parameters parameters) {
    return ConceptSubsumptionOutcome.fromCode(
        parameters.getParameter("outcome").primitiveValue());
  }

  @Data
  @NoArgsConstructor
  public static class TranslationPart {

    @Nonnull
    private Coding concept;

    @Nonnull
    private CodeType equivalence;
  }

  @Nonnull
  private static TranslationPart matchToTranslationPart(
      @Nonnull final Parameters.ParametersParameterComponent component) {
    return ParametersUtils.partsToBean(component, TranslationPart::new);
  }

  @Nonnull
  public static Stream<TranslationPart> toTranslationParts(final @Nonnull Parameters parameters) {
    return toBoolean(parameters)
           ? parameters.getParameter().stream()
               .filter(pc -> "match".equals(pc.getName()))
               .map(ParametersUtils::matchToTranslationPart)
           : Stream.empty();
  }
}
