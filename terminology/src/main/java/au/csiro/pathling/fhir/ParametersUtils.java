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
 * Helper functions for dealing with FHIR {@link Parameters} resource.
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

  /**
   * Extracts the boolean value of the 'result' parameter.
   *
   * @param parameters the parameters to convert.
   * @return the boolean value of the 'result' parameter.
   */
  public static boolean toBooleanResult(final @Nonnull Parameters parameters) {
    return parameters.getParameterBool("result");
  }

  /**
   * Extracts the {@link ConceptSubsumptionOutcome} value of the 'outcome' parameter.
   *
   * @param parameters the parameters to convert.
   * @return the {@link ConceptSubsumptionOutcome} value.
   */
  @Nonnull
  public static ConceptSubsumptionOutcome toSubsumptionOutcome(
      final @Nonnull Parameters parameters) {
    return ConceptSubsumptionOutcome.fromCode(
        parameters.getParameter("outcome").primitiveValue());
  }

  /**
   * Object representation of a 'match' part from 'translate()' result.
   */
  @Data
  @NoArgsConstructor
  public static class MatchPart {

    @Nonnull
    private Coding concept;

    @Nonnull
    private CodeType equivalence;
  }

  @Nonnull
  private static MatchPart componentToMatchPart(
      @Nonnull final Parameters.ParametersParameterComponent component) {
    return ParametersUtils.partsToBean(component, MatchPart::new);
  }

  /**
   * Extracts 'match' parts from the result of 'translate()'
   *
   * @param parameters the parameters to convert.
   * @return the stream of 'match' parts.
   */
  @Nonnull
  public static Stream<MatchPart> toMatchParts(final @Nonnull Parameters parameters) {
    return toBooleanResult(parameters)
           ? parameters.getParameter().stream()
               .filter(pc -> "match".equals(pc.getName()))
               .map(ParametersUtils::componentToMatchPart)
           : Stream.empty();
  }
}
