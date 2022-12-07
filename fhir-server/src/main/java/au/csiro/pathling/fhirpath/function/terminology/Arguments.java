package au.csiro.pathling.fhirpath.function.terminology;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.function.NamedFunctionInput;
import au.csiro.pathling.fhirpath.literal.LiteralPath;
import org.hl7.fhir.r4.model.Type;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * Helper class for dealing with optional arguments.
 */
public class Arguments {

  @Nonnull
  private final List<FhirPath> arguments;

  private Arguments(@Nonnull final List<FhirPath> arguments) {
    this.arguments = arguments;
  }

  /**
   * Gets the value of an optional literal argument or the default value it the argument is
   * missing.
   *
   * @param index the 0-based index of the argument.
   * @param defaultValue the default value.
   * @param <T> the Java type of the argument value.
   * @return the java value of the requested argument.
   */
  @SuppressWarnings("unchecked")
  @Nonnull
  public <T extends Type> T getValueOr(final int index, @Nonnull final T defaultValue) {
    return (index < arguments.size())
           ? getValue(index, (Class<T>) defaultValue.getClass())
           : defaultValue;
  }

  /**
   * Gets the value of an optional literal argument that does not have a default value.
   *
   * @param index the 0-based index of the argument.
   * @param <T> the Java type of the argument value.
   * @return the java value of the requested argument.
   */
  @Nullable
  public <T extends Type> T getNullableValue(final int index,
      @Nonnull final Class<T> valueClass) {
    final LiteralPath<?> literalPath;
    try {
      literalPath = (LiteralPath<?>) arguments.get(index);
    } catch (final IndexOutOfBoundsException e) {
      return null;
    }
    return valueClass.cast(literalPath.getValue());
  }

  /**
   * Gets the value of the required literal argument.
   *
   * @param index the 0-based index of the argument
   * @param valueClass the expected Java  class of the argument value
   * @param <T> the HAPI type of the argument value
   * @return the java value of the requested argument
   */
  @Nonnull
  public <T extends Type> T getValue(final int index, @Nonnull final Class<T> valueClass) {
    return Objects.requireNonNull(getNullableValue(index, valueClass));
  }

  /**
   * Construct Arguments for given {@link NamedFunctionInput}
   *
   * @param input the function input.
   * @return the Arguments for the input.
   */
  @Nonnull
  public static Arguments of(@Nonnull final NamedFunctionInput input) {
    return new Arguments(input.getArguments());
  }
}
