package au.csiro.pathling;

import au.csiro.pathling.errors.InvalidUserInputError;
import jakarta.annotation.Nullable;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.Type;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * @author Felix Naumann
 */
public class ParamUtil {

  public static <T> Collection<T> extractManyFromParameters(
      Collection<ParametersParameterComponent> parts,
      String partName,
      Class<T> typeClazz,
      boolean lenient,
      RuntimeException onError
  ) {
    return extractManyFromParameters(parts, partName, typeClazz, false, null, lenient, onError);
  }
  
  public static <T extends Type> Collection<T> extractManyFromParameters(
      Collection<ParametersParameterComponent> parts,
      String partName,
      Class<T> typeClazz,
      @Nullable Collection<T> defaultValue,
      boolean lenient
  ) {
    return extractManyFromParameters(parts, partName, typeClazz, true, defaultValue, lenient, null);
  }
  
  public static <T> Collection<T> extractManyFromParameters(
      Collection<ParametersParameterComponent> parts,
      String partName,
      Class<T> typeClazz,
      boolean useDefaultValueOnEmpty,
      @Nullable Collection<T> defaultValue,
      boolean lenient,
      RuntimeException onError) {
    Collection<T> types = parts.stream()
        .filter(param -> partName.equals(param.getName()))
        .map(typeClazz::cast)
        .toList();
    if(!types.isEmpty()) {
      return types;
    }
    if(useDefaultValueOnEmpty || lenient) {
      return defaultValue;
    }
    throw onError;
  }

  public static  <T,R> R extractFromPart(
      Collection<ParametersParameterComponent> parts,
      String partName,
      Class<? extends T> clazz,
      Function<T, R> mapper,
      boolean lenient,
      RuntimeException onError) {
    return extractFromPart(parts, partName, clazz, mapper, false, null, lenient, onError);
  }

  public static  <T,R> R extractFromPart(
      Collection<ParametersParameterComponent> parts,
      String partName,
      Class<? extends T> typeClazz,
      Function<T, R> mapper,
      boolean useDefaultValue,
      @Nullable R defaultValue,
      boolean lenient
      ) {
    return extractFromPart(parts, partName, typeClazz, mapper, useDefaultValue, defaultValue, lenient, null);
  }

  public static  <T,R> R extractFromPart(
      Collection<ParametersParameterComponent> parts,
      String partName,
      Class<? extends T> typeClazz,
      Function<T, R> mapper,
      boolean useDefaultValue,
      @Nullable R defaultValue,
      boolean lenient,
      RuntimeException onError) {
    Optional<Type> type = parts.stream()
        .filter(param -> partName.equals(param.getName()))
        .findFirst()
        .map(ParametersParameterComponent::getValue);
    if(type.isPresent()) {
      T casted = typeClazz.cast(type.get());
      try {
        return mapper.apply(casted);
      } catch (IllegalArgumentException e) {
       if(lenient && useDefaultValue) {
         return defaultValue;
       }
       if(onError != null) {
         onError.initCause(e);
         throw onError;
       }
       throw e;
      }
    }
    if(useDefaultValue || lenient) {
      return defaultValue;
    }
    throw onError;
  }
}
