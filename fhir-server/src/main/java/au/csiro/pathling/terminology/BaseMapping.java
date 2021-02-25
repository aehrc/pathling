package au.csiro.pathling.terminology;

import static au.csiro.pathling.utilities.Preconditions.checkResponse;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.beanutils.BeanUtils;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;

public class BaseMapping {

  protected BaseMapping() {
  }

  private static void setProperty(@Nonnull final Object bean, @Nonnull final String name,
      @Nullable final Object value) {
    try {
      BeanUtils.setProperty(bean, name, value);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  public static <T> T partToBean(ParametersParameterComponent component, Supplier<T> supplier) {
    final T result = supplier.get();
    component.getPart().forEach(p -> setProperty(result, p.getName(), p.getValue()));
    return result;
  }


  @Nonnull
  public static Parameters parametersFromEntry(@Nonnull final BundleEntryComponent entry) {
    checkResponse("200".equals(entry.getResponse().getStatus()),
        "Failed entry in response bundle with status: %s",
        entry.getResponse().getStatus()
    );
    return (Parameters) entry.getResource();
  }
}
