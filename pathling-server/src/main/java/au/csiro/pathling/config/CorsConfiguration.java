package au.csiro.pathling.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import java.util.List;

/**
 * Represents configuration relating to Cross-Origin Resource Sharing (CORS).
 */
@Data
public class CorsConfiguration {

  @NotNull
  private List<String> allowedOrigins;

  @NotNull
  private List<String> allowedOriginPatterns;

  @NotNull
  private List<String> allowedMethods;

  @NotNull
  private List<String> allowedHeaders;

  @NotNull
  private List<String> exposedHeaders;

  @NotNull
  @Min(0)
  private Long maxAge;

}
