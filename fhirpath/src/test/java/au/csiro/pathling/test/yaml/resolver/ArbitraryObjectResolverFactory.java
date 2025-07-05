package au.csiro.pathling.test.yaml.resolver;

import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.fhirpath.definition.def.DefDefinitionContext;
import au.csiro.pathling.fhirpath.definition.def.DefResourceDefinition;
import au.csiro.pathling.fhirpath.definition.def.DefResourceTag;
import au.csiro.pathling.fhirpath.execution.DefResourceResolver;
import au.csiro.pathling.test.yaml.YamlSupport;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * Factory for creating resource resolvers from arbitrary object representations. This class handles
 * the conversion of YAML-defined test data into a format that can be used for FHIRPath expression
 * evaluation.
 */
@Slf4j
@Value(staticConstructor = "of")
public class ArbitraryObjectResolverFactory implements Function<RuntimeContext, ResourceResolver> {

  @Nonnull
  Map<Object, Object> subjectOM;  // Changed back to Map<Object, Object>

  @Override
  @Nonnull
  public ResourceResolver apply(final RuntimeContext rt) {
    final String subjectResourceCode = Optional.ofNullable(subjectOM.get("resourceType"))
        .map(String.class::cast)
        .orElse("Test");

    final DefResourceDefinition subjectDefinition = (DefResourceDefinition) YamlSupport
        .yamlToDefinition(subjectResourceCode, subjectOM);
    final StructType subjectSchema = YamlSupport.definitionToStruct(subjectDefinition);

    final String subjectOMJson = YamlSupport.omToJson(subjectOM);
    log.trace("subjectOMJson: \n{}", subjectOMJson);
    final Dataset<Row> inputDS = rt.getSpark().read().schema(subjectSchema)
        .json(rt.getSpark().createDataset(List.of(subjectOMJson),
            Encoders.STRING()));

    log.trace("Yaml definition: {}", subjectDefinition);
    log.trace("Subject schema: {}", subjectSchema.treeString());

    return DefResourceResolver.of(
        DefResourceTag.of(subjectResourceCode),
        DefDefinitionContext.of(subjectDefinition),
        inputDS
    );
  }

  @Override
  @Nonnull
  public String toString() {
    return YamlSupport.YAML.dump(subjectOM);
  }
}
