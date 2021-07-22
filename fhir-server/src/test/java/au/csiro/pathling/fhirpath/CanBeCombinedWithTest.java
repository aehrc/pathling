/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.*;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ResourcePathBuilder;
import au.csiro.pathling.test.builders.UntypedResourcePathBuilder;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author John Grimes
 */
@SpringBootTest
@TestInstance(Lifecycle.PER_CLASS)
@Tag("UnitTest")
public class CanBeCombinedWithTest {

  private final ResourcePath resourcePath;
  private final UntypedResourcePath untypedResourcePath;

  private final ElementPath booleanPath;
  private final ElementPath codingPath;
  private final ElementPath datePath;
  private final ElementPath dateTimePath;
  private final ElementPath decimalPath;
  private final ElementPath integerPath;
  private final ElementPath referencePath;
  private final ElementPath stringPath;
  private final ElementPath timePath;

  private final BooleanLiteralPath booleanLiteralPath;
  private final CodingLiteralPath codingLiteralPath;
  private final DateLiteralPath dateLiteralPath;
  private final DateTimeLiteralPath dateTimeLiteralPath;
  private final DecimalLiteralPath decimalLiteralPath;
  private final IntegerLiteralPath integerLiteralPath;
  private final NullLiteralPath nullLiteralPath;
  private final StringLiteralPath stringLiteralPath;
  private final TimeLiteralPath timeLiteralPath;

  private final Set<FhirPath> paths;

  @Autowired
  public CanBeCombinedWithTest(@Nonnull final SparkSession spark) throws ParseException {
    resourcePath = new ResourcePathBuilder(spark)
        .build();
    untypedResourcePath = new UntypedResourcePathBuilder(spark)
        .build();

    booleanPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.BOOLEAN)
        .build();
    codingPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODING)
        .build();
    datePath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.DATE)
        .build();
    dateTimePath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.DATETIME)
        .build();
    decimalPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.DECIMAL)
        .build();
    integerPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.INTEGER)
        .build();
    referencePath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.REFERENCE)
        .build();
    stringPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.STRING)
        .build();
    timePath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.TIME)
        .build();

    booleanLiteralPath = BooleanLiteralPath.fromString("true", resourcePath);
    codingLiteralPath = CodingLiteralPath
        .fromString("http://snomed.info/sct|27699000", resourcePath);
    dateLiteralPath = DateLiteralPath.fromString("@1983-06-21", resourcePath);
    dateTimeLiteralPath = DateTimeLiteralPath
        .fromString("@2015-02-08T13:28:17-05:00", resourcePath);
    decimalLiteralPath = DecimalLiteralPath.fromString("9.5", resourcePath);
    integerLiteralPath = IntegerLiteralPath.fromString("14", resourcePath);
    nullLiteralPath = NullLiteralPath.build(resourcePath);
    stringLiteralPath = StringLiteralPath.fromString("'foo'", resourcePath);
    timeLiteralPath = TimeLiteralPath.fromString("T12:30", resourcePath);

    paths = new HashSet<>(Arrays.asList(
        resourcePath,
        untypedResourcePath,
        booleanPath,
        codingPath,
        datePath,
        dateTimePath,
        decimalPath,
        integerPath,
        referencePath,
        stringPath,
        timePath,
        booleanLiteralPath,
        codingLiteralPath,
        dateLiteralPath,
        dateTimeLiteralPath,
        decimalLiteralPath,
        integerLiteralPath,
        nullLiteralPath,
        stringLiteralPath,
        timeLiteralPath
    ));
  }

  @Value
  @EqualsAndHashCode
  private static class TestParameters {

    @Nonnull
    FhirPath source;

    @Nonnull
    FhirPath target;

    boolean expectation;

    @Override
    public String toString() {
      return source.getClass().getSimpleName() + ", " + target.getClass().getSimpleName() + ": "
          + expectation;
    }

  }

  public Stream<TestParameters> parameters() {
    return Stream.of(
        buildParameters(resourcePath, Collections.singletonList(resourcePath)),
        buildParameters(untypedResourcePath, Collections.singletonList(untypedResourcePath)),
        buildParameters(booleanPath, Arrays.asList(booleanPath, booleanLiteralPath)),
        buildParameters(codingPath, Arrays.asList(codingPath, codingLiteralPath)),
        buildParameters(datePath, Arrays.asList(datePath, dateLiteralPath)),
        buildParameters(dateTimePath, Arrays.asList(dateTimePath, dateTimeLiteralPath)),
        buildParameters(decimalPath, Arrays.asList(decimalPath, decimalLiteralPath)),
        buildParameters(integerPath, Arrays.asList(integerPath, integerLiteralPath)),
        buildParameters(referencePath, Collections.singletonList(referencePath)),
        buildParameters(stringPath, Arrays.asList(stringPath, stringLiteralPath)),
        buildParameters(timePath, Arrays.asList(timePath, timeLiteralPath)),
        buildParameters(booleanLiteralPath, Arrays.asList(booleanLiteralPath, booleanPath)),
        buildParameters(codingLiteralPath, Arrays.asList(codingLiteralPath, codingPath)),
        buildParameters(dateLiteralPath, Arrays.asList(dateLiteralPath, datePath)),
        buildParameters(dateTimeLiteralPath, Arrays.asList(dateTimeLiteralPath, dateTimePath)),
        buildParameters(decimalLiteralPath, Arrays.asList(decimalLiteralPath, decimalPath)),
        buildParameters(integerLiteralPath, Arrays.asList(integerLiteralPath, integerPath)),
        buildParameters(stringLiteralPath, Arrays.asList(stringLiteralPath, stringPath)),
        buildParameters(timeLiteralPath, Arrays.asList(timeLiteralPath, timePath)),
        buildParameters(nullLiteralPath, paths)
    ).flatMap(x -> x).distinct();
  }

  private Stream<TestParameters> buildParameters(@Nonnull final FhirPath source,
      @Nonnull final Collection<FhirPath> canBeCombined) {
    return paths.stream().map(path -> new TestParameters(source, path,
        canBeCombined.contains(path) || path == nullLiteralPath));
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void canBeCombined(@Nonnull final TestParameters parameters) {
    final boolean result = parameters.getSource().canBeCombinedWith(parameters.getTarget());
    assertEquals(parameters.isExpectation(), result);
  }

}
