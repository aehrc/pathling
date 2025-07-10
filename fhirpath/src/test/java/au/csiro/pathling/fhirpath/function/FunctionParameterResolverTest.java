package au.csiro.pathling.fhirpath.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.Concepts;
import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.CodingCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.function.resolver.FunctionInvocation;
import au.csiro.pathling.fhirpath.function.resolver.FunctionParameterResolver;
import au.csiro.pathling.fhirpath.path.ParserPaths;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.Value;
import org.junit.jupiter.api.Test;

class FunctionParameterResolverTest {

  final EvaluationContext evaluationContext = mock(EvaluationContext.class);

  @Nullable
  @SuppressWarnings("unused")
  static Collection funcOptionalArg(@Nonnull final Collection input,
      @Nullable final Collection optionalArgument) {
    return null;
  }

  @Nullable
  @SuppressWarnings("unused")
  static Collection funcRequiredArg(@Nonnull final Collection input,
      @Nonnull final IntegerCollection requiredArgument) {
    return null;
  }

  @Nullable
  @SuppressWarnings("unused")
  static Collection funcAllTypes(@Nonnull final Collection input,
      @Nonnull final Collection collectionArgument,
      @Nonnull final BooleanCollection booleanArgument, @Nonnull final Concepts concepts,
      @Nonnull final TypeSpecifier typeSpecifier) {
    return null;
  }

  @Nullable
  @SuppressWarnings("unused")
  public static Collection funcTransform(@Nonnull final Collection input,
      @Nonnull final CollectionTransform transform) {
    return null;
  }


  @Nullable
  @SuppressWarnings("unused")
  public static Collection funcConcepts(@Nonnull final Concepts input) {
    return null;
  }

  @Nullable
  @SuppressWarnings("unused")
  public static Collection funcStringCollection(@Nonnull final StringCollection input) {
    return null;
  }

  @Nullable
  @SuppressWarnings("unused")
  public static Collection funcBooleanCollection(@Nonnull final BooleanCollection input) {
    return null;
  }

  @Nullable
  @SuppressWarnings("unused")
  static Collection invalidFunction() {
    return null;
  }


  @Value(staticConstructor = "of")
  static class ConstPath implements FhirPath {

    Collection result;

    @Nonnull
    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      return result;
    }
  }

  @Nonnull
  static Method getMethod(final String methodName) {
    return Stream.of(FunctionParameterResolverTest.class.getDeclaredMethods())
        .filter(method -> method.getName().equals(methodName))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("Method not found: " + methodName));
  }

  @Test
  void testValueArgumentBindings() {
    final Collection input = mock(Collection.class);

    final StringCollection stringArgument = mock(StringCollection.class);
    final Collection booleanArgument = mock(Collection.class);
    final BooleanCollection booleanRepresentation = mock(BooleanCollection.class);
    final CodingCollection codingArgument = mock(CodingCollection.class);
    final Concepts concepts = mock(Concepts.Set.class);
    final TypeSpecifier typeSpecifier = mock(TypeSpecifier.class);

    // mock booleanArgument to return booleanRepresentation when asBooleanPath() is called
    when(booleanArgument.asBooleanPath()).thenReturn(booleanRepresentation);
    // mock codingArgument to return concept when toConcepts() is called
    when(codingArgument.toConcepts()).thenReturn(Optional.of(concepts));

    final FunctionParameterResolver resolver = new FunctionParameterResolver(evaluationContext,
        input,
        List.of(
            ConstPath.of(stringArgument),
            ConstPath.of(booleanArgument),
            ConstPath.of(codingArgument),
            new ParserPaths.TypeSpecifierPath(typeSpecifier)
        ));
    final Method method = getMethod("funcAllTypes");
    final FunctionInvocation invocation = resolver.bind(method);
    assertEquals(new FunctionInvocation(method, new Object[]{input,
        stringArgument, booleanRepresentation, concepts, typeSpecifier
    }
    ), invocation);
  }


  @Test
  void testCollectionTransformArgumentBinding() {
    final Collection input = mock(Collection.class);
    final StringCollection transformArgument = mock(StringCollection.class);
    final BooleanCollection transformResult = mock(BooleanCollection.class);
    final FhirPath transformPath = mock(FhirPath.class);
    when(transformPath.apply(transformArgument, evaluationContext)).thenReturn(transformResult);

    final FunctionParameterResolver resolver = new FunctionParameterResolver(evaluationContext,
        input,
        List.of(
            transformPath
        ));
    final Method method = getMethod("funcTransform");
    final FunctionInvocation invocation = resolver.bind(method);
    assertEquals(method, invocation.method());
    assertEquals(input, invocation.arguments()[0]);
    // test that the transform is bound correctly to the evaluation context
    assertEquals(transformResult,
        ((CollectionTransform) invocation.arguments()[1]).apply(transformArgument));
  }


  @Test
  void testSpecializedInputType() {
    final Collection input = mock(StringCollection.class);
    final FunctionParameterResolver resolver = new FunctionParameterResolver(evaluationContext,
        input, List.of());
    final Method method = getMethod("funcStringCollection");

    final FunctionInvocation invocation = resolver.bind(method);
    assertEquals(new FunctionInvocation(method, new Object[]{input}), invocation);
  }


  @Test
  void testConceptsInputType() {
    final CodingCollection codingInput = mock(CodingCollection.class);
    final Concepts concepts = mock(Concepts.Set.class);
    // mock codingInput to return concept when toConcepts() is called
    when(codingInput.toConcepts()).thenReturn(Optional.of(concepts));

    final FunctionParameterResolver resolver = new FunctionParameterResolver(evaluationContext,
        codingInput, List.of());
    final Method method = getMethod("funcConcepts");
    final FunctionInvocation invocation = resolver.bind(method);
    assertEquals(new FunctionInvocation(method, new Object[]{concepts}), invocation);
  }

  @Test
  void testBooleanInputType() {
    final BooleanCollection booleanRepresentation = mock(BooleanCollection.class);
    final Collection inputCollection = mock(Collection.class);
    // mock codingInput to return concept when toConcepts() is called
    when(inputCollection.asBooleanPath()).thenReturn(booleanRepresentation);

    final FunctionParameterResolver resolver = new FunctionParameterResolver(evaluationContext,
        inputCollection, List.of());
    final Method method = getMethod("funcBooleanCollection");
    final FunctionInvocation invocation = resolver.bind(method);
    assertEquals(new FunctionInvocation(method, new Object[]{booleanRepresentation}), invocation);
  }

  @Test
  void testOptionalArgNullIfNotProvided() {
    final Collection input = mock(Collection.class);
    final FunctionParameterResolver resolver = new FunctionParameterResolver(evaluationContext,
        input,
        List.of());
    final Method method = getMethod("funcOptionalArg");

    final FunctionInvocation invocation = resolver.bind(method);
    assertEquals(new FunctionInvocation(method, new Object[]{input, null}),
        invocation);
  }

  @Test
  void failsIfRequiredArgIsMissing() {
    final Collection input = mock(Collection.class);
    final FunctionParameterResolver resolver = new FunctionParameterResolver(evaluationContext,
        input,
        List.of());
    final Method method = getMethod("funcRequiredArg");

    final InvalidUserInputError ex = assertThrows(
        InvalidUserInputError.class, () -> resolver.bind(method));
    assertEquals(
        "Function 'funcRequiredArg', argument 0 (IntegerCollection): Parameter is required but no argument was provided",
        ex.getMessage());
  }

  @Test
  void failsIfTooManyArgs() {
    final Collection input = mock(Collection.class);
    final FunctionParameterResolver resolver = new FunctionParameterResolver(evaluationContext,
        input,
        List.of(mock(FhirPath.class), mock(FhirPath.class)));
    final Method method = getMethod("funcRequiredArg");

    final InvalidUserInputError ex = assertThrows(
        InvalidUserInputError.class, () -> resolver.bind(method));

    assertEquals("Function 'funcRequiredArg': Too many arguments provided. Expected 1, got 2",
        ex.getMessage());
  }


  @Test
  void failsForInvalidFunction() {
    final Collection input = mock(Collection.class);
    final FunctionParameterResolver resolver = new FunctionParameterResolver(evaluationContext,
        input, List.of());
    final Method method = getMethod("invalidFunction");

    final AssertionError ex = assertThrows(
        AssertionError.class, () -> resolver.bind(method));

    assertEquals(
        "Function 'invalidFunction' does not accept any parameters and is a not a valid FhirPath function implementation",
        ex.getMessage());
  }

  @Test
  void failsForTypeMismatchInCollection() {
    final IntegerCollection input = mock(IntegerCollection.class);
    final FunctionParameterResolver resolver = new FunctionParameterResolver(evaluationContext,
        input,
        List.of());
    final Method method = getMethod("funcStringCollection");

    final InvalidUserInputError ex = assertThrows(
        InvalidUserInputError.class, () -> resolver.bind(method));

    assertEquals(
        "Function 'funcStringCollection', input: Type mismatch: expected StringCollection but got IntegerCollection",
        ex.getMessage());
  }

  @Test
  void failsForFailedConversionToConcepts() {
    final CodingCollection codingInput = mock(CodingCollection.class);
    // Mock toConcepts to return empty Optional (conversion failure)
    when(codingInput.toConcepts()).thenReturn(Optional.empty());

    final FunctionParameterResolver resolver = new FunctionParameterResolver(evaluationContext,
        codingInput, List.of());
    final Method method = getMethod("funcConcepts");

    final InvalidUserInputError ex = assertThrows(
        InvalidUserInputError.class, () -> resolver.bind(method));

    assertEquals(
        "Function 'funcConcepts', input: Cannot convert collection of type CodingCollection to Concepts",
        ex.getMessage());
  }

  @Test
  void failsForInvalidTypeArgument() {
    final Collection input = mock(Collection.class);
    final StringCollection stringCollection = mock(StringCollection.class);

    final FunctionParameterResolver resolver = new FunctionParameterResolver(evaluationContext,
        input,
        List.of(ConstPath.of(stringCollection)));
    final Method method = getMethod("funcRequiredArg");

    final InvalidUserInputError ex = assertThrows(
        InvalidUserInputError.class, () -> resolver.bind(method));

    assertEquals(
        "Function 'funcRequiredArg', argument 0 (IntegerCollection): Type mismatch: expected IntegerCollection but got StringCollection",
        ex.getMessage());
  }
}
