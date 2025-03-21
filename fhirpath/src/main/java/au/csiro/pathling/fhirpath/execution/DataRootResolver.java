/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.fhirpath.execution;

import static au.csiro.pathling.fhirpath.FhirPathConstants.PredefinedVariables.CONTEXT;
import static au.csiro.pathling.fhirpath.FhirPathConstants.PredefinedVariables.RESOURCE;
import static au.csiro.pathling.fhirpath.FhirPathConstants.PredefinedVariables.ROOT_RESOURCE;
import static au.csiro.pathling.fhirpath.execution.DataRoot.asUntypedResolveRoot;
import static au.csiro.pathling.fhirpath.execution.FhirPathsUtils.asResolve;
import static au.csiro.pathling.fhirpath.execution.FhirPathsUtils.asResource;
import static au.csiro.pathling.fhirpath.execution.FhirPathsUtils.asReverseResolve;
import static au.csiro.pathling.fhirpath.execution.FhirPathsUtils.getTypeSpecifierArg;
import static au.csiro.pathling.utilities.Functions.maybeCast;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.ReferenceCollection;
import au.csiro.pathling.fhirpath.definition.ResourceTypeSet;
import au.csiro.pathling.fhirpath.execution.DataRoot.ResolveRoot;
import au.csiro.pathling.fhirpath.execution.DataRoot.ResourceRoot;
import au.csiro.pathling.fhirpath.execution.DataRoot.ReverseResolveRoot;
import au.csiro.pathling.fhirpath.path.Paths;
import au.csiro.pathling.fhirpath.path.Paths.EvalFunction;
import au.csiro.pathling.fhirpath.path.Paths.ExternalConstantPath;
import au.csiro.pathling.fhirpath.path.Paths.Resource;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Analyzes FHIRPath expressions to identify all data roots required for evaluation.
 * <p>
 * A data root represents a source of data that needs to be accessed during FHIRPath evaluation.
 * This class traverses FHIRPath expressions to identify:
 * <ul>
 *   <li>Resource roots - direct access to resource types</li>
 *   <li>Resolve roots - resources accessed through reference resolution</li>
 *   <li>Reverse resolve roots - resources that reference other resources</li>
 * </ul>
 * <p>
 * The resolver handles various FHIRPath constructs including:
 * <ul>
 *   <li>Resource paths (e.g., Patient)</li>
 *   <li>Reference resolution (e.g., subject.resolve())</li>
 *   <li>Reverse reference resolution (e.g., reverseResolve(Condition.subject))</li>
 *   <li>Type filtering (e.g., reference.resolve().ofType(Condition))</li>
 *   <li>External constants (e.g., %resource, %rootResource)</li>
 * </ul>
 * <p>
 * This class is essential for evaluation of fhirpath expressions by identifying all required
 * data dependencies before evaluation begins, allowing for creation of the full data view
 * representation.
 */
@Value
@Slf4j
public class DataRootResolver {

  @Nonnull
  ResourceType subjectResource;

  @Nonnull
  FhirContext fhirContext;

  /**
   * Finds all data roots required to evaluate the given FHIRPath expression.
   * <p>
   * This method analyzes the FHIRPath expression and identifies all resource types and join
   * operations that will be needed during evaluation. It starts with the subject resource as the
   * initial data root and traverses the expression to find additional data roots.
   *
   * @param path The FHIRPath expression to analyze
   * @return An immutable set of all data roots required to evaluate the expression
   */
  @Nonnull
  public Set<DataRoot> findDataRoots(@Nonnull final FhirPath path) {
    final ResourceRoot subjectRoot = ResourceRoot.of(subjectResource);
    final Set<DataRoot> dataRoots = new HashSet<>();
    dataRoots.add(subjectRoot);
    collectDataRoots(subjectRoot, path, FhirPath.nullPath(), dataRoots);
    return Collections.unmodifiableSet(dataRoots);
  }


  /**
   * Finds all data roots required to evaluate a collection of FHIRPath expressions.
   * <p>
   * This method analyzes each FHIRPath expression in the collection and identifies all resource
   * types and join operations that will be needed during evaluation. It always includes the "this"
   * context path in addition to the provided paths.
   *
   * @param paths A collection of FHIRPath expressions to analyze
   * @return An immutable set of all data roots required to evaluate all the expressions
   */
  @Nonnull
  public Set<DataRoot> findDataRoots(@Nonnull final Collection<FhirPath> paths) {
    // always include this as  context path
    return Stream.concat(Stream.of(new Paths.This()), paths.stream())
        .map(this::findDataRoots)
        .flatMap(Collection::stream)
        .collect(Collectors.toUnmodifiableSet());
  }

  /**
   * Recursively collects all data roots required to evaluate a FHIRPath expression.
   * <p>
   * This method traverses the FHIRPath expression tree and identifies all resource types and join
   * operations that will be needed during evaluation. It handles different types of paths such as
   * resource paths, resolve paths, reverse resolve paths, etc.
   *
   * @param contextRoot The current context data root
   * @param fhirPath The FHIRPath expression to analyze
   * @param traversalPath The current traversal path (accumulated path from the root)
   * @param dataRoots The set to collect all identified data roots
   */
  void collectDataRoots(@Nonnull final DataRoot contextRoot,
      @Nonnull final FhirPath fhirPath,
      @Nonnull final FhirPath traversalPath,
      @Nonnull final Set<DataRoot> dataRoots) {

    final FhirPath headPath = fhirPath.first();

    if (asResource(headPath, fhirContext) instanceof Resource resource) {
      handleResourcePath(contextRoot, fhirPath, traversalPath, dataRoots, resource);
    } else if (asReverseResolve(headPath) instanceof EvalFunction reverseResolve) {
      handleReverseResolvePath(contextRoot, fhirPath, traversalPath, dataRoots, reverseResolve);
    } else if (asResolve(headPath) instanceof EvalFunction) {
      handleResolvePath(contextRoot, fhirPath, traversalPath, dataRoots);
    } else if (FhirPathsUtils.asTypeOf(headPath) instanceof EvalFunction ofType) {
      handleOfTypePath(contextRoot, fhirPath, traversalPath, dataRoots, ofType);
    } else if (headPath instanceof ExternalConstantPath ecp) {
      handleExternalConstantPath(contextRoot, fhirPath, traversalPath, dataRoots, ecp);
    } else if (FhirPathsUtils.isPropagatesArguments(headPath)) {
      handlePropagatesArgumentsPath(contextRoot, fhirPath, traversalPath, dataRoots, headPath);
    } else if (!headPath.isNull()) {
      handleNonNullPath(contextRoot, fhirPath, traversalPath, dataRoots, headPath);
    } else {
      handleNullPath(contextRoot);
    }
  }

  private void handleResourcePath(@Nonnull final DataRoot contextRoot,
      @Nonnull final FhirPath fhirPath,
      @Nonnull final FhirPath traversalPath,
      @Nonnull final Set<DataRoot> dataRoots,
      @Nonnull final Resource resource) {
    // add this resource root and use it as the context for the rest of the path
    // clear the traversalPath as we are starting from a new root
    final ResourceRoot resourceRoot = ResourceRoot.of(resource.getResourceType());
    dataRoots.add(resourceRoot);
    collectDataRoots(resourceRoot, fhirPath.suffix(), FhirPath.nullPath(), dataRoots);
  }

  private void handleReverseResolvePath(@Nonnull final DataRoot contextRoot,
      @Nonnull final FhirPath fhirPath,
      @Nonnull final FhirPath traversalPath,
      @Nonnull final Set<DataRoot> dataRoots,
      @Nonnull final EvalFunction reverseResolve) {
    // add this reverse resolve root and use it as the context for the rest of the path
    // clear the traversalPath as we are starting from a new root
    final ReverseResolveRoot reverseResolveRoot = ExecutorUtils.fromPath(contextRoot,
        reverseResolve);
    dataRoots.add(reverseResolveRoot);
    collectDataRoots(reverseResolveRoot, fhirPath.suffix(), FhirPath.nullPath(), dataRoots);
  }

  private void handleResolvePath(@Nonnull final DataRoot contextRoot,
      @Nonnull final FhirPath fhirPath,
      @Nonnull final FhirPath traversalPath,
      @Nonnull final Set<DataRoot> dataRoots) {

    // get the reference types for the current context root and traversal path
    final ResourceTypeSet referenceTypes = getReferenceTypes(contextRoot, traversalPath);

    // create a resolve root with the reference type if it is known
    final ResolveRoot resolveRoot = referenceTypes.asSingleResourceType()
        .map(referenceType -> ResolveRoot.of(contextRoot, referenceType,
            traversalPath.toExpression()))
        .orElse(ResolveRoot.untyped(contextRoot, traversalPath.toExpression()));

    if (!resolveRoot.isUntyped()) {
      // only add the root if its type is known
      // untyped resolve roots are added if when their type is resolved with ofType() path
      dataRoots.add(resolveRoot);
    }
    collectDataRoots(resolveRoot, fhirPath.suffix(), FhirPath.nullPath(), dataRoots);
  }

  private void handleOfTypePath(@Nonnull final DataRoot contextRoot,
      @Nonnull final FhirPath fhirPath,
      @Nonnull final FhirPath traversalPath,
      @Nonnull final Set<DataRoot> dataRoots,
      @Nonnull final EvalFunction ofType) {

    // check if the context root is a polymorphic resolve root ( indicated by its resource type set to RESOURCE)
    if (asUntypedResolveRoot(contextRoot) instanceof ResolveRoot rr) {
      // add this typed resolve root and use it as the context for the rest of the path
      // clear the traversalPath as we are starting from a new root
      final TypeSpecifier typeSpecifier = getTypeSpecifierArg(ofType, 0);
      final ResolveRoot typedRoot = rr.resolveType(typeSpecifier.toResourceType());
      dataRoots.add(typedRoot);
      collectDataRoots(typedRoot, fhirPath.suffix(), FhirPath.nullPath(), dataRoots);
    } else {
      // we assume that the path is valid so we just add the ofType to the traversal path
      // to resolve the type of the polymorphic collection
      collectDataRoots(contextRoot, fhirPath.suffix(), traversalPath.andThen(ofType),
          dataRoots);
    }
  }

  private void handleExternalConstantPath(@Nonnull final DataRoot contextRoot,
      @Nonnull final FhirPath fhirPath,
      @Nonnull final FhirPath traversalPath,
      @Nonnull final Set<DataRoot> dataRoots,
      @Nonnull final ExternalConstantPath ecp) {
    if (CONTEXT.equals(ecp.getName())) {
      // TODO: add support for context variables different than %resource
      log.warn("%context is always resolved to the %rootResource");
    }
    if (RESOURCE.equals(ecp.getName()) || ROOT_RESOURCE.equals(ecp.getName()) || CONTEXT
        .equals(ecp.getName())) {
      // we do not add a new as the subject root should already be added but 
      // we need to switch context to the subject root and start with the empty traversal path
      collectDataRoots(ResourceRoot.of(subjectResource), fhirPath.suffix(), FhirPath.nullPath(),
          dataRoots);
    }
    // other variables should be literals so we should not need to resolve them
  }

  private void handlePropagatesArgumentsPath(@Nonnull final DataRoot contextRoot,
      @Nonnull final FhirPath fhirPath,
      @Nonnull final FhirPath traversalPath,
      @Nonnull final Set<DataRoot> dataRoots,
      @Nonnull final FhirPath headPath) {
    // some paths such as combine or iif needs to be processed differently as one of more of 
    // their arguments may need to be further resolved
    // for example `iif(..., resolve().id).ofType(Condition)` the true branch of the iif 
    // needs to be resolved to identify the type of the reference
    FhirPathsUtils.getPropagatesArguments(headPath)
        .forEach(head -> collectDataRoots(contextRoot, head.andThen(fhirPath.suffix()),
            traversalPath, dataRoots));
  }

  private void handleNonNullPath(@Nonnull final DataRoot contextRoot,
      @Nonnull final FhirPath fhirPath,
      @Nonnull final FhirPath traversalPath,
      @Nonnull final Set<DataRoot> dataRoots,
      @Nonnull final FhirPath headPath) {
    // by default, we collect data roots for all the children of the head path 
    // (e.g. arguments of a function call)
    headPath.children()
        .forEach(child -> collectDataRoots(contextRoot, child, traversalPath, dataRoots));
    // and then for the rest of the path extending the traversal path if needed
    final FhirPath newTraversalPath = traversalPath.andThen(FhirPathsUtils.toTraversal(headPath));
    collectDataRoots(contextRoot, fhirPath.suffix(), newTraversalPath, dataRoots);
  }

  private void handleNullPath(@Nonnull final DataRoot contextRoot) {
    // we have a polymorphic resolve root as contex which cannot be further resolved
    if (asUntypedResolveRoot(contextRoot) instanceof ResolveRoot rr) {
      throw new IllegalStateException("Unresolved polymorphic resolve root:" + rr);
    }
  }

  /**
   * Get the reference types for a given context root and traversal path. Its assumes that the
   * context is typed resource root and that the traversal path points to a Reference. Throws
   * IllegalArgumentException otherwise.
   *
   * @param contextRoot The context root
   * @param traversalPath The traversal path
   * @return The reference types
   * @throws IllegalArgumentException if the context root is not a typed root or the traversal path
   * is not a reference
   */
  private @Nonnull ResourceTypeSet getReferenceTypes(@Nonnull final DataRoot contextRoot,
      @Nonnull final FhirPath traversalPath) {
    if (contextRoot.isUntyped()) {
      throw new IllegalArgumentException(
          "Cannot resolve reference types for untyped root:" + contextRoot);
    }
    final FhirpathEvaluator evaluator = NullEvaluator.of(contextRoot.getResourceType(),
        fhirContext);
    final ReferenceCollection referenceCollection = Optional.of(evaluator.evaluate(traversalPath))
        .flatMap(maybeCast(ReferenceCollection.class))
        .orElseThrow(() -> new IllegalArgumentException(
            "Traversal path does not resolve to a reference:" + traversalPath.toExpression()));
    return referenceCollection.getReferenceTypes();
  }
}
