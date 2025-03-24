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

import au.csiro.pathling.fhir.FhirUtils;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.operator.CombineOperator;
import au.csiro.pathling.fhirpath.path.ParserPaths.TypeSpecifierPath;
import au.csiro.pathling.fhirpath.path.Paths;
import au.csiro.pathling.fhirpath.path.Paths.EvalFunction;
import au.csiro.pathling.fhirpath.path.Paths.EvalOperator;
import au.csiro.pathling.fhirpath.path.Paths.Traversal;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.experimental.UtilityClass;

import java.util.stream.Stream;

import static au.csiro.pathling.fhirpath.FhirPathConstants.Functions;

/**
 * Utility class providing helper methods for working with FHIRPath expressions. These methods
 * assist in analyzing, transforming, and extracting information from FHIRPath expression trees.
 */
@UtilityClass
public class FhirPathsUtils {

    /**
     * Checks if a path represents a specific function and returns it if it does.
     *
     * @param path         The FHIRPath expression to check
     * @param functionName The name of the function to check for
     * @return The path cast to EvalFunction if it matches the function name, null otherwise
     */
    @Nullable
    public static FhirPath asFunction(@Nonnull final FhirPath path,
                                      @Nonnull final String functionName) {
        if (path instanceof EvalFunction evalFunction && evalFunction.getFunctionIdentifier()
                .equals(functionName)) {
            return evalFunction;
        }
        return null;
    }

    /**
     * Checks if a path represents a reverseResolve() function and returns it if it does. The
     * reverseResolve() function is used to navigate backwards through references.
     *
     * @param path The FHIRPath expression to check
     * @return The path cast to EvalFunction if it's a reverseResolve function, null otherwise
     */
    @Nullable
    public static FhirPath asReverseResolve(@Nonnull final FhirPath path) {
        return asFunction(path, Functions.REVERSE_RESOLVE);
    }

    /**
     * Checks if a path represents a resolve() function and returns it if it does. The resolve()
     * function is used to follow references to their target resources.
     *
     * @param path The FHIRPath expression to check
     * @return The path cast to EvalFunction if it's a resolve function, null otherwise
     */
    @Nullable
    public static FhirPath asResolve(@Nonnull final FhirPath path) {
        return asFunction(path, Functions.RESOLVE);
    }

    /**
     * Checks if a path represents an ofType() function and returns it if it does. The ofType()
     * function is used to filter a collection by resource type.
     *
     * @param path The FHIRPath expression to check
     * @return The path cast to EvalFunction if it's an ofType function, null otherwise
     */
    @Nullable
    public static FhirPath asTypeOf(@Nonnull final FhirPath path) {
        return asFunction(path, Functions.OF_TYPE);
    }

    /**
     * Checks if a path represents a valid FHIR resource type and returns it if it does. This method
     * validates that the resource type is known in the provided FHIR context.
     *
     * @param path        The FHIRPath expression to check
     * @param fhirContext The FHIR context used to validate the resource type
     * @return The path cast to Resource if it's a valid resource type, null otherwise
     */
    public static FhirPath asResource(@Nonnull final FhirPath path,
                                      @Nonnull final FhirContext fhirContext) {
        return path instanceof Paths.Resource resource
                && FhirUtils.isKnownResource(resource.getResourceCode(), fhirContext)
                ? resource
                : null;
    }

    /**
     * Checks if a path represents an extension() function. The extension() function is used to access
     * FHIR extensions.
     *
     * @param path The FHIRPath expression to check
     * @return true if the path is an extension function, false otherwise
     */
    public static boolean isExtension(@Nonnull final FhirPath path) {
        return asFunction(path, Functions.EXTENSION) != null;
    }

    /**
     * Checks if a path represents an iif() function. The iif() function is the FHIRPath conditional
     * operator (if-then-else).
     *
     * @param path The FHIRPath expression to check
     * @return true if the path is an iif function, false otherwise
     */
    public static boolean isIif(@Nonnull final FhirPath path) {
        return asFunction(path, Functions.IIF) != null;
    }

    /**
     * Converts a path to a traversal path if possible. This is used to extract the property name from
     * a path for traversal operations.
     *
     * @param path The FHIRPath expression to convert
     * @return A Traversal path if conversion is possible, or a null path otherwise
     */
    @Nonnull
    public static FhirPath toTraversal(@Nonnull final FhirPath path) {
        if (path instanceof Paths.Traversal traversal) {
            return traversal;
        } else if (isExtension(path)) {
            // the property name is the same as the function name (extension)
            return new Paths.Traversal(Functions.EXTENSION);
        } else {
            return FhirPath.nullPath();
        }
    }

    /**
     * Checks if a path represents a combine operator (union). Combine operators join two collections
     * together.
     *
     * @param path The FHIRPath expression to check
     * @return true if the path is a combine operator, false otherwise
     */
    public static boolean isCombineOperator(@Nonnull final FhirPath path) {
        return path instanceof EvalOperator evalOperator
                && evalOperator.getOperator() instanceof CombineOperator;
    }

    /**
     * Checks if a path propagates arguments to its children. This is true for operators like combine
     * and functions like iif that need to process their arguments in special ways.
     *
     * @param path The FHIRPath expression to check
     * @return true if the path propagates arguments, false otherwise
     */
    public static boolean isPropagatesArguments(@Nonnull final FhirPath path) {
        return isCombineOperator(path) || isIif(path);
    }

    /**
     * Gets the arguments that should be propagated from a path. For combine operators, this returns
     * all children. For iif functions, this returns all children except the first (condition).
     *
     * @param path The FHIRPath expression to get arguments from
     * @return A stream of FHIRPath expressions representing the arguments to propagate
     * @throws IllegalArgumentException if the path does not propagate arguments
     */
    public static Stream<FhirPath> getPropagatesArguments(@Nonnull final FhirPath path) {
        if (isCombineOperator(path)) {
            return path.children();
        } else if (isIif(path)) {
            return path.children().skip(1);
        } else {
            throw new IllegalArgumentException("Path does not propagate arguments:" + path);
        }
    }

    /**
     * Gets the type specifier argument from a function at a specific index.
     *
     * @param evalFunction The function eval path to get the argument from
     * @param index        The index of the argument to get
     * @return The type specifier argument at the specified index
     */
    @Nonnull
    public static TypeSpecifier getTypeSpecifierArg(@Nonnull final EvalFunction evalFunction,
                                                    int index) {
        return ((TypeSpecifierPath) evalFunction.getArguments()
                .get(index)).getValue();
    }
  
    /**
     * Checks if a path is a traversal-only path. A traversal-only path is a path that consists only
     * of traversals and does not contain any functions or operators.
     *
     * @param path The FHIRPath expression to check
     * @return true if the path is a traversal-only path, false otherwise
     */
    public static boolean isTraversalOnly(@Nonnull final FhirPath path) {
        return path.asStream().allMatch(Traversal.class::isInstance);
    }
}
