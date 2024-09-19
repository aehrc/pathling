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

package au.csiro.pathling.fhirpath.evaluation;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.registry.FunctionRegistry;
import au.csiro.pathling.fhirpath.operator.BinaryOperator;
import org.apache.spark.sql.SparkSession;

/**
 * Represents dependencies required by the process of evaluating a {@link FhirPath} into a
 * {@link Collection}.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
public interface EvaluationContext {

  /**
   * @return The {@link SparkSession} to use for building Spark queries
   */
  SparkSession spark();

  FunctionRegistry<NamedFunction<? extends Collection>> functionRegistry();

  FunctionRegistry<BinaryOperator> operatorRegistry();

}
