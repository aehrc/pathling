/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2025 Commonwealth Scientific and Industrial Research
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
 *
 */

package au.csiro.pathling.encoders

import ca.uhn.fhir.context.{BaseRuntimeElementCompositeDefinition, BaseRuntimeElementDefinition}

import scala.collection.mutable

private[encoders] class EncodingContext {

  private val definitionCounters = new mutable.HashMap[Any, Int]()

  private def pushDefinition(definition: BaseRuntimeElementCompositeDefinition[_]): Unit = {
    val lastCounter = definitionCounters.getOrElseUpdate(definition, 0)
    definitionCounters.update(definition, lastCounter + 1)
  }

  private def popDefinition(definition: BaseRuntimeElementCompositeDefinition[_]): Unit = {
    val lastCounter = definitionCounters.getOrElse(definition, 0)
    assert(lastCounter > 0)
    if (lastCounter == 1) {
      definitionCounters.remove(definition)
    } else {
      definitionCounters.update(definition, lastCounter - 1)
    }
  }

  private def nestingLevel(definition: BaseRuntimeElementDefinition[_]): Int = {
    definitionCounters.getOrElse(definition, 0)
  }
}

/**
 * Provides a context for tracking nesting depth of element definition for encoders.
 * Usage:
 *
 * {{{
 *   EncodingContext.runWithContext {
 *
 *    // anywhere in the call stack to increase the nesting level
 *    // of someDefinition
 *
 *    EncodingContext.withDefinition(someDefinition) {
 *      // anywhere in the call stack to check the current nesting
 *      // level of someDefinition
 *
 *      EncodingContext.currentNestingLevel(someDefinition)
 *    }
 *   }
 * }}}
 *
 */
object EncodingContext {
  private val CONTEXT_STORAGE = new ThreadLocal[EncodingContext]()

  private def currentContext(): EncodingContext = {
    val thisContext = CONTEXT_STORAGE.get()
    assert(thisContext != null, "Current EncodingContext does not exist.")
    thisContext
  }

  /**
   * Obtains the nesting level of a element definition from the current encoding context.
   *
   * @param definition the element definition
   * @return the current nesting level
   */
  def currentNestingLevel(definition: BaseRuntimeElementDefinition[_]): Int = {
    currentContext().nestingLevel(definition)
  }

  /**
   * Evaluates given code in a new encoding context.
   *
   * @param body the code to evaluate
   * @tparam T the return type for the code to evaluate
   * @return the results of the code evaluation
   */
  def runWithContext[T](body: => T): T = {
    assert(CONTEXT_STORAGE.get() == null, "There should be no current context.")
    try {
      CONTEXT_STORAGE.set(new EncodingContext())
      val result = body
      // on successful exit current should be empty
      assert(CONTEXT_STORAGE.get().definitionCounters.isEmpty, "All nesting levels should be 0")
      result
    } finally
      CONTEXT_STORAGE.remove()
  }


  /**
   * Evaluates given code in the context with increased nesting level for given element definition.
   *
   * @param definition the element definition to increased the nesting level for.
   * @param body       the code to evaluate
   * @tparam T the return type for the code to evaluate
   * @return the results of the code evaluation
   */
  def withDefinition[T](definition: BaseRuntimeElementCompositeDefinition[_])(body: => T): T = {
    try {
      currentContext().pushDefinition(definition)
      body
    } finally {
      currentContext().popDefinition(definition)
    }
  }
}
