/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.views;

import au.csiro.pathling.views.validation.ValidName;
import jakarta.annotation.Nonnull;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hl7.fhir.instance.model.api.IBase;

/**
 * Constant that can be used in FHIRPath expressions.
 *
 * <p>A constant is a string that is injected into a FHIRPath expression through the use of a
 * FHIRPath external constant with the same name.
 *
 * @author John Grimes
 * @see <a
 *     href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.constant">ViewDefinition.constant</a>
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ConstantDeclaration {

  /**
   * Name of constant (referred to in FHIRPath as {@code %[name]}).
   *
   * @see <a
   *     href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.constant.name">ViewDefinition.constant.name</a>
   */
  @Nonnull @NotNull @ValidName String name;

  /**
   * The string that will be substituted in place of the constant reference.
   *
   * @see <a
   *     href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.constant.value">ViewDefinition.constant.value</a>
   */
  @Nonnull @NotNull IBase value;
}
