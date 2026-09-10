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

package au.csiro.pathling.vcl;

import java.io.Serializable;

/**
 * The right-hand side of a VCL filter, or the source of a property navigation. One of a single
 * code, a quoted string, an enumerated code list, a value set URI, or a nested filter list; a
 * wildcard is also permitted as a navigation source.
 *
 * @author John Grimes
 */
public interface VclFilterValue extends Serializable {}
