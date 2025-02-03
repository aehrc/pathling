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

package au.csiro.pathling.extract;

import org.junit.jupiter.api.Test;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Base64;
import java.util.function.Function;

public class JavaTest {


  @FunctionalInterface
  public interface MyFunction extends Function<String, String>, Serializable {

  }

  @Test
  public void testSomeghing() throws IOException {
    System.out.println("Hello World");
    final MyFunction f1 = String::toString;
    final MyFunction f2 = String::toString;
    final MyFunction f3 = String::toLowerCase;

    System.out.println((f1.equals(f2)));
    System.out.println(f1 == f2);
    System.out.println(f1 instanceof Serializable);
    System.out.println(f1);
    System.out.println(f2);

    final ByteArrayOutputStream buff1 = new ByteArrayOutputStream();
    new ObjectOutputStream(buff1).writeObject(f1);
    final ByteArrayOutputStream buff2 = new ByteArrayOutputStream();
    new ObjectOutputStream(buff2).writeObject(f2);
    System.out.println(buff1.toByteArray());
    System.out.println(buff2.toByteArray());

    final String str1 = Base64.getEncoder().encodeToString(buff1.toByteArray());
    final String str2 = Base64.getEncoder().encodeToString(buff2.toByteArray());

    System.out.println(str1);
    System.out.println(str2);
    
    
  }

}
