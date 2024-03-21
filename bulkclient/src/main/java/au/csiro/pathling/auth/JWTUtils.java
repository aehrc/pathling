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

package au.csiro.pathling.auth;

import com.auth0.jwt.algorithms.Algorithm;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.jwk.AsymmetricJWK;
import com.nimbusds.jose.jwk.JWK;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.PrivateKey;
import java.util.Arrays;
import javax.annotation.Nonnull;

public class JWTUtils {

  private JWTUtils() {
  }

  @Nonnull
  public static Algorithm getAsymmSigningAlgorithm(@Nonnull final JWK privateKeyJWT) {
    if (!(privateKeyJWT instanceof AsymmetricJWK)) {
      throw new IllegalArgumentException("Only asymmetric keys are supported");
    }
    if (!privateKeyJWT.isPrivate()) {
      throw new IllegalArgumentException("Only private keys are supported");
    }
    final String jwtAlgoName = privateKeyJWT.getAlgorithm().getName();
    try {
      final PrivateKey privateKey = ((AsymmetricJWK) privateKeyJWT).toPrivateKey();
      final Method algoFactoryMethod = Arrays.stream(Algorithm.class.getDeclaredMethods())
          .filter(m -> m.getName().equals(toAuto0AlgoName(jwtAlgoName)))
          .filter(m -> m.getReturnType().equals(Algorithm.class))
          .filter(m -> m.getParameterCount() == 1)
          .filter(m -> m.getParameters()[0].getType().isAssignableFrom(privateKey.getClass()))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("Unsupported algorithm: " + jwtAlgoName));
      return (Algorithm) algoFactoryMethod.invoke(null, privateKey);
    } catch (final JOSEException | InvocationTargetException | IllegalAccessException e) {
      throw new RuntimeException("Exception while creating Algorithm for: " + jwtAlgoName, e);
    }
  }

  public static String toAuto0AlgoName(@Nonnull final String jwkAlgorithm) {
    return jwkAlgorithm
        .replace("RS", "RSA")
        .replace("ES", "ECDSA")
        .replace("HC", "HMAC");
  }
}
