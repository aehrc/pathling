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

package au.csiro.pathling.schema.annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/** Tests that the memoised computation of annotations computes each text once per thread. */
class PerThreadLruCacheTest {

  @Test
  void aTextIsComputedOnceWhileItIsCached() {
    final PerThreadLruCache<String, String> cache = new PerThreadLruCache<>(4, text -> text + "!");
    final String first = cache.get("a");
    assertSame(first, cache.get("a"));
    assertEquals(1, cache.getComputationCount());
  }

  @Test
  void anEqualInputReadAgainIsNotComputedAgain() {
    // The annotations of one value read it one after the other, each from an input of its own.
    final PerThreadLruCache<List<String>, String> cache =
        new PerThreadLruCache<>(4, input -> String.join("|", input));
    final String first = cache.get(List.of(new String("1.5"), new String("kg")));
    assertSame(first, cache.get(List.of(new String("1.5"), new String("kg"))));
    cache.get(List.of("2", "g"));
    assertSame(first, cache.get(List.of("1.5", "kg")));
    assertEquals(2, cache.getComputationCount());
  }

  @Test
  void twoPassesOverAsManyTextsAsTheCacheHoldsComputeEachOnce() {
    // The two bounds of the dates of an array, read in two passes.
    final PerThreadLruCache<String, String> cache = new PerThreadLruCache<>(64, text -> text + "!");
    final List<String> texts = IntStream.range(0, 64).mapToObj(Integer::toString).toList();
    texts.forEach(cache::get);
    texts.forEach(cache::get);
    assertEquals(64, cache.getComputationCount());
  }

  @Test
  void theLeastRecentlyReadTextIsEvicted() {
    final PerThreadLruCache<String, String> cache = new PerThreadLruCache<>(2, text -> text + "!");
    cache.get("a");
    cache.get("b");
    // Reading a makes b the least recently read, so c evicts b.
    cache.get("a");
    cache.get("c");
    cache.get("a");
    assertEquals(3, cache.getComputationCount());
    cache.get("b");
    assertEquals(4, cache.getComputationCount());
  }

  @Test
  void aFailureIsNotCached() {
    final PerThreadLruCache<String, String> cache =
        new PerThreadLruCache<>(
            4,
            text -> {
              throw new IllegalArgumentException(text);
            });
    assertThrows(IllegalArgumentException.class, () -> cache.get("x"));
    assertThrows(IllegalArgumentException.class, () -> cache.get("x"));
    assertEquals(2, cache.getComputationCount());
  }

  @Test
  void eachThreadHasItsOwnCache() {
    final PerThreadLruCache<String, String> cache = new PerThreadLruCache<>(4, text -> text + "!");
    cache.get("a");
    CompletableFuture.runAsync(() -> cache.get("a")).join();
    assertEquals(2, cache.getComputationCount());
  }
}
