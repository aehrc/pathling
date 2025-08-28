/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.utilities;

import static au.csiro.pathling.utilities.Preconditions.check;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.io.IOUtils.closeQuietly;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.Closeable;
import java.io.Serializable;
import java.util.function.Function;
import org.apache.hadoop.util.ShutdownHookManager;

/**
 * Represents a scoped storage for instances of type V that can be created from configuration of
 * type C. The concrete implementations can use various strategies for storing the instances. For
 * example a singleton can manage a single instance per process.
 *
 * @param <C> the type of the configuration
 * @param <V> the type of the instance
 * @author Piotr Szul
 */
public interface ObjectHolder<C extends Serializable, V> {

  /**
   * Retrieves the instance associated with given configuration and if necessary creates ones.
   *
   * @param config the configuration for the instance.
   * @return the instance associated with the holder.
   */
  V getOrCreate(@Nonnull final C config);

  /**
   * Destroys all the instances stored the underlying scope.
   */
  void reset();

  /**
   * A singleton implementation of ObjectHolder that holds a single instance of something.
   *
   * @param <C> the configuration type
   * @param <V> the value type
   */
  class SingletonHolder<C extends Serializable, V> implements ObjectHolder<C, V>, Closeable {

    @Nonnull
    private final Function<C, V> constructor;

    @Nullable
    private C configuration;
    @Nullable
    private V instance;

    /**
     * The default priority of the shutdown hook. This is set to be executed after the SparkContext
     * shutdown hook (which has priority 50) and before temporary dirs are cleaned up (priority 25).
     * See {@link org.apache.spark.util.ShutdownHookManager} for more info on Spark related shutdown
     * hooks.
     */
    public static final int DEFAULT_SHUTDOWN_HOOK_PRIORITY = 30;

    private SingletonHolder(@Nonnull final Function<C, V> constructor) {
      this.constructor = constructor;
    }

    @Override
    @Nonnull
    public synchronized V getOrCreate(@Nonnull final C config) {
      if (configuration == null) {
        instance = constructor.apply(config);
        configuration = config;
      } else {
        check(configuration.equals(config),
            "Attempt to create SingletonHolder with different configuration");
      }
      return requireNonNull(instance);
    }

    @Override
    public synchronized void reset() {
      if (instance instanceof final Closeable closeable) {
        closeQuietly(closeable);
        instance = null;
        configuration = null;
      }
    }

    /**
     * Closes this singleton and releases any system resources associated with it. If the singleton
     * is already closed then invoking this method has no effect.
     */
    @Override
    public void close() {
      reset();
    }
  }

  /**
   * Creates the holder allowing a single instance to be created per process. Attempts to create
   * another instance with different configuration will fail.
   *
   * @param constructor the function that creates new instance from the configuration.
   * @param closeOnShutdown whether to close the instance on JVM shutdown.
   * @param shutdownHookPriority the priority of the shutdown hook.
   * @param <C> the type of the configuration.
   * @param <V> the type of the instance.
   * @return the singleton holder.
   */
  static <C extends Serializable, V> ObjectHolder<C, V> singleton(
      @Nonnull final Function<C, V> constructor, final boolean closeOnShutdown,
      final int shutdownHookPriority) {
    final SingletonHolder<C, V> singletonHolder = new SingletonHolder<>(constructor);
    if (closeOnShutdown) {
      ShutdownHookManager.get().addShutdownHook(() -> closeQuietly(singletonHolder),
          shutdownHookPriority);
    }
    return singletonHolder;
  }

  /**
   * Creates the holder allowing a single instance to be created per process. Attempts to create
   * another instance with different configuration will fail. The instance will be closed on JVM
   * shutdown.
   *
   * @param constructor the function that creates new instance from the configuration.
   * @param <C> the type of the configuration.
   * @param <V> the type of the instance.
   * @return the singleton holder.
   */
  static <C extends Serializable, V> ObjectHolder<C, V> singleton(
      @Nonnull final Function<C, V> constructor) {
    return singleton(constructor, true, SingletonHolder.DEFAULT_SHUTDOWN_HOOK_PRIORITY);
  }
}
