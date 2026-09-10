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

package au.csiro.pathling.util;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import org.slf4j.LoggerFactory;

/**
 * Captures log events emitted by a class under test. The logger level is lowered so that events
 * filtered out by the test profile are still captured, and restored when the capture is closed.
 * Intended for use in a try-with-resources block.
 *
 * @author John Grimes
 */
public final class LogCapture implements AutoCloseable {

  @Nonnull private final Logger logger;

  @Nullable private final Level previousLevel;

  @Nonnull private final ListAppender<ILoggingEvent> appender;

  private LogCapture(@Nonnull final Class<?> loggerClass, @Nonnull final Level level) {
    logger = (Logger) LoggerFactory.getLogger(loggerClass);
    previousLevel = logger.getLevel();
    logger.setLevel(level);
    appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
  }

  /**
   * Starts capturing events for the given class's logger at INFO level and below.
   *
   * @param loggerClass the class whose logger should be captured
   * @return the active capture
   */
  @Nonnull
  public static LogCapture forClass(@Nonnull final Class<?> loggerClass) {
    return new LogCapture(loggerClass, Level.INFO);
  }

  /**
   * Returns the events captured so far.
   *
   * @return the captured log events
   */
  @Nonnull
  public List<ILoggingEvent> events() {
    return appender.list;
  }

  @Override
  public void close() {
    logger.detachAppender(appender);
    logger.setLevel(previousLevel);
    appender.stop();
  }
}
