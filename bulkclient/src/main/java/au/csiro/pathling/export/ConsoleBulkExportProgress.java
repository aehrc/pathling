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

package au.csiro.pathling.export;

import javax.annotation.Nonnull;
import java.net.URI;

public class ConsoleBulkExportProgress implements BulkExportProgress {

  
  private static  final ConsoleBulkExportProgress INSTANCE = new ConsoleBulkExportProgress();
  
  
  private ConsoleBulkExportProgress() {
  }
  
  @Override
  public void onStart() {
    System.out.println("onStart()");
  }

  @Override
  public void onKickOffSent(@Nonnull final URI requestUri) {
    System.out.println("onKickOffSent: " + requestUri);
  }

  @Override
  public void onKickOffComplete() {
    System.out.println("onKickOffComplete");
  }

  @Override
  public void onExportStart() {
    System.out.println("onExportStart");
  }

  @Override
  public void onExportProgress() {
    System.out.println("onExportProgress");
  }

  @Override
  public void onExportComplete(@Nonnull final BulkExportResponse response) {
    System.out.println("onExportComplete: " + response);
  }

  @Override
  public void onComplete() {
    System.out.println("onComplete: ");
  }

  public static ConsoleBulkExportProgress instance() {
    return INSTANCE;
  }
}
