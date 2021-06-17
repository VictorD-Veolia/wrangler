/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.wrangler.proto.workspace.v2;

import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Spec for the workspace sample
 */
public class SampleSpec {
  private final String connectionName;
  private final String connectionType;
  private final String path;
  private final Set<StageSpec> relatedPlugins;

  public SampleSpec(String connectionName, String connectionType, @Nullable String path,
                    Set<StageSpec> relatedPlugins) {
    this.connectionName = connectionName;
    this.connectionType = connectionType;
    this.path = path;
    this.relatedPlugins = relatedPlugins;
  }

  public String getConnectionName() {
    return connectionName;
  }

  public String getConnectionType() {
    return connectionType;
  }

  // path is not there for an upgraded workspace
  @Nullable
  public String getPath() {
    return path;
  }

  public Set<StageSpec> getRelatedPlugins() {
    return relatedPlugins;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SampleSpec that = (SampleSpec) o;
    return Objects.equals(connectionName, that.connectionName) &&
             Objects.equals(connectionType, that.connectionType) &&
             Objects.equals(path, that.path) &&
             Objects.equals(relatedPlugins, that.relatedPlugins);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connectionName, connectionType, path, relatedPlugins);
  }
}
