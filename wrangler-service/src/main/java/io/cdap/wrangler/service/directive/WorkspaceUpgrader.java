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

package io.cdap.wrangler.service.directive;

import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.proto.connection.ConnectorDetail;
import io.cdap.cdap.etl.proto.connection.SpecGenerationRequest;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.wrangler.PropertyIds;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.dataset.workspace.Workspace;
import io.cdap.wrangler.dataset.workspace.WorkspaceDataset;
import io.cdap.wrangler.proto.NotFoundException;
import io.cdap.wrangler.proto.workspace.v2.Artifact;
import io.cdap.wrangler.proto.workspace.v2.Plugin;
import io.cdap.wrangler.proto.workspace.v2.SampleSpec;
import io.cdap.wrangler.proto.workspace.v2.StageSpec;
import io.cdap.wrangler.proto.workspace.v2.WorkspaceDetail;
import io.cdap.wrangler.proto.workspace.v2.WorkspaceId;
import io.cdap.wrangler.store.upgrade.UpgradeStore;
import io.cdap.wrangler.store.workspace.WorkspaceStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Upgrader for workspaces. This upgrade should be run after the {@link ConnectionUpgrader}
 */
public class WorkspaceUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(WorkspaceUpgrader.class);

  private final UpgradeStore upgradeStore;
  private final SystemHttpServiceContext context;
  private final long upgradeBeforeTsSecs;
  private final ConnectionDiscoverer discoverer;
  private final WorkspaceStore wsStore;

  public WorkspaceUpgrader(UpgradeStore upgradeStore, SystemHttpServiceContext context, long upgradeBeforeTsSecs,
                           WorkspaceStore wsStore) {
    this.upgradeStore = upgradeStore;
    this.context = context;
    this.upgradeBeforeTsSecs = upgradeBeforeTsSecs;
    this.discoverer = new ConnectionDiscoverer(context);
    this.wsStore = wsStore;
  }

  public void upgradeWorkspaces() throws Exception {
    List<NamespaceSummary> namespaces = context.listNamespaces();
    for (NamespaceSummary ns : namespaces) {
      if (!upgradeStore.isWorkspaceUpgradeComplete(ns)) {
        upgradeWorkspacesInConnections(ns);
      }
    }
    upgradeStore.setWorkspaceUpgradeComplete();
  }

  private void upgradeWorkspacesInConnections(NamespaceSummary namespace) {
    List<Workspace> workspaces = TransactionRunners.run(context, ctx -> {
      WorkspaceDataset wsDataset = WorkspaceDataset.get(ctx);
      return wsDataset.listWorkspaces(namespace, upgradeBeforeTsSecs);
    });

    for (Workspace workspace : workspaces) {
      List<Row> sample = Collections.emptyList();
      try {
        sample = DirectivesHandler.fromWorkspace(workspace);
      } catch (Exception e) {
        // this should not happen, but guard here to avoid failing the entire upgrade process
        LOG.warn("Error retrieving the sample for workspace {}", workspace.getName(), e);
      }
      List<String> directives = workspace.getRequest() == null ? Collections.emptyList() :
                                  workspace.getRequest().getRecipe().getDirectives();
      WorkspaceId workspaceId = new WorkspaceId(namespace, workspace.getNamespacedId().getId());

      long now = System.currentTimeMillis();
      io.cdap.wrangler.proto.workspace.v2.Workspace.Builder ws =
        io.cdap.wrangler.proto.workspace.v2.Workspace
          .builder(workspace.getName(), workspace.getNamespacedId().getId()).setDirectives(directives)
          .setCreatedTimeMillis(now).setUpdatedTimeMillis(now);

      String connectorName = SpecificationUpgradeUtils.getConnectorName(
        workspace.getProperties().get(PropertyIds.CONNECTION_TYPE).toLowerCase());
      String path = SpecificationUpgradeUtils.getPath(workspace);
      // if the connection type cannot be upgraded, create a workspace with empty sample spec, so there will be
      // no sources created when converting to pipeline
      if (path == null) {
        wsStore.saveWorkspace(workspaceId, new WorkspaceDetail(ws.build(), sample));
        continue;
      }

      String connection = workspace.getProperties().get(PropertyIds.CONNECTION_ID);
      try {
        ConnectorDetail detail =
          discoverer.getSpecification(namespace.getName(), connection,
                                      new SpecGenerationRequest(path, Collections.emptyMap()));

        SampleSpec spec = new SampleSpec(
          connection, connectorName, path,
          detail.getRelatedPlugins().stream().map(plugin -> {
            ArtifactSelectorConfig artifact = plugin.getArtifact();
            Plugin pluginSpec = new Plugin(
              plugin.getName(), plugin.getType(), plugin.getProperties(),
              new Artifact(artifact.getName(), artifact.getVersion(), artifact.getScope()));
            return new StageSpec(plugin.getSchema(), pluginSpec);
          }).collect(Collectors.toSet()));
        wsStore.saveWorkspace(workspaceId, new WorkspaceDetail(ws.setSampleSpec(spec).build(), sample));
      } catch (NotFoundException e) {
        LOG.warn("Connection {} related to workspace {} does not exist. " +
                   "The workspace will be upgraded without that information", connection, workspace.getName());
        wsStore.saveWorkspace(workspaceId, new WorkspaceDetail(ws.build(), sample));
      } catch (Exception e) {
        LOG.warn("Unable to get the spec from connection {} for workspace {}. " +
                   "The workspace will be upgraded without that information", connection, workspace.getName());
        wsStore.saveWorkspace(workspaceId, new WorkspaceDetail(ws.build(), sample));
      }
    }

    upgradeStore.setWorkspaceUpgradeComplete(namespace);
  }
}
