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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.proto.connection.SampleResponse;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.wrangler.PropertyIds;
import io.cdap.wrangler.RequestExtractor;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.proto.BadRequestException;
import io.cdap.wrangler.proto.ServiceResponse;
import io.cdap.wrangler.proto.workspace.v2.DirectiveExecutionRequest;
import io.cdap.wrangler.proto.workspace.v2.DirectiveExecutionResponse;
import io.cdap.wrangler.proto.workspace.v2.DirectiveUsage;
import io.cdap.wrangler.proto.workspace.v2.SampleSpec;
import io.cdap.wrangler.proto.workspace.v2.StageSpec;
import io.cdap.wrangler.proto.workspace.v2.Workspace;
import io.cdap.wrangler.proto.workspace.v2.WorkspaceCreationRequest;
import io.cdap.wrangler.proto.workspace.v2.WorkspaceDetail;
import io.cdap.wrangler.proto.workspace.v2.WorkspaceId;
import io.cdap.wrangler.proto.workspace.v2.WorkspaceSpec;
import io.cdap.wrangler.registry.DirectiveInfo;
import io.cdap.wrangler.store.workspace.WorkspaceStore;
import io.cdap.wrangler.utils.SchemaConverter;
import io.cdap.wrangler.utils.StructuredToRowTransformer;
import org.apache.commons.lang3.StringEscapeUtils;

import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * V2 endpoints for workspace
 */
public class WorkspaceHandler extends AbstractDirectiveHandler {
  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();
  private WorkspaceStore store;
  private ConnectionDiscoverer discoverer;

  @Override
  public void initialize(SystemHttpServiceContext context) throws Exception {
    super.initialize(context);
    store = new WorkspaceStore(context);
    discoverer = new ConnectionDiscoverer(context);
  }

  @POST
  @Path("v2/contexts/{context}/workspaces")
  public void createWorkspace(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("context") String namespace) {
    respond(responder, namespace, ns -> {
      if (ns.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        throw new BadRequestException("Creating workspace in system namespace is currently not supported");
      }

      WorkspaceCreationRequest creationRequest =
        GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(), WorkspaceCreationRequest.class);

      if (creationRequest.getConnection() == null) {
        throw new BadRequestException("Connection name has to be provided to create a workspace");
      }

      SampleRequest sampleRequest = creationRequest.getSampleRequest();
      if (sampleRequest == null) {
        throw new BadRequestException("Sample request has to be provided to create a workspace");
      }

      SampleResponse sampleResponse = discoverer.retrieveSample(namespace, creationRequest.getConnection(),
                                                                sampleRequest);
      List<Row> rows = new ArrayList<>();
      if (!sampleResponse.getSample().isEmpty()) {
        for (StructuredRecord record : sampleResponse.getSample()) {
          rows.add(StructuredToRowTransformer.transform(record));
        }
      }

      SampleSpec spec = new SampleSpec(creationRequest.getConnection(), sampleRequest.getPath(),
                                       sampleResponse.getSchema(), sampleResponse.getSpec().getProperties());
      WorkspaceId wsId = new WorkspaceId(ns);
      long now = System.currentTimeMillis();
      Workspace workspace = Workspace.builder(generateWorkspaceName(wsId, creationRequest.getSampleRequest().getPath()),
                                              wsId.getWorkspaceId())
                              .setCreatedTimeMillis(now).setUpdatedTimeMillis(now).setSampleSpec(spec).build();
      store.saveWorkspace(wsId, new WorkspaceDetail(workspace, rows));
      responder.sendJson(wsId.getWorkspaceId());
    });
  }

  @GET
  @Path("v2/contexts/{context}/workspaces")
  public void listWorkspaces(HttpServiceRequest request, HttpServiceResponder responder,
                             @PathParam("context") String namespace) {
    respond(responder, namespace, ns -> {
      if (ns.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        throw new BadRequestException("Listing workspaces in system namespace is currently not supported");
      }
      responder.sendJson(new ServiceResponse<>(store.listWorkspaces(ns)));
    });
  }

  @GET
  @Path("v2/contexts/{context}/workspaces/{id}")
  public void getWorkspace(HttpServiceRequest request, HttpServiceResponder responder,
                           @PathParam("context") String namespace,
                           @PathParam("id") String workspaceId) {
    respond(responder, namespace, ns -> {
      if (ns.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        throw new BadRequestException("Getting workspace in system namespace is currently not supported");
      }
      responder.sendJson(store.getWorkspace(new WorkspaceId(ns, workspaceId)));
    });
  }

  @DELETE
  @Path("v2/contexts/{context}/workspaces/{id}")
  public void deleteWorkspace(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("context") String namespace,
                              @PathParam("id") String workspaceId) {
    respond(responder, namespace, ns -> {
      if (ns.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        throw new BadRequestException("Deleting workspace in system namespace is currently not supported");
      }
      store.deleteWorkspace(new WorkspaceId(ns, workspaceId));
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    });
  }

  /**
   * Upload data to the workspace, the workspace is created automatically on fly.
   */
  @POST
  @Path("v2/contexts/{context}/workspaces/upload")
  public void upload(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("context") String namespace) {
    respond(responder, namespace, ns -> {
      if (ns.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        throw new BadRequestException("Uploading data in system namespace is currently not supported");
      }

      String name = request.getHeader(PropertyIds.FILE_NAME);
      if (name == null) {
        throw new BadRequestException("Name must be provided in the 'file' header");
      }

      RequestExtractor handler = new RequestExtractor(request);

      // For back-ward compatibility, we check if there is delimiter specified
      // using 'recorddelimiter' or 'delimiter'
      String delimiter = handler.getHeader(RECORD_DELIMITER_HEADER, "\\u001A");
      delimiter = handler.getHeader(DELIMITER_HEADER, delimiter);
      String content = handler.getContent(StandardCharsets.UTF_8);
      if (content == null) {
        throw new BadRequestException(
          "Body not present, please post the file containing the records to create a workspace.");
      }

      delimiter = StringEscapeUtils.unescapeJava(delimiter);
      List<Row> sample = new ArrayList<>();
      for (String line : content.split(delimiter)) {
        sample.add(new Row(COLUMN_NAME, line));
      }

      WorkspaceId id = new WorkspaceId(ns);
      long now = System.currentTimeMillis();
      Workspace workspace = Workspace.builder(name, id.getWorkspaceId())
                              .setCreatedTimeMillis(now).setUpdatedTimeMillis(now).build();
      store.saveWorkspace(id, new WorkspaceDetail(workspace, sample));
      responder.sendJson(id.getWorkspaceId());
    });
  }

  /**
   * Executes the directives on the record.
   */
  @POST
  @Path("v2/contexts/{context}/workspaces/{id}/execute")
  public void execute(HttpServiceRequest request, HttpServiceResponder responder,
                      @PathParam("context") String namespace,
                      @PathParam("id") String workspaceId) {
    respond(responder, namespace, ns -> {
      if (ns.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        throw new BadRequestException("Executing directives in system namespace is currently not supported");
      }

      DirectiveExecutionRequest executionRequest =
        GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(), DirectiveExecutionRequest.class);
      WorkspaceId wsId = new WorkspaceId(ns, workspaceId);
      WorkspaceDetail detail = store.getWorkspaceDetail(wsId);
      List<Row> result = executeDirectives(ns.getName(), executionRequest.getDirectives(), detail.getSample());
      DirectiveExecutionResponse response = generateExecutionResponse(result, executionRequest.getLimit());

      Workspace newWorkspace = Workspace.builder(detail.getWorkspace())
                                 .setDirectives(executionRequest.getDirectives())
                                 .setUpdatedTimeMillis(System.currentTimeMillis()).build();
      store.saveWorkspace(wsId, new WorkspaceDetail(newWorkspace, detail.getSample()));
      responder.sendJson(response);
    });
  }

  /**
   * Retrieve the directives available in the namespace
   */
  @GET
  @Path("v2/contexts/{context}/directives")
  public void getDirectives(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("context") String namespace) {
    respond(responder, namespace, ns -> {
      // CDAP-15397 - reload must be called before it can be safely used
      composite.reload(namespace);
      List<DirectiveUsage> directives = new ArrayList<>();
      for (DirectiveInfo directive : composite.list(namespace)) {
        DirectiveUsage directiveUsage = new DirectiveUsage(directive.name(), directive.usage(),
                                                           directive.description(), directive.scope().name(),
                                                           directive.definition(), directive.categories());
        directives.add(directiveUsage);
      }
      responder.sendJson(new ServiceResponse<>(directives));
    });
  }

  /**
   * Get the summary for the workspace
   * TODO: figure out to return plugin version
   */
  @GET
  @Path("v2/contexts/{context}/workspaces/{id}/specification")
  public void specification(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("context") String namespace,
                            @PathParam("id") String workspaceId) {
    respond(responder, namespace, ns -> {
      if (ns.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        throw new BadRequestException("Getting specification in system namespace is currently not supported");
      }

      WorkspaceId wsId = new WorkspaceId(ns, workspaceId);
      WorkspaceDetail detail = store.getWorkspaceDetail(wsId);
      List<String> directives = detail.getWorkspace().getDirectives();
      List<Row> result = executeDirectives(ns.getName(), directives, detail.getSample());

      SchemaConverter schemaConvertor = new SchemaConverter();
      Schema schema = schemaConvertor.toSchema("record", createUberRecord(result));
      Map<String, String> properties = ImmutableMap.of("directives", String.join("\n", directives));
      SampleSpec sampleSpec = detail.getWorkspace().getSampleSpec();
      StageSpec srcSpec = sampleSpec == null ? null : new StageSpec(sampleSpec.getSampleSchema(),
                                                                    sampleSpec.getProperties());

      // really hacky way for the parse-as-csv directive, should get removed once we have support to provide the
      // format properties when doing sampling
      boolean shouldCopyHeader =
        directives.stream()
          .map(String::trim)
          .anyMatch(directive -> directive.startsWith("parse-as-csv") && directive.endsWith("true"));
      if (shouldCopyHeader && srcSpec != null) {
        Map<String, String> srcProperties = new HashMap<>(srcSpec.getProperties());
        srcProperties.put("copyHeader", "true");
        srcSpec = new StageSpec(srcSpec.getSchema(), srcProperties);
      }

      responder.sendJson(new WorkspaceSpec(srcSpec, new StageSpec(schema, properties)));
    });
  }

  /**
   * Get the workspace name, the generation rule is like:
   * 1. If the path is not null or empty, the name will be last portion of path starting from "/".
   * If "/" does not exist, the name will be the path itself.
   * 2. If path is null or empty or equal to "/", the name will be the uuid for the workspace
   */
  private String generateWorkspaceName(WorkspaceId id, @Nullable String path) {
    if (Strings.isNullOrEmpty(path) || path.equals("/")) {
      return id.getWorkspaceId();
    }

    if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }

    int last = path.lastIndexOf('/');
    if (last < 0) {
      return path;
    }
    return path.substring(last);
  }
}
