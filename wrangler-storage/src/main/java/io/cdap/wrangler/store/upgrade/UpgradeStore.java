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

package io.cdap.wrangler.store.upgrade;

import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Upgrade store to store upgrade information for each namespace,
 * Use system namespace to track the overall upgrade status
 */
public class UpgradeStore {
  private static final NamespaceSummary SYSTEM_NS = new NamespaceSummary("system", "", 0L);
  private static final StructuredTableId TABLE_ID = new StructuredTableId("wrangler_upgrade");

  private static final String NAMESPACE_COL = "namespace";
  private static final String GENERATION_COL = "generation";
  private static final String CONNECTION_UPGRADE_COMPLETE_FIELD = "connection_upgrade_complete";
  private static final String WORKSPACE_UPGRADE_COMPLETE_FIELD = "workspace_upgrade_complete";
  private static final String UPGRADE_TIMESTAMP = "upgrade_timestamp";

  public static final StructuredTableSpecification UPGRADE_TABLE_SPEC =
    new StructuredTableSpecification.Builder()
      .withId(TABLE_ID)
      .withFields(Fields.stringType(NAMESPACE_COL),
                  Fields.longType(GENERATION_COL),
                  Fields.stringType(CONNECTION_UPGRADE_COMPLETE_FIELD),
                  Fields.stringType(WORKSPACE_UPGRADE_COMPLETE_FIELD),
                  Fields.longType(UPGRADE_TIMESTAMP))
      .withPrimaryKeys(NAMESPACE_COL, GENERATION_COL)
      .build();

  private final TransactionRunner transactionRunner;

  public UpgradeStore(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  /**
   * Set the upgrade timestamp if not there and return the upgrade timestamp.
   * This method should only be used once when the service starts up.
   * The upgrade will only operate connections and workspaces created before this timestamp.
   *
   * @return the upgrade timestamp
   */
  public long setAndRetrieveUpgradeTimestampMillis() {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      Collection<Field<?>> fields = getNamespaceKeys(SYSTEM_NS);
      Optional<StructuredRow> row = table.read(fields);

      long tsNow = System.currentTimeMillis();
      fields.add(Fields.longField(UPGRADE_TIMESTAMP, tsNow));

      // return if it is still there
      if (row.isPresent() && row.get().getLong(UPGRADE_TIMESTAMP) != null) {
        return row.get().getLong(UPGRADE_TIMESTAMP);
      }

      table.upsert(fields);
      return tsNow;
    });
  }

  public void setWorkspaceUpgradeComplete() {
    TransactionRunners.run(transactionRunner, context -> {
      setComplete(SYSTEM_NS, context, WORKSPACE_UPGRADE_COMPLETE_FIELD);
    });
  }

  public void setWorkspaceUpgradeComplete(NamespaceSummary namespace) {
    TransactionRunners.run(transactionRunner, context -> {
      setComplete(namespace, context, WORKSPACE_UPGRADE_COMPLETE_FIELD);
    });
  }

  public void setConnectionUpgradeComplete() {
    TransactionRunners.run(transactionRunner, context -> {
      setComplete(SYSTEM_NS, context, CONNECTION_UPGRADE_COMPLETE_FIELD);
    });
  }

  public void setConnectionUpgradeComplete(NamespaceSummary namespace) {
    TransactionRunners.run(transactionRunner, context -> {
      setComplete(namespace, context, CONNECTION_UPGRADE_COMPLETE_FIELD);
    });
  }

  public boolean isWorkspaceUpgradeComplete() {
    return TransactionRunners.run(transactionRunner, context -> {
      return isComplete(SYSTEM_NS, context, WORKSPACE_UPGRADE_COMPLETE_FIELD);
    });
  }

  public boolean isWorkspaceUpgradeComplete(NamespaceSummary namespace) {
    return TransactionRunners.run(transactionRunner, context -> {
      return isComplete(namespace, context, WORKSPACE_UPGRADE_COMPLETE_FIELD);
    });
  }

  public boolean isConnectionUpgradeComplete() {
    return TransactionRunners.run(transactionRunner, context -> {
      return isComplete(SYSTEM_NS, context, CONNECTION_UPGRADE_COMPLETE_FIELD);
    });
  }

  public boolean isConnectionUpgradeComplete(NamespaceSummary namespace) {
    return TransactionRunners.run(transactionRunner, context -> {
      return isComplete(namespace, context, CONNECTION_UPGRADE_COMPLETE_FIELD);
    });
  }

  public boolean isUpgradeComplete() {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      Collection<Field<?>> fields = getNamespaceKeys(SYSTEM_NS);
      Optional<StructuredRow> row = table.read(fields);
      return row.isPresent() && "true".equals(row.get().getString(WORKSPACE_UPGRADE_COMPLETE_FIELD)) &&
               "true".equals(row.get().getString(CONNECTION_UPGRADE_COMPLETE_FIELD));
    });
  }

  void clear() {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      table.deleteAll(Range.all());
    });
  }

  private void setComplete(NamespaceSummary namespace, StructuredTableContext context, String col) throws IOException {
    StructuredTable table = context.getTable(TABLE_ID);
    Collection<Field<?>> fields = getNamespaceKeys(namespace);
    fields.add(Fields.stringField(col, "true"));
    table.upsert(fields);
  }

  private Boolean isComplete(NamespaceSummary namespace, StructuredTableContext context,
                             String col) throws IOException {
    StructuredTable table = context.getTable(TABLE_ID);
    Collection<Field<?>> fields = getNamespaceKeys(namespace);
    Optional<StructuredRow> row = table.read(fields);
    return row.isPresent() && "true".equals(row.get().getString(col));
  }

  private Collection<Field<?>> getNamespaceKeys(NamespaceSummary namespace) {
    List<Field<?>> keys = new ArrayList<>();
    keys.add(Fields.stringField(NAMESPACE_COL, namespace.getName()));
    keys.add(Fields.longField(GENERATION_COL, namespace.getGeneration()));
    return keys;
  }
}
