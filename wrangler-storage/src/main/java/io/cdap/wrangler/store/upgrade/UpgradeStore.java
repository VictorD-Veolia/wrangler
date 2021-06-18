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

import com.google.gson.Gson;
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
 * Use system namespace to track the overall upgrade status.
 * This upgrade store can be used as a general purpose for app upgrade:
 * it has primary key as [namespace][namespace-generation][entity-type] and value [upgrade-ts-millis][upgrade-state]
 * upgrade state is a serialized string which represents the overall upgrade information and status
 */
public class UpgradeStore {
  private static final NamespaceSummary SYSTEM_NS = new NamespaceSummary("system", "", 0L);
  private static final StructuredTableId TABLE_ID = new StructuredTableId("app_upgrade");
  private static final Gson GSON = new Gson();

  private static final String NAMESPACE_COL = "namespace";
  private static final String GENERATION_COL = "generation";
  private static final String ENTITY_TYPE_COL = "entity_type";
  private static final String UPGRADE_STATE_COL = "upgrade_state";
  private static final String UPGRADE_TIMESTAMP = "upgrade_timestamp";

  public static final StructuredTableSpecification UPGRADE_TABLE_SPEC =
    new StructuredTableSpecification.Builder()
      .withId(TABLE_ID)
      .withFields(Fields.stringType(NAMESPACE_COL),
                  Fields.longType(GENERATION_COL),
                  Fields.stringType(ENTITY_TYPE_COL),
                  Fields.stringType(UPGRADE_STATE_COL),
                  Fields.longType(UPGRADE_TIMESTAMP))
      .withPrimaryKeys(NAMESPACE_COL, GENERATION_COL, ENTITY_TYPE_COL)
      .build();

  private final TransactionRunner transactionRunner;

  public UpgradeStore(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  /**
   * Set the upgrade timestamp for the given upgrade types if not there and return the upgrade timestamp.
   * This method should only be used once when the service starts up.
   * The upgrade will only operate on entities created before this timestamp.
   *
   * @return the upgrade timestamp
   */
  public long setAndRetrieveUpgradeTimestampMillis(UpgradeEntityType type, long timestampMillis) {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      Collection<Field<?>> fields = getPrimaryKeys(SYSTEM_NS, type);
      Optional<StructuredRow> row = table.read(fields);

      fields.add(Fields.longField(UPGRADE_TIMESTAMP, timestampMillis));

      // return if it is still there
      if (row.isPresent() && row.get().getLong(UPGRADE_TIMESTAMP) != null) {
        return row.get().getLong(UPGRADE_TIMESTAMP);
      }

      table.upsert(fields);
      return timestampMillis;
    });
  }

  /**
   * Set the upgrade complete status for the entity type
   */
  public void setEntityUpgradeComplete(UpgradeEntityType type) {
    TransactionRunners.run(transactionRunner, context -> {
      setComplete(SYSTEM_NS, context, type);
    });
  }

  /**
   * Set the upgrade complete status for the entity type in a namespace
   *
   * @param namespace namespace that completed upgrade
   */
  public void setEntityUpgradeComplete(NamespaceSummary namespace, UpgradeEntityType type) {
    TransactionRunners.run(transactionRunner, context -> {
      setComplete(namespace, context, type);
    });
  }

  /**
   * Checks whether an entity type is upgrade complete
   */
  public boolean isEntityUpgradeComplete(UpgradeEntityType type) {
    return TransactionRunners.run(transactionRunner, context -> {
      return isComplete(SYSTEM_NS, context, type);
    });
  }

  /**
   * Checks whether an entity type is upgrade complete in a namespace
   */
  public boolean isEntityUpgradeComplete(NamespaceSummary namespace, UpgradeEntityType type) {
    return TransactionRunners.run(transactionRunner, context -> {
      return isComplete(namespace, context, type);
    });
  }

  // visible for testing, storage do not have guava dependency so cannot add annotation
  void clear() {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      table.deleteAll(Range.all());
    });
  }

  private void setComplete(NamespaceSummary namespace, StructuredTableContext context,
                           UpgradeEntityType type) throws IOException {
    StructuredTable table = context.getTable(TABLE_ID);
    Collection<Field<?>> fields = getPrimaryKeys(namespace, type);
    fields.add(Fields.stringField(UPGRADE_STATE_COL, GSON.toJson(new UpgradeState(true))));
    table.upsert(fields);
  }

  private boolean isComplete(NamespaceSummary namespace, StructuredTableContext context,
                             UpgradeEntityType type) throws IOException {
    StructuredTable table = context.getTable(TABLE_ID);
    Collection<Field<?>> fields = getPrimaryKeys(namespace, type);
    Optional<StructuredRow> row = table.read(fields);
    if (!row.isPresent() || row.get().getString(UPGRADE_STATE_COL) == null) {
      return false;
    }

    return GSON.fromJson(row.get().getString(UPGRADE_STATE_COL), UpgradeState.class).isUpgradeComplete();
  }

  private Collection<Field<?>> getPrimaryKeys(NamespaceSummary namespace, UpgradeEntityType entityType) {
    List<Field<?>> keys = new ArrayList<>(getNamespaceKeys(namespace));
    keys.add(Fields.stringField(ENTITY_TYPE_COL, entityType.name()));
    return keys;
  }

  private Collection<Field<?>> getNamespaceKeys(NamespaceSummary namespace) {
    List<Field<?>> keys = new ArrayList<>();
    keys.add(Fields.stringField(NAMESPACE_COL, namespace.getName()));
    keys.add(Fields.longField(GENERATION_COL, namespace.getGeneration()));
    return keys;
  }
}
