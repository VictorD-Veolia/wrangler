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

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.test.SystemAppTestBase;
import io.cdap.cdap.test.TestConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Upgrade store test
 */
public class UpgradeStoreTest extends SystemAppTestBase {
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);
  private static UpgradeStore store;

  @BeforeClass
  public static void setupTest() throws Exception {
    getStructuredTableAdmin().create(UpgradeStore.UPGRADE_TABLE_SPEC);
    store = new UpgradeStore(getTransactionRunner());
  }

  @After
  public void cleanupTest() throws Exception {
    store.clear();
  }

  @Test
  public void testUpgradeTimestampDoesNotChange() throws Exception {
    long tsNow = System.currentTimeMillis();
    long upgradeTs = store.setAndRetrieveUpgradeTimestampMillis(UpgradeEntityType.CONNECTION, tsNow);
    Assert.assertTrue(upgradeTs > 0);
    // wait for time to pass at least 1 milli second
    Tasks.waitFor(true, () -> System.currentTimeMillis() > upgradeTs, 5, TimeUnit.MILLISECONDS);
    Assert.assertEquals(upgradeTs, store.setAndRetrieveUpgradeTimestampMillis(UpgradeEntityType.CONNECTION,
                                                                              System.currentTimeMillis()));
  }

  @Test
  public void testUpgradeStore() throws Exception {
    List<NamespaceSummary> namespaces = ImmutableList.of(
      new NamespaceSummary("default", "", 0L),
      new NamespaceSummary("test1", "", 1L),
      new NamespaceSummary("test2", "", 0L),
      new NamespaceSummary("test3", "", 5L));

    Assert.assertFalse(store.isEntityUpgradeComplete(UpgradeEntityType.CONNECTION));
    Assert.assertFalse(store.isEntityUpgradeComplete(UpgradeEntityType.WORKSPACE));

    // assert connection upgrade completion
    namespaces.forEach(ns -> {
      store.setEntityUpgradeComplete(ns, UpgradeEntityType.CONNECTION);
      Assert.assertTrue(store.isEntityUpgradeComplete(ns, UpgradeEntityType.CONNECTION));
      Assert.assertFalse(store.isEntityUpgradeComplete(UpgradeEntityType.CONNECTION));
      Assert.assertFalse(store.isEntityUpgradeComplete(UpgradeEntityType.WORKSPACE));
    });

    // connection upgrade is done
    store.setEntityUpgradeComplete(UpgradeEntityType.CONNECTION);
    Assert.assertTrue(store.isEntityUpgradeComplete(UpgradeEntityType.CONNECTION));
    Assert.assertFalse(store.isEntityUpgradeComplete(UpgradeEntityType.WORKSPACE));

    // assert workspace upgrade completion
    namespaces.forEach(ns -> {
      store.setEntityUpgradeComplete(ns, UpgradeEntityType.WORKSPACE);
      Assert.assertTrue(store.isEntityUpgradeComplete(ns, UpgradeEntityType.WORKSPACE));
      Assert.assertFalse(store.isEntityUpgradeComplete(UpgradeEntityType.WORKSPACE));
      Assert.assertTrue(store.isEntityUpgradeComplete(UpgradeEntityType.CONNECTION));
    });

    // workspace upgrade is done
    store.setEntityUpgradeComplete(UpgradeEntityType.WORKSPACE);

    // upgrade is done
    Assert.assertTrue(store.isEntityUpgradeComplete(UpgradeEntityType.CONNECTION));
    Assert.assertTrue(store.isEntityUpgradeComplete(UpgradeEntityType.WORKSPACE));
  }
}
