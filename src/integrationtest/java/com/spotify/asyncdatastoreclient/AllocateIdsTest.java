/*
 * Copyright (c) 2011-2015 Spotify AB
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
 */

package com.spotify.asyncdatastoreclient;

import io.vertx.core.Future;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(VertxUnitRunner.class)
@Category(IntegrationTest.class)
public class AllocateIdsTest extends DatastoreTest {

  @Test
  public void testAllocateIds(TestContext context) {
    System.out.println("Started allocate ids test");
    final AllocateIds allocate = QueryBuilder.allocate()
        .add("employee")
        .add(Key.builder("employee").build());

    datastore.executeAsync(allocate).compose(allocateResult -> {
      System.out.println("Received allocation result");
      final List<Key> keys = allocateResult.getKeys();
      assertEquals(2, keys.size());
      assertTrue(keys.get(0).getId() > 0);
      assertTrue(keys.get(1).getId() > 0);

      return Future.succeededFuture();
    }).setHandler(context.asyncAssertSuccess());
  }
}
