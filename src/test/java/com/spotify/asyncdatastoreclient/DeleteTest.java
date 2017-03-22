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

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
@Category(IntegrationTest.class)
public class DeleteTest extends DatastoreTest {

  @Test
  public void testDeleteEntity(TestContext context) throws Exception {
    final Insert insert = QueryBuilder.insert("employee", 1234567L)
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);

    datastore.executeAsync(insert).compose(insertSuccessful -> {
      final Delete delete = QueryBuilder.delete("employee", 1234567L);
      return datastore.executeAsync(delete);
    }).setHandler(context.asyncAssertSuccess(deleteResult -> {
      context.assertTrue(deleteResult.getIndexUpdates() > 0);
    }));
  }

  @Test
  public void testDeleteNotExists(TestContext context) throws Exception {
    final Delete delete = QueryBuilder.delete("employee", 1234567L);
    datastore.executeAsync(delete).setHandler(context.asyncAssertSuccess(deleteResult -> {
      context.assertEquals(0, deleteResult.getIndexUpdates());
    }));
  }

  @Test
  public void testDeleteByKey(TestContext context) throws Exception {
    final Insert insert = QueryBuilder.insert("employee", 1234567L)
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);
    datastore.executeAsync(insert).compose(insertResult -> {
      final Delete delete = QueryBuilder.delete(Key.builder("employee", 1234567L).build());
      return datastore.executeAsync(delete);
    }).setHandler(context.asyncAssertSuccess(deleteResult -> {
      context.assertTrue(deleteResult.getIndexUpdates() > 0);
    }));
  }
}
