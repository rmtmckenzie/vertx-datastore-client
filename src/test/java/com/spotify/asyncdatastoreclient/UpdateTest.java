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

import static org.junit.Assert.*;

@RunWith(VertxUnitRunner.class)
@Category(IntegrationTest.class)
public class UpdateTest extends DatastoreTest {

  @Test
  public void testUpdateExisting(TestContext context) {
    final Insert insert = QueryBuilder.insert("employee")
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);

    datastore.executeAsync(insert).compose(existingResult -> {
      final Update update = QueryBuilder.update(existingResult.getInsertKey())
              .value("age", 41);
      return datastore.executeAsync(update).compose(updateResult -> {
        context.assertTrue(updateResult.getIndexUpdates() > 0);
        return Future.succeededFuture(existingResult);
      });
    }).compose(existingResult -> {
      final KeyQuery get = QueryBuilder.query(existingResult.getInsertKey());
      return datastore.executeAsync(get);
    }).compose(getResult -> {
      context.assertEquals(41, getResult.getEntity().getInteger("age"));
      return Future.succeededFuture();
    }).setHandler(context.asyncAssertSuccess());
  }

  @Test
  public void testUpdateNotExisting(TestContext context) {
    final Update update = QueryBuilder.update("employee", 1234567L)
        .value("age", 41);

    datastore.executeAsync(update).setHandler(context.asyncAssertFailure(cause -> {
      if(cause instanceof DatastoreException) {
        context.assertEquals(400, ((DatastoreException)cause).getStatusCode());
      } else {
        context.fail(cause);
      }
    }));
  }

  @Test
  public void testUpdateNotExistingWithUpsert(TestContext context) {
    final Update update = QueryBuilder.update("employee", 1234567L)
        .value("age", 41)
        .upsert();

    datastore.executeAsync(update).compose(updateResult -> {
      context.assertTrue(updateResult.getIndexUpdates() > 0);

      final KeyQuery get = QueryBuilder.query("employee", 1234567L);
      return datastore.executeAsync(get);
    }).compose(getResult -> {
      context.assertEquals(41, getResult.getEntity().getInteger("age"));
      return Future.succeededFuture();
    }).setHandler(context.asyncAssertSuccess());
  }

  @Test
  public void testUpdateEntity(TestContext context) {
    final Entity existing = Entity.builder("employee")
        .property("fullname", "Fred Blinge")
        .property("age", 40, false)
        .build();

    datastore.executeAsync(QueryBuilder.insert(existing)).compose(insertResult -> {
      final Key existingKey = insertResult.getInsertKey();

      final Entity entity = Entity.builder(existing)
              .key(existingKey)
              .property("age", 41)
              .build();
      final Update update = QueryBuilder.update(entity);

      return datastore.executeAsync(update).compose(updateResult -> {
        context.assertTrue(updateResult.getIndexUpdates() > 0);

        final KeyQuery get = QueryBuilder.query(entity.getKey());
        return datastore.executeAsync(get);
      });
    }).compose(getResult -> {
      context.assertEquals(41, getResult.getEntity().getInteger("age"));
      return Future.succeededFuture();
    }).setHandler(context.asyncAssertSuccess());
  }

  @Test
  public void testUpdateAddProperty(TestContext context) {
    final Entity existing = Entity.builder("employee")
        .property("fullname", "Fred Blinge")
        .build();

    datastore.executeAsync(QueryBuilder.insert(existing)).compose(insertResult -> {
      final Key existingKey = insertResult.getInsertKey();
      final Entity entity = Entity.builder(existing)
              .key(existingKey)
              .property("age", 40)
              .build();
      final Update update = QueryBuilder.update(entity);
      return datastore.executeAsync(update).compose(updateResult -> {
        context.assertTrue(updateResult.getIndexUpdates() > 0);

        final KeyQuery get = QueryBuilder.query(entity.getKey());
        return datastore.executeAsync(get);
      });
    }).compose(getResult -> {
      context.assertEquals(40, getResult.getEntity().getInteger("age"));
      return Future.succeededFuture();
    }).setHandler(context.asyncAssertSuccess());
  }

  @Test
  public void testUpdateRemoveProperty(TestContext context) {
    final Entity existing = Entity.builder("employee")
        .property("fullname", "Fred Blinge")
        .property("age", 40)
        .build();

    datastore.executeAsync(QueryBuilder.insert(existing))
    .compose(insertResult -> {
      final Key existingKey = insertResult.getInsertKey();

      final Entity entity = Entity.builder(existing)
          .key(existingKey)
          .remove("age")
          .build();

      final Update update = QueryBuilder.update(entity);
      return datastore.executeAsync(update).compose(updateResult -> {
        context.assertTrue(updateResult.getIndexUpdates() > 0);
        return Future.succeededFuture(entity);
      });
    }).compose(entity -> {
      final KeyQuery get = QueryBuilder.query(entity.getKey());
      return datastore.executeAsync(get);
    }).compose(getResult -> {
      assertNull(getResult.getEntity().getInteger("age"));
      return Future.succeededFuture();
    }).setHandler(context.asyncAssertSuccess());
  }
}
