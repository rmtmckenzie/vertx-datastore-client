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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.vertx.core.Future;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Date;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(VertxUnitRunner.class)
@Category(IntegrationTest.class)
public class InsertTest extends DatastoreTest {

  @Test
  public void testInsert(TestContext context) {
    final Insert insert = QueryBuilder.insert("employee", 1234567L)
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);

    datastore.executeAsync(insert).compose(insertResult -> {
      context.assertTrue(insertResult.getIndexUpdates() > 0);
      return Future.succeededFuture();
    }).setHandler(context.asyncAssertSuccess());
  }

  @Test
  public void testInsertAuto(TestContext context) {
    final Insert insert = QueryBuilder.insert("employee")
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);

    datastore.executeAsync(insert).compose(insertResult -> {
      context.assertTrue(insertResult.getIndexUpdates() > 0);
      context.assertEquals("employee", insertResult.getInsertKey().getKind());
      context.assertTrue(insertResult.getInsertKey().getId() > 0);
      return Future.succeededFuture();
    }).setHandler(context.asyncAssertSuccess());
  }

  @Test
  public void testInsertEntity(TestContext context) {
    final Date now = new Date();
    final ByteString picture = ByteString.copyFrom(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9});
    final Entity address = Entity.builder("address", 222222L)
        .property("first_line", "22 Arcadia Ave")
        .property("zipcode", "90210").build();

    final Entity entity = Entity.builder("employee")
        .property("fullname", "Fred Blinge")
        .property("nickname", "Freddie", false)
        .property("height", 2.43)
        .property("holiday_allowance", 22.5, false)
        .property("payroll_number", 123456789)
        .property("age", 40, false)
        .property("senior_role", false)
        .property("active", true, false)
        .property("start_date", now)
        .property("update_date", now, false)
        .property("picture", picture)
        .property("address", address)
        .property("manager", Key.builder("employee", 234567L).build())
        .property("workdays", ImmutableList.of("Monday", "Tuesday", "Friday"))
        .property("overtime_hours", ImmutableList.of(2, 3, 4))
        .build();

    final Insert insert = QueryBuilder.insert(entity);
    datastore.executeAsync(insert).compose(insertResult -> {
      context.assertFalse(insertResult.getInsertKeys().isEmpty());
      return Future.succeededFuture();
    }).setHandler(context.asyncAssertSuccess());
  }

  @Test
  public void testInsertAlreadyExists(TestContext context) {
    final Insert insertFirst = QueryBuilder.insert("employee", 1234567L)
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);

    datastore.executeAsync(insertFirst).compose(insertFirstResult -> {
      final Insert insertSecond = QueryBuilder.insert("employee", 1234567L)
          .value("fullname", "Jack Spratt")
          .value("age", 21, false);
      return datastore.executeAsync(insertSecond);
    }).setHandler(context.asyncAssertFailure(cause -> {
      context.assertTrue(cause instanceof DatastoreException);
      context.assertEquals(400, ((DatastoreException)cause).getStatusCode());
    }));
  }

  @Test
  public void testInsertBlob(TestContext context) {
    final byte[] randomBytes = new byte[2000];
    new Random().nextBytes(randomBytes);
    final ByteString large = ByteString.copyFrom(randomBytes);
    final ByteString small = ByteString.copyFrom(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9});

    final Insert insert = QueryBuilder.insert("employee")
        .value("picture1", small)
        .value("picture2", large, false);

    datastore.executeAsync(insert).compose(result -> {
      context.assertFalse(result.getInsertKeys().isEmpty());
      return Future.succeededFuture();
    }).setHandler(context.asyncAssertSuccess());
  }

  @Test
  public void testInsertAutoThenGet(TestContext context) {
    final Insert insert = QueryBuilder.insert("employee")
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);
    datastore.executeAsync(insert).compose(insertResult -> {
      final KeyQuery get = QueryBuilder.query(insertResult.getInsertKey());
      return datastore.executeAsync(get);
    }).compose(getResult -> {
      context.assertEquals("Fred Blinge", getResult.getEntity().getString("fullname"));
      context.assertEquals(40, getResult.getEntity().getInteger("age").intValue());
      return Future.succeededFuture();
    }).setHandler(context.asyncAssertSuccess());
  }

  @Test
  public void testInsertFullThenGet(TestContext context) {
    final Key key = Key.builder("employee", 1234567L).build();
    final Insert insert = QueryBuilder.insert(key)
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);
    datastore.executeAsync(insert).compose(insertResult -> {
      final KeyQuery get = QueryBuilder.query(key);
      return datastore.executeAsync(get);
    }).compose(getResult -> {
      context.assertEquals("employee", getResult.getEntity().getKey().getKind());
      context.assertEquals(1234567L, getResult.getEntity().getKey().getId());
      return Future.succeededFuture();
    }).setHandler(context.asyncAssertSuccess());
  }

  @Test
  public void testInsertWithParent(TestContext context) {
    final Key employeeKey = Key.builder("employee", 1234567L).build();
    final Key salaryKey = Key.builder("payments", 222222L, employeeKey).build();

    final Insert insert = QueryBuilder.insert(salaryKey)
        .value("salary", 1000.00);
    datastore.executeAsync(insert).compose(insertResult -> {
      final KeyQuery get = QueryBuilder.query(salaryKey);
      return datastore.executeAsync(get);
    }).compose(getResult -> {
      context.assertEquals("employee", getResult.getEntity().getKey().getPath().get(0).getKind());
      context.assertEquals(1234567L, getResult.getEntity().getKey().getPath().get(0).getId());
      context.assertEquals("payments", getResult.getEntity().getKey().getPath().get(1).getKind());
      context.assertEquals(222222L, getResult.getEntity().getKey().getPath().get(1).getId());
      return Future.succeededFuture();
    }).setHandler(context.asyncAssertSuccess());
  }

  @Test
  public void testInsertBatch(TestContext context) {
    final Key parent = Key.builder("parent", "root").build();

    final Insert insert1 = QueryBuilder.insert(Key.builder("employee", parent).build())
        .value("fullname", "Jack Spratt")
        .value("age", 21, false);

    final Insert insert2 = QueryBuilder.insert(Key.builder("employee", parent).build())
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);

    final Insert insert3 = QueryBuilder.insert(Key.builder("employee", parent).build())
        .value("fullname", "Harry Ramsdens")
        .value("age", 50, false);

    final Batch batch = QueryBuilder.batch()
        .add(insert1)
        .add(insert2)
        .add(insert3);

    datastore.executeAsync(batch).compose(batchResult -> {
      assertFalse(batchResult.getInsertKeys().isEmpty());

      final Query getAll = QueryBuilder.query()
              .kindOf("employee")
              .filterBy(QueryBuilder.ancestor(parent))
              .orderBy(QueryBuilder.asc("fullname"));
      return datastore.executeAsync(getAll);
    }).compose(getAllResult -> {
      final List<Entity> entities = getAllResult.getAll();
      assertEquals(3, entities.size());
      assertEquals("Fred Blinge", entities.get(0).getString("fullname"));
      assertEquals("Harry Ramsdens", entities.get(1).getString("fullname"));
      assertEquals("Jack Spratt", entities.get(2).getString("fullname"));
      return Future.succeededFuture();
    }).setHandler(context.asyncAssertSuccess());
  }
}
