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

@RunWith(VertxUnitRunner.class)
@Category(IntegrationTest.class)
public class TransactionTest extends DatastoreTest {

  @Test
  public void testInsert(TestContext context) {
    datastore.transactionAsync().compose(txn -> {
      final Insert insert = QueryBuilder.insert("employee")
          .value("fullname", "Fred Blinge")
          .value("age", 40, false);
      return datastore.executeAsync(insert, txn);
    }).compose(insertResult -> {
      final KeyQuery get = QueryBuilder.query(insertResult.getInsertKey());
      return datastore.executeAsync(get);
    }).compose(getResult -> {
      context.assertEquals("Fred Blinge", getResult.getEntity().getString("fullname"));
      context.assertEquals(40, getResult.getEntity().getInteger("age").intValue());
      return Future.succeededFuture();
    }).setHandler(context.asyncAssertSuccess());
  }

  @Test
  public void testGetThenInsert(TestContext context) {
    datastore.transactionAsync().compose(txn -> {
      final KeyQuery get = QueryBuilder.query("employee", 1234567L);
      return datastore.executeAsync(get, txn).compose(getResult -> {
        assertEquals(0, getResult.getAll().size());
        final Insert insert = QueryBuilder.insert("employee", 1234567L)
                .value("fullname", "Fred Blinge")
                .value("age", 40, false);
        return datastore.executeAsync(insert, txn);
      });
    }).compose(insertResult -> {
      context.assertTrue(insertResult.getIndexUpdates() > 0);
      return Future.succeededFuture();
    }).setHandler(context.asyncAssertSuccess());
  }

  @Test
  public void testTransactionExpired(TestContext context) {
    datastore.transactionAsync().compose(txn -> {
      final Insert insertFirst = QueryBuilder.insert("employee")
              .value("fullname", "Fred Blinge")
              .value("age", 40, false);
      return datastore.executeAsync(insertFirst, txn).compose(firstInsertResult -> Future.succeededFuture(txn));
    }).compose(txn -> {
      final Insert insertSecond = QueryBuilder.insert("employee")
              .value("fullname", "Fred Blinge")
              .value("age", 40, false);
      return datastore.executeAsync(insertSecond, txn);
    }).setHandler(context.asyncAssertFailure(cause -> {
      if(cause instanceof DatastoreException) {
        context.assertEquals(400, ((DatastoreException)cause).getStatusCode()); // bad request
      } else {
        context.fail(cause);
      }
    }));
  }

  @Test
  public void testTransactionWriteConflict(TestContext context) {
    final Insert insert = QueryBuilder.insert("employee", 1234567L)
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);

    datastore.executeAsync(insert).compose(insertResult -> {
      return datastore.transactionAsync(); // initiate transition
    }).compose(txn -> {
      final KeyQuery get = QueryBuilder.query("employee", 1234567L);
      return datastore.executeAsync(get, txn).compose(getResult -> {
        context.assertNotNull(getResult.getEntity());

        final Update update = QueryBuilder.update("employee", 1234567L)
                .value("age", 41, false);

        return datastore.executeAsync(update); // update outside transaction
      }).compose(updateResult -> {
        //now, update within transaction
        final Update update = QueryBuilder.update("employee", 1234567L)
                .value("age", 41, false);
        return datastore.executeAsync(update, txn);
      });
    }).setHandler(context.asyncAssertFailure(secondUpdateFailure -> {
      // ensure that update using transaction after updating outside transaction fails
      if(secondUpdateFailure instanceof DatastoreException) {
        context.assertEquals(409, ((DatastoreException)secondUpdateFailure).getStatusCode()); // conflict
      } else {
        context.fail(secondUpdateFailure);
      }
    }));
  }

  @Test
  public void testTransactionRead(TestContext context) {
    final Insert insert = QueryBuilder.insert("employee", 1234567L)
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);
    datastore.executeAsync(insert).compose(insertResult -> {
      return datastore.transactionAsync(); // initiate transition
    }).compose(txn -> {
      final KeyQuery get = QueryBuilder.query("employee", 1234567L);
      return datastore.executeAsync(get, txn).compose(getResult1 -> {
        context.assertEquals(40, getResult1.getEntity().getInteger("age").intValue());

        final Update update = QueryBuilder.update("employee", 1234567L)
                .value("age", 41, false);
        return datastore.executeAsync(update);
      }).compose(afterUpdate -> {
        return datastore.executeAsync(get, txn); // read inside transaction
      });
    }).compose(getResult2 -> {
      context.assertEquals(40, getResult2.getEntity().getInteger("age").intValue());
      return Future.succeededFuture();
    }).setHandler(context.asyncAssertSuccess());
  }

  @Test
  public void testInsertBatchInTransaction(TestContext context) {
    datastore.transactionAsync().compose(txn -> {
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

      return datastore.executeAsync(batch, txn).compose(batchInsert -> {
        context.assertFalse(batchInsert.getInsertKeys().isEmpty());

        final Query getAll = QueryBuilder.query()
                .kindOf("employee")
                .filterBy(QueryBuilder.ancestor(parent))
                .orderBy(QueryBuilder.asc("fullname"));
        return datastore.executeAsync(getAll);
      });
    }).compose(getAllResult -> {
      final List<Entity> entities = getAllResult.getAll();
      context.assertEquals(3, entities.size());
      context.assertEquals("Fred Blinge", entities.get(0).getString("fullname"));
      context.assertEquals("Harry Ramsdens", entities.get(1).getString("fullname"));
      context.assertEquals("Jack Spratt", entities.get(2).getString("fullname"));
      return Future.succeededFuture();
    }).setHandler(context.asyncAssertSuccess());
  }

  @Test
  public void testQueryInTransaction(TestContext context) {
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
      context.assertFalse(batchResult.getInsertKeys().isEmpty());
      return datastore.transactionAsync();
    }).compose(txn -> {
      final Query getAll = QueryBuilder.query()
              .kindOf("employee")
              .filterBy(QueryBuilder.ancestor(parent))
              .orderBy(QueryBuilder.asc("fullname"));
      return datastore.executeAsync(getAll, txn).compose(getAllResult -> {
        final List<Entity> entities = getAllResult.getAll();
        context.assertEquals(3, entities.size());
        context.assertEquals("Fred Blinge", entities.get(0).getString("fullname"));
        context.assertEquals("Harry Ramsdens", entities.get(1).getString("fullname"));
        context.assertEquals("Jack Spratt", entities.get(2).getString("fullname"));

        return datastore.commitAsync(txn);
      });
    }).setHandler(context.asyncAssertSuccess());
  }
}
