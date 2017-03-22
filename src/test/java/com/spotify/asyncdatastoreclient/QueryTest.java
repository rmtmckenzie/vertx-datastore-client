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
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

@RunWith(VertxUnitRunner.class)
@Category(IntegrationTest.class)
public class QueryTest extends DatastoreTest {

  private final Random random = new Random();

  private String randomString(final int length) {
    return random.ints(random.nextInt(length + 1), 'a', 'z' + 1)
        .mapToObj((i) -> (char) i)
        .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append)
        .toString();
  }

  private Future<List<MutationResult>> insertRandom(final int entities, final String kind) {
    final List<String> role = ImmutableList.of("manager", "engineer", "sales", "marketing");

    List<Future> inserts = new ArrayList<>(entities);

    for (int entity = 0; entity < entities; entity++) {
      final Insert insert = QueryBuilder.insert(kind, entity + 1)
          .value("fullname", randomString(20))
          .value("age", random.nextInt(60), false)
          .value("payroll", entity + 1)
          .value("senior", entity % 2 == 0)
          .value("role", role.get(entity % 4))
          .value("started", new Date());

      inserts.add(datastore.executeAsync(insert));
    }

    return CompositeFuture.all(inserts).compose(compositeFuture -> {
      List<MutationResult> results = new ArrayList<>(compositeFuture.size());
      for(int i = 0; i < compositeFuture.size(); ++i) {
        results.add(compositeFuture.resultAt(i));
      }

      noThrowWaitForConsistency();

      return Future.succeededFuture(results);
    });
  }

  private static void noThrowWaitForConsistency() {
    try {
      waitForConsistency();
    } catch (Exception ignored) {}
  }

  private static void waitForConsistency() throws Exception {
    // Ugly hack to minimise test failures due to inconsistencies.
    // An alternative, if running locally, is to this run `gcd` with `--consistency=1.0`
    Thread.sleep(300);
  }

  @Test
  public void testKeyQuery(TestContext context) {
    final Insert insert = QueryBuilder.insert("employee", 1234567L)
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);

    datastore.executeAsync(insert).compose(insertResult -> {
      noThrowWaitForConsistency();
      final KeyQuery get = QueryBuilder.query("employee", 1234567L);
      return datastore.executeAsync(get);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      final List<Entity> entities = getResult.getAll();
      context.assertEquals(1, entities.size());
      context.assertEquals("Fred Blinge", entities.get(0).getString("fullname"));
      context.assertEquals(40, entities.get(0).getInteger("age"));
    }));
  }

  @Test
  public void testMultiKeyQuery(TestContext context) {
    final Insert insert1 = QueryBuilder.insert("employee", 1234567L)
      .value("fullname", "Fred Blinge")
      .value("age", 40, false);
    final Insert insert2 = QueryBuilder.insert("employee", 2345678L)
      .value("fullname", "Jack Spratt")
      .value("age", 21);

    CompositeFuture.all(datastore.executeAsync(insert1),  datastore.executeAsync(insert2)).compose(inserts -> {
      noThrowWaitForConsistency();
      final List<KeyQuery> keys = ImmutableList.of(
              QueryBuilder.query("employee", 1234567L), QueryBuilder.query("employee", 2345678L));
      return datastore.executeAsync(keys);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      final List<Entity> entities = getResult.getAll();
      assertEquals(2, entities.size());
      final List<Entity> sorted = entities
              .stream()
              .sorted(Comparator.comparingLong(a -> a.getKey().getId()))
              .collect(Collectors.toList());
      context.assertEquals("Fred Blinge", sorted.get(0).getString("fullname"));
      context.assertEquals(40, sorted.get(0).getInteger("age"));
      context.assertEquals("Jack Spratt", sorted.get(1).getString("fullname"));
      context.assertEquals(21, sorted.get(1).getInteger("age"));
    }));
  }

  @Test
  public void testKeyQueryNotExist(TestContext context) {
    final KeyQuery get = QueryBuilder.query("employee", 1234567L);
    datastore.executeAsync(get).setHandler(context.asyncAssertSuccess(getResult -> {
      context.assertEquals(0, getResult.getAll().size());
      context.assertNull(getResult.getEntity());
    }));
  }

  @Test
  public void testKeyQueryBadKey(TestContext context) {
    final KeyQuery get = QueryBuilder.query(Key.builder("incomplete").build());

    datastore.executeAsync(get).setHandler(context.asyncAssertFailure(cause -> {
      if(cause instanceof DatastoreException) {
        context.assertEquals(400, ((DatastoreException)cause).getStatusCode());
      } else {
        context.fail(cause);
      }
    }));
  }

  @Test
  public void testSimpleQuery(TestContext context) {
    insertRandom(10, "employee").compose(insertResults -> {
      final Query get = QueryBuilder.query()
              .kindOf("employee");
      return datastore.executeAsync(get);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      final List<Entity> entities = getResult.getAll();
      context.assertEquals(10, entities.size());
    }));
  }

  @Test
  public void testQueryOrderAsc(TestContext context) {
    insertRandom(10, "employee").compose(insertResults -> {
      final Query get = QueryBuilder.query()
        .kindOf("employee")
        .orderBy(QueryBuilder.asc("payroll"));
      return datastore.executeAsync(get);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      final List<Entity> entities = getResult.getAll();
      context.assertEquals(10, entities.size());
      context.assertEquals(1, entities.get(0).getInteger("payroll"));
      context.assertEquals(10, entities.get(9).getInteger("payroll"));
    }));
  }

  @Test
  public void testQueryOrderDesc(TestContext context) {
    insertRandom(10, "employee").compose(insertResults -> {
      final Query get = QueryBuilder.query()
          .kindOf("employee")
          .orderBy(QueryBuilder.desc("payroll"));
      return datastore.executeAsync(get);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      final List<Entity> entities = getResult.getAll();
      context.assertEquals(10, entities.size());
      context.assertEquals(10, entities.get(0).getInteger("payroll"));
      context.assertEquals(1, entities.get(9).getInteger("payroll"));
    }));
  }

  @Test
  public void testQueryOrderNotIndexed(TestContext context) {
    insertRandom(10, "employee").compose(insertResults -> {
      final Query get = QueryBuilder.query()
          .kindOf("employee")
          .orderBy(QueryBuilder.desc("age"));
      return datastore.executeAsync(get);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      final List<Entity> entities = getResult.getAll();
      context.assertEquals(0, entities.size()); // non-indexed properties are ignored
    }));
  }

  @Test
  public void testQueryOrderNotExists(TestContext context) {
    insertRandom(10, "employee").compose(insertResults -> {
      final Query get = QueryBuilder.query()
          .kindOf("employee")
          .orderBy(QueryBuilder.asc("not_exists"));
      return datastore.executeAsync(get);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      final List<Entity> entities = getResult.getAll();
      context.assertEquals(0, entities.size()); // non-existing properties are ignored
    }));
  }

  @Test
  public void testQueryMultipleOrders(TestContext context) {
    insertRandom(10, "employee").compose(insertResults -> {
      final Query get = QueryBuilder.query()
          .kindOf("employee")
          .orderBy(QueryBuilder.asc("senior"))
          .orderBy(QueryBuilder.desc("payroll"));
      return datastore.executeAsync(get);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      final List<Entity> entities = getResult.getAll();
      context.assertEquals(10, entities.size());
      context.assertEquals(true, entities.get(0).getBoolean("senior"));
      context.assertEquals(true, entities.get(9).getBoolean("senior"));
      context.assertEquals(10, entities.get(0).getInteger("payroll"));
      context.assertEquals(1, entities.get(9).getInteger("payroll"));
    }));
  }

  @Test
  public void testQueryOrdersByKey(TestContext context) {
    insertRandom(10, "employee").compose(insertResults -> {
      final Query get = QueryBuilder.query()
          .kindOf("employee");
      return datastore.executeAsync(get);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      final List<Entity> entities = getResult.getAll();
      context.assertEquals(10, entities.size());
      context.assertEquals(1, entities.get(0).getKey().getId());
      context.assertEquals(10, entities.get(9).getKey().getId());
    }));
  }

  @Test
  public void testQueryEqFilter(TestContext context) {
    insertRandom(20, "employee").compose(insertResults -> {
      final Query get = QueryBuilder.query()
          .kindOf("employee")
          .filterBy(QueryBuilder.eq("role", "engineer"));
      return datastore.executeAsync(get);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      final List<Entity> entities = getResult.getAll();
      context.assertEquals(5, entities.size());
      context.assertEquals("engineer", entities.get(0).getString("role"));
      context.assertEquals("engineer", entities.get(4).getString("role"));
    }));
  }

  @Test
  public void testQueryLtFilter(TestContext context) {
    insertRandom(20, "employee").compose(insertResults -> {
      final Query get = QueryBuilder.query()
          .kindOf("employee")
          .filterBy(QueryBuilder.lt("payroll", 10));
      return datastore.executeAsync(get);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      final List<Entity> entities = getResult.getAll();
      context.assertEquals(9, entities.size());
    }));
  }

  @Test
  public void testQueryLteFilter(TestContext context) {
    insertRandom(20, "employee").compose(insertResults -> {
      final Query get = QueryBuilder.query()
          .kindOf("employee")
          .filterBy(QueryBuilder.lte("payroll", 10));
      return datastore.executeAsync(get);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      final List<Entity> entities = getResult.getAll();
      context.assertEquals(10, entities.size());
    }));
  }

  @Test
  public void testQueryGtFilter(TestContext context) {
    insertRandom(20, "employee").compose(insertResults -> {
      final Query get = QueryBuilder.query()
          .kindOf("employee")
          .filterBy(QueryBuilder.gt("payroll", 10));
      return datastore.executeAsync(get);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      final List<Entity> entities = getResult.getAll();
      context.assertEquals(10, entities.size());
    }));
  }

  @Test
  public void testQueryGteFilter(TestContext context) {
    insertRandom(20, "employee").compose(insertResults -> {
      final Query get = QueryBuilder.query()
          .kindOf("employee")
          .filterBy(QueryBuilder.gte("payroll", 10));
      return datastore.executeAsync(get);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      final List<Entity> entities = getResult.getAll();
      context.assertEquals(11, entities.size());
    }));
  }

  @Test
  public void testQueryMultipleFilters(TestContext context) {
    insertRandom(20, "employee").compose(insertResults -> {
      final Query get = QueryBuilder.query()
          .kindOf("employee")
          .filterBy(QueryBuilder.gte("payroll", 10))
          .filterBy(QueryBuilder.lte("payroll", 10));
      return datastore.executeAsync(get);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      final List<Entity> entities = getResult.getAll();
      context.assertEquals(1, entities.size());
      context.assertEquals(10, entities.get(0).getInteger("payroll"));
    }));
  }

  @Test
  public void testQueryFilterNotIndexed(TestContext context) {
    insertRandom(20, "employee").compose(insertResults -> {
      final Query get = QueryBuilder.query()
          .kindOf("employee")
          .filterBy(QueryBuilder.lte("age", 40));
      return datastore.executeAsync(get);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      final List<Entity> entities = getResult.getAll();
      context.assertEquals(0, entities.size());
    }));
  }

  @Test
  public void testQueryFilterNotExist(TestContext context) {
    insertRandom(20, "employee").compose(insertResults -> {
      final Query get = QueryBuilder.query()
          .kindOf("employee")
          .filterBy(QueryBuilder.lte("not_exist", 40));
      return datastore.executeAsync(get);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      final List<Entity> entities = getResult.getAll();
      context.assertEquals(0, entities.size());
    }));
  }

  @Test
  public void testQueryDateFilter(TestContext context) {
    insertRandom(10, "employee").compose(insertResults -> {
      final Calendar today = Calendar.getInstance();
      today.set(Calendar.HOUR_OF_DAY, 0);
      today.set(Calendar.MINUTE, 0);
      today.set(Calendar.SECOND, 0);
      today.set(Calendar.MILLISECOND, 0);

      final Query get = QueryBuilder.query()
          .kindOf("employee")
          .filterBy(QueryBuilder.gte("started", today.getTime()));
      return datastore.executeAsync(get);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      final List<Entity> entities = getResult.getAll();
      context.assertEquals(10, entities.size());
    }));
  }


  @Test
  public void testQueryKeyFilter(TestContext context) {
    final Key record = Key.builder("record", 2345678L).build();
    final Insert insert = QueryBuilder.insert("employee", 1234567L)
        .value("fullname", "Fred Blinge")
        .value("record", record);
    datastore.executeAsync(insert).compose(insertResult -> {
      noThrowWaitForConsistency();

      final Query get = QueryBuilder.query()
              .kindOf("employee")
              .filterBy(QueryBuilder.eq("record", record));
      return datastore.executeAsync(get);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      final List<Entity> entities = getResult.getAll();
      context.assertEquals(1, entities.size());
    }));
  }

  @Test
  public void testQueryAncestorFilter(TestContext context) {
    final Key employeeKey = Key.builder("employee", 1234567L).build();
    final Key salaryKey = Key.builder("payments", 222222L, employeeKey).build();

    final Insert insert = QueryBuilder.insert(salaryKey)
        .value("salary", 1000.00);
    datastore.executeAsync(insert).compose(insertResult -> {
      noThrowWaitForConsistency();
      final Query get = QueryBuilder.query()
          .kindOf("payments")
          .filterBy(QueryBuilder.ancestor(employeeKey));
      return datastore.executeAsync(get);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      final List<Entity> entities = getResult.getAll();
      context.assertEquals(1, entities.size());
    }));
  }

  @Test
  public void testQueryGroupBy(TestContext context) {
    insertRandom(20, "employee").compose(insertResults -> {
      final Query get = QueryBuilder.query()
          .kindOf("employee")
          .groupBy(QueryBuilder.group("senior"));
      return datastore.executeAsync(get);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      final List<Entity> entities = getResult.getAll();
      context.assertEquals(2, entities.size());
    }));
  }

  @Test
  public void testQueryFilterAndGroupBy(TestContext context) {
    insertRandom(20, "employee").compose(insertResults -> {
      final Query get = QueryBuilder.query()
          .kindOf("employee")
          .filterBy(QueryBuilder.eq("role", "engineer"))
          .groupBy(QueryBuilder.group("senior"));
      return datastore.executeAsync(get);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      final List<Entity> entities = getResult.getAll();
      context.assertEquals(1, entities.size());
    }));
  }

  @Test
  public void testQueryFilterAndGroupByAndOrderBy(TestContext context) {
    insertRandom(20, "employee").compose(insertResults -> {
      final Query get = QueryBuilder.query()
          .kindOf("employee")
          .filterBy(QueryBuilder.gt("payroll", 10))
          .groupBy(QueryBuilder.group("payroll"))
          .orderBy(QueryBuilder.asc("payroll"));
      return datastore.executeAsync(get);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      final List<Entity> entities = getResult.getAll();
      context.assertEquals(10, entities.size());
    }));
  }

  @Test
  public void testProjectionQuery(TestContext context) {
    final Insert insert1 = QueryBuilder.insert("employee", 1234567L)
        .value("fullname", "Fred Blinge")
        .value("payroll", 1000)
        .value("age", 40);
    final Insert insert2 = QueryBuilder.insert("employee", 2345678L)
        .value("fullname", "Jack Spratt")
        .value("payroll", 1001)
        .value("age", 21);

    CompositeFuture.all(datastore.executeAsync(insert1), datastore.executeAsync(insert2)).compose(insertResults -> {
      noThrowWaitForConsistency();

      final Query get = QueryBuilder.query()
          .properties("fullname", "payroll")
          .kindOf("employee")
          .orderBy(QueryBuilder.asc("fullname"));
      return datastore.executeAsync(get);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      final List<Entity> entities = getResult.getAll();

      context.assertEquals(2, entities.size());

      context.assertEquals("Fred Blinge", entities.get(0).getString("fullname"));
      context.assertEquals(1000, entities.get(0).getInteger("payroll"));
      context.assertNull(entities.get(0).getInteger("age"));
      context.assertEquals("Jack Spratt", entities.get(1).getString("fullname"));
      context.assertEquals(1001, entities.get(1).getInteger("payroll"));
      context.assertNull(entities.get(1).getInteger("age"));
    }));
  }

  @Test
  public void testQueryKeysOnly(TestContext context) {
    final Insert insert1 = QueryBuilder.insert("employee", 1234567L)
            .value("fullname", "Fred Blinge")
            .value("payroll", 1000)
            .value("age", 40);
    final Insert insert2 = QueryBuilder.insert("employee", 2345678L)
            .value("fullname", "Jack Spratt")
            .value("payroll", 1001)
            .value("age", 21);

    CompositeFuture.all(datastore.executeAsync(insert1), datastore.executeAsync(insert2)).compose(insertResults -> {
      noThrowWaitForConsistency();

      final Query get = QueryBuilder.query()
              .keysOnly()
              .kindOf("employee")
              .orderBy(QueryBuilder.asc("fullname"));
      return datastore.executeAsync(get);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      final List<Entity> entities = getResult.getAll();

      context.assertEquals(2, entities.size());

      context.assertEquals(1234567L, entities.get(0).getKey().getId());
      context.assertNull(entities.get(0).getString("fullname"));
      context.assertNull(entities.get(0).getInteger("payroll"));
      context.assertNull(entities.get(0).getInteger("age"));
      context.assertEquals(2345678L, entities.get(1).getKey().getId());
      context.assertNull(entities.get(1).getString("fullname"));
      context.assertNull(entities.get(1).getInteger("payroll"));
      context.assertNull(entities.get(1).getInteger("age"));
    }));
  }

  @Test
  public void testQueryIterator(TestContext context) {
    insertRandom(20, "employee").compose(insertResult -> {
      final Query get = QueryBuilder.query()
              .kindOf("employee")
              .filterBy(QueryBuilder.eq("role", "engineer"));
      return datastore.executeAsync(get);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      int entityCount = 0;
      for (final Entity entity : getResult) {
        context.assertEquals("engineer", entity.getString("role"));
        entityCount++;
      }
      context.assertEquals(5, entityCount);
    }));
  }

  @Test
  public void testQueryLimit(TestContext context) {
    insertRandom(20, "employee").compose(insertResult -> {
      final Query get = QueryBuilder.query()
          .kindOf("employee")
          .limit(10);
      return datastore.executeAsync(get);
    }).setHandler(context.asyncAssertSuccess(getResult -> {
      final List<Entity> entities = getResult.getAll();
      assertEquals(10, entities.size());
    }));
  }

  @Test
  public void testQueryPaged(TestContext context) {

    class Accumulate {
      int total = 0;
      int batches = 0;
    }
    Accumulate accumulate = new Accumulate();

    insertRandom(100, "employee").compose(insertResult -> {
      Query getFirst = QueryBuilder.query()
              .kindOf("employee")
              .limit(10);

      Future<Void> done = Future.future();

      Handler<QueryResult> handler = new Handler<QueryResult>() {
        @Override
        public void handle(QueryResult result) {
          final List<Entity> entities = result.getAll();
          if (entities.isEmpty()) {
            done.complete();
            return;
          }

          final Query get = QueryBuilder.query()
                  .fromCursor(result.getCursor())
                  .kindOf("employee")
                  .limit(10);
          accumulate.batches += 1;
          accumulate.total += entities.size();

          datastore.executeAsync(get).compose(this, done);
        }
      };

      return datastore.executeAsync(getFirst).compose(handler, done);
    }).setHandler(context.asyncAssertSuccess(void_ -> {
      context.assertEquals(100, accumulate.total);
      context.assertEquals(10, accumulate.batches);
    }));
  }
}