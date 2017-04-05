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

package com.spotify.asyncdatastoreclient.example;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.common.base.Throwables;
import com.spotify.asyncdatastoreclient.*;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.Date;

import static com.spotify.asyncdatastoreclient.QueryBuilder.asc;
import static com.spotify.asyncdatastoreclient.QueryBuilder.eq;

/**
 * Some simple asynchronous examples that should help you get started.
 */
public final class ExampleAsync {

  private ExampleAsync() {
  }

  private static Future<MutationResult> addData(final Datastore datastore) {
    final Insert insert = QueryBuilder.insert("employee", 1234567L)
        .value("fullname", "Fred Blinge")
        .value("inserted", new Date())
        .value("age", 40);
    return datastore.executeAsync(insert);
  }

  private static Future<MutationResult> addDataInTransaction(final Datastore datastore) {
    return datastore.transactionAsync().compose(txn -> {
      final KeyQuery get = QueryBuilder.query("employee", 2345678L);
      return datastore.executeAsync(get, txn).compose((QueryResult result) -> {
        if (result.getEntity() == null) {
          datastore.rollbackAsync(txn); // fire and forget
          return Future.succeededFuture(MutationResult.build());
        }

        final Insert insert = QueryBuilder.insert("employee", 2345678L)
            .value("fullname", "Fred Blinge")
            .value("inserted", new Date())
            .value("age", 40);
        return datastore.executeAsync(insert);
      });
    });
  }

  private static Future<QueryResult> queryData(final Datastore datastore) {
    final Query get = QueryBuilder.query()
        .kindOf("employee")
        .filterBy(eq("age", 40))
        .orderBy(asc("fullname"));
    return datastore.executeAsync(get);
  }

  private static Future<MutationResult> deleteData(final Datastore datastore) {
    final Batch delete = QueryBuilder.batch()
        .add(QueryBuilder.delete("employee", 1234567L))
        .add(QueryBuilder.delete("employee", 2345678L));
    return datastore.executeAsync(delete);
  }

  public static void main(final String... args) throws Exception {
    Vertx vertx = Vertx.vertx();

    final DatastoreConfig config = DatastoreConfig.builder()
        .connectTimeout(5000)
        .requestTimeout(1000)
        .maxConnections(5)
        .requestRetry(3)
        .project("my-dataset")
        .namespace("my-namespace")
        .credential(GoogleCredential.getApplicationDefault())
        .build();

    final Datastore datastore = Datastore.create(vertx, config);

    // Add a two entities asynchronously
    final Future<MutationResult> addFirst = addData(datastore);
    final Future<MutationResult> addSecond = addDataInTransaction(datastore);

    final CompositeFuture addBoth = CompositeFuture.all(addFirst, addSecond);

    // Query the entities we've just inserted
    final Future<QueryResult> query = addBoth.compose(compositeFuture -> queryData(datastore));

    // Print the query results before clean up
    final Future<MutationResult> delete = query.compose((QueryResult result) -> {
      for (final Entity entity : result) {
        System.out.println("Employee name: " + entity.getString("fullname"));
        System.out.println("Employee age: " + entity.getInteger("age").intValue());
      }
      return deleteData(datastore);
    });

    delete.setHandler(operationsResult -> {
      if(operationsResult.succeeded()) {
        System.out.println("All complete.");
      } else {
        System.err.println("Storage exception: " + Throwables.getRootCause(operationsResult.cause()).getMessage());
      }
    });
  }
}
