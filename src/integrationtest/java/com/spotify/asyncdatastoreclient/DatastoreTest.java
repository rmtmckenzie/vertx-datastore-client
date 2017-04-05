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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@RunWith(VertxUnitRunner.class)
public abstract class DatastoreTest {

  public static final String DATASTORE_HOST = System.getProperty("host", "http://localhost:8080");
  public static final String PROJECT = System.getProperty("dataset", "async-test");
  public static final String NAMESPACE = System.getProperty("namespace", "test");
  public static final String KEY_PATH = System.getProperty("keypath");
  public static final String VERSION = System.getProperty("version", "v1");

  protected static Datastore datastore;

  @Rule
  public RunTestOnContext rule = new RunTestOnContext(() -> {
    // set blocked thread interval to three minutes to allow for debugging.
    VertxOptions options = new VertxOptions()
            .setBlockedThreadCheckInterval(1000*60*3);
    return Vertx.vertx(options);
  });

  @Rule
  public Timeout timeout = Timeout.seconds(100);

  @Before
  public void before(TestContext context) throws URISyntaxException {
    Vertx vertx = rule.vertx();
    vertx.exceptionHandler(context.exceptionHandler());
    datastore = Datastore.create(vertx, datastoreConfig());

    resetDatastore().setHandler(context.asyncAssertSuccess());
  }

  private DatastoreConfig datastoreConfig() {
    final DatastoreConfig.Builder config = DatastoreConfig.builder()
        .connectTimeout(5000)
        .requestTimeout(1000)
        .maxConnections(5)
        .requestRetry(3)
        .version(VERSION)
        .host(DATASTORE_HOST)
        .project(PROJECT);

    if (NAMESPACE != null) {
      config.namespace(NAMESPACE);
    }

    if (KEY_PATH != null) {
      try {
        FileInputStream creds = new FileInputStream(new File(KEY_PATH));
        config.credential(GoogleCredential.fromStream(creds).createScoped(DatastoreConfig.SCOPES));
      } catch (final IOException e) {
        System.err.println("Failed to load credentials " + e.getMessage());
        System.exit(1);
      }
    }

    return config.build();
  }

  private Future<CompositeFuture> resetDatastore() {
    // add other kinds here as necessary...
    final Future<CompositeFuture> removeEmployees = removeAll("employee");
    final Future<CompositeFuture> removePayments = removeAll("payments");

    return CompositeFuture.all(removeEmployees, removePayments);
  }

  private Future<CompositeFuture> removeAll(final String kind) {
    final Query queryAll = QueryBuilder.query().kindOf(kind).keysOnly();
    return datastore.executeAsync(queryAll).compose(result -> {
      final List<Future> collect = StreamSupport.stream(result.spliterator(), false).map(entity -> datastore.executeAsync(QueryBuilder.delete(entity.getKey()))).collect(Collectors.toList());

      return CompositeFuture.all(collect);
    });
  }

  @After
  public void after() {
    datastore.close();
  }
}
