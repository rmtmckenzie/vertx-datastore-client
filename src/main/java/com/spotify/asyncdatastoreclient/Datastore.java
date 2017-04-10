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

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.protobuf.ProtoHttpContent;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.datastore.v1.*;
import com.google.protobuf.ByteString;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.impl.HttpUtils;
import io.vertx.core.net.JksOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

/**
 * The Datastore class encapsulates the Cloud Datastore API and handles
 * calling the datastore backend.
 * <p>
 * To create a Datastore object, call the static method {@code Datastore.create()}
 * passing configuration. A scheduled task will begin that automatically refreshes
 * the API access token for you.
 * <p>
 * Call {@code close()} to perform all necessary clean up.
 */
public final class Datastore implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(Datastore.class);

  public static final String VERSION = "1.0.0";
  public static final String USER_AGENT = "Datastore-Java-Client/" + VERSION + " (gzip)";

  private final DatastoreConfig config;
  private final HttpClient httpClient;
  private final String prefixUri;

  private final ScheduledExecutorService executor;
  private volatile String accessToken;


  private Datastore(final Vertx vertx, final DatastoreConfig config) throws URISyntaxException {
    this.config = config;

    URI uri = HttpUtils.resolveURIReference(config.getHost(), "");

    int port = uri.getPort();
    String protocol = uri.getScheme();
    char chend = protocol.charAt(protocol.length() - 1);

    if (chend == 'p' && port == -1) {
      port = 80;
    } else if (chend == 's' && port == -1) {
        port = 443;
    } else if (port == -1) {
      throw new UnsupportedOperationException("Do not support protocols other than http and https at this time when no port defined");
    }

    // set ssl to true if protocol is https
    boolean ssl = chend == 's';

    final HttpClientOptions httpClientOptions = new HttpClientOptions()
            .setConnectTimeout(config.getConnectTimeout())
            .setIdleTimeout(config.getRequestTimeout()/1000)
            .setMaxPoolSize(config.getMaxConnections())
            .setHttp2MaxPoolSize(config.getMaxConnections())
            .setDefaultHost(uri.getHost())
            .setDefaultPort(port)
            .setTryUseCompression(true)
            .setReceiveBufferSize(5000)
            .setSsl(ssl)
            .setTrustStoreOptions(new JksOptions()
              .setPath(System.getProperty("java.home") + "/lib/security/cacerts"));
    //TODO: retry is missing, eg:
    //final AsyncHttpClientConfig httpConfig = new AsyncHttpClientConfig.Builder()
    //    .setMaxRequestRetry(config.getRequestRetry())

    httpClient = vertx.createHttpClient(httpClientOptions);
    prefixUri = String.format("%s/projects/%s:", config.getVersion(), config.getProject());

    executor = Executors.newSingleThreadScheduledExecutor();

    if (config.getCredential() != null) {
      // block while retrieving an access token for the first time
      refreshAccessToken();
      // store the first time as refresh doesn't
      this.accessToken = config.getCredential().getAccessToken();

      // wake up every 10 seconds to check if access token has expired
      executor.scheduleAtFixedRate(this::refreshAccessToken, 10, 10, TimeUnit.SECONDS);
    }
  }

  public static Datastore create(final Vertx vertx, final DatastoreConfig config) throws URISyntaxException {
    return new Datastore(vertx, config);
  }

  @Override
  public void close() {
    executor.shutdown();
    httpClient.close();
  }

  private void refreshAccessToken() {
    final Credential credential = config.getCredential();
    final Long expiresIn = credential.getExpiresInSeconds();

    // trigger refresh if token is about to expire
    if (credential.getAccessToken() == null || expiresIn != null && expiresIn <= 60) {
      try {
        credential.refreshToken();
        final String accessTokenLocal = credential.getAccessToken();
        if (accessTokenLocal != null) {
          this.accessToken = accessTokenLocal;
        }
      } catch (final IOException e) {
        log.error("Storage exception", Throwables.getRootCause(e));
      }
    }
  }

  private static boolean isSuccessful(final int statusCode) {
    return statusCode >= 200 && statusCode < 300;
  }

  private <R> Function<HttpClientResponse, Future<R>> getResult(ThrowingFunction<InputStream, R> func) {
    return (httpClientResponse -> {
      Future<R> future = Future.future();
      httpClientResponse.bodyHandler(body -> {
        if(!isSuccessful(httpClientResponse.statusCode())) {
          future.fail(new DatastoreException(httpClientResponse.statusCode(), body.toString()));
        }
        try {
          future.complete(func.apply(streamBufferResponse(body, httpClientResponse.headers())));
        } catch (Exception e) {
          future.fail(e);
        }
      });
      return future;
    });
  }

  public interface ThrowingFunction<T, R> {
    R apply(T t) throws Exception;
  }

  private Future<HttpClientResponse> initiateRequest(final String method, final ProtoHttpContent payload) {
    Buffer buffer;
    try {
      buffer = Buffer.buffer((int)payload.getLength());
      payload.writeTo(new OutputStream() {
        @Override
        public void write(int b) throws IOException {
          buffer.appendByte((byte)(b & 0xff));
        }
      });
    } catch (IOException e) {
      return Future.failedFuture(e);
    }

    Future<HttpClientResponse> doPost = Future.future();

    httpClient.post(prefixUri + method)
            .putHeader("Authorization", "Bearer " + accessToken)
            .putHeader("Content-Type", "application/x-protobuf")
            .putHeader("User-Agent", USER_AGENT)
            .putHeader("Accept-Encoding", "gzip")
            .handler(doPost::complete)
            .exceptionHandler(cause -> {
              doPost.fail(cause);
            })
            .end(buffer);

    return doPost;
  }

  private InputStream streamBufferResponse(final Buffer responseBody, MultiMap headers) throws IOException {
    final InputStream bufferStream = new InputStream() {
      private int curPos = 0;
      private int len = responseBody.length();
      @Override
      public int read() throws IOException {
        return curPos < len ? ((int)(responseBody.getByte(curPos++)) & 0xff) : -1;
      }
    };
    final boolean compressed = "gzip".equals(headers.get("Content-Encoding"));
    return compressed ? new GZIPInputStream(bufferStream) : bufferStream;
  }

  /**
   * Start a new transaction.
   *
   * The returned {@code TransactionResult} contains the transaction if the
   * request is successful.
   *
   * @return the result of the transaction request.
   */
  public Future<TransactionResult> transactionAsync() {

    final BeginTransactionRequest.Builder request = BeginTransactionRequest.newBuilder();
    final ProtoHttpContent payload = new ProtoHttpContent(request.build());

    return initiateRequest("beginTransaction", payload).compose(getResult(result -> {
      final BeginTransactionResponse transaction = BeginTransactionResponse.parseFrom(result);
      return TransactionResult.build(transaction);
    }));
  }

  /**
   * Rollback a given transaction.
   *
   * You normally rollback a transaction in the event of a Datastore failure.
   *
   * @param txn the transaction.
   * @return the result of the rollback request.
   */
  public Future<RollbackResult> rollbackAsync(final TransactionResult txn) {
    final ByteString transaction = txn.getTransaction();
    if(transaction == null) {
      return Future.failedFuture(new DatastoreException("Invalid transaction."));
    }
    final RollbackRequest.Builder request = RollbackRequest.newBuilder();
    final ProtoHttpContent payload = new ProtoHttpContent(request.build());

    return initiateRequest("rollback", payload).compose(getResult(result -> {
      final RollbackResponse rollback = RollbackResponse.parseFrom(result);
      return RollbackResult.build(rollback);
    }));
  }

  /**
   * Commit a given transaction.
   *
   * You normally manually commit a transaction after performing read-only
   * operations without mutations.
   *
   * @param txn the transaction.
   * @return the result of the commit request.
   */
  public Future<MutationResult> commitAsync(final TransactionResult txn) {
    return executeAsync((MutationStatement) null, txn);
  }

  /**
   * Execute a allocate ids statement.
   *
   * @param statement the statement to execute.
   * @return the result of the allocate ids request.
   */
  public Future<AllocateIdsResult> executeAsync(final AllocateIds statement) {

    final AllocateIdsRequest.Builder request = AllocateIdsRequest.newBuilder()
            .addAllKeys(statement.getPb(config.getNamespace()));
    final ProtoHttpContent payload = new ProtoHttpContent(request.build());

    return initiateRequest("allocateIds", payload).compose(getResult(result -> {
      final AllocateIdsResponse allocate = AllocateIdsResponse.parseFrom(result);
      return AllocateIdsResult.build(allocate);
    }));
  }

  /**
   * Execute a keyed query statement.
   *
   * @param statement the statement to execute.
   * @return the result of the query request.
   */
  public Future<QueryResult> executeAsync(final KeyQuery statement) {
    return executeAsync(statement, TransactionResult.build());
  }

  /**
   * Execute a multi-keyed query statement.
   *
   * @param statements the statements to execute.
   * @return the result of the query request.
   */
  public Future<QueryResult> executeAsync(final List<KeyQuery> statements) {
    return executeAsync(statements, TransactionResult.build());
  }

  /**
   * Execute a keyed query statement in a given transaction.
   *
   * @param statement the statement to execute.
   * @param txn the transaction to execute the query.
   * @return the result of the query request.
   */
  public Future<QueryResult> executeAsync(final KeyQuery statement, final TransactionResult txn) {
    return executeAsync(ImmutableList.of(statement), txn);
  }

  /**
   * Execute a multi-keyed query statement in a given transaction.
   *
   * @param statements the statements to execute.
   * @param txn the transaction to execute the query.
   * @return the result of the query request.
   */
  public Future<QueryResult> executeAsync(final List<KeyQuery> statements, final TransactionResult txn) {
    final List<com.google.datastore.v1.Key> keys = statements
            .stream().map(s -> s.getKey().getPb(config.getNamespace())).collect(Collectors.toList());
    final LookupRequest.Builder request = LookupRequest.newBuilder().addAllKeys(keys);
    final ByteString transaction = txn.getTransaction();
    if (transaction != null) {
      request.setReadOptions(ReadOptions.newBuilder().setTransaction(transaction));
    }
    final ProtoHttpContent payload = new ProtoHttpContent(request.build());
    return initiateRequest("lookup", payload).compose(getResult(result -> {
      final LookupResponse query = LookupResponse.parseFrom(result);
      return QueryResult.build(query);
    }));
  }

  /**
   * Execute a mutation query statement.
   *
   * @param statement the statement to execute.
   * @return the result of the mutation request.
   */
  public Future<MutationResult> executeAsync(final MutationStatement statement) {
    return executeAsync(statement, TransactionResult.build());
  }

  /**
   * Execute a mutation query statement in a given transaction.
   *
   * @param statement the statement to execute.
   * @param txn the transaction to execute the query.
   * @return the result of the mutation request.
   */
  public Future<MutationResult> executeAsync(final MutationStatement statement, final TransactionResult txn) {
    final List<Mutation> mutations = Optional
      .ofNullable(statement)
      .flatMap(s -> Optional.of(ImmutableList.of(s.getPb(config.getNamespace()))))
      .orElse(ImmutableList.of());

    return executeAsyncMutations(mutations, txn);
  }


  /**
   * Execute a batch mutation query statement.
   *
   * @param batch to execute.
   * @return the result of the mutation request.
   */
  public Future<MutationResult> executeAsync(final Batch batch) {
    return executeAsync(batch, TransactionResult.build());
  }

  /**
   * Execute a batch mutation query statement in a given transaction.
   *
   * @param batch to execute.
   * @param txn the transaction to execute the query.
   * @return the result of the mutation request.
   */
  public Future<MutationResult> executeAsync(final Batch batch, final TransactionResult txn) {
    return executeAsyncMutations(batch.getPb(config.getNamespace()), txn);
  }

  private Future<MutationResult> executeAsyncMutations(final List<Mutation> mutations, final TransactionResult txn) {
    final CommitRequest.Builder request = CommitRequest.newBuilder();
    if (mutations != null) {
      request.addAllMutations(mutations);
    }

    final ByteString transaction = txn.getTransaction();
    if (transaction != null) {
      request.setTransaction(transaction);
    } else {
      request.setMode(CommitRequest.Mode.NON_TRANSACTIONAL);
    }

    final ProtoHttpContent payload = new ProtoHttpContent(request.build());
    return initiateRequest("commit", payload).compose(getResult(result -> {
      final CommitResponse commit = CommitResponse.parseFrom(result);
      return MutationResult.build(commit);
    }));
  }

  /**
   * Execute a query statement.
   *
   * @param statement the statement to execute.
   * @return the result of the query request.
   */
  public Future<QueryResult> executeAsync(final Query statement) {
    return executeAsync(statement, TransactionResult.build());
  }

  /**
   * Execute a query statement in a given transaction.
   *
   * @param statement the statement to execute.
   * @param txn the transaction to execute the query.
   * @return the result of the query request.
   */
  public Future<QueryResult> executeAsync(final Query statement, final TransactionResult txn) {
    final String namespace = config.getNamespace();
    final RunQueryRequest.Builder request = RunQueryRequest.newBuilder()
            .setQuery(statement.getPb(namespace != null ? namespace : ""));
    if (namespace != null) {
      request.setPartitionId(PartitionId.newBuilder().setNamespaceId(namespace));
    }
    final ByteString transaction = txn.getTransaction();
    if (transaction != null) {
      request.setReadOptions(ReadOptions.newBuilder().setTransaction(transaction));
    }
    final ProtoHttpContent payload = new ProtoHttpContent(request.build());
    return initiateRequest("runQuery", payload).compose(getResult(result -> {
      final RunQueryResponse query = RunQueryResponse.parseFrom(result);
      return QueryResult.build(query);
    }));
  }
}
