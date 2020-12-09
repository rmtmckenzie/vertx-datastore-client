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
import com.google.common.collect.ImmutableList;
import com.google.datastore.v1.*;
import com.google.protobuf.ByteString;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.*;
import io.vertx.core.http.impl.HttpUtils;
import io.vertx.core.net.JksOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
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

    public static final String VERSION = "1.0.0";
    public static final String USER_AGENT = "Datastore-Java-Client/" + VERSION + " (gzip)";
    private static final Logger log = LoggerFactory.getLogger(Datastore.class);
    private final DatastoreConfig config;
    private final HttpClient httpClient;
    private final String prefixUri;
    private final Vertx vertx;

    private final Long periodicTimerId;
    private volatile String accessToken;
    private int curNumRequests = 0;


    private Datastore(final Vertx vertx, final DatastoreConfig config) throws URISyntaxException, IOException {
        this.config = config;
        this.vertx = vertx;

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
                .setIdleTimeout(config.getRequestTimeout() / 1000)
                .setMaxPoolSize(config.getMaxConnections())
                .setHttp2MaxPoolSize(config.getMaxConnections())
                .setDefaultHost(uri.getHost())
                .setDefaultPort(port)
                .setTryUseCompression(true)
                .setReceiveBufferSize(8192)
                .setSsl(ssl)
                .setProtocolVersion(config.getUseHttp2() ? HttpVersion.HTTP_2 : HttpClientOptions.DEFAULT_PROTOCOL_VERSION)
                .setUseAlpn(config.getUseHttp2() && ssl || HttpClientOptions.DEFAULT_USE_ALPN)
                .setTrustStoreOptions(new JksOptions()
                        .setPath(System.getProperty("java.home") + "/lib/security/cacerts"));

        //TODO: retry is missing, eg:
        //final AsyncHttpClientConfig httpConfig = new AsyncHttpClientConfig.Builder()
        //    .setMaxRequestRetry(config.getRequestRetry())

        httpClient = vertx.createHttpClient(httpClientOptions);
        prefixUri = String.format("/%s/projects/%s:", config.getVersion(), config.getProject());

        if (config.getCredential() != null) {
            // block while retrieving an access token for the first time
            Credential credential = config.getCredential();
            String accessToken = credential.getAccessToken();

            if (accessToken == null) {
                credential.refreshToken();
                accessToken = credential.getAccessToken();
            }

            this.accessToken = accessToken;
            this.periodicTimerId = vertx.setPeriodic(10000, (id) -> this.refreshAccessToken());

        } else {
            this.periodicTimerId = null;
        }
    }

    public static Datastore create(final Vertx vertx, final DatastoreConfig config) throws URISyntaxException, IOException {
        return new Datastore(vertx, config);
    }

    private static boolean isSuccessful(final int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }

    @Override
    public void close() {
        if (this.periodicTimerId != null) {
            vertx.cancelTimer(this.periodicTimerId);
        }
        httpClient.close();
    }

    private Future<String> refreshAccessToken() {
        final Credential credential = config.getCredential();
        final Long expiresIn = credential.getExpiresInSeconds();

        // don't need to check accessToken because it will always have
        // been tried once before refresh is called.
        if (expiresIn != null && expiresIn <= 60) {
            // definitely need to refresh
            return vertx.<String>executeBlocking(blocker -> {
                try {
                    credential.refreshToken();
                    blocker.complete(credential.getAccessToken());
                } catch (IOException e) {
                    blocker.fail(e);
                }
            }).map(result -> {
                if (result != null) {
                    this.accessToken = result;
                }
                return this.accessToken;
            });
        } else {
            return Future.succeededFuture(this.accessToken);
        }
    }

    private <R> Function<HttpClientResponse, Future<R>> getResult(ThrowingFunction<InputStream, R> func) {
        return (httpClientResponse -> {
            return  httpClientResponse.body().compose(body -> {
              if (!isSuccessful(httpClientResponse.statusCode())) {
                return Future.failedFuture(new DatastoreCountException(curNumRequests, httpClientResponse.statusCode(), body.toString()));
              }
              try {
                R result = func.apply(streamBufferResponse(body, httpClientResponse.headers()));
                return Future.succeededFuture(result);
              } catch (Exception e) {
                return Future.failedFuture(new DatastoreCountException(curNumRequests, e));
              }
            });
        });
    }

    private Future<HttpClientResponse> initiateRequest(final String method, final ProtoHttpContent payload) {
        Buffer buffer;
        try {
            buffer = Buffer.buffer((int) payload.getLength());
            payload.writeTo(new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                    buffer.appendByte((byte) (b & 0xff));
                }
            });
        } catch (IOException e) {
            return Future.failedFuture(e);
        }

        curNumRequests += 1;
        try {
          return httpClient.request(HttpMethod.POST, prefixUri + method).compose(request -> {
            return request.putHeader("Authorization", "Bearer " + accessToken)
                    .putHeader("Content-Type", "application/x-protobuf")
                    .putHeader("User-Agent", USER_AGENT)
                    .putHeader("Accept-Encoding", "gzip")
                    .end(buffer).compose(v -> request.response());

          }).onSuccess(result -> {
            curNumRequests -= 1;
          }).recover(cause -> {
            curNumRequests -= 1;
            if (cause instanceof SocketException) {
              if (cause.getMessage().contains("Network is unreachable: datastore.googleapis.com")) {
                log.warn("This is most likely a DNS issue - if you are on compute engine set " +
                        "java.net.preferIPv4Stack=true, see https://github.com/netty/netty/pull/5659. " +
                        " Caused by compute engine not supporting ipv6");
              }
            }

            return Future.failedFuture(new DatastoreCountException(curNumRequests, cause));
          });
        } catch (Throwable t) {
          curNumRequests -=1;
          return Future.failedFuture(t);
        }

    }

    private InputStream streamBufferResponse(final Buffer responseBody, MultiMap headers) throws IOException {
        final InputStream bufferStream = new InputStream() {
            private int curPos = 0;
            private int len = responseBody.length();

            @Override
            public int read() throws IOException {
                return curPos < len ? ((int) (responseBody.getByte(curPos++)) & 0xff) : -1;
            }
        };
        final boolean compressed = "gzip".equals(headers.get("Content-Encoding"));
        return compressed ? new GZIPInputStream(bufferStream) : bufferStream;
    }

    /**
     * Get the number of current concurrent requests.
     * <p>
     * Each time a request is sent, the underlying counter is incremented,
     * and then when it completes it is decremented.
     *
     * @return the number of current requests
     */
    public int getCurNumRequests() {
        return curNumRequests;
    }

    /**
     * Start a new transaction.
     * <p>
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
     * <p>
     * You normally rollback a transaction in the event of a Datastore failure.
     *
     * @param txn the transaction.
     * @return the result of the rollback request.
     */
    public Future<RollbackResult> rollbackAsync(final TransactionResult txn) {
        final ByteString transaction = txn.getTransaction();
        if (transaction == null) {
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
     * <p>
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
     * @param txn       the transaction to execute the query.
     * @return the result of the query request.
     */
    public Future<QueryResult> executeAsync(final KeyQuery statement, final TransactionResult txn) {
        return executeAsync(ImmutableList.of(statement), txn);
    }

    /**
     * Execute a multi-keyed query statement in a given transaction.
     *
     * @param statements the statements to execute.
     * @param txn        the transaction to execute the query.
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
     * @param txn       the transaction to execute the query.
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
     * @param txn   the transaction to execute the query.
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
     * @param txn       the transaction to execute the query.
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

    public interface ThrowingFunction<T, R> {
        R apply(T t) throws Exception;
    }
}
