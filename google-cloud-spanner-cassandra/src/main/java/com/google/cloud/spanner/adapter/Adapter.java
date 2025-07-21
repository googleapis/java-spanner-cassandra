/*
Copyright 2025 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.google.cloud.spanner.adapter;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodec;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.request.Batch;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.Query;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.GaxProperties;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.google.spanner.adapter.v1.AdaptMessageRequest;
import com.google.spanner.adapter.v1.AdapterClient;
import com.google.spanner.adapter.v1.AdapterSettings;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import static com.google.cloud.spanner.adapter.util.ErrorMessageUtils.result;
import static com.google.cloud.spanner.adapter.util.ErrorMessageUtils.unpreparedResponse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.nio.charset.StandardCharsets;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages client connections, acting as an intermediary for communication with Spanner. */
@NotThreadSafe
final class Adapter {
  private static final Logger LOG = LoggerFactory.getLogger(Adapter.class);
  private static final String RESOURCE_PREFIX_HEADER_KEY = "google-cloud-resource-prefix";
  private static final long MAX_GLOBAL_STATE_SIZE = (long) (1e8 / 256); // ~100 MB
  private static final int DEFAULT_CONNECTION_BACKLOG = 50;
  private static final String ENV_VAR_GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS =
      "GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS";
  private static final String USER_AGENT_KEY = "user-agent";
  private static final String CLIENT_LIBRARY_LANGUAGE = "java-spanner-cassandra";
  private static final String CLIENT_VERSION = "0.3.0"; // {x-release-please-version}
  public static final String DEFAULT_USER_AGENT =
      CLIENT_LIBRARY_LANGUAGE
          + "/v"
          + CLIENT_VERSION
          + GaxProperties.getLibraryVersion(Adapter.class);
  private static final ImmutableSet<String> SCOPES =
      ImmutableSet.of(
          "https://www.googleapis.com/auth/cloud-platform",
          "https://www.googleapis.com/auth/spanner.data");

  private AdapterClientWrapper adapterClientWrapper;
  private Selector socketSelector;
  private ServerSocketChannel serverSocketChannel;
  private Thread tcpServingThread;
  private boolean started = false;
  private AdapterOptions options;
   private ExecutorService executor;

  private static final String PREPARED_QUERY_ID_ATTACHMENT_PREFIX = "pqid/";
  private static final char WRITE_ACTION_QUERY_ID_PREFIX = 'W';
  private static final String ROUTE_TO_LEADER_HEADER_KEY = "x-goog-spanner-route-to-leader";

  private final GrpcCallContext defaultContext;
  private final GrpcCallContext defaultContextWithLAR;
  private static final Map<String, List<String>> ROUTE_TO_LEADER_HEADER_MAP =
      ImmutableMap.of(ROUTE_TO_LEADER_HEADER_KEY, Collections.singletonList("true"));
  private static final int defaultStreamId = -1;


  private static final ByteBufAllocator byteBufAllocator = ByteBufAllocator.DEFAULT;
  private static final FrameCodec<ByteBuf> serverFrameCodec =
      FrameCodec.defaultServer(new ByteBufPrimitiveCodec(byteBufAllocator), Compressor.none());
  //private final ByteBuffer headerBuffer = ByteBuffer.allocate(9);
  //private ByteBuffer bodyBuffer; // Will be allocated dynamically

                AdapterClient adapterClient;
               AttachmentsCache attachmentsCache;
               SessionManager sessionManager;
  

  /**
   * Constructor for the Adapter class, specifying a specific address to bind to.
   *
   * @param options options for init.
   */
  Adapter(AdapterOptions options) {
    this.options = options;
    this.defaultContext = GrpcCallContext.createDefault();
    this.defaultContextWithLAR =
        GrpcCallContext.createDefault().withExtraHeaders(ROUTE_TO_LEADER_HEADER_MAP);
  }

  /** Starts the adapter, initializing the local TCP server and handling client connections. */
  void start() {
    if (started) {
      return;
    }

    try {
      Credentials credentials = options.getCredentials();
      if (credentials == null) {
        credentials = GoogleCredentials.getApplicationDefault();
      }
      final CredentialsProvider credentialsProvider = setUpCredentialsProvider(credentials);

      InstantiatingGrpcChannelProvider.Builder channelProviderBuilder =
          AdapterSettings.defaultGrpcTransportProviderBuilder();

      channelProviderBuilder
          .setAllowNonDefaultServiceAccount(true)
          .setChannelPoolSettings(
              ChannelPoolSettings.staticallySized(options.getNumGrpcChannels()));

      if (isEnableDirectPathXdsEnv()) {
        channelProviderBuilder.setAttemptDirectPath(true);
        // This will let the credentials try to fetch a hard-bound access token if the runtime
        // environment supports it.
        channelProviderBuilder.setAllowHardBoundTokenTypes(
            Collections.singletonList(InstantiatingGrpcChannelProvider.HardBoundTokenTypes.ALTS));
        channelProviderBuilder.setAttemptDirectPathXds();
      }
      final HeaderProvider headerProvider =
          FixedHeaderProvider.create(
              RESOURCE_PREFIX_HEADER_KEY,
              options.getDatabaseUri(),
              USER_AGENT_KEY,
              DEFAULT_USER_AGENT);
      AdapterSettings settings =
          AdapterSettings.newBuilder()
              .setEndpoint(options.getSpannerEndpoint())
              .setTransportChannelProvider(
                  MoreObjects.firstNonNull(
                      options.getChannelProvider(), channelProviderBuilder.build()))
              .setCredentialsProvider(credentialsProvider)
              .setHeaderProvider(headerProvider)
              .build();



       adapterClient = AdapterClient.create(settings);

       attachmentsCache = new AttachmentsCache(MAX_GLOBAL_STATE_SIZE);
       sessionManager = new SessionManager(adapterClient, options.getDatabaseUri());

      // Create initial session to verify database existence
      sessionManager.getSession();


      // Start listening on the specified host and port.
      InetSocketAddress isa = new InetSocketAddress(options.getInetAddress(), options.getTcpPort());
      socketSelector = SelectorProvider.provider().openSelector();
      serverSocketChannel = ServerSocketChannel.open();
      serverSocketChannel.configureBlocking(false);
      serverSocketChannel.socket().bind(isa);
      serverSocketChannel.register(socketSelector, SelectionKey.OP_ACCEPT);
      
      executor = Executors.newFixedThreadPool(10);

      // Start accepting client connections.
      executor.execute(this::runSelectLoop);


    LOG.info("Local TCP server started on {}:{}", options.getInetAddress(), options.getTcpPort());

    started = true;
    LOG.info("Adapter started for database '{}'.", options.getDatabaseUri());

    } catch (IOException | RuntimeException e) {
      LOG.error("Failed to start the adapter.", e);
      throw new AdapterStartException(e);
    }
  }


  public void runSelectLoop() {
    LOG.info("runSelectLoop");
    while (started) {
     // LOG.info("running runSelectLoop");
      // wait for events
			int readyCount = 0;
      try {
        readyCount = socketSelector.select();
        if (readyCount == 0) {
          continue;
        }

      Iterator<SelectionKey> selectionKeyIterator = socketSelector.selectedKeys().iterator();
      while (selectionKeyIterator.hasNext()) {
        SelectionKey key = selectionKeyIterator.next();
        if (!key.isValid()) {
          selectionKeyIterator.remove();
          continue;
        }

        selectionKeyIterator.remove();

        if (key.isAcceptable()) { // Accept client connections
					doAccept(key);
				} else if (key.isReadable()) { // Read from client
					doRead(key);
				} else if (key.isWritable()) {
					// write data to client...
				}
      }
            } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  private void doAccept(SelectionKey key) throws IOException {
    //LOG.info("doAccept");
    ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
    SocketChannel socketChannel = serverSocketChannel.accept();
    socketChannel.configureBlocking(false);
    socketChannel.socket().setTcpNoDelay(true);
    socketChannel.register(socketSelector, SelectionKey.OP_READ);
  }

    private void doRead(SelectionKey key) throws IOException {
      SocketChannel channel = (SocketChannel) key.channel();
      byte[] responseToWrite;
    int streamId = -1;
     ByteBuffer headerBuffer = ByteBuffer.allocate(9);
     try {
      channel.read(headerBuffer);
      headerBuffer.flip();
      int bodyLength = ByteBuffer.wrap(headerBuffer.array(), 5, 4).getInt();
      ByteBuffer bodyBuffer = ByteBuffer.allocate(bodyLength);
      channel.read(bodyBuffer);

      bodyBuffer.flip();
      int a = headerBuffer.remaining();
      int b = bodyBuffer.remaining();
      //LOG.info("doRead: a: {}, b: {}", a, b);

      // 1. Combine header and body into a single payload array.
      byte[] payload = new byte[a + bodyLength];
      headerBuffer.get(payload, 0, a);
      bodyBuffer.get(payload, a, bodyLength);
      //LOG.info("payload length: {}", payload.length);

      if (payload.length >  0) {
        // 2. Prepare the payload (same logic as before).
        PreparePayloadResult prepareResult = preparePayload(payload);
        streamId = prepareResult.getStreamId();
        Optional<byte[]> response = prepareResult.getAttachmentErrorResponse();

        // 3. If needed, send the gRPC request (same logic as before).
        if (!response.isPresent()) {
          AdaptMessageResponseObserver responseObserver =
          new AdaptMessageResponseObserver(streamId, channel, attachmentsCache);
          
          AdaptMessageRequest request =
          AdaptMessageRequest.newBuilder()
              .setName(sessionManager.getSession().getName())
              .setProtocol("cassandra")
              .putAllAttachments(prepareResult.getAttachments())
              .setPayload(ByteString.copyFrom(payload))
              .build();

        adapterClient
            .adaptMessageCallable()
            .call(request, responseObserver);
        } else {
          channel.write(ByteBuffer.wrap(result(streamId)));
        }
      }
     } catch (IOException e) {
      //LOG.info("error: " + e);
    } 
    }

    private int load8Unsigned(byte[] bytes, int offset) {
    // Directly access the byte and apply the 0xFF mask.
    // This promotes the byte to an int and ensures the value is treated as unsigned.
    return bytes[offset] & 0xFF;
  }

      private PreparePayloadResult preparePayload(byte[] payload) {
    ByteBuf payloadBuf = Unpooled.wrappedBuffer(payload);
    Frame frame = serverFrameCodec.decode(payloadBuf);
    payloadBuf.release();

    Map<String, String> attachments = new HashMap<>();
    if (frame.message instanceof Execute) {
      return prepareExecuteMessage((Execute) frame.message, frame.streamId, attachments);
    } else if (frame.message instanceof Batch) {
      return prepareBatchMessage((Batch) frame.message, frame.streamId, attachments);
    } else if (frame.message instanceof Query) {
      return prepareQueryMessage((Query) frame.message, frame.streamId, attachments);
    } else {
      return new PreparePayloadResult(defaultContext, frame.streamId);
    }
  }

  private PreparePayloadResult prepareExecuteMessage(
      Execute message, int streamId, Map<String, String> attachments) {
    ApiCallContext context;
    if (message.queryId != null
        && message.queryId.length > 0
        && message.queryId[0] == WRITE_ACTION_QUERY_ID_PREFIX) {
      context = defaultContextWithLAR;
    } else {
      context = defaultContext;
    }
    Optional<byte[]> errorResponse =
        prepareAttachmentForQueryId(streamId, attachments, message.queryId);
    return new PreparePayloadResult(context, streamId, attachments, errorResponse);
  }

  private PreparePayloadResult prepareBatchMessage(
      Batch message, int streamId, Map<String, String> attachments) {
    Optional<byte[]> attachmentErrorResponse = Optional.empty();
    for (Object obj : message.queriesOrIds) {
      if (obj instanceof byte[]) {
        Optional<byte[]> errorResponse =
            prepareAttachmentForQueryId(streamId, attachments, (byte[]) obj);
        if (errorResponse.isPresent()) {
          attachmentErrorResponse = errorResponse;
          break;
        }
      }
    }
    return new PreparePayloadResult(
        defaultContextWithLAR, streamId, attachments, attachmentErrorResponse);
  }

  private PreparePayloadResult prepareQueryMessage(
      Query message, int streamId, Map<String, String> attachments) {
    ApiCallContext context = defaultContext;
    return new PreparePayloadResult(context, streamId, attachments);
  }

  private Optional<byte[]> prepareAttachmentForQueryId(
      int streamId, Map<String, String> attachments, byte[] queryId) {
    return Optional.of(result(streamId));
  }

  private static String constructKey(byte[] queryId) {
    return PREPARED_QUERY_ID_ATTACHMENT_PREFIX + new String(queryId, StandardCharsets.UTF_8);
  }

  /**
   * Stops the adapter, shutting down the executor, closing the server socket, and closing the
   * adapter client.
   *
   * @throws IOException If an I/O error occurs while closing the server socket.
   */
  void stop() throws IOException {
  }

  private static CredentialsProvider setUpCredentialsProvider(final Credentials credentials) {
    Credentials scopedCredentials = getScopedCredentials(credentials);
    if (scopedCredentials != null && scopedCredentials != NoCredentials.getInstance()) {
      return FixedCredentialsProvider.create(scopedCredentials);
    }
    return NoCredentialsProvider.create();
  }

  private static Credentials getScopedCredentials(final Credentials credentials) {
    Credentials credentialsToReturn = credentials;
    if (credentials instanceof GoogleCredentials
        && ((GoogleCredentials) credentials).createScopedRequired()) {
      credentialsToReturn = ((GoogleCredentials) credentials).createScoped(SCOPES);
    }
    return credentialsToReturn;
  }

  private static boolean isEnableDirectPathXdsEnv() {
    return Boolean.parseBoolean(System.getenv(ENV_VAR_GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS));
  }

  private static final class AdapterStartException extends RuntimeException {
    public AdapterStartException(Throwable cause) {
      super("Failed to start the adapter.", cause);
    }
  }
}
