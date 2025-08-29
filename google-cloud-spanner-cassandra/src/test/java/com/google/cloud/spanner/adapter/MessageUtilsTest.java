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

import static com.google.common.truth.Truth.assertThat;

import com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodec;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Supported;
import com.datastax.oss.protocol.internal.response.error.Unprepared;
import com.google.cloud.spanner.adapter.util.MessageUtils;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.junit.Test;

public class MessageUtilsTest {

  private static final FrameCodec<ByteBuf> clientFrameCodec =
      FrameCodec.defaultClient(
          new ByteBufPrimitiveCodec(ByteBufAllocator.DEFAULT), Compressor.none());

  @Test
  public void supportedResponse_createsCorrectFrame() {
    int streamId = 123;

    ByteString responseByteString = MessageUtils.supportedResponse(streamId);
    Frame decodedFrame = decodeFrame(responseByteString);

    assertThat(decodedFrame.streamId).isEqualTo(streamId);
    assertThat(decodedFrame.message).isInstanceOf(Supported.class);

    Supported supported = (Supported) decodedFrame.message;
    assertThat(supported.options).containsKey("CQL_VERSION");
    assertThat(supported.options.get("CQL_VERSION")).containsExactly("3.0.0");
    assertThat(supported.options).containsKey("COMPRESSION");
    assertThat(supported.options.get("COMPRESSION")).isEmpty();
  }

  @Test
  public void unpreparedResponse_createsCorrectFrame() {
    int streamId = 456;
    byte[] queryId = new byte[] {1, 2, 3, 4};

    ByteString responseByteString = MessageUtils.unpreparedResponse(streamId, queryId);
    Frame decodedFrame = decodeFrame(responseByteString);

    assertThat(decodedFrame.streamId).isEqualTo(streamId);
    assertThat(decodedFrame.message).isInstanceOf(Unprepared.class);

    Unprepared unprepared = (Unprepared) decodedFrame.message;
    assertThat(unprepared.code).isEqualTo(ProtocolConstants.ErrorCode.UNPREPARED);
    assertThat(unprepared.id).isEqualTo(queryId);
  }

  @Test
  public void serverErrorResponse_createsCorrectFrame() {
    int streamId = 789;
    String errorMessage = "This is a test error";

    ByteString responseByteString = MessageUtils.serverErrorResponse(streamId, errorMessage);
    Frame decodedFrame = decodeFrame(responseByteString);

    assertThat(decodedFrame.streamId).isEqualTo(streamId);
    assertThat(decodedFrame.message).isInstanceOf(Error.class);

    Error error = (Error) decodedFrame.message;
    assertThat(error.code).isEqualTo(ProtocolConstants.ErrorCode.SERVER_ERROR);
    assertThat(error.message).isEqualTo(errorMessage);
  }

  private Frame decodeFrame(ByteString byteString) {
    ByteBuf responseBuf = Unpooled.wrappedBuffer(byteString.asReadOnlyByteBuffer());
    try {
      return clientFrameCodec.decode(responseBuf);
    } finally {
      responseBuf.release();
    }
  }
}
