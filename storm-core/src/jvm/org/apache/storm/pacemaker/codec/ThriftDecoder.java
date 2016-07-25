/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.pacemaker.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.storm.generated.HBMessage;
import org.apache.storm.generated.HBServerMessageType;
import org.apache.storm.messaging.netty.ControlMessage;
import org.apache.storm.messaging.netty.SaslMessageToken;
import org.apache.storm.utils.Utils;

import java.util.List;

public class ThriftDecoder extends ByteToMessageDecoder {

    private static final int INTEGER_SIZE = 4;

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf buf, List<Object> list) throws Exception {
        long available = buf.readableBytes();
        if(available < INTEGER_SIZE) {
            return;
        }

        buf.markReaderIndex();

        int thriftLen = buf.readInt();
        available -= INTEGER_SIZE;

        if(available < thriftLen) {
            // We haven't received the entire object yet, return and wait for more bytes.
            buf.resetReaderIndex();
            return;
        }

        byte serialized[] = new byte[thriftLen];
        buf.readBytes(serialized, 0, thriftLen);
        HBMessage m = (HBMessage)Utils.thriftDeserialize(HBMessage.class, serialized);

        if(m.get_type() == HBServerMessageType.CONTROL_MESSAGE) {
            ControlMessage cm = ControlMessage.read(m.get_data().get_message_blob());
            list.add(cm);
        }
        else if(m.get_type() == HBServerMessageType.SASL_MESSAGE_TOKEN) {
            SaslMessageToken sm = SaslMessageToken.read(m.get_data().get_message_blob());
            list.add(sm);
        }
        else {
            list.add(m);
        }
    }

}
