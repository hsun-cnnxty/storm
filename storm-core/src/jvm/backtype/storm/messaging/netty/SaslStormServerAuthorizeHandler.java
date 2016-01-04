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
package backtype.storm.messaging.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authorize or deny client requests based on existence and completeness of
 * client's SASL authentication.
 */
public class SaslStormServerAuthorizeHandler extends ChannelInboundHandlerAdapter {

	private static final Logger LOG = LoggerFactory.getLogger(SaslStormServerAuthorizeHandler.class);

	/**
	 * Constructor.
	 */
	public SaslStormServerAuthorizeHandler() {
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg == null)
			return;

		LOG.debug("messageReceived: Checking whether the client is authorized to send messages to the server ");

		LOG.debug("context is: " + ctx);
		LOG.debug("channel is: " + ctx.channel());
		LOG.debug("context attributes are: " + ctx.channel().attr(SaslNettyServerState.SASL_NETTY_SERVER));

		// Authorize: client is allowed to doRequest() if and only if the client
		// has successfully authenticated with this server.
		SaslNettyServer saslNettyServer = ctx.channel().attr(SaslNettyServerState.SASL_NETTY_SERVER).get();

		if (saslNettyServer == null) {
			LOG.warn("messageReceived: This client is *NOT* authorized to perform "
					+ "this action since there's no saslNettyServer to "
					+ "authenticate the client: "
					+ "refusing to perform requested action: " + msg);
			return;
		}

		if (!saslNettyServer.isComplete()) {
			LOG.warn("messageReceived: This client is *NOT* authorized to perform "
					+ "this action because SASL authentication did not complete: "
					+ "refusing to perform requested action: " + msg);
			// Return now *WITHOUT* sending upstream here, since client
			// not authorized.
			return;
		}

		LOG.debug("messageReceived: authenticated client: "
				+ saslNettyServer.getUserName()
				+ " is authorized to do request " + "on server.");

		// We call fireMessageReceived since the client is allowed to perform
		// this request. The client's request will now proceed to the next
		// pipeline component.
		ctx.fireChannelRead(msg);
	}
}