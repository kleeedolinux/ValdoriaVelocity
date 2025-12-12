/*
 * Copyright (C) 2025 Velocity Contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.velocitypowered.proxy.raknet;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monitors channel writability to detect congestion and trigger packet prioritization/shedding.
 * This handler ensures that when the TCP window is full (backpressure), we actively drop
 * non-essential packets to prevent buffer bloat and maintain low latency.
 */
public class RakNetFlowControlHandler extends ChannelDuplexHandler {

  private static final Logger logger = LoggerFactory.getLogger(RakNetFlowControlHandler.class);

  private final int highWaterMark = 64 * 1024; // 64KB
  private final int lowWaterMark = 32 * 1024;  // 32KB

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    ctx.channel().config().setWriteBufferHighWaterMark(highWaterMark);
    ctx.channel().config().setWriteBufferLowWaterMark(lowWaterMark);
    super.channelActive(ctx);
  }

  @Override
  public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
    if (!ctx.channel().isWritable()) {
      if (logger.isDebugEnabled()) {
        logger.debug("Channel {} is congested. Triggering packet shedding.", ctx.channel());
      }

      PacketPrioritizationHandler prioritizationHandler = ctx.pipeline().get(PacketPrioritizationHandler.class);
      if (prioritizationHandler != null) {
        prioritizationHandler.dropLowPriorityPackets();
      }
    }
    super.channelWritabilityChanged(ctx);
  }
}
