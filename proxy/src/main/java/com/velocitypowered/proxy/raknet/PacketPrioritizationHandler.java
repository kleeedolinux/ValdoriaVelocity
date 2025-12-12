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

import com.velocitypowered.proxy.protocol.MinecraftPacket;
import com.velocitypowered.proxy.protocol.packet.AvailableCommandsPacket;
import com.velocitypowered.proxy.protocol.packet.BossBarPacket;
import com.velocitypowered.proxy.protocol.packet.BundleDelimiterPacket;
import com.velocitypowered.proxy.protocol.packet.ClientSettingsPacket;
import com.velocitypowered.proxy.protocol.packet.ClientboundCookieRequestPacket;
import com.velocitypowered.proxy.protocol.packet.ClientboundSoundEntityPacket;
import com.velocitypowered.proxy.protocol.packet.ClientboundStopSoundPacket;
import com.velocitypowered.proxy.protocol.packet.ClientboundStoreCookiePacket;
import com.velocitypowered.proxy.protocol.packet.DialogClearPacket;
import com.velocitypowered.proxy.protocol.packet.DialogShowPacket;
import com.velocitypowered.proxy.protocol.packet.DisconnectPacket;
import com.velocitypowered.proxy.protocol.packet.EncryptionRequestPacket;
import com.velocitypowered.proxy.protocol.packet.EncryptionResponsePacket;
import com.velocitypowered.proxy.protocol.packet.HandshakePacket;
import com.velocitypowered.proxy.protocol.packet.HeaderAndFooterPacket;
import com.velocitypowered.proxy.protocol.packet.JoinGamePacket;
import com.velocitypowered.proxy.protocol.packet.KeepAlivePacket;
import com.velocitypowered.proxy.protocol.packet.LegacyDisconnect;
import com.velocitypowered.proxy.protocol.packet.LegacyHandshakePacket;
import com.velocitypowered.proxy.protocol.packet.LegacyPingPacket;
import com.velocitypowered.proxy.protocol.packet.LegacyPlayerListItemPacket;
import com.velocitypowered.proxy.protocol.packet.LoginAcknowledgedPacket;
import com.velocitypowered.proxy.protocol.packet.LoginPluginMessagePacket;
import com.velocitypowered.proxy.protocol.packet.LoginPluginResponsePacket;
import com.velocitypowered.proxy.protocol.packet.PingIdentifyPacket;
import com.velocitypowered.proxy.protocol.packet.PluginMessagePacket;
import com.velocitypowered.proxy.protocol.packet.RemovePlayerInfoPacket;
import com.velocitypowered.proxy.protocol.packet.RemoveResourcePackPacket;
import com.velocitypowered.proxy.protocol.packet.ResourcePackRequestPacket;
import com.velocitypowered.proxy.protocol.packet.ResourcePackResponsePacket;
import com.velocitypowered.proxy.protocol.packet.RespawnPacket;
import com.velocitypowered.proxy.protocol.packet.ServerDataPacket;
import com.velocitypowered.proxy.protocol.packet.ServerLoginPacket;
import com.velocitypowered.proxy.protocol.packet.ServerLoginSuccessPacket;
import com.velocitypowered.proxy.protocol.packet.ServerboundCookieResponsePacket;
import com.velocitypowered.proxy.protocol.packet.ServerboundCustomClickActionPacket;
import com.velocitypowered.proxy.protocol.packet.SetCompressionPacket;
import com.velocitypowered.proxy.protocol.packet.StatusPingPacket;
import com.velocitypowered.proxy.protocol.packet.StatusRequestPacket;
import com.velocitypowered.proxy.protocol.packet.StatusResponsePacket;
import com.velocitypowered.proxy.protocol.packet.TabCompleteRequestPacket;
import com.velocitypowered.proxy.protocol.packet.TabCompleteResponsePacket;
import com.velocitypowered.proxy.protocol.packet.TransferPacket;
import com.velocitypowered.proxy.protocol.packet.UpsertPlayerInfoPacket;
import com.velocitypowered.proxy.protocol.packet.chat.ChatAcknowledgementPacket;
import com.velocitypowered.proxy.protocol.packet.chat.PlayerChatCompletionPacket;
import com.velocitypowered.proxy.protocol.packet.chat.SystemChatPacket;
import com.velocitypowered.proxy.protocol.packet.chat.keyed.KeyedPlayerChatPacket;
import com.velocitypowered.proxy.protocol.packet.chat.keyed.KeyedPlayerCommandPacket;
import com.velocitypowered.proxy.protocol.packet.chat.legacy.LegacyChatPacket;
import com.velocitypowered.proxy.protocol.packet.chat.session.SessionPlayerChatPacket;
import com.velocitypowered.proxy.protocol.packet.chat.session.SessionPlayerCommandPacket;
import com.velocitypowered.proxy.protocol.packet.chat.session.UnsignedPlayerCommandPacket;
import com.velocitypowered.proxy.protocol.packet.config.ActiveFeaturesPacket;
import com.velocitypowered.proxy.protocol.packet.config.ClientboundCustomReportDetailsPacket;
import com.velocitypowered.proxy.protocol.packet.config.ClientboundServerLinksPacket;
import com.velocitypowered.proxy.protocol.packet.config.CodeOfConductAcceptPacket;
import com.velocitypowered.proxy.protocol.packet.config.CodeOfConductPacket;
import com.velocitypowered.proxy.protocol.packet.config.FinishedUpdatePacket;
import com.velocitypowered.proxy.protocol.packet.config.KnownPacksPacket;
import com.velocitypowered.proxy.protocol.packet.config.RegistrySyncPacket;
import com.velocitypowered.proxy.protocol.packet.config.StartUpdatePacket;
import com.velocitypowered.proxy.protocol.packet.config.TagsUpdatePacket;
import com.velocitypowered.proxy.protocol.packet.title.GenericTitlePacket;
import com.velocitypowered.proxy.protocol.packet.title.LegacyTitlePacket;
import com.velocitypowered.proxy.protocol.packet.title.TitleActionbarPacket;
import com.velocitypowered.proxy.protocol.packet.title.TitleClearPacket;
import com.velocitypowered.proxy.protocol.packet.title.TitleSubtitlePacket;
import com.velocitypowered.proxy.protocol.packet.title.TitleTextPacket;
import com.velocitypowered.proxy.protocol.packet.title.TitleTimesPacket;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Handles packet prioritization for RakNet connections.
 */
public class PacketPrioritizationHandler extends ChannelOutboundHandlerAdapter {

  private static class PendingWrite {
    final Object msg;
    final ChannelPromise promise;
    final int priority;
    final long order;

    PendingWrite(Object msg, ChannelPromise promise, int priority, long order) {
      this.msg = msg;
      this.promise = promise;
      this.priority = priority;
      this.order = order;
    }
  }

  private final AtomicLong orderCounter = new AtomicLong();
  private final Queue<PendingWrite> queue = new PriorityQueue<>(
      Comparator.<PendingWrite>comparingInt(o -> o.priority)
          .thenComparingLong(o -> o.order)
  );

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    int priority = 10; // Default (Medium)

    if (msg instanceof MinecraftPacket) {
      if (msg instanceof KeepAlivePacket || msg instanceof DisconnectPacket
          || msg instanceof LegacyDisconnect) {
        priority = 0; // Critical
      } else if (msg instanceof HandshakePacket || msg instanceof LegacyHandshakePacket
          || msg instanceof EncryptionRequestPacket || msg instanceof EncryptionResponsePacket
          || msg instanceof LoginPluginResponsePacket || msg instanceof LoginPluginMessagePacket
          || msg instanceof LoginAcknowledgedPacket || msg instanceof ServerLoginPacket
          || msg instanceof ServerLoginSuccessPacket || msg instanceof SetCompressionPacket
          || msg instanceof LegacyPingPacket || msg instanceof PingIdentifyPacket
          || msg instanceof StatusPingPacket || msg instanceof StatusRequestPacket
          || msg instanceof StatusResponsePacket) {
        priority = 1; // High - Connection Setup
      } else if (msg instanceof ClientSettingsPacket || msg instanceof ResourcePackResponsePacket
          || msg instanceof ResourcePackRequestPacket || msg instanceof RemoveResourcePackPacket
          || msg instanceof ClientboundCookieRequestPacket
          || msg instanceof ClientboundStoreCookiePacket
          || msg instanceof ServerboundCookieResponsePacket || msg instanceof ServerDataPacket
          || msg instanceof TransferPacket || msg instanceof BundleDelimiterPacket
          || msg instanceof ActiveFeaturesPacket || msg instanceof ClientboundServerLinksPacket
          || msg instanceof KnownPacksPacket || msg instanceof RegistrySyncPacket
          || msg instanceof StartUpdatePacket || msg instanceof FinishedUpdatePacket
          || msg instanceof TagsUpdatePacket || msg instanceof JoinGamePacket
          || msg instanceof RespawnPacket) {
        priority = 2; // High - State & World
      } else if (msg instanceof TabCompleteRequestPacket || msg instanceof TabCompleteResponsePacket
          || msg instanceof PluginMessagePacket
          || msg instanceof ServerboundCustomClickActionPacket
          || msg instanceof AvailableCommandsPacket || msg instanceof DialogShowPacket
          || msg instanceof DialogClearPacket || msg instanceof PlayerChatCompletionPacket
          || msg instanceof ChatAcknowledgementPacket || msg instanceof SystemChatPacket
          || msg instanceof ClientboundCustomReportDetailsPacket
          || msg instanceof CodeOfConductAcceptPacket
          || msg instanceof CodeOfConductPacket
          || msg instanceof LegacyChatPacket
          || msg instanceof KeyedPlayerChatPacket || msg instanceof KeyedPlayerCommandPacket
          || msg instanceof SessionPlayerChatPacket || msg instanceof SessionPlayerCommandPacket
          || msg instanceof UnsignedPlayerCommandPacket) {
        priority = 5; // Medium-High - Interaction & Chat
      } else if (msg instanceof UpsertPlayerInfoPacket || msg instanceof RemovePlayerInfoPacket
          || msg instanceof LegacyPlayerListItemPacket) {
        priority = 8; // Medium - Player Info
      } else if (msg instanceof BossBarPacket || msg instanceof HeaderAndFooterPacket
          || msg instanceof ClientboundSoundEntityPacket || msg instanceof ClientboundStopSoundPacket
          || msg instanceof GenericTitlePacket || msg instanceof LegacyTitlePacket
          || msg instanceof TitleActionbarPacket || msg instanceof TitleClearPacket
          || msg instanceof TitleSubtitlePacket || msg instanceof TitleTextPacket
          || msg instanceof TitleTimesPacket) {
        priority = 20; // Low - Cosmetic
      }
    } else if (msg instanceof io.netty.buffer.ByteBuf) {
      io.netty.buffer.ByteBuf buf = (io.netty.buffer.ByteBuf) msg;
      int readable = buf.readableBytes();
      if (readable < 128) {
        // Heuristic: Small packets are likely movement, actions, or keepalives.
        // Prioritize them to reduce latency perception.
        priority = 3;
      } else if (readable > 4096) {
        // Heuristic: Large packets are likely chunks or bulk data.
        // Deprioritize to avoid blocking small urgent packets.
        priority = 15;
      }
    }

    queue.add(new PendingWrite(msg, promise, priority, orderCounter.getAndIncrement()));
  }

  @Override
  public void flush(ChannelHandlerContext ctx) throws Exception {
    boolean flushed = false;
    while (!queue.isEmpty()) {
      PendingWrite pw = queue.poll();
      ctx.write(pw.msg, pw.promise);
      flushed = true;
    }
    if (flushed) {
      ctx.flush();
    }
  }

  /**
   * Drops low priority packets to relieve congestion.
   */
  public void dropLowPriorityPackets() {
    // Drop packets with priority >= 15 (Low/Bulk)
    queue.removeIf(pw -> {
      if (pw.priority >= 15) {
        // Fail the promise so the sender knows it failed
        pw.promise.tryFailure(new Exception("Packet dropped due to congestion"));
        if (pw.msg instanceof io.netty.util.ReferenceCounted) {
          ((io.netty.util.ReferenceCounted) pw.msg).release();
        }
        return true;
      }
      return false;
    });
  }
}
