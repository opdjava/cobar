/*
 * Copyright 1999-2012 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.cobar.net.factory;

import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SocketChannel;

import com.alibaba.cobar.net.BackendConnection;
import com.alibaba.cobar.net.NIOConnector;
import com.alibaba.cobar.net.buffer.BufferQueue;

/**
 * 后端连接工厂
 * 
 * @author xianmao.hexm
 */
public abstract class BackendConnectionFactory {

    protected int socketRecvBuffer = 16 * 1024;//socket接受缓冲大小
    protected int socketSendBuffer = 8 * 1024;//socket发射缓冲大小
    protected int packetHeaderSize = 4;//数据包头大小
    protected int maxPacketSize = 16 * 1024 * 1024;//最大数据包
    protected int writeQueueCapcity = 8;//写队列容量
    protected long idleTimeout = 8 * 3600 * 1000L;//空闲超时

    /**
     * 打开socket通道,返回通道
     * @return
     * @throws IOException
     */
    protected SocketChannel openSocketChannel() throws IOException {
        SocketChannel channel = null;
        try {
            channel = SocketChannel.open();
            channel.configureBlocking(false);
            Socket socket = channel.socket();
            socket.setReceiveBufferSize(socketRecvBuffer);
            socket.setSendBufferSize(socketSendBuffer);
            socket.setTcpNoDelay(true);//Nagle算法，将小包缓存组成大包发送
            socket.setKeepAlive(true);//常连接
        } catch (IOException e) {
            closeChannel(channel);
            throw e;
        } catch (RuntimeException e) {
            closeChannel(channel);
            throw e;
        }
        return channel;
    }

    protected void postConnect(BackendConnection c, NIOConnector connector) {
        c.setPacketHeaderSize(packetHeaderSize);
        c.setMaxPacketSize(maxPacketSize);
        c.setWriteQueue(new BufferQueue(writeQueueCapcity));
        c.setIdleTimeout(idleTimeout);
        c.setConnector(connector);
        connector.postConnect(c);
    }

    public int getSocketRecvBuffer() {
        return socketRecvBuffer;
    }

    public void setSocketRecvBuffer(int socketRecvBuffer) {
        this.socketRecvBuffer = socketRecvBuffer;
    }

    public int getSocketSendBuffer() {
        return socketSendBuffer;
    }

    public void setSocketSendBuffer(int socketSendBuffer) {
        this.socketSendBuffer = socketSendBuffer;
    }

    public int getPacketHeaderSize() {
        return packetHeaderSize;
    }

    public void setPacketHeaderSize(int packetHeaderSize) {
        this.packetHeaderSize = packetHeaderSize;
    }

    public int getMaxPacketSize() {
        return maxPacketSize;
    }

    public void setMaxPacketSize(int maxPacketSize) {
        this.maxPacketSize = maxPacketSize;
    }

    public int getWriteQueueCapcity() {
        return writeQueueCapcity;
    }

    public void setWriteQueueCapcity(int writeQueueCapcity) {
        this.writeQueueCapcity = writeQueueCapcity;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    private static void closeChannel(SocketChannel channel) {
        if (channel == null) {
            return;
        }
        Socket socket = channel.socket();
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
            }
        }
        try {
            channel.close();
        } catch (IOException e) {
        }
    }

}
