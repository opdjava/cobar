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
package com.alibaba.cobar.net;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.alibaba.cobar.config.ErrorCode;

/**
 * NIO连接器，处理后端连接，把连接加入到NIO处理器的读反应器
 * 
 * @author xianmao.hexm
 */
public final class NIOConnector extends Thread {
    private static final Logger LOGGER = Logger.getLogger(NIOConnector.class);
    private static final ConnectIdGenerator ID_GENERATOR = new ConnectIdGenerator();

    private final String name;
    private final Selector selector;
    private final BlockingQueue<BackendConnection> connectQueue;
    private NIOProcessor[] processors;
    private int nextProcessor;
    private long connectCount;

    public NIOConnector(String name) throws IOException {
        super.setName(name);
        this.name = name;
        this.selector = Selector.open();
        this.connectQueue = new LinkedBlockingQueue<BackendConnection>();
    }

    /**
     * 连接次数
     * @return
     */
    public long getConnectCount() {
        return connectCount;
    }

    /**
     * 设置NIO处理器
     * @param processors
     */
    public void setProcessors(NIOProcessor[] processors) {
        this.processors = processors;
    }

    /**
     * 添加后端连接
     * @param c
     */
    public void postConnect(BackendConnection c) {
        connectQueue.offer(c);
        selector.wakeup();
    }

    /**
     * 处理连接，把连接加入读反应器
     */
    @Override
    public void run() {
        final Selector selector = this.selector;
        for (;;) {
            ++connectCount;
            try {
                selector.select(1000L);
                connect(selector);
                Set<SelectionKey> keys = selector.selectedKeys();
                try {
                    for (SelectionKey key : keys) {
                        Object att = key.attachment();
                        if (att != null && key.isValid() && key.isConnectable()) {
                            finishConnect(key, att);
                        } else {
                            key.cancel();
                        }
                    }
                } finally {
                    keys.clear();
                }
            } catch (Throwable e) {
                LOGGER.warn(name, e);
            }
        }
    }

    /**
     * 在连接队列里取出后端连接，注册到selector
     * @param selector
     */
    private void connect(Selector selector) {
        BackendConnection c = null;
        while ((c = connectQueue.poll()) != null) {
            try {
                c.connect(selector);
            } catch (Throwable e) {
                c.error(ErrorCode.ERR_CONNECT_SOCKET, e);
            }
        }
    }

    /**
     * 建立连接，把连接添加到读反应器，取消key
     * @param key
     * @param att
     */
    private void finishConnect(SelectionKey key, Object att) {
        BackendConnection c = (BackendConnection) att;
        try {
            if (c.finishConnect()) {
                clearSelectionKey(key);
                c.setId(ID_GENERATOR.getId());
                NIOProcessor processor = nextProcessor();
                c.setProcessor(processor);
                processor.postRegister(c);
            }
        } catch (Throwable e) {
            clearSelectionKey(key);
            c.error(ErrorCode.ERR_FINISH_CONNECT, e);
        }
    }

    /**
     * 取消该KEY，将从selector中移除
     * @param key
     */
    private void clearSelectionKey(SelectionKey key) {
        if (key.isValid()) {
            key.attach(null);
            key.cancel();
        }
    }

    /**
     * 返回下一个NIO处理器
     * @return
     */
    private NIOProcessor nextProcessor() {
        if (++nextProcessor == processors.length) {
            nextProcessor = 0;
        }
        return processors[nextProcessor];
    }

    /**
     * 后端连接ID生成器
     * 
     * @author xianmao.hexm
     */
    private static class ConnectIdGenerator {

        private static final long MAX_VALUE = Long.MAX_VALUE;

        private long connectId = 0L;
        private final Object lock = new Object();

        private long getId() {
            synchronized (lock) {
                if (connectId >= MAX_VALUE) {
                    connectId = 0L;
                }
                return ++connectId;
            }
        }
    }

}
