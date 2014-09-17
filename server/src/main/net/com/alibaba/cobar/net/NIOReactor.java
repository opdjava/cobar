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
 * 网络事件反应器
 * 
 * @author xianmao.hexm
 */
public final class NIOReactor {
    private static final Logger LOGGER = Logger.getLogger(NIOReactor.class);

    private final String name;
    private final R reactorR;
    private final W reactorW;

    /**
     * 初始化读/写反应器
     * @param name 网络时间反应器名称
     * @throws IOException
     */
    public NIOReactor(String name) throws IOException {
        this.name = name;
        this.reactorR = new R();
        this.reactorW = new W();
    }

    /**
     * 启动反应器
     */
    final void startup() {
        new Thread(reactorR, name + "-R").start();
        new Thread(reactorW, name + "-W").start();
    }

    /**
     * 添加到读反应器
     * @param c
     */
    final void postRegister(NIOConnection c) {
        reactorR.registerQueue.offer(c);
        reactorR.selector.wakeup();
    }

    /**
     * 返回读反应器连接队列NIOConnection
     * @return
     */
    final BlockingQueue<NIOConnection> getRegisterQueue() {
        return reactorR.registerQueue;
    }

    /**
     * 反应次数
     * @return
     */
    final long getReactCount() {
        return reactorR.reactCount;
    }

    /**
     * 添加到写队列
     * @param c
     */
    final void postWrite(NIOConnection c) {
        reactorW.writeQueue.offer(c);
    }

    /**
     * 返回写队列
     * @return
     */
    final BlockingQueue<NIOConnection> getWriteQueue() {
        return reactorW.writeQueue;
    }

    /**
     * 读反应器
     * @author Administrator
     *
     */
    private final class R implements Runnable {
        private final Selector selector;
        private final BlockingQueue<NIOConnection> registerQueue;
        private long reactCount;

        private R() throws IOException {
            this.selector = Selector.open();
            this.registerQueue = new LinkedBlockingQueue<NIOConnection>();
        }

        /**
         * 读反应器处理网络事件
         */
        @Override
        public void run() {
            final Selector selector = this.selector;
            for (;;) {
                ++reactCount;
                try {
                    selector.select(1000L);
                    register(selector);
                    Set<SelectionKey> keys = selector.selectedKeys();
                    try {
                        for (SelectionKey key : keys) {
                            Object att = key.attachment();
                            if (att != null && key.isValid()) {
                                int readyOps = key.readyOps();
                                if ((readyOps & SelectionKey.OP_READ) != 0) {
                                    read((NIOConnection) att);
                                } else if ((readyOps & SelectionKey.OP_WRITE) != 0) {
                                    write((NIOConnection) att);
                                } else {
                                    key.cancel();
                                }
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
         * 在队列里取出NIOConnection，注册网络事件
         * @param selector
         */
        private void register(Selector selector) {
            NIOConnection c = null;
            while ((c = registerQueue.poll()) != null) {
                try {
                    c.register(selector);
                } catch (Throwable e) {
                    c.error(ErrorCode.ERR_REGISTER, e);
                }
            }
        }

        /**
         * 处理连接读操作，NIOConnection读操作
         * @param c
         */
        private void read(NIOConnection c) {
            try {
                c.read();
            } catch (Throwable e) {
                c.error(ErrorCode.ERR_READ, e);
            }
        }

        /**
         * 处理连接写操作，NIOConnection写操作
         */
        private void write(NIOConnection c) {
            try {
                c.writeByEvent();
            } catch (Throwable e) {
                c.error(ErrorCode.ERR_WRITE_BY_EVENT, e);
            }
        }
    }

    /**
     * 写反应器
     * @author Administrator
     *
     */
    private final class W implements Runnable {
        private final BlockingQueue<NIOConnection> writeQueue;

        private W() {
            this.writeQueue = new LinkedBlockingQueue<NIOConnection>();
        }

        /**
         * 写反应器，从队列中取出直接写
         */
        @Override
        public void run() {
            NIOConnection c = null;
            for (;;) {
                try {
                    if ((c = writeQueue.take()) != null) {
                        write(c);
                    }
                } catch (Throwable e) {
                    LOGGER.warn(name, e);
                }
            }
        }

        /**
         * 写入到连接的写队列
         * @param c
         */
        private void write(NIOConnection c) {
            try {
                c.writeByQueue();
            } catch (Throwable e) {
                c.error(ErrorCode.ERR_WRITE_BY_QUEUE, e);
            }
        }
    }

}
