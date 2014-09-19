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
package com.alibaba.cobar.net.buffer;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 缓冲队列
 * @author xianmao.hexm
 */
public final class BufferQueue {

    private int takeIndex;
    private int putIndex;
    private int count;
    private final ByteBuffer[] items;
    private final ReentrantLock lock;
    private final Condition notFull;//条件锁
    private ByteBuffer attachment;

    /**
     * 
     * @param capacity 队列容量
     */
    public BufferQueue(int capacity) {
        items = new ByteBuffer[capacity];
        lock = new ReentrantLock();
        notFull = lock.newCondition();
    }

    /**
     * 返回附件
     * @return
     */
    public ByteBuffer attachment() {
        return attachment;
    }

    /**
     * 添加附件
     * @param buffer
     */
    public void attach(ByteBuffer buffer) {
        this.attachment = buffer;
    }

    /**
     * 队列大小
     * @return
     */
    public int size() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return count;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 加入队列，队列满，阻塞
     * @param buffer
     * @throws InterruptedException
     */
    public void put(ByteBuffer buffer) throws InterruptedException {
        final ByteBuffer[] items = this.items;
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            try {
                while (count == items.length) {
                    notFull.await();
                }
            } catch (InterruptedException ie) {
                notFull.signal();
                throw ie;
            }
            insert(buffer);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 取出并删除
     * @return
     */
    public ByteBuffer poll() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (count == 0) {
                return null;
            }
            return extract();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 加入队列
     * @param buffer
     */
    private void insert(ByteBuffer buffer) {
        items[putIndex] = buffer;
        putIndex = inc(putIndex);
        ++count;
    }

    /**
     * 取出byteBuffer
     * @return
     */
    private ByteBuffer extract() {
        final ByteBuffer[] items = this.items;
        ByteBuffer buffer = items[takeIndex];
        items[takeIndex] = null;
        takeIndex = inc(takeIndex);
        --count;
        notFull.signal();
        return buffer;
    }

    private int inc(int i) {
        return (++i == items.length) ? 0 : i;
    }

}
