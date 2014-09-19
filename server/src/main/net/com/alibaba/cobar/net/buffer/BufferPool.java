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
import java.util.concurrent.locks.ReentrantLock;

/**
 * 缓冲池
 * @author xianmao.hexm
 */
public final class BufferPool {

    private final int chunkSize;
    private final ByteBuffer[] items;
    private final ReentrantLock lock;
    private int putIndex;
    private int takeIndex;
    private int count;
    private volatile int newCount;

    /**
     * 
     * @param bufferSize 缓冲池大小
     * @param chunkSize 块大小
     */
    public BufferPool(int bufferSize, int chunkSize) {
        this.chunkSize = chunkSize;
        int capacity = bufferSize / chunkSize;
        capacity = (bufferSize % chunkSize == 0) ? capacity : capacity + 1;
        this.items = new ByteBuffer[capacity];
        this.lock = new ReentrantLock();
        for (int i = 0; i < capacity; i++) {
            insert(create(chunkSize));
        }
    }

    /**
     * 容量
     * @return
     */
    public int capacity() {
        return items.length;
    }

    /**
     * count
     * @return
     */
    public int size() {
        return count;
    }

    /**
     * 初始化后新建的byteBuffer数
     * @return
     */
    public int getNewCount() {
        return newCount;
    }

    /**
     * 分配一块byteBuffer，如果分配的为null，新建byteBuffer
     * @return
     */
    public ByteBuffer allocate() {
        ByteBuffer node = null;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            node = (count == 0) ? null : extract();
        } finally {
            lock.unlock();
        }
        if (node == null) {
            ++newCount;
            return create(chunkSize);
        } else {
            return node;
        }
    }

    /**
     * 回收，清空byteBuffer加入缓冲池
     * @param buffer
     */
    public void recycle(ByteBuffer buffer) {
        // 拒绝回收null和容量大于chunkSize的缓存
        if (buffer == null || buffer.capacity() > chunkSize) {
            return;
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (count != items.length) {
                buffer.clear();
                insert(buffer);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 加入到缓冲池，count++
     * @param buffer
     */
    private void insert(ByteBuffer buffer) {
        items[putIndex] = buffer;
        putIndex = inc(putIndex);
        ++count;
    }

    /**
     * 取出一个byteBuffer，把位置制null，count--
     * @return
     */
    private ByteBuffer extract() {
        final ByteBuffer[] items = this.items;
        ByteBuffer item = items[takeIndex];
        items[takeIndex] = null;
        takeIndex = inc(takeIndex);
        --count;
        return item;
    }

    /**
     * 移动指针，超过容量回到0
     * @param i
     * @return 返回++
     */
    private int inc(int i) {
        return (++i == items.length) ? 0 : i;
    }

    /**
     * 新建ByteBuffer
     * @param size 大小
     * @return
     */
    private ByteBuffer create(int size) {
        return ByteBuffer.allocate(size);
    }

}
