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
package com.alibaba.cobar.mysql;

import java.nio.ByteBuffer;

/**
 * 缓冲区读写工具
 * @author xianmao.hexm 2010-9-3 下午02:29:44
 */
public class BufferUtil {

    /**
     * 写2字节到缓冲区
     * @param buffer
     * @param i
     */
    public static final void writeUB2(ByteBuffer buffer, int i) {
        buffer.put((byte) (i & 0xff));
        buffer.put((byte) (i >>> 8));
    }

    /**
     * 写3字节到缓冲区
     * @param buffer
     * @param i
     */
    public static final void writeUB3(ByteBuffer buffer, int i) {
        buffer.put((byte) (i & 0xff));
        buffer.put((byte) (i >>> 8));
        buffer.put((byte) (i >>> 16));
    }

    /**
     * 写4字节到缓冲区
     * @param buffer
     * @param i
     */
    public static final void writeInt(ByteBuffer buffer, int i) {
        buffer.put((byte) (i & 0xff));
        buffer.put((byte) (i >>> 8));
        buffer.put((byte) (i >>> 16));
        buffer.put((byte) (i >>> 24));
    }

    /**
     * 写4字节int类型表示的float形式到缓冲区
     * @param buffer
     * @param f
     */
    public static final void writeFloat(ByteBuffer buffer, float f) {
        writeInt(buffer, Float.floatToIntBits(f));
    }

    /**
     * 写4个字节到缓冲区
     * @param buffer
     * @param l
     */
    public static final void writeUB4(ByteBuffer buffer, long l) {
        buffer.put((byte) (l & 0xff));
        buffer.put((byte) (l >>> 8));
        buffer.put((byte) (l >>> 16));
        buffer.put((byte) (l >>> 24));
    }

    /**
     * 写8字节到缓冲区
     * @param buffer
     * @param l
     */
    public static final void writeLong(ByteBuffer buffer, long l) {
        buffer.put((byte) (l & 0xff));
        buffer.put((byte) (l >>> 8));
        buffer.put((byte) (l >>> 16));
        buffer.put((byte) (l >>> 24));
        buffer.put((byte) (l >>> 32));
        buffer.put((byte) (l >>> 40));
        buffer.put((byte) (l >>> 48));
        buffer.put((byte) (l >>> 56));
    }

    /**
     * 写8个字节long表示的double形式到缓冲区
     * @param buffer
     * @param d
     */
    public static final void writeDouble(ByteBuffer buffer, double d) {
        writeLong(buffer, Double.doubleToLongBits(d));
    }

    /**
     * 把长度写入到缓冲区，先写一位标记位，<251直接写长度,<0x10000先写252到缓冲区再写2字节,<0x1000000先写253再写3字节,其他先写254再写4字节
     * @param buffer
     * @param l
     */
    public static final void writeLength(ByteBuffer buffer, long l) {
        if (l < 251) {
            buffer.put((byte) l);
        } else if (l < 0x10000L) {
            buffer.put((byte) 252);
            writeUB2(buffer, (int) l);
        } else if (l < 0x1000000L) {
            buffer.put((byte) 253);
            writeUB3(buffer, (int) l);
        } else {
            buffer.put((byte) 254);
            writeLong(buffer, l);
        }
    }

    /**
     * 写到缓冲区，结尾写一个0位
     * @param buffer
     * @param src
     */
    public static final void writeWithNull(ByteBuffer buffer, byte[] src) {
        buffer.put(src);
        buffer.put((byte) 0);
    }

    /**
     * 先写包长度到缓冲区，再写包
     * @param buffer
     * @param src
     */
    public static final void writeWithLength(ByteBuffer buffer, byte[] src) {
        int length = src.length;
        if (length < 251) {
            buffer.put((byte) length);
        } else if (length < 0x10000L) {
            buffer.put((byte) 252);
            writeUB2(buffer, length);
        } else if (length < 0x1000000L) {
            buffer.put((byte) 253);
            writeUB3(buffer, length);
        } else {
            buffer.put((byte) 254);
            writeLong(buffer, length);
        }
        buffer.put(src);
    }

    /**
     * 如果包为空，写nullValue到缓冲区。不为空，先写包长度到缓冲区，再写包
     * @param buffer
     * @param src
     * @param nullValue
     */
    public static final void writeWithLength(ByteBuffer buffer, byte[] src, byte nullValue) {
        if (src == null) {
            buffer.put(nullValue);
        } else {
            writeWithLength(buffer, src);
        }
    }

    /**
     * length<251,返回1,<0x10000L返回3,<0x1000000L返回4,其他返回9
     * @param length
     * @return
     */
    public static final int getLength(long length) {
        if (length < 251) {
            return 1;
        } else if (length < 0x10000L) {
            return 3;
        } else if (length < 0x1000000L) {
            return 4;
        } else {
            return 9;
        }
    }

    /**
     * src长度<251返回长度+1,<0x10000L返回长度+3,<0x1000000L返回长度+4,其他返回长度+9
     * @param src
     * @return
     */
    public static final int getLength(byte[] src) {
        int length = src.length;
        if (length < 251) {
            return 1 + length;
        } else if (length < 0x10000L) {
            return 3 + length;
        } else if (length < 0x1000000L) {
            return 4 + length;
        } else {
            return 9 + length;
        }
    }

}
