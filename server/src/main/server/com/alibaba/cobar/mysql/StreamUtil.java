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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * 流读写工具
 * @author xianmao.hexm 2011-5-5 下午09:34:11
 */
public class StreamUtil {
    private static final long NULL_LENGTH = -1;
    private static final byte[] EMPTY_BYTES = new byte[0];

    /**
     * 流中读到btye[]
     * @param in 输入流
     * @param b 读到b中
     * @param offset 偏移
     * @param length 读长度
     * @throws IOException
     */
    public static final void read(InputStream in, byte[] b, int offset, int length) throws IOException {
        for (int got = 0; length > 0;) {
            got = in.read(b, offset, length);
            if (got < 0) {
                throw new EOFException();
            }
            offset += got;
            length -= got;
        }
    }

    /**
     * 读输入流下一字节
     * @param in
     * @return
     * @throws IOException
     */
    public static final byte read(InputStream in) throws IOException {
        int got = in.read();
        if (got < 0) {
            throw new EOFException();
        }
        return (byte) (got & 0xff);
    }

    /**
     * 从数据流读2字节，拼int类型数据
     * @param in
     * @return
     * @throws IOException
     */
    public static final int readUB2(InputStream in) throws IOException {
        byte[] b = new byte[2];
        read(in, b, 0, b.length);
        int i = b[0] & 0xff;
        i |= (b[1] & 0xff) << 8;
        return i;
    }

    /**
     * 从数据流读3字节，拼int类型数据
     * @param in
     * @return
     * @throws IOException
     */
    public static final int readUB3(InputStream in) throws IOException {
        byte[] b = new byte[3];
        read(in, b, 0, b.length);
        int i = b[0] & 0xff;
        i |= (b[1] & 0xff) << 8;
        i |= (b[2] & 0xff) << 16;
        return i;
    }

    /**
     * 从数据流读4字节，拼int类型数据
     * @param in
     * @return
     * @throws IOException
     */
    public static final int readInt(InputStream in) throws IOException {
        byte[] b = new byte[4];
        read(in, b, 0, b.length);
        int i = b[0] & 0xff;
        i |= (b[1] & 0xff) << 8;
        i |= (b[2] & 0xff) << 16;
        i |= (b[3] & 0xff) << 24;
        return i;
    }

    /**
     * 从数据流读4字节，拼int类型数据,返回int对应位形式的float
     * @param in
     * @return
     * @throws IOException
     */
    public static final float readFloat(InputStream in) throws IOException {
        return Float.intBitsToFloat(readInt(in));
    }

    /**
     * 从数据流读4字节，拼long类型数据
     * @param in
     * @return
     * @throws IOException
     */
    public static final long readUB4(InputStream in) throws IOException {
        byte[] b = new byte[4];
        read(in, b, 0, b.length);
        long l = (long) (b[0] & 0xff);
        l |= (long) (b[1] & 0xff) << 8;
        l |= (long) (b[2] & 0xff) << 16;
        l |= (long) (b[3] & 0xff) << 24;
        return l;
    }

    /**
     * 从数据流读8字节，拼long类型数据
     * @param in
     * @return
     * @throws IOException
     */
    public static final long readLong(InputStream in) throws IOException {
        byte[] b = new byte[8];
        read(in, b, 0, b.length);
        long l = (long) (b[0] & 0xff);
        l |= (long) (b[1] & 0xff) << 8;
        l |= (long) (b[2] & 0xff) << 16;
        l |= (long) (b[3] & 0xff) << 24;
        l |= (long) (b[4] & 0xff) << 32;
        l |= (long) (b[5] & 0xff) << 40;
        l |= (long) (b[6] & 0xff) << 48;
        l |= (long) (b[7] & 0xff) << 56;
        return l;
    }

    /**
     * 从数据流读8字节，拼long类型数据,返回long对应位形式的double
     * @param in
     * @return
     * @throws IOException
     */
    public static final double readDouble(InputStream in) throws IOException {
        return Double.longBitsToDouble(readLong(in));
    }

    /**
     * 读流中全部数据
     * @param in
     * @return
     * @throws IOException
     */
    public static final byte[] readWithLength(InputStream in) throws IOException {
        int length = (int) readLength(in);
        if (length <= 0) {
            return EMPTY_BYTES;
        }
        byte[] b = new byte[length];
        read(in, b, 0, b.length);
        return b;
    }

    /**
     * 写1个字节到流
     * @param out
     * @param b
     * @throws IOException
     */
    public static final void write(OutputStream out, byte b) throws IOException {
        out.write(b & 0xff);
    }

    /**
     * 写2个字节到流
     * @param out
     * @param i
     * @throws IOException
     */
    public static final void writeUB2(OutputStream out, int i) throws IOException {
        byte[] b = new byte[2];
        b[0] = (byte) (i & 0xff);
        b[1] = (byte) (i >>> 8);
        out.write(b);
    }

    /**
     * 写3个字节到流
     * @param out
     * @param i
     * @throws IOException
     */
    public static final void writeUB3(OutputStream out, int i) throws IOException {
        byte[] b = new byte[3];
        b[0] = (byte) (i & 0xff);
        b[1] = (byte) (i >>> 8);
        b[2] = (byte) (i >>> 16);
        out.write(b);
    }

    /**
     * 写个4字节到流
     * @param out
     * @param i
     * @throws IOException
     */
    public static final void writeInt(OutputStream out, int i) throws IOException {
        byte[] b = new byte[4];
        b[0] = (byte) (i & 0xff);
        b[1] = (byte) (i >>> 8);
        b[2] = (byte) (i >>> 16);
        b[3] = (byte) (i >>> 24);
        out.write(b);
    }

    /**
     * 写4个字节的int类型表示的float到流
     * @param out
     * @param f
     * @throws IOException
     */
    public static final void writeFloat(OutputStream out, float f) throws IOException {
        writeInt(out, Float.floatToIntBits(f));
    }

    /**
     * 写4个字节到流
     * @param out
     * @param l
     * @throws IOException
     */
    public static final void writeUB4(OutputStream out, long l) throws IOException {
        byte[] b = new byte[4];
        b[0] = (byte) (l & 0xff);
        b[1] = (byte) (l >>> 8);
        b[2] = (byte) (l >>> 16);
        b[3] = (byte) (l >>> 24);
        out.write(b);
    }

    /**
     * 写8个字节到流
     * @param out
     * @param l
     * @throws IOException
     */
    public static final void writeLong(OutputStream out, long l) throws IOException {
        byte[] b = new byte[8];
        b[0] = (byte) (l & 0xff);
        b[1] = (byte) (l >>> 8);
        b[2] = (byte) (l >>> 16);
        b[3] = (byte) (l >>> 24);
        b[4] = (byte) (l >>> 32);
        b[5] = (byte) (l >>> 40);
        b[6] = (byte) (l >>> 48);
        b[7] = (byte) (l >>> 56);
        out.write(b);
    }

    /**
     * 写8个字节long类型表示的double的形式到流
     * @param out
     * @param d
     * @throws IOException
     */
    public static final void writeDouble(OutputStream out, double d) throws IOException {
        writeLong(out, Double.doubleToLongBits(d));
    }

    /**
     * 读长度
     * @param in
     * @return
     * @throws IOException
     */
    public static final long readLength(InputStream in) throws IOException {
        int length = in.read();
        if (length < 0)
            throw new EOFException();
        switch (length) {
        case 251:
            return NULL_LENGTH;
        case 252:
            return readUB2(in);
        case 253:
            return readUB3(in);
        case 254:
            return readLong(in);
        default:
            return length;
        }
    }

    /**
     * 把数据长度写到流，超过1字节加标记位，<251写1字节,<0x10000L写2字节,<0x1000000写3字节,其他写4字节
     * @param out
     * @param length
     * @throws IOException
     */
    public static final void writeLength(OutputStream out, long length) throws IOException {
        if (length < 251) {
            out.write((byte) length);
        } else if (length < 0x10000L) {
            out.write((byte) 252);
            writeUB2(out, (int) length);
        } else if (length < 0x1000000L) {
            out.write((byte) 253);
            writeUB3(out, (int) length);
        } else {
            out.write((byte) 254);
            writeLong(out, length);
        }
    }

    /**
     * 写入到流，最后加一0位
     * @param out
     * @param src
     * @throws IOException
     */
    public static final void writeWithNull(OutputStream out, byte[] src) throws IOException {
        out.write(src);
        out.write((byte) 0);
    }

    /**
     * 先写入数据长度，再写入数据到流
     * @param out
     * @param src
     * @throws IOException
     */
    public static final void writeWithLength(OutputStream out, byte[] src) throws IOException {
        int length = src.length;
        if (length < 251) {
            out.write((byte) length);
        } else if (length < 0x10000L) {
            out.write((byte) 252);
            writeUB2(out, length);
        } else if (length < 0x1000000L) {
            out.write((byte) 253);
            writeUB3(out, length);
        } else {
            out.write((byte) 254);
            writeLong(out, length);
        }
        out.write(src);
    }

}
