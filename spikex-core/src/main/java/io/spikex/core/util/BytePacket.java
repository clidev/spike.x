/**
 *
 * Copyright (c) 2015 NG Modular Oy.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.spikex.core.util;

import java.nio.ByteBuffer;
import javax.xml.bind.DatatypeConverter;

/**
 * Hex to byte utility class.
 *
 * @author cli
 */
public class BytePacket {

    private static final BytePacket PACKET_EMPTY = new BytePacket(ByteBuffer.allocate(0));
    private final ByteBuffer m_data;

    public BytePacket(final ByteBuffer data) {
        m_data = data;
    }

    public final ByteBuffer getData() {
        return m_data;
    }

    public final boolean isEmpty() {
        return (this == PACKET_EMPTY);
    }

    public final byte[] toBytes() {
        return m_data.array();
    }

    public final String toHex() {
        return BytePacket.toHex(toBytes());
    }

    public static BytePacket createEmpty() {
        return PACKET_EMPTY;
    }

    public static BytePacket create(final String hex) {
        return create(DatatypeConverter.parseHexBinary(hex));
    }

    public static BytePacket create(final byte[] data) {
        ByteBuffer buf = ByteBuffer.allocate(data.length).put(data);
        buf.flip();
        return new BytePacket(buf);
    }

    public static String toHex(final byte[] data) {
        return DatatypeConverter.printHexBinary(data);
    }
}
