/**
 *
 * Copyright (c) 2015 NG Modular Oy.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.spikex.core.util;

import com.google.common.base.Preconditions;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import static java.nio.ByteOrder.BIG_ENDIAN;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import javax.xml.bind.DatatypeConverter;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;

/**
 *
 * @author cli
 */
public class XXHash32 {

    private final static XXHashFactory FACTORY = XXHashFactory.fastestInstance();

    public static int hash(
            final String utf8Str,
            final int salt) {

        Preconditions.checkArgument((utf8Str != null && utf8Str.length() > 0),
                "utf8Str is null or empty");

        byte[] data = utf8Str.getBytes(StandardCharsets.UTF_8);
        return FACTORY.hash32().hash(data, 0, data.length, salt);
    }

    public static String hashAsHex(
            final String utf8Str,
            final int salt) {

        Preconditions.checkArgument((utf8Str != null && utf8Str.length() > 0),
                "utf8Str is null or empty");

        byte[] data = utf8Str.getBytes(StandardCharsets.UTF_8);
        int hash = FACTORY.hash32().hash(data, 0, data.length, salt);
        byte[] hashBytes = ByteBuffer.allocate(4).putInt(hash).order(BIG_ENDIAN).array();
        return DatatypeConverter.printHexBinary(hashBytes).toLowerCase();
    }

    public static int hashOfFile(
            final Path file,
            final int salt) throws IOException {

        Preconditions.checkNotNull(file, "file is null");

        StreamingXXHash32 hash32 = FACTORY.newStreamingHash32(salt);
        ByteBuffer buf = ByteBuffer.allocate(8192);
        byte[] b = new byte[8192];

        try (FileInputStream in = new FileInputStream(file.toFile())) {
            FileChannel channel = in.getChannel();
            long len;
            while ((len = channel.read(buf)) != -1) {
                buf.flip();
                buf.get(b, 0, (int) len);
                hash32.update(b, 0, (int) len);
                buf.clear();
            }
        }
        return hash32.getValue();
    }
}
