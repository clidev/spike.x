/**
 *
 * Copyright (c) 2015 NG Modular Oy.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package io.spikex.filter.internal;

import org.vertx.java.core.file.FileProps;

/**
 *
 * @author chl
 */
public final class FilePointer {

    private FileProps m_props;
    private long m_firstBytes; // Magic bytes
    private long m_chunkOffset;

    public FilePointer(
            final FileProps props,
            final long firstBytes) {

        m_props = props;
        m_firstBytes = firstBytes;
        m_chunkOffset = 0L;
    }

    public boolean isNew() {
        return (m_props == null);
    }

    public FileProps getFileProps() {
        return m_props;
    }

    public long getFirstBytes() {
        return m_firstBytes;
    }

    public void setFileProps(final FileProps props) {
        m_props = props;
    }

    public void setFirstBytes(final long firstBytes) {
        m_firstBytes = firstBytes;
    }

    public long nextSize(
            final long curSize,
            final int chunkSize) {

        long chunkOffset = m_chunkOffset;

        // Reset
        if (curSize < chunkOffset) {
            m_chunkOffset = 0L;
        } else if (curSize != chunkOffset) {

            // Next offset/size
            chunkOffset += chunkSize;

            // Handle end of file
            if (chunkOffset > curSize) {
                chunkOffset = curSize;
            }

            m_chunkOffset = chunkOffset;
        }

        return chunkOffset;
    }
}
