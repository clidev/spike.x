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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.attribute.FileTime;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public final class DirEvent {

    private final Path m_base;
    private final Path m_file;
    private final Kind m_kind;
    private final int m_hash;
    private final FileTime m_modtm;

    private static final String FIELD_BASE = "base";
    private static final String FIELD_FILE = "file";
    private static final String FIELD_KIND = "kind";
    private static final String FIELD_HASH = "hash";
    private static final String FIELD_MODTM = "modtm";

    public DirEvent(
            final Path base,
            final Path file,
            final Kind kind,
            final int hash,
            final FileTime modtm) {

        m_base = base;
        m_file = file;
        m_kind = kind;
        m_hash = hash;
        m_modtm = modtm;
    }

    public DirEvent(final JsonObject json) {
        m_base = Paths.get(json.getString(FIELD_BASE));
        m_file = Paths.get(json.getString(FIELD_FILE));
        m_hash = json.getInteger(FIELD_HASH);
        m_modtm = FileTime.fromMillis(json.getLong(FIELD_MODTM));

        switch (json.getString(FIELD_KIND)) {
            case "ENTRY_CREATE":
                m_kind = StandardWatchEventKinds.ENTRY_CREATE;
                break;
            case "ENTRY_MODIFY":
                m_kind = StandardWatchEventKinds.ENTRY_MODIFY;
                break;
            case "ENTRY_DELETE":
                m_kind = StandardWatchEventKinds.ENTRY_DELETE;
                break;
            default:
                m_kind = null;
                break;
        }
    }

    public FileTime getLastModifiedTime() {
        return m_modtm;
    }

    public int getHash() {
        return m_hash;
    }

    public Kind getKind() {
        return m_kind;
    }

    public Path getBase() {
        return m_base;
    }

    public Path getFile() {
        return m_file;
    }

    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        json.putString(FIELD_BASE, getBase().toString());
        json.putString(FIELD_FILE, getFile().toString());
        json.putNumber(FIELD_HASH, getHash());
        json.putNumber(FIELD_MODTM, getLastModifiedTime().toMillis());
        json.putString(FIELD_KIND, getKind().name());
        return json;
    }
}
