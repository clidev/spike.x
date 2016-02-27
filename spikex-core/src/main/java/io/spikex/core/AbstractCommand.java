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
package io.spikex.core;

import com.google.common.base.Preconditions;
import static io.spikex.core.helper.Commands.CMD_FIELD_ARGS;
import static io.spikex.core.helper.Commands.CMD_FIELD_REASON;
import static io.spikex.core.helper.Commands.CMD_FIELD_RETURN_CODE;
import static io.spikex.core.helper.Commands.RESULT_OK;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public abstract class AbstractCommand {

    private final JsonObject m_json;

    public AbstractCommand(final JsonObject json) {
        // Sanity check
        Preconditions.checkNotNull(json, "json is null");
        m_json = json;
    }

    public JsonArray getArgs() {
        return m_json.getArray(CMD_FIELD_ARGS);
    }

    public abstract void execute(Message message);

    public JsonObject toJson() {
        return m_json;
    }

    protected JsonObject resultOk() {
        return RESULT_OK;
    }

    protected JsonObject resultNok(
            final int code,
            final String reason) {

        return RESULT_OK
                .putNumber(CMD_FIELD_RETURN_CODE, code)
                .putString(CMD_FIELD_REASON, reason);
    }
}
