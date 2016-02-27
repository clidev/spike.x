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
package io.spikex.core.helper;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.spikex.core.AbstractActivator;
import io.spikex.core.AbstractActivator.DeployVerticleCommand;
import io.spikex.core.AbstractActivator.InitVerticleCommand;
import io.spikex.core.AbstractActivator.UndeployVerticleCommand;
import io.spikex.core.AbstractCommand;
import io.spikex.core.util.IBuilder;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public class Commands {

    // Reserved return codes
    public static final int RET_OK = 0;
    public static final int RET_DEPLOYMENT_ID = 10001;

    public static final String CMD_FIELD_CMD = "cmd";
    public static final String CMD_FIELD_ARGS = "args";
    public static final String CMD_FIELD_RETURN_CODE = "retcode";
    public static final String CMD_FIELD_REASON = "reason";

    // Reserved error codes
    public static final int ERR_UNDEPLOY_FAILED = 70001;
    public static final int ERR_DEPLOY_FAILED = 70002;

    // Reserved return messages
    public static final String REASON_UNDEPLOY_FAILED = "Failed to undeploy verticle: ";
    public static final String REASON_DEPLOY_FAILED = "Failed to deploy verticle: ";

    // Reserved commands
    public static final String CMD_INIT_VERTICLE = "spikex-init-verticle";
    public static final String CMD_DEPLOY_VERTICLE = "spikex-deploy-verticle";
    public static final String CMD_UNDEPLOY_VERTICLE = "spikex-undeploy-verticle";

    public static final JsonObject RESULT_OK = new Result().toJson();

    public static JsonObject jsonCommand(
            final String cmd,
            final Object... args) {

        // Sanity check
        Preconditions.checkArgument(!Strings.isNullOrEmpty(cmd), "cmd is empty");

        JsonObject command = new JsonObject();
        command.putString(CMD_FIELD_CMD, cmd);
        JsonArray cmdArgs = new JsonArray();
        for (Object arg : args) {
            cmdArgs.add(arg);
        }
        command.putArray(CMD_FIELD_ARGS, cmdArgs);
        return command;
    }

    public static JsonObject result(final int retCode) {
        return result(retCode, null);
    }

    public static JsonObject result(
            final int retCode,
            final String reason) {

        JsonObject json = new JsonObject();
        json.putNumber(CMD_FIELD_RETURN_CODE, retCode);
        json.putString(CMD_FIELD_REASON, (reason == null ? "" : reason));
        return json;
    }

    public static void call(
            final EventBus eventBus,
            final String address,
            final AbstractCommand command) {

        eventBus.send(address, command.toJson());
    }

    public static void call(
            final EventBus eventBus,
            final String address,
            final AbstractCommand command,
            final Handler<Message<JsonObject>> handler) {

        eventBus.send(address, command.toJson(), handler);
    }

    public static AbstractCommand cmdInitVerticle() {
        return new InitVerticleCommand();
    }

    public static AbstractCommand cmdDeployVerticle(
            final String verticle,
            final JsonObject config,
            final int instances,
            final boolean multiThreaded,
            final boolean worker) {

        return new DeployVerticleCommand(
                verticle,
                config,
                instances,
                multiThreaded,
                worker);
    }

    public static AbstractCommand cmdUndeployVerticle(final String verticle) {
        return new UndeployVerticleCommand(verticle);
    }

    public static Builder builder(
            final AbstractActivator activator,
            final JsonObject json) {

        return new Builder(activator, json);
    }

    /**
     * Based on:
     * http://stackoverflow.com/questions/17164375/subclassing-a-java-builder-class
     *
     * @param <E>
     */
    public static class Builder<E extends Builder>
            implements IBuilder<AbstractCommand> {

        private final AbstractActivator m_activator;
        private final JsonObject m_json;

        protected Builder(
                final AbstractActivator activator,
                final JsonObject json) {

            // Sanity check
            Preconditions.checkNotNull(activator);
            m_activator = activator;
            m_json = (json != null ? json : new JsonObject());
        }

        protected AbstractActivator getActivator() {
            return m_activator;
        }

        protected JsonObject getJson() {
            return m_json;
        }

        @Override
        public AbstractCommand build() {

            AbstractCommand command = new DummyCommand(getJson());
            String jsonCmd = m_json.getString(CMD_FIELD_CMD, "");

            switch (jsonCmd) {
                case CMD_INIT_VERTICLE:
                    command = new InitVerticleCommand(m_activator, getJson());
                    break;
                case CMD_DEPLOY_VERTICLE:
                    command = new DeployVerticleCommand(m_activator, getJson());
                    break;
                case CMD_UNDEPLOY_VERTICLE:
                    command = new UndeployVerticleCommand(m_activator, getJson());
                    break;
            }

            return command;
        }
    }

    public static final class Result {

        private final JsonObject m_json;

        public Result() {
            m_json = new JsonObject();
            m_json.putNumber(CMD_FIELD_RETURN_CODE, 0);
            m_json.putString(CMD_FIELD_REASON, "");
        }

        public Result(final JsonObject json) {
            m_json = json;
        }

        public boolean isOk() {
            return (getReturnCode() == 0);
        }

        public boolean isReturnCode(final int code) {
            return (getReturnCode() == code);
        }

        public int getReturnCode() {
            return m_json.getInteger(CMD_FIELD_RETURN_CODE, 0);
        }

        public String getReason() {
            return m_json.getString(CMD_FIELD_REASON, "");
        }

        public Result setReason(
                final int retCode,
                final String reason) {

            m_json.putNumber(CMD_FIELD_RETURN_CODE, retCode);
            m_json.putString(CMD_FIELD_REASON, (reason == null ? "" : reason));
            return this;
        }

        public JsonObject toJson() {
            return m_json;
        }
    }

    private static final class DummyCommand extends AbstractCommand {

        private DummyCommand(final JsonObject json) {
            super(json);
        }

        @Override
        public void execute(final Message message) {
            // Do nothing by default
            message.reply(RESULT_OK);
        }
    }
}
