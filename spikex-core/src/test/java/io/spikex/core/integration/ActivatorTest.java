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
package io.spikex.core.integration;

import com.eaio.uuid.UUID;
import io.spikex.core.AbstractActivator;
import io.spikex.core.AbstractCommand;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CONF_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_DATA_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_HOME_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_LOCAL_ADDRESS;
import static io.spikex.core.AbstractVerticle.CONF_KEY_TMP_PATH;
import io.spikex.core.helper.Commands;
import static io.spikex.core.helper.Commands.CMD_FIELD_CMD;
import static io.spikex.core.helper.Commands.RET_DEPLOYMENT_ID;
import io.spikex.core.helper.Commands.Result;
import static io.spikex.core.integration.ActivatorTest.LogCommand.CMD_LOG_INFO;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;

/**
 * Test variables.
 *
 * @author cli
 */
public class ActivatorTest extends TestVerticle {

    private static final String LOCAL_ADDRESS = "ActivatorTestAddress." + new UUID().toString();
    private final Logger m_logger = LoggerFactory.getLogger(ActivatorTest.class);

    @Test
    public void testCommand() throws InterruptedException {

        JsonObject config = new JsonObject();
        config.putValue(CONF_KEY_HOME_PATH, ".");
        config.putValue(CONF_KEY_CONF_PATH, ".");
        config.putValue(CONF_KEY_DATA_PATH, ".");
        config.putValue(CONF_KEY_TMP_PATH, ".");
        config.putValue(CONF_KEY_LOCAL_ADDRESS, LOCAL_ADDRESS);

        container.deployWorkerVerticle(Activator.class.getName(), config, 1, false,
                new AsyncResultHandler<String>() {
                    @Override
                    public void handle(final AsyncResult<String> ar) {
                        if (ar.failed()) {
                            Assert.fail("Failed to deploy verticle: " + ar.cause());
                        } else {
                            initActivator(ar.result());
                        }
                    }
                });
    }

    private void initActivator(final String deploymentId) {
        m_logger.info("Initializing activator: {}", deploymentId);
        Commands.call(
                vertx.eventBus(),
                LOCAL_ADDRESS,
                Commands.cmdInitVerticle(),
                new Handler<Message<JsonObject>>() {

                    @Override
                    public void handle(Message<JsonObject> message) {
                        callLogCommand();
                    }
                });
    }

    private void callLogCommand() {
        vertx.eventBus().send(LOCAL_ADDRESS, Activator.TestCommands.logCommand().toJson(),
                new Handler<Message<JsonObject>>() {
                    @Override
                    public void handle(final Message<JsonObject> message) {
                        m_logger.info("Command result: {}", message.body());
                    }
                });
    }

    public static class Activator extends AbstractActivator {

        @Override
        protected AbstractCommand buildCommand(final JsonObject json) {
            logger().info("Building custom command: {}", json.encode());
            return TestCommands.builder(this, json).build();
        }

        @Override
        protected void startVerticle() {
            // Do nothing...
        }

        @Override
        protected void initVerticle() {
            Commands.call(
                    eventBus(),
                    LOCAL_ADDRESS,
                    Commands.cmdDeployVerticle(
                            TestVerticle1.class.getName(),
                            new JsonObject(),
                            1,
                            false,
                            false),
                    new Handler<Message<JsonObject>>() {

                        @Override
                        public void handle(Message<JsonObject> message) {
                            Result result = new Result(message.body());
                            if (result.getReturnCode() != RET_DEPLOYMENT_ID) {
                                throw new IllegalStateException(result.getReason());
                            } else {
                                undeployVerticle(result.getReason()); // Deployment ID is in reason
                            }
                        }
                    });
        }

        private void undeployVerticle(final String deploymentId) {
            Commands.call(
                    eventBus(),
                    LOCAL_ADDRESS,
                    Commands.cmdUndeployVerticle(deploymentId),
                    new Handler<Message<JsonObject>>() {

                        @Override
                        public void handle(Message<JsonObject> message) {
                            Result result = new Result(message.body());
                            if (!result.isOk()) {
                                throw new IllegalStateException(result.getReason());
                            }
                            // Stop test
                            VertxAssert.testComplete();
                        }
                    });
        }

        public static class TestCommands extends Commands {

            public static LogCommand logCommand() {
                return new LogCommand();
            }

            public static Builder builder(
                    final AbstractActivator activator,
                    final JsonObject json) {

                return new Builder(activator, json);
            }

            public static class Builder extends Commands.Builder<Builder> {

                protected Builder(
                        final AbstractActivator activator,
                        final JsonObject json) {

                    super(activator, json);
                }

                @Override
                public AbstractCommand build() {
                    AbstractCommand command;
                    String jsonCmd = getJson().getString(CMD_FIELD_CMD, "");
                    switch (jsonCmd) {
                        case CMD_LOG_INFO:
                            command = new LogCommand(getJson());
                            break;
                        default:
                            command = super.build();
                            break;
                    }
                    return command;
                }
            }
        }
    }

    public static class LogCommand extends AbstractCommand {

        public static final String CMD_LOG_INFO = "cmd-log-info";
        private final Logger m_logger = LoggerFactory.getLogger(LogCommand.class);

        private LogCommand() {
            super(Commands.jsonCommand(CMD_LOG_INFO));
        }

        private LogCommand(final JsonObject json) {
            super(json);
        }

        @Override
        public void execute(final Message message) {
            m_logger.info("Command: {}", toJson());
            message.reply(resultOk());
        }
    }
}
