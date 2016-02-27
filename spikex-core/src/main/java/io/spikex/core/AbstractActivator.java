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

import io.spikex.core.helper.Commands;
import static io.spikex.core.helper.Commands.CMD_DEPLOY_VERTICLE;
import static io.spikex.core.helper.Commands.CMD_INIT_VERTICLE;
import static io.spikex.core.helper.Commands.CMD_UNDEPLOY_VERTICLE;
import static io.spikex.core.helper.Commands.ERR_DEPLOY_FAILED;
import static io.spikex.core.helper.Commands.ERR_UNDEPLOY_FAILED;
import static io.spikex.core.helper.Commands.REASON_DEPLOY_FAILED;
import static io.spikex.core.helper.Commands.REASON_UNDEPLOY_FAILED;
import static io.spikex.core.helper.Commands.RESULT_OK;
import static io.spikex.core.helper.Commands.RET_DEPLOYMENT_ID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * @author cli
 */
public abstract class AbstractActivator extends AbstractVerticle {

    /**
     * Handle JVM local message. Remember to call this method in a child class
     * if you require the normal command processing.
     *
     * @param message the received message
     */
    @Override
    protected void handleLocalMessage(final Message message) {
        processCommand(message);
    }

    protected void processCommand(final Message message) {
        Object msgBody = message.body();
        if (msgBody instanceof JsonObject) {
            JsonObject json = (JsonObject) msgBody;
            logger().debug("Received command: {}", json);
            AbstractCommand cmd = buildCommand(json);
            cmd.execute(message);
        }
    }

    protected AbstractCommand buildCommand(final JsonObject json) {
        return Commands.builder(this, json).build();
    }

    protected void deployVerticle(
            final String verticle,
            final JsonObject config,
            final int instances,
            final boolean multiThreaded,
            final boolean worker,
            final Handler<AsyncResult<String>> handler) {

        logger().info("Deploying verticle: {} instances: {} multi-threaded: {} worker: {}",
                verticle, instances, multiThreaded, worker);

        // Omit local address from merged config
        String localAddress = config.getString(CONF_KEY_LOCAL_ADDRESS);
        config.mergeIn(config()); // Add config available to any verticle
        config.putString(CONF_KEY_LOCAL_ADDRESS, localAddress);

        if (worker) {
            container.deployWorkerVerticle(
                    verticle,
                    config,
                    instances,
                    multiThreaded,
                    handler);
        } else {
            container.deployVerticle(
                    verticle,
                    config,
                    instances,
                    handler);
        }
    }

    protected void undeployVerticle(
            final String verticle,
            final Handler<AsyncResult<Void>> handler) {

        logger().info("Undeploying verticle: {}", verticle);
        container.undeployVerticle(verticle, handler);
    }

    public static final class InitVerticleCommand extends AbstractCommand {

        private AbstractActivator m_activator;

        public InitVerticleCommand() {
            super(Commands.jsonCommand(CMD_INIT_VERTICLE));
        }

        public InitVerticleCommand(
                final AbstractActivator activator,
                final JsonObject json) {

            super(json);
            m_activator = activator;
        }

        @Override
        public void execute(final Message message) {
            // Call verticle init method
            m_activator.initVerticle();
            message.reply(RESULT_OK);
        }
    }

    public static final class DeployVerticleCommand extends AbstractCommand {

        private AbstractActivator m_executor;
        private final Logger m_logger = LoggerFactory.getLogger(DeployVerticleCommand.class);

        public DeployVerticleCommand(
                final String verticle,
                final JsonObject config,
                final int instances,
                final boolean multiThreaded,
                final boolean worker) {

            super(Commands.jsonCommand(
                    CMD_DEPLOY_VERTICLE,
                    verticle,
                    config,
                    instances,
                    multiThreaded,
                    worker));
        }

        public DeployVerticleCommand(
                final AbstractActivator executor,
                final JsonObject json) {

            super(json);
            m_executor = executor;
        }

        @Override
        public void execute(final Message message) {
            // Call verticle deploy method
            JsonArray args = getArgs();
            final String verticle = (String) args.get(0);
            JsonObject config = (JsonObject) args.get(1);
            int instances = (Integer) args.get(2);
            boolean multiThreaded = (Boolean) args.get(3);
            boolean worker = (Boolean) args.get(4);
            
            m_executor.deployVerticle(
                    verticle,
                    config,
                    instances,
                    multiThreaded,
                    worker,
                    new Handler<AsyncResult<String>>() {

                        @Override
                        public void handle(AsyncResult<String> ar) {
                            if (ar.failed()) {
                                m_logger.error("Failed to deploy verticle: {}",
                                        verticle, ar.cause());
                                message.reply(Commands.result(
                                                ERR_DEPLOY_FAILED,
                                                REASON_DEPLOY_FAILED + ar.cause()));
                            } else {
                                m_logger.info("Successfully deployed verticle: {} ({})",
                                        verticle, ar.result());
                                message.reply(Commands.result(
                                                RET_DEPLOYMENT_ID,
                                                ar.result()));
                            }
                        }
                    }
            );
        }
    }

    public static final class UndeployVerticleCommand extends AbstractCommand {

        private AbstractActivator m_executor;
        private final Logger m_logger = LoggerFactory.getLogger(UndeployVerticleCommand.class);

        public UndeployVerticleCommand(final String verticle) {
            super(Commands.jsonCommand(CMD_UNDEPLOY_VERTICLE, verticle));
        }

        public UndeployVerticleCommand(
                final AbstractActivator executor,
                final JsonObject json) {

            super(json);
            m_executor = executor;
        }

        @Override
        public void execute(final Message message) {
            // Call verticle undeploy method
            JsonArray args = getArgs();
            final String verticle = (String) args.get(0);

            m_executor.undeployVerticle(
                    verticle,
                    new Handler<AsyncResult<Void>>() {

                        @Override
                        public void handle(AsyncResult<Void> ar) {
                            if (ar.failed()) {
                                m_logger.error("Failed to undeploy verticle: {}",
                                        verticle, ar.cause());
                                message.reply(Commands.result(
                                                ERR_UNDEPLOY_FAILED,
                                                REASON_UNDEPLOY_FAILED + ar.cause()));
                            } else {
                                message.reply(RESULT_OK);
                            }
                        }
                    }
            );
        }
    }

    public static class DeploymentHandler
            implements Handler<Message<JsonObject>> {

        private final String m_verticle;

        public DeploymentHandler(final String verticle) {
            m_verticle = verticle;
        }

        public String getVerticle() {
            return m_verticle;
        }

        @Override
        public void handle(final Message<JsonObject> message) {
            Commands.Result result = new Commands.Result(message.body());
            if (!result.isReturnCode(RET_DEPLOYMENT_ID)) {
                handleFailure(result);
            } else {
                String deploymentId = result.getReason();
                handleSuccess(deploymentId);
            }
        }

        public void handleFailure(final Commands.Result result) {
            throw new IllegalStateException(result.getReason());
        }

        public void handleSuccess(final String deploymentId) {
            // Do nothing by default...
        }
    }

    public static class UndeploymentHandler
            implements Handler<Message<JsonObject>> {

        private final String m_verticle;

        public UndeploymentHandler(final String verticle) {
            m_verticle = verticle;
        }

        public String getVerticle() {
            return m_verticle;
        }

        @Override
        public void handle(final Message<JsonObject> message) {
            Commands.Result result = new Commands.Result(message.body());
            if (!result.isOk()) {
                handleFailure(result);
            } else {
                handleSuccess();
            }
        }

        public void handleFailure(final Commands.Result result) {
            throw new IllegalStateException(result.getReason());
        }

        public void handleSuccess() {
            // Do nothing by default...
        }
    }
}
