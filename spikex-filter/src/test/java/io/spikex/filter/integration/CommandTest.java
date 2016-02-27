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
package io.spikex.filter.integration;

import static io.spikex.core.AbstractFilter.CONF_KEY_CHAIN_NAME;
import static io.spikex.core.AbstractFilter.CONF_KEY_DEST_ADDRESS;
import static io.spikex.core.AbstractFilter.CONF_KEY_SOURCE_ADDRESS;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CLUSTER_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_CONF_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_DATA_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_HOME_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_LOCAL_ADDRESS;
import static io.spikex.core.AbstractVerticle.CONF_KEY_NODE_NAME;
import static io.spikex.core.AbstractVerticle.CONF_KEY_TMP_PATH;
import static io.spikex.core.AbstractVerticle.CONF_KEY_UPDATE_INTERVAL;
import static io.spikex.core.AbstractVerticle.CONF_KEY_USER;
import io.spikex.core.util.HostOs;
import io.spikex.filter.Command;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;

/**
 * Command tester.
 * <p>
 * @author cli
 */
public class CommandTest extends TestVerticle {

    private static final String EVENT_SRC_ADDRESS = "command-src";
    private static final String EVENT_DEST_ADDRESS = "command-dest";

    private final Logger m_logger = LoggerFactory.getLogger(CommandTest.class);

    @Test
    public void testInputEmptyLineCommand() {
        final JsonObject config = createEmptyLineConfig();
        execInputCommand(config, true);
    }

    @Test
    public void testInputDirCommand() {
        final JsonObject config = createDirConfig();
        execInputCommand(config, false);
    }

    @Test
    public void testOutput() {
        final JsonObject config = createPipedOutputConfig();
        execOutputCommand(config);
    }

    private void execInputCommand(
            final JsonObject config,
            final boolean stopAfterDeploy) {

        // Command launched periodically
        config.putNumber(CONF_KEY_UPDATE_INTERVAL, 1500L);

        vertx.eventBus().registerLocalHandler(EVENT_DEST_ADDRESS,
                new Handler<Message<JsonObject>>() {

                    @Override
                    public void handle(final Message<JsonObject> event) {
                        JsonObject evn = event.body();
                        //
                        m_logger.info("Received event: {}", evn);
                        //
                        // Stop test
                        //
                        VertxAssert.testComplete();
                    }
                });

        container.deployVerticle(Command.class.getName(), config, 1,
                new AsyncResultHandler<String>() {
                    @Override
                    public void handle(final AsyncResult<String> ar) {
                        if (ar.failed()) {
                            m_logger.error("Failed to deploy verticle", ar.cause());
                            Assert.fail();
                        }
                        if (stopAfterDeploy) {
                            try {
                                Thread.sleep(3000L);
                            } catch (InterruptedException e) {
                                m_logger.error("Sleep interrupted", e);
                            }
                            //
                            // Stop test
                            //
                            VertxAssert.testComplete();
                        }
                    }
                });
    }

    private void execOutputCommand(final JsonObject config) {

        container.deployVerticle(Command.class.getName(), config, 1,
                new AsyncResultHandler<String>() {
                    @Override
                    public void handle(final AsyncResult<String> ar) {
                        if (ar.succeeded()) {
                            // Emit event
                            JsonObject event = EventCreator.create("Command");
                            vertx.eventBus().publish(EVENT_SRC_ADDRESS, event);
                            //
                            // Stop test
                            //
                            VertxAssert.testComplete();
                        } else {
                            m_logger.error("Failed to deploy verticle", ar.cause());
                            Assert.fail();
                        }
                    }
                });
    }

    private JsonObject createBaseConfig() {
        JsonObject config = new JsonObject();
        config.putString(CONF_KEY_CHAIN_NAME, "command-test");
        config.putString(CONF_KEY_LOCAL_ADDRESS, "my-local-address");
        config.putString(CONF_KEY_SOURCE_ADDRESS, EVENT_SRC_ADDRESS);
        config.putString(CONF_KEY_DEST_ADDRESS, EVENT_DEST_ADDRESS);
        config.putString(CONF_KEY_NODE_NAME, "node-name");
        config.putString(CONF_KEY_CLUSTER_NAME, "cluster-name");
        config.putString(CONF_KEY_HOME_PATH, "build");
        config.putString(CONF_KEY_CONF_PATH, "build");
        config.putString(CONF_KEY_DATA_PATH, "build");
        config.putString(CONF_KEY_TMP_PATH, "build");
        config.putString(CONF_KEY_USER, "spikex");
        m_logger.info("Host operating system: {}", HostOs.operatingSystemFull());
        return config;
    }

    private JsonObject createEmptyLineConfig() {

        // Command configuration
        final JsonObject config = createBaseConfig();

        if (HostOs.isWindows()) {
            config.putString("command", "cmd");
            JsonArray args = new JsonArray();
            args.addString("/c");
            args.addString("echo.");
            config.putArray("args", args);
        } else {
            config.putString("command", "echo");
        }

        config.putString("work-dir", "build");

        return config;
    }

    private JsonObject createDirConfig() {

        // Command configuration
        final JsonObject config = createBaseConfig();

        if (HostOs.isWindows()) {
            config.putString("encoding", "Windows-1252");
            config.putNumber("skip-lines-start", 5);
            config.putNumber("skip-lines-end", 3);
            config.putString("command", "cmd");
            JsonArray args = new JsonArray();
            args.addString("/c");
            args.addString("dir");
            args.addString("/-c");
            args.addString("/q");
            args.addString("/a:d");
            args.addString("/t:w");
            config.putArray("args", args);
        } else if (HostOs.isUnix()) {
            config.putNumber("skip-lines-start", 1);
            config.putString("command", "/bin/ls");
            config.putArray("args", new JsonArray("[ \"-l\", \"-a\" ]"));
        } else {
            Assert.fail("Unsupported operating system: " + HostOs.operatingSystem());
        }
        config.putString("work-dir", "build");

        // Output format
        JsonObject outFormat = new JsonObject();
        outFormat.putString("type", "dsv");
        outFormat.putString("delimiter", "<SPACE>");
        outFormat.putString("line-terminator", System.lineSeparator());
        outFormat.putBoolean("trunc-dup-delimiters", true);

        // Mapping
        JsonObject mapping = new JsonObject();
        JsonArray fields = new JsonArray();
        if (HostOs.isWindows()) {
            fields.addArray(new JsonArray("[ \"modified\" ]"));
            fields.addArray(new JsonArray("[ \"modified\", \"StrReplace(([0-9:]+), $1)\" ]"));
            fields.addArray(new JsonArray("[ \"dir\" ]"));
            fields.addArray(new JsonArray("[ \"owner\" ]"));
            fields.addArray(new JsonArray("[ \"file\" ]"));
        } else {
            fields.addArray(new JsonArray("[ \"permission\" ]"));
            fields.addArray(new JsonArray("[ \"inodes\", \"Long\" ]"));
            fields.addArray(new JsonArray("[ \"owner\" ]"));
            fields.addArray(new JsonArray("[ \"group\" ]"));
            fields.addArray(new JsonArray("[ \"size\", \"Long\" ]"));
            fields.addArray(new JsonArray("[ \"modified\" ]"));
            fields.addArray(new JsonArray("[ \"modified\", \"StrReplace(([0-9]+), $1)\" ]"));
            fields.addArray(new JsonArray("[ \"modified\", \"StrReplace(([0-9]+\\\\:[0-9]+), $1)\" ]"));
            fields.addArray(new JsonArray("[ \"file\" ]"));
        }
        mapping.putArray("fields", fields);
        outFormat.putObject("mapping", mapping);
        config.putObject("output-format", outFormat);

        return config;
    }

    private JsonObject createPipedOutputConfig() {

        // Command configuration
        final JsonObject config = createBaseConfig();

        JsonArray args = new JsonArray();
        Path path = Paths.get("build").toAbsolutePath();
        String outFile = path.resolve("cmd-output.txt").toString();

        if (HostOs.isWindows()) {
            config.putString("command", "cmd");
            args.addString("/c");
            args.addString("more");
            args.addString(">");
            args.addString(outFile);
        } else {
            config.putString("command", "/bin/sh");
            args.addString("-c");
            args.addString("less > " + outFile);
        }
        config.putArray("args", args);
        config.putString("work-dir", "build");
        config.putBoolean("line-reader", false);

        return config;
    }
}
