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
package io.spikex.core.util.unit;

import io.spikex.core.util.HostOs;
import io.spikex.core.util.process.ChildProcess;
import io.spikex.core.util.process.DefaultProcessHandler;
import io.spikex.core.util.process.LineReader;
import io.spikex.core.util.process.LineWriter;
import io.spikex.core.util.process.ProcessExecutor;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import junit.framework.Assert;
import org.junit.Test;

/**
 * Process tester.
 * <p>
 * @author cli
 */
public class ProcessTest {

    @Test
    public void testCommandOutput() throws InterruptedException {

        // The Linux implementation seems to have a race condition in some circumstances
        // That's why we need the sleep a little before ls
        // Sleep not needed in OS X and OpenSUSE 13.2
        String cmd = "sh";
        String[] args = {"-c", "sleep 0.3 && ls -t -1"};

        if (HostOs.isWindows()) {
            cmd = "cmd";
            args = new String[]{"/c", "dir", "/a", "/b"};
        }

        ChildProcess process = new ProcessExecutor()
                .command(cmd, args)
                .start();

        Assert.assertEquals(0, process.waitForExit());
    }

    @Test
    public void testLineReader() throws InterruptedException {

     // The Linux implementation seems to have a race condition in some circumstances
        // That's why we need the sleep a little before ls
        // Sleep not needed in OS X and OpenSUSE 13.2
        String cmd = "sh";
        String[] args = {"-c", "sleep 0.3 && ls -t -1"};

        if (HostOs.isWindows()) {
            cmd = "cmd";
            args = new String[]{"/c", "dir", "/a", "/b"};
        }

     // We have to force LineReader to call closeStdin on Linux.. hence read max 4 lines in this case...
        // LineReader reader = new LineReader(); seems to work in OS X and Windows
        LineReader reader = new LineReader(4L);
        Assert.assertTrue(reader.getFirstLine() == null);
        Assert.assertTrue(reader.getLines().length == 0);
        Assert.assertTrue(reader.getLineCount() == 0L);

        ChildProcess process = new ProcessExecutor()
                .command(cmd, args)
                .handler(reader)
                .start();

        Assert.assertEquals(0, process.waitForExit());
        Assert.assertTrue(reader.getLines().length > 0);
        Assert.assertTrue(reader.getFirstLine().length() > 0);
        Assert.assertTrue(reader.getLineCount() > 0L);
    }

    @Test
    public void testStdinStdout() throws InterruptedException {

        String cmd = "cat";
        String[] args = {"-u"};
        if (HostOs.isWindows()) {
            cmd = "cmd";
            Path busyBoxPath = Paths.get("build/resources/test/busybox.exe");
            args = new String[]{"/c", busyBoxPath.toAbsolutePath().toString(), "cat"};
        }

        ChildProcess process = new ProcessExecutor()
                .command(cmd, args)
                .timeout(5, TimeUnit.SECONDS)
                .wantWrite(true)
                .handler(new LineReader(3L) {
                    @Override
                    public void onStart(final ChildProcess process) {
                        super.onStart(process);
                        process.wantWrite();
                    }

                    @Override
                    public boolean onStdinReady(final ByteBuffer buffer) {
                        buffer.put("Hello1".getBytes());
                        buffer.put(System.lineSeparator().getBytes());
                        buffer.put("Hello2".getBytes());
                        buffer.put(System.lineSeparator().getBytes());
                        buffer.put(System.lineSeparator().getBytes());
                        buffer.flip();
                        return false;
                    }
                })
                .start();

        Assert.assertEquals(0, process.waitForExit());

        LineReader reader = process.getHandler();
        Assert.assertEquals(3, reader.getLines().length);
        Assert.assertTrue(reader.getFirstLine().length() > 0);
        Assert.assertEquals(3, reader.getLineCount());

        String[] lines = reader.getLines();
        Assert.assertEquals("Hello1", lines[0]);
        Assert.assertEquals("Hello2", lines[1]);
        Assert.assertEquals("", lines[2]);
    }

    @Test
    public void testSkipLines() throws InterruptedException {

        String cmd = "cat";
        String[] args = {"-u"};
        if (HostOs.isWindows()) {
            cmd = "cmd";
            Path busyBoxPath = Paths.get("build/resources/test/busybox.exe");
            args = new String[]{"/c", busyBoxPath.toAbsolutePath().toString(), "cat"};
        }

        ChildProcess process = new ProcessExecutor()
                .command(cmd, args)
                .timeout(5, TimeUnit.SECONDS)
                .wantWrite(true)
                .handler(new LineReader(1, 1, 3L) {

                    @Override
                    public boolean onStdinReady(final ByteBuffer buffer) {
                        buffer.put("Hello1".getBytes());
                        buffer.put(System.lineSeparator().getBytes());
                        buffer.put("Hello2".getBytes());
                        buffer.put(System.lineSeparator().getBytes());
                        buffer.put(System.lineSeparator().getBytes());
                        buffer.flip();
                        return false;
                    }
                })
                .start();

        Assert.assertEquals(0, process.waitForExit());

        LineReader reader = process.getHandler();
        int count = 1;
        Assert.assertEquals(count, reader.getLines().length);
        Assert.assertEquals(count, reader.getLineCount());

        String[] lines = reader.getLines();
        Assert.assertEquals("Hello2", lines[0]);
    }

    @Test
    public void testLineWriter() throws InterruptedException {

        String cmd = "sh";
        String[] args = {"-c", "sleep 0.1 && less"};
        if (HostOs.isWindows()) {
            cmd = "cmd";
            args = new String[]{"/c", "more"};
        }

        ChildProcess process = new ProcessExecutor()
                .command(cmd, args)
                .handler(new LineWriter() {

                    private volatile int m_counter = 0;

                    @Override
                    public boolean onWrite(final PrintWriter out) {
                        if (m_counter++ < 50) {
                            out.println("Hello from writer: " + m_counter);
                            return true;
                        } else {
                            return false;
                        }
                    }

                })
                .timeout(3000L, TimeUnit.MILLISECONDS)
                .start();

        Assert.assertEquals(0, process.waitForExit());
    }

    @Test
    public void testMissingCommand() throws InterruptedException {

        ChildProcess process = new ProcessExecutor()
                .command("NonSenseCommand22")
                .start();

        Assert.assertTrue(process.waitForExit() != 0);
    }

    @Test
    public void testTimeout() throws InterruptedException {

        String cmd = "sleep";
        String[] args = {"5"};
        if (HostOs.isWindows()) {
            cmd = "cmd";
            args = new String[]{"/c", "timeout", "5", ">", "nul"};
        }

        long tm = System.currentTimeMillis();
        ChildProcess process = new ProcessExecutor()
                .command(cmd, args)
                .timeout(1000L, TimeUnit.MILLISECONDS)
                .start();

        Assert.assertTrue(process.waitForExit() != 0);
        Assert.assertTrue((System.currentTimeMillis() - tm) < 2000L);
    }

    @Test
    public void testProcessStdin() throws Exception {

        String cmd = "cat";
        String[] args = {"-u"};
        if (HostOs.isWindows()) {
            cmd = "cmd";
            Path busyBoxPath = Paths.get("build/resources/test/busybox.exe");
            args = new String[]{"/c", busyBoxPath.toAbsolutePath().toString(), "cat"};
        }

        ChildProcess process = new ProcessExecutor()
                .command(cmd, args)
                .wantWrite(true)
                .handler(new DefaultProcessHandler() {
                    @Override
                    public boolean onStdinReady(final ByteBuffer buffer) {
                        buffer.put("Hello world!".getBytes());
                        buffer.put(System.lineSeparator().getBytes());
                        buffer.flip();
                        return false;
                    }
                })
                .start();

        Assert.assertEquals(0, process.waitForExit());
    }

    @Test
    public void testBigOutput() throws Exception {

        //
        // 1. Count lines
        //
        Path basePath = Paths.get("build/resources/test").toAbsolutePath();
        Path txtPath = basePath.resolve("pg15610.txt");

        List<String> lines = Files.readAllLines(txtPath, StandardCharsets.UTF_8);
        int count = lines.size() + 1;

        //
        // 2. Output whole file to LineReader
        //
        // Unix handles lines differently than Windows:
        // http://stackoverflow.com/questions/729692/why-should-files-end-with-a-newline
        String cmd = "cat";
        // Disable output buffering (-u)
        String[] args = {"-u", txtPath.toString(), "-"};
        if (HostOs.isWindows()) {
            cmd = "cmd";
            Path busyBoxPath = Paths.get("build/resources/test/busybox.exe");
            args = new String[]{"/c", busyBoxPath.toAbsolutePath().toString(), "cat", txtPath.toString()};
        }

        LineReader reader = new LineReader(2095); // 2095 = line count in pg15610
        ChildProcess process = new ProcessExecutor()
                .command(cmd, args)
                .handler(reader)
                .timeout(5L, TimeUnit.SECONDS)
                .start();

        Assert.assertEquals(0, process.waitForExit());
        Assert.assertEquals(count, reader.getLines().length);
        Assert.assertEquals(count, reader.getLineCount());
    }
}
