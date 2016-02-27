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
package io.spikex.core.util;

import io.spikex.core.util.process.ChildProcess;
import io.spikex.core.util.process.LineReader;
import io.spikex.core.util.process.ProcessExecutor;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Helper class to resolve host OS and name.
 * <p>
 * Based on <a
 * href="http://www.mkyong.com/java/how-to-detect-os-in-java-systemgetpropertyosname">OSValidator</a>
 * by mkyong.
 *
 * @author cli
 */
public class HostOs {

    private static final String OPERATING_SYSTEM
            = System.getProperty("os.name").toLowerCase();

    private static final String OPERATING_SYSTEM_FULL
            = System.getProperty("os.name")
            + " " + System.getProperty("os.version")
            + " (" + System.getProperty("os.arch") + ")";

    private static final String OS_ARCH
            = System.getProperty("os.arch");

    private static final String HOSTNAME
            = resolveHostName();

    private static final List<InetAddress> ADDRESSES_IP
            = resolveHostAddresses();

    public static boolean is64bit() {
        return OS_ARCH.startsWith("x86_64") || OS_ARCH.startsWith("amd64");
    }

    public static boolean isWindows() {
        return (OPERATING_SYSTEM.contains("win"));
    }

    public static boolean isLinux() {
        return (OPERATING_SYSTEM.contains("linux"));
    }

    public static boolean isMac() {
        return (OPERATING_SYSTEM.contains("mac"));
    }

    public static boolean isUnix() {
        return (OPERATING_SYSTEM.contains("nix")
                || OPERATING_SYSTEM.contains("nux")
                || OPERATING_SYSTEM.contains("ix")
                || OPERATING_SYSTEM.contains("bsd")
                || OPERATING_SYSTEM.contains("os x"));
    }

    public static boolean isSolaris() {
        return (OPERATING_SYSTEM.contains("sunos"));
    }

    public static boolean isFreeBSD() {
        return (OPERATING_SYSTEM.contains("freebsd"));
    }

    public static boolean isNetBSD() {
        return (OPERATING_SYSTEM.contains("netbsd"));
    }

    public static boolean isOpenBSD() {
        return (OPERATING_SYSTEM.contains("openbsd"));
    }

    public static String operatingSystem() {
        return OPERATING_SYSTEM;
    }

    public static String operatingSystemFull() {
        return OPERATING_SYSTEM_FULL;
    }

    /**
     * Returns the resolved the host name.
     *
     * @return the name of the host
     */
    public static String hostName() {
        return HOSTNAME;
    }

    /**
     * Returns the resolved IPv4 and IPv6 host addresses.
     *
     * @return the active addresses of the host
     */
    public static List<InetAddress> hostAddresses() {
        return ADDRESSES_IP;
    }

    /**
     * Resolve the host name. Please be aware that this might be an expensive
     * operation.
     *
     * @return the name of the host
     */
    private static String resolveHostName() {
        String name;
        if (isWindows()) {
            name = System.getenv("COMPUTERNAME");
        } else {
            name = System.getenv("HOSTNAME");
        }
        if (isUnix()
                && (name == null || name.length() == 0)) {

            try {
                LineReader lines = new LineReader();
                ChildProcess cmd = new ProcessExecutor()
                        .command("/bin/sh", "-c", "sleep 0.1 && uname -n")
                        .timeout(1500L, TimeUnit.MILLISECONDS)
                        .handler(lines)
                        .start();

                cmd.waitForExit();
                name = lines.getFirstLine();

            } catch (InterruptedException e) {
                // Do nothing...
            }
        }
        return (name != null ? name.toLowerCase() : "");
    }

    /**
     * Resolve the host addresses. Please be aware that this might be an
     * expensive operation.
     *
     * @return the active addresses of the host
     */
    private static List<InetAddress> resolveHostAddresses() {
        List<InetAddress> addresses = new ArrayList();
        List<InetAddress> inet6Addresses = new ArrayList();
        Enumeration<NetworkInterface> ifaces;
        try {
            ifaces = NetworkInterface.getNetworkInterfaces();
            while (ifaces.hasMoreElements()) {
                NetworkInterface nic = ifaces.nextElement();
                if (nic.isUp() && !nic.isLoopback()) {
                    List<InterfaceAddress> addrs = nic.getInterfaceAddresses();
                    for (final InterfaceAddress addr : addrs) {
                        InetAddress inetAddr = addr.getAddress();
                        if (!inetAddr.isAnyLocalAddress()
                                && !inetAddr.isMulticastAddress()) {
                            if (inetAddr instanceof Inet6Address) {
                                if (!inet6Addresses.contains(inetAddr)) {
                                    inet6Addresses.add(inetAddr);
                                }
                            } else {
                                if (!addresses.contains(inetAddr)) {
                                    addresses.add(inetAddr);
                                }
                            }
                        }
                    }
                }
            }
        } catch (SocketException e) {
            // Do nothing...
        }
        // Combine addresses (IPv4 addresses first...)
        addresses.addAll(inet6Addresses);
        return addresses;
    }
}
