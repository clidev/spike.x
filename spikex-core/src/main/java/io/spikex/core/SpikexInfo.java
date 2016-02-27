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

import io.spikex.core.util.HostOs;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URL;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * Outputs Spike.x version and host information.
 *
 * TODO build version is required
 *
 * @author cli
 */
public class SpikexInfo {

    private static String IMPL_VERSION = "Unknown";
    private static String IMPL_DOC_URL = "Unknown";
    private static String IMPL_LICENSE = "Unknown";
    private static String IMPL_VENDOR = "Unknown";
    private static String IMPL_BUILD_JDK = "Unknown";
    private static String IMPL_BUILD_TM = "Unknown";
    private static String IMPL_BUILD_OS = "Unknown";

    static {
        String name = SpikexInfo.class.getSimpleName() + ".class";
        URL location = SpikexInfo.class.getResource(name);
        if (location != null) {
            String classPath = location.toString();
            //
            if (classPath.startsWith("jar")) {
                int pos = classPath.lastIndexOf("!") + 1;
                String manifestPath = classPath.substring(0, pos)
                        + "/META-INF/MANIFEST.MF";
                //
                try (InputStream in = new URL(manifestPath).openStream()) {
                    Manifest mf = new Manifest(in);
                    Attributes attrs = mf.getMainAttributes();
                    IMPL_VERSION = attrs.getValue("Implementation-Version");
                    IMPL_DOC_URL = attrs.getValue("Implementation-DocURL");
                    IMPL_LICENSE = attrs.getValue("Implementation-License");
                    IMPL_VENDOR = attrs.getValue("Implementation-Vendor");
                    IMPL_BUILD_JDK = attrs.getValue("Build-Jdk");
                    IMPL_BUILD_OS = attrs.getValue("Build-Platform");
                    IMPL_BUILD_TM = attrs.getValue("Built-Timestamp");
                } catch (Exception e) {
                    System.err.println("Failed to open Spike.x manifest file: "
                            + manifestPath + " error: " + e.toString());
                }
            }
        }
    }

    public static void main(final String[] args) {
        outputInfo();
    }

    public static String version() {
        return IMPL_VERSION;
    }

    public static String docUrl() {
        return IMPL_DOC_URL;
    }

    public static String license() {
        return IMPL_LICENSE;
    }

    public static String vendor() {
        return IMPL_VENDOR;
    }

    public static String buildJdk() {
        return IMPL_BUILD_JDK;
    }

    public static String buildPlatform() {
        return IMPL_BUILD_OS;
    }

    public static String buildTimestamp() {
        return IMPL_BUILD_TM;
    }

    private static void outputInfo() {

        String nl = System.getProperty("line.separator");
        StringBuilder info = new StringBuilder();
        info.append("Spike.x version \"");
        info.append(version());
        info.append("\"");
        info.append(nl);

        info.append("Spike.x doc url \"");
        info.append(docUrl());
        info.append("\"");
        info.append(nl);

        info.append("Spike.x license \"");
        info.append(license());
        info.append("\"");
        info.append(nl);

        info.append("Spike.x vendor \"");
        info.append(vendor());
        info.append("\"");
        info.append(nl);

        info.append("Spike.x build jdk \"");
        info.append(buildJdk());
        info.append("\"");
        info.append(nl);

        info.append("Spike.x build timestamp \"");
        info.append(buildTimestamp());
        info.append("\"");
        info.append(nl);

        info.append("Spike.x build platform \"");
        info.append(buildPlatform());
        info.append("\"");
        info.append(nl);

        info.append("Host operating system \"");
        info.append(HostOs.operatingSystemFull());
        info.append("\"");
        info.append(nl);

        info.append("Host name \"");
        info.append(HostOs.hostName());
        info.append("\"");
        info.append(nl);

        info.append("Host addresses:");
        info.append(nl);
        List<InetAddress> addresses = HostOs.hostAddresses();
        for (final InetAddress addr : addresses) {
            info.append("\"");
            info.append(addr.getHostAddress());
            info.append("\"");
            info.append(nl);
        }
        System.out.print(info.toString());
    }
}
