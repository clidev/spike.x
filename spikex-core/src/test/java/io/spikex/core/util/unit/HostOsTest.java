/**
 *
 * Copyright (c) 2015 NG Modular Oy.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.spikex.core.util.unit;

import io.spikex.core.util.HostOs;
import java.net.InetAddress;
import java.util.List;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test host info.
 *
 * @author cli
 */
public class HostOsTest {
    
    private final Logger m_logger = LoggerFactory.getLogger(HostOsTest.class);
    
    @Test
    public void testHostName() {
        if (HostOs.isFreeBSD()
                || HostOs.isNetBSD()
                || HostOs.isOpenBSD()
                || HostOs.isLinux()
                || HostOs.isMac()
                || HostOs.isSolaris()
                || HostOs.isWindows()) {
            String hostName = HostOs.hostName();
            m_logger.info("Host name: {}", hostName);
            Assert.assertNotNull(hostName);
            Assert.assertTrue(hostName.length() > 0);
        }
    }
    
    @Test
    public void testHostAddresses() {
        if (HostOs.isFreeBSD()
                || HostOs.isNetBSD()
                || HostOs.isOpenBSD()
                || HostOs.isLinux()
                || HostOs.isMac()
                || HostOs.isSolaris()
                || HostOs.isWindows()) {
            List<InetAddress> addresses = HostOs.hostAddresses();
            m_logger.info("Host addresses: {}", addresses);
            Assert.assertTrue(addresses.size() > 0);
        }
    }
}
