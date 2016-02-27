/**
 *
 * Copyright (c) 2016 NG Modular Oy.
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

import io.spikex.core.util.StringReplace;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import junit.framework.Assert;
import org.junit.Test;

/**
 * Throttle tester.
 *
 * @author cli
 */
public class StringReplaceTest {

    private static final String TEST_STRING_ORIG
            = "Even today I am astonished at the startling effect which the\n"
            + "contemplation of that miniature produced upon me, and how I remained\n"
            + "in ecstasy, scarcely breathing, devouring the portrait with my eyes. I\n"
            + "had already seen here and there prints representing beautiful women.\n"
            + "It often happened that in the illustrated papers, in the mythological\n"
            + "engravings of our dining-room, or in a shop-window, that a beautiful\n"
            + "face, or a harmonious and graceful figure attracted my precociously\n"
            + "artistic gaze. But the miniature encountered in my aunt's drawer,\n"
            + "apart from its great beauty, appeared to me as if animated by a subtle\n"
            + "and vital breath; you could see it was not the caprice of a painter,\n"
            + "but the image of a real and actual person of flesh and blood. The warm\n"
            + "and rich tone of the tints made you surmise that the blood was tepid\n"
            + "beneath that mother-of-pearl skin. The lips were slightly parted to\n"
            + "disclose the enameled teeth; and to complete the illusion there ran\n"
            + "round the frame a border of natural hair, chestnut in color, wavy and\n"
            + "silky, which had grown on the temples of the original.";

    private static final String TEST_STRING_RESULT
            = "Even today I am astonished at the startling effect which the\n"
            + "contemplation of that miniature produced upon all of us, and how I remained\n"
            + "in ecstasy, scarcely breathing, devouring the portrait with my eyes. I\n"
            + "had already seen here and there prints representing beautiful woall of usn.\n"
            + "It often happened that in the illustrated papers, in the mythological\n"
            + "engravings of our dining-room, or in a shop-window, that a beautiful\n"
            + "face, or a harmonious and graceful figure attracted my precociously\n"
            + "artistic gaze. But the miniature encountered in my aunt's drawer,\n"
            + "apart from its great beauty, appeared to all of us as if animated by a subtle\n"
            + "and vital breath; you could see it was not the caprice of a painter,\n"
            + "but the image of a real and actual person of flesh and blood. The warm\n"
            + "and rich tone of the tints made you surmise that the blood was tepid\n"
            + "beneath that mother-of-pearl skin. The lips were slightly parted to\n"
            + "disclose the enaall of usled teeth; and to complete the illusion there ran\n"
            + "round the fraall of us a border of natural hair, chestnut in color, wavy and\n"
            + "silky, which had grown on the temples of the original.";

    @Test
    public void testShortString() {
        String result = StringReplace.replace(TEST_STRING_ORIG, "me", "all of us");
        Assert.assertEquals(TEST_STRING_RESULT, result);
    }

    @Test
    public void testLongString() throws IOException {
        Path basePath = Paths.get("build/resources/test").toAbsolutePath();
        Path txtPathOrig = basePath.resolve("pg15610.txt");
        Path txtPathHis = basePath.resolve("pg15610_his.txt");

        // Original
        String txtOrig = new String(Files.readAllBytes(txtPathOrig));

        // Expected result
        String txtExpected = new String(Files.readAllBytes(txtPathHis));

        /*
         for (int i = 0; i < 100; i++) {
         String result = strReplace.replaceAll(txtOrig);
         m_logger.info("Match count: {}", strReplace.getMatchCount());
         Assert.assertEquals(txtExpected, result);
         }
         */
        String result = StringReplace.replace(txtOrig, "her", "his");
        Path txtPathHis2 = basePath.resolve("pg15610_his2.txt");
        Files.write(txtPathHis2, result.getBytes());

        for (int i = 0; i < 10000; i++) {
            result = StringReplace.replace(txtOrig, "her", "his");
            Assert.assertEquals(txtExpected, result);
        }

    }
}
