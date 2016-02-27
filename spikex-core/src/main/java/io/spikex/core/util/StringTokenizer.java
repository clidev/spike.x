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
package io.spikex.core.util;

import com.google.common.base.Preconditions;

/**
 * StringTokenizer based on <a
 * href="https://gist.github.com/jskorpan/1056060">https://gist.github.com/jskorpan/1056060</a>.
 *
 * @author jskorpan, cli
 */
public class StringTokenizer {

    private static final ThreadLocal<String[]> m_temp = new ThreadLocal();
    private static final ThreadLocal<Integer> m_remaining = new ThreadLocal();

    public static String[] tokenize(
            final String str,
            final String delim) {

        // Sanity checks
        Preconditions.checkArgument(str != null && str.length() > 0,
                "str is null or empty");
        Preconditions.checkArgument(delim != null && delim.length() > 0,
                "delim is null or empty");

        String[] temp = m_temp.get();
        int tempLen = (str.length() / 2) + 2;
        int delimLen = delim.length();

        if (temp == null || temp.length < tempLen) {
            temp = new String[tempLen];
            m_temp.set(temp);
        }

        int wordCount = 0;
        int i = 0;
        int j = str.indexOf(delim);

        while (j >= 0) {
            temp[wordCount++] = str.substring(i, j);
            i = j + delimLen;
            j = str.indexOf(delim, i);
        }

        //temp[wordCount++] = str.substring(i);
        m_remaining.set(str.substring(i).getBytes().length);

        String[] result = new String[wordCount];
        System.arraycopy(temp, 0, result, 0, wordCount);
        return result;
    }
    
    public static int remainingBytes() {
        return m_remaining.get();
    }
}
