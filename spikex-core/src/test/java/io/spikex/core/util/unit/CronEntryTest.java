/**
 *
 * Copyright (c) 2015 NG Modular Oy.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.spikex.core.util.unit;

import io.spikex.core.util.CronEntry;
import org.junit.Assert;
import org.junit.Test;


/**
 * DefaultScheduler test driver
 *
 * @version $Revision$
 * @author $Author$
 */
public class CronEntryTest {

    /**
     *   field        allowed values
     *   -----        --------------
     *   minute       0-59
     *   hour         0-23
     *   day of month 1-31
     *   month        1-12 (or names, see below)
     *   day of week  0-7 (0 or 7 is Sun, or use names)
     *   timezone     any valid timezone name
     */
    @Test
    public void testCronEntryParseOk() {
        //
        // * * * * *
        //
        CronEntry entry = parseEntry("* * * * * Europe/Helsinki");
        Assert.assertArrayEquals(entry.getMonths(), new int[]{-1});
        Assert.assertArrayEquals(entry.getDays(), new int[]{-1});
        Assert.assertArrayEquals(entry.getDows(), new int[]{-1});
        Assert.assertArrayEquals(entry.getHours(), new int[]{-1});
        Assert.assertArrayEquals(entry.getMinutes(), new int[]{-1});
        Assert.assertEquals("Europe/Helsinki", entry.getTimezone());
        //
        // */1 */2 */3 */4 */5
        //
        entry = parseEntry("*/1 */2 */3 */4 */5");
        Assert.assertArrayEquals(entry.getMonths(), new int[]{1, 5, 9});
        Assert.assertArrayEquals(entry.getDays(), new int[]{1, 4, 7, 10, 13, 16, 19, 22, 25, 28});
        Assert.assertArrayEquals(entry.getDows(), new int[]{1});
        Assert.assertArrayEquals(entry.getHours(), new int[]{0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22});
        Assert.assertArrayEquals(entry.getMinutes(), new int[]{-1});
        Assert.assertEquals("UTC", entry.getTimezone());
        //
        // 5,10,9-20 */11 1-31 1-5,Jun,8-12 Mon,TUE,Sun
        //
        entry = parseEntry("5,10,9-20 */11 1-31 1-5,Jun,8-12 Mon,TUE,Sun");
        Assert.assertArrayEquals(entry.getMonths(), new int[]{1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12});
        Assert.assertArrayEquals(entry.getDays(), new int[]{-1});
        Assert.assertArrayEquals(entry.getDows(), new int[]{1, 2, 7});
        Assert.assertArrayEquals(entry.getHours(), new int[]{0, 11});
        Assert.assertArrayEquals(entry.getMinutes(), new int[]{5, 10, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20});
        Assert.assertEquals("UTC", entry.getTimezone());
        //
        // 0-59\t   0-23/4    31-31/1   1-12/2   1-7/7
        //
        entry = parseEntry("0-59\t   0-23/4    31-31/1   1-12/2   1-7/7");
        Assert.assertArrayEquals(entry.getMonths(), new int[]{1, 3, 5, 7, 9, 11});
        Assert.assertArrayEquals(entry.getDays(), new int[]{31});
        Assert.assertArrayEquals(entry.getDows(), new int[]{1});
        Assert.assertArrayEquals(entry.getHours(), new int[]{0, 4, 8, 12, 16, 20});
        Assert.assertArrayEquals(entry.getMinutes(), new int[]{-1});
        Assert.assertEquals("UTC", entry.getTimezone());
        //
        // 59,12,1 2,23,1,0-23 1,2,10,1 1,12 0,7
        //
        entry = parseEntry("59,12,1 2,23,0-23,1,0-23 1,2,10,1 1,12 1,7");
        Assert.assertArrayEquals(entry.getMonths(), new int[]{1, 12});
        Assert.assertArrayEquals(entry.getDays(), new int[]{1, 2, 10});
        Assert.assertArrayEquals(entry.getDows(), new int[]{1, 7});
        Assert.assertArrayEquals(entry.getHours(), new int[]{-1});
        Assert.assertArrayEquals(entry.getMinutes(), new int[]{59, 12, 1});
        Assert.assertEquals("UTC", entry.getTimezone());
        //
        // */60 */24 */31 jan,feb,Mar,apR,MAY,jun,JuL,Aug,Sep,Oct,Nov,Dec tue,mon,wed,suN,SAT,fri,Thu
        //
        entry = parseEntry("*/60 */24 */31 jan,feb,Mar,apR,MAY,jun,JuL,Aug,Sep,Oct,Nov,Dec tue,mon,wed,suN,SAT,fri,Thu");
        Assert.assertArrayEquals(entry.getMonths(), new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12});
        Assert.assertArrayEquals(entry.getDays(), new int[]{1});
        Assert.assertArrayEquals(entry.getDows(), new int[]{2, 1, 3, 7, 6, 5, 4});
        Assert.assertArrayEquals(entry.getHours(), new int[]{0});
        Assert.assertArrayEquals(entry.getMinutes(), new int[]{0});
        Assert.assertEquals("UTC", entry.getTimezone());
    }

    @Test
    public void testCronEntryParseNok() {
        try {
            CronEntry.create("");
            assert false : "parse did not blow up when given an empty string";
        } catch (Exception e) {
            // OK
        }
        try {
            CronEntry.create("* * * *");
            assert false : "parse did not blow up when given too few fields";
        } catch (Exception e) {
            // OK
        }
        try {
            CronEntry.create("1-b * * * *");
            assert false : "parse did not blow up when given an invalid minute field";
        } catch (Exception e) {
            // OK
        }
        try {
            CronEntry.create("*/3 12-15/= * * *");
            assert false : "parse did not blow up when given an invalid hour field";
        } catch (Exception e) {
            // OK
        }
        try {
            CronEntry.create("1,3,4-5 22 1-20,24,*/2 * *");
            assert false : "parse did not blow up when given an invalid day field";
        } catch (Exception e) {
            // OK
        }
        try {
            CronEntry.create("* * * */0 *");
            assert false : "parse did not blow up when given an invalid month field";
        } catch (Exception e) {
            // OK
        }
        try {
            CronEntry.create("*/78 * * * *");
            assert false : "parse did not blow up when given a too big step value";
        } catch (Exception e) {
            // OK
        }
        try {
            CronEntry.create("* * * * blue");
            assert false : "parse did not blow up when given an invalid dow field";
        } catch (Exception e) {
            // OK
        }
        try {
            CronEntry.create("* 0-1000 * * *");
            assert false : "parse did not blow up when given a too big range";
        } catch (Exception e) {
            // OK
        }
        try {
            CronEntry.create("* * 0 * *");
            assert false : "parse did not blow up when given a too small single value";
        } catch (Exception e) {
            // OK
        }
        try {
            CronEntry.create("* * * * * Mars/Venus");
            assert false : "parse did not blow up when given an unknown timezone";
        } catch (Exception e) {
            // OK
        }
    }

    private CronEntry parseEntry(String def) {
        System.out.println(def);
        CronEntry entry = CronEntry.create(def);
        System.out.println(entry);
        return entry;
    }
}
