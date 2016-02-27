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

import com.google.common.base.Strings;
import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Contains simple convenience methods for parsing numbers.
 *
 * @version $Revision: 316 $
 * @author cli
 */
public final class Numbers {

    private static final String TAG_TRUE = "TRUE";
    private static final String TAG_FALSE = "FALSE";
    //
    // These were taken from the Java API documentation
    // See: http://docs.oracle.com/javase/6/docs/api/java/lang/Double.html
    //
    // Chose the more correct version, than the faster "Jon Skeet" one:
    // http://stackoverflow.com/questions/359625/most-elegant-way-to-detect-if-a-string-is-a-number#359632
    //
    private static final String Digits = "(\\p{Digit}+)";
    private static final String HexDigits = "(\\p{XDigit}+)";
    private static final String OctDigits = "([01234567]+)";
    private static final String BinDigits = "([01]+)";
    // an exponent is 'e' or 'E' followed by an optionally 
    // signed decimal integer.
    private static final String Exp = "[eE][+-]?" + Digits;
    private static final String fpRegex
            = ("[\\x00-\\x20]*" + // Optional leading "whitespace"
            "[+-]?(" + // Optional sign character
            "NaN|" + // "NaN" string
            "Infinity|"
            + // "Infinity" string
            // A decimal floating-point string representing a finite positive
            // number without a leading sign has at most five basic pieces:
            // Digits . Digits ExponentPart FloatTypeSuffix
            // 
            // Since this method allows integer-only strings as input
            // in addition to strings of floating-point literals, the
            // two sub-patterns below are simplifications of the grammar
            // productions from the Java Language Specification, 2nd 
            // edition, section 3.10.2.
            // Digits ._opt Digits_opt ExponentPart_opt FloatTypeSuffix_opt
            "(((" + Digits + "(\\.)?(" + Digits + "?)(" + Exp + ")?)|"
            + // . Digits ExponentPart_opt FloatTypeSuffix_opt
            "(\\.(" + Digits + ")(" + Exp + ")?)|"
            + // Hexadecimal strings
            "(("
            + // 0[xX] HexDigits ._opt BinaryExponent FloatTypeSuffix_opt
            "(0[xX]" + HexDigits + "(\\.)?)|"
            + // 0[xX] HexDigits_opt . HexDigits BinaryExponent FloatTypeSuffix_opt
            "(0[xX]" + HexDigits + "?(\\.)" + HexDigits + ")"
            + ")[pP][+-]?" + Digits + "))"
            + "[fFdD]?))"
            + "[\\x00-\\x20]*");// Optional trailing "whitespace"
    //
    private static final String decRegex = "[+-]?(([,\\.]"
            + Digits + ")|(" + Digits + "[,\\.]))" + Digits;
    //
    private static final String intRegex = "[+-]?" + Digits;
    private static final String hexRegex = "[+-]?" + HexDigits;
    private static final String octRegex = "[+-]?" + OctDigits;
    private static final String binRegex = "[+-]?" + BinDigits;
    //
    private static final Pattern REGEXP_FP = Pattern.compile(fpRegex);
    private static final Pattern REGEXP_DEC = Pattern.compile(decRegex);
    private static final Pattern REGEXP_INT = Pattern.compile(intRegex);
    private static final Pattern REGEXP_HEX = Pattern.compile(hexRegex);
    private static final Pattern REGEXP_OCT = Pattern.compile(octRegex);
    private static final Pattern REGEXP_BIN = Pattern.compile(binRegex);

    /**
     * Prevent instantiation of number utility.
     */
    private Numbers() {
    }

    /**
     * Returns true if the given string represents a float or double number
     * (floating-point value). This method uses the regexp presented in the Java
     * API of <a
     * href="http://docs.oracle.com/javase/6/docs/api/java/lang/Double.html">Double</a>
     * to parse a floating-point value.
     *
     * @param str the string to parse
     * @return true if the given string can be converted into a float or double
     */
    public static boolean isFloatingPoint(final String str) {
        boolean match = false;
        //
        // Sanity check
        //
        if (str == null) {
            throw new IllegalArgumentException("str is null");
        }
        //
        Matcher m = REGEXP_FP.matcher(str);
        if (m.matches()) {
            match = true;
        }
        return match;
    }

    /**
     * Return true if the given string represents a simple decimal number.
     * Please note that only a dot or comma is considered as the decimal
     * separator. Spaces are not allowed between digits and literals.
     * <p>
     * The following strings are considered simple decimal numbers:
     * <ul>
     * <li>1.921</li>
     * <li>-1223.12</li>
     * <li>1,231</li>
     * <li>.0123</li>
     * <li>1234567,89</li>
     * </ul>
     *
     * @param str the string to parse
     * @return true if decimal number
     */
    public static boolean isDecimal(final String str) {
        boolean match = false;
        //
        // Sanity check
        //
        if (str == null) {
            throw new IllegalArgumentException("str is null");
        }
        Matcher m = REGEXP_DEC.matcher(str);
        if (m.matches()) {
            match = true;
        }
        return match;
    }

    /**
     * Return true if the given string represents a simple integer number.
     * Spaces are not allowed between digits and literals.
     * <p>
     * The following strings are considered simple integer numbers:
     * <ul>
     * <li>1921</li>
     * <li>-1223</li>
     * <li>1231</li>
     * <li>1234567</li>
     * <li>
     * </ul>
     *
     * @param str the string to parse
     * @return true if the given string can be converted into an integer
     */
    public static boolean isInteger(final String str) {
        boolean match = false;
        //
        // Sanity check
        //
        if (str == null) {
            throw new IllegalArgumentException("str is null");
        }
        //
        Matcher m = REGEXP_INT.matcher(str);
        if (m.matches()) {
            match = true;
        }
        return match;
    }

    public static boolean isHex(final String str) {
        boolean match = false;
        //
        // Sanity check
        //
        if (str == null) {
            throw new IllegalArgumentException("str is null");
        }
        //
        Matcher m = REGEXP_HEX.matcher(str);
        if (m.matches()) {
            match = true;
        }
        return match;
    }

    public static boolean isOctal(final String str) {
        boolean match = false;
        //
        // Sanity check
        //
        if (str == null) {
            throw new IllegalArgumentException("str is null");
        }
        //
        Matcher m = REGEXP_OCT.matcher(str);
        if (m.matches()) {
            match = true;
        }
        return match;
    }

    public static boolean isBinary(final String str) {
        boolean match = false;
        //
        // Sanity check
        //
        if (str == null) {
            throw new IllegalArgumentException("str is null");
        }
        //
        Matcher m = REGEXP_BIN.matcher(str);
        if (m.matches()) {
            match = true;
        }
        return match;
    }

    public static boolean isBoolean(final String str) {
        boolean match = false;
        //
        // Sanity check
        //
        if (str == null) {
            throw new IllegalArgumentException("str is null");
        }
        //
        if (TAG_TRUE.equalsIgnoreCase(str)
                || TAG_FALSE.equalsIgnoreCase(str)) {
            match = true;
        }
        return match;
    }

    public static Boolean parseBoolean(final String str) throws ParseException {
        //
        // Sanity checks
        //
        if (str == null || str.length() == 0) {
            throw new ParseException("Could not parse null or empty string "
                    + "as boolean", 0);
        }
        Boolean b = null;
        if (TAG_TRUE.equalsIgnoreCase(str)) {
            b = Boolean.TRUE;
        } else if (TAG_FALSE.equalsIgnoreCase(str)) {
            b = Boolean.FALSE;
        } else {
            throw new ParseException("Given string does not represent"
                    + " a boolean: " + str, 0);
        }
        return b;
    }

    public static Number parseNumber(final String strValue) throws ParseException {
        //
        // Sanity check
        //
        if (Strings.isNullOrEmpty(strValue)) {
            throw new ParseException("Could not parse null or empty string "
                    + "as number", 0);
        }
        Number value = null;
        if (isDecimal(strValue)) {
            value = Double.valueOf(strValue);
        } else {
            // Assume long
            value = Long.valueOf(strValue);
        }
        return value;
    }
}
