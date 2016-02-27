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

import dk.brics.automaton.Automaton;
import dk.brics.automaton.RegExp;
import java.util.Objects;

/**
 * Represents a regular expression. This class is a simple wrapper around the
 * automaton provided by the <a
 * href="http://www.brics.dk/automaton">dk.brics.automaton</a> package.
 * 
 * "There is no support for capturing groups"
 * 
 * @author cli
 */
public final class Pattern implements Comparable<Pattern> {

    private final String m_regexp;
    private Automaton m_automaton;

    /**
     * Creates a new pattern based on the given regular expression. Please see
     * <a
     * href="http://www.brics.dk/automaton/doc/index.html?dk/brics/automaton/RegExp.html">dk.brics.automaton.RegExp</a>
     * for the syntax.
     * <p>
     * This class is not thread-safe.
     *
     * @param regexp the regular expression
     */
    public Pattern(final String regexp) {
        m_regexp = regexp;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if ((obj == null) || (obj.getClass() != this.getClass())) {
            return false;
        }
        Pattern tmp = (Pattern) obj;
        return m_regexp.equals(tmp.getRegexp());
    }

    @Override
    public int hashCode() {
        return 203 + Objects.hashCode(m_regexp);
    }

    @Override
    public int compareTo(final Pattern pattern) {
        if (pattern == this) {
            return 0;
        }
        String tmp = pattern.getRegexp();
        return m_regexp.compareTo(tmp);
    }

    /**
     * Returns the original regular expression that was used to create this
     * pattern.
     *
     * @return the regular expression
     */
    public String getRegexp() {
        return m_regexp;
    }

    /**
     * Returns true if the regular expression has been compiled into a
     * deterministic automaton.
     *
     * @return true if deterministic
     */
    public boolean isDeterministic() {
        return internalCompile().isDeterministic();
    }

    /**
     * Creates an automaton from the regular expression.
     */
    public void compile() {
        internalCompile();
    }

    /**
     * Runs the automaton on the given string. Returns true if the regular
     * expression represented by this pattern matches.
     *
     * @param str the input string
     * @return true if the regular expression matches the given string
     */
    public boolean matches(final String str) {
        Automaton am = internalCompile();
        return am.run(str);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Pattern[");
        sb.append(hashCode());
        sb.append("] regexp: ");
        sb.append(getRegexp());
        return sb.toString();
    }

    private Automaton internalCompile() {
        Automaton am = m_automaton;
        if (am == null) {
            RegExp re = new RegExp(getRegexp());
            am = re.toAutomaton(true);
            am.minimize();
            m_automaton = am;
        }
        return am;
    }
}
