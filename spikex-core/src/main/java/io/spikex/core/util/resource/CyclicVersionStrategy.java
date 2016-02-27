/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package io.spikex.core.util.resource;

import io.spikex.core.util.IVersion;
import io.spikex.core.util.Version;

/**
 *
 * @author christofferlindevall
 */
public final class CyclicVersionStrategy implements IVersionStrategy {

    private final int m_firstVersion;
    private final int m_lastVersion;

    public CyclicVersionStrategy() {
        this(1, 2);
    }

    public CyclicVersionStrategy(final int lastVersion) {
        this(1, lastVersion);
    }

    public CyclicVersionStrategy(
            final int firstVersion,
            final int lastVersion) {
        m_firstVersion = firstVersion;
        m_lastVersion = lastVersion;
    }

    @Override
    public int getFirstVersion() {
        return m_firstVersion;
    }

    public int getLastVersion() {
        return m_lastVersion;
    }

    @Override
    public IVersion nextVersion(final IVersion version) {
        //
        // Sanity check
        //
        if (version == null) {
            throw new IllegalArgumentException("version is null");
        }
        //
        int nextVersion = version.getSequence();
        if (nextVersion >= getLastVersion()) {
            nextVersion = getFirstVersion();
        } else {
            nextVersion++;
        }
        return Version.create(version.getId(), nextVersion);
    }
}
