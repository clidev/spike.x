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
public final class IncreasingVersionStrategy implements IVersionStrategy {

    private static final int VERSION_ONE = 1;
    //
    private final int m_firstVersion;

    public IncreasingVersionStrategy() {
        this(VERSION_ONE);
    }

    public IncreasingVersionStrategy(int firstVersion) {
        m_firstVersion = firstVersion;
    }

    @Override
    public int getFirstVersion() {
        return m_firstVersion;
    }

    @Override
    public IVersion nextVersion(IVersion version) {
        //
        // Sanity check
        //
        if (version == null) {
            throw new IllegalArgumentException("version is null");
        }
        //
        return Version.create(version.getId(), version.getSequence() + 1);
    }
}
