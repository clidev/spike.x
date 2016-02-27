/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package io.spikex.core.util.resource;

import io.spikex.core.util.IVersion;

/**
 *
 * @author christofferlindevall
 */
public final class UnchangingVersionStrategy implements IVersionStrategy {

    private static final int VERSION_ONE = 1;
    //
    private final int m_firstVersion;

    public UnchangingVersionStrategy() {
        this(VERSION_ONE);
    }

    public UnchangingVersionStrategy(final int firstVersion) {
        m_firstVersion = firstVersion;
    }

    @Override
    public int getFirstVersion() {
        return m_firstVersion;
    }

    @Override
    public IVersion nextVersion(IVersion version) {
        return version;
    }
}
