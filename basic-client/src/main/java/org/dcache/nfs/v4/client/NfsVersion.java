package org.dcache.nfs.v4.client;

public enum NfsVersion {
    V3(3),
    V4(4);

    public final int version;

    NfsVersion(int version) {
        this.version = version;
    }
}
