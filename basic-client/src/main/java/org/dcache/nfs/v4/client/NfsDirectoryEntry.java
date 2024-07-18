package org.dcache.nfs.v4.client;

import jakarta.annotation.Nonnull;

import static org.dcache.nfs.v4.xdr.nfs_ftype4.NF4DIR;

public record NfsDirectoryEntry(
        @Nonnull String name,
        Fattr4StandardAttributes attributes
) {


    public boolean isDirectory() {
        return this.attributes.type() == NF4DIR;
    }

}
