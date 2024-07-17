package org.dcache.nfs.v4.client;

import jakarta.annotation.Nonnull;

public record NfsDirectoryEntry(
        @Nonnull String name,
        Fattr4StandardAttributes attributes
) {


    public boolean isDirectory() {
        return (this.attributes.mode() & 0x4000) == (0x4000);
    }

}
