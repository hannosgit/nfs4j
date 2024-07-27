package org.dcache.nfs.v3.client;

import org.dcache.nfs.v3.xdr.fattr3;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import testutil.NfsServerTestcontainer;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;


@Testcontainers
class Nfs3ClientTest {

    @Container
    private static final NfsServerTestcontainer CONTAINER = new NfsServerTestcontainer();

    @Test
    void testClient() throws IOException {
        try (final Nfs3Client nfs3Client = new Nfs3Client(CONTAINER.getHost(), CONTAINER.getExport(), CONTAINER.getFirstMappedPort(), CONTAINER.getFirstMappedPort(), null)) {
            final fattr3 attr = nfs3Client.getAttr();

            assertThat(attr.mode.value).isNotNull();
        }
    }
}
