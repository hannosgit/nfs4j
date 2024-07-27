package org.dcache.nfs.v3.client;

import org.dcache.nfs.v3.xdr.fattr3;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import testutil.NfsServerTestcontainer;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;


@Testcontainers
class Nfs3ClientTest {

    @Container
    private static final NfsServerTestcontainer CONTAINER = new NfsServerTestcontainer();

    @Test
    void testClient() throws IOException {
        try (final Nfs3Client nfs3Client = getNfs3Client()) {
            final fattr3 attr = nfs3Client.getAttr(nfs3Client.getRootHandle());

            assertThat(attr.mode.value).isNotNull();
        }
    }

    @Test
    void mkdir() throws IOException {
        try (final Nfs3Client nfs3Client = getNfs3Client()) {
            assertThatCode(() -> nfs3Client.mkdir(nfs3Client.getRootHandle(), "bart")).doesNotThrowAnyException();

            assertThatCode(() -> nfs3Client.lookUp(nfs3Client.getRootHandle(), "bart")).doesNotThrowAnyException();
        }
    }

    private static @NotNull Nfs3Client getNfs3Client() {
//        return new Nfs3Client("localhost", "/data", 9051, 9051, null);
        return new Nfs3Client(CONTAINER.getHost(), CONTAINER.getExport(), CONTAINER.getFirstMappedPort(), CONTAINER.getFirstMappedPort(), null);
    }


}
