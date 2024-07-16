package org.dcache.nfs.v4.client;


import org.dcache.nfs.status.ExistException;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import testutil.NfsServerTestcontainer;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class NfsClientTest {

    @Container
    private static final NfsServerTestcontainer<?> CONTAINER = new NfsServerTestcontainer<>("/nfsshare");

    @Test
    void test_readDir() throws Exception {
        try (NfsClient nfsClient = getNfsClientForTest()) {
            nfsClient.mkDir("bla");

            assertThatThrownBy(() -> nfsClient.mkDir("bla")).isInstanceOf(ExistException.class);
        }
    }

    private static NfsClient getNfsClientForTest() throws IOException, InterruptedException {
        System.out.println(CONTAINER.getFirstMappedPort());
        System.out.println(CONTAINER.getHost());

        return new NfsClient(CONTAINER.getFirstMappedPort(), NfsVersion.V4, CONTAINER.getHost(), CONTAINER.getExportPath());
    }

}
