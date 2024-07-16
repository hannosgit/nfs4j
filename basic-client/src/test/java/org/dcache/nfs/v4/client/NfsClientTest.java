package org.dcache.nfs.v4.client;


import org.dcache.nfs.status.ExistException;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import testutil.NfsServerTestcontainer;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class NfsClientTest {

    @Container
    private static final NfsServerTestcontainer<?> CONTAINER = new NfsServerTestcontainer<>("/nfsshare");

    @Test
    void mkdir() throws Exception {
        try (NfsClient nfsClient = getNfsClientForTest()) {
            nfsClient.mkDir("bla");

            assertThatThrownBy(() -> nfsClient.mkDir("bla")).isInstanceOf(ExistException.class);
        }
    }

    @Test
    void readDir_empty() throws Exception {
        try (NfsClient nfsClient = getNfsClientForTest()) {
            nfsClient.mkDir("aDir");

            final List<String> children = nfsClient.readDir("aDir");
            assertThat(children).isEmpty();
        }
    }

    @Test
    void readDir_1child() throws Exception {
        try (NfsClient nfsClient = getNfsClientForTest()) {
            nfsClient.mkDir("bDir");
            nfsClient.createFile("bDir/bla.txt");

            final List<String> children = nfsClient.readDir("bDir");
            assertThat(children).hasSize(1);
        }
    }

    private static NfsClient getNfsClientForTest() throws IOException {
        System.out.println(CONTAINER.getFirstMappedPort());
        System.out.println(CONTAINER.getHost());

        return new NfsClient(CONTAINER.getHost(), CONTAINER.getFirstMappedPort(), CONTAINER.getExportPath());
    }

}
