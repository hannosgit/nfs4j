package org.dcache.nfs.v3.client;

import org.dcache.nfs.v3.xdr.entryplus3;
import org.dcache.nfs.v3.xdr.fattr3;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;


class Nfs3ClientTest {

    @Test
    void testClient() throws IOException {
        try (final Nfs3Client nfs3Client = new Nfs3Client("127.0.0.1", 9051, "/data");) {
            final fattr3 attr = nfs3Client.getAttr();

            System.out.println(attr.mode.value.value);

            final List<entryplus3> entryplus3s = nfs3Client.readDirPlus();
            entryplus3s.forEach(entryplus3 -> {
                System.out.println(entryplus3.name.value);
            });
        }
    }
}
