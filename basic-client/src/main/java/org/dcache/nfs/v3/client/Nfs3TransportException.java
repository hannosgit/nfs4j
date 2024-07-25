package org.dcache.nfs.v3.client;

import java.io.IOException;

public class Nfs3TransportException extends RuntimeException {
    public Nfs3TransportException(IOException e) {
        super(e);
    }
}
