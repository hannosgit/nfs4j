package org.dcache.nfs.v3.client;

public class Nfs3Exception extends RuntimeException {
    public Nfs3Exception(String call, int status) {
        super(call + " returned status: " + status);
    }
}
