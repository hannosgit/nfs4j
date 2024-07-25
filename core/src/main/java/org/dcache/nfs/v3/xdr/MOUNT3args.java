package org.dcache.nfs.v3.xdr;

import org.dcache.oncrpc4j.rpc.OncRpcException;
import org.dcache.oncrpc4j.xdr.XdrAble;
import org.dcache.oncrpc4j.xdr.XdrDecodingStream;
import org.dcache.oncrpc4j.xdr.XdrEncodingStream;

import java.io.IOException;

public class MOUNT3args implements XdrAble {

    /**
     * An ASCII string that describes a directory on the server.
     */
    private String _exportPointPath;

    public MOUNT3args(String exportPointPath) {
        _exportPointPath = exportPointPath;
    }

    @Override
    public void xdrEncode(XdrEncodingStream xdr) throws OncRpcException, IOException {
        xdr.xdrEncodeString(_exportPointPath);
    }

    @Override
    public void xdrDecode(XdrDecodingStream xdr) throws OncRpcException, IOException {
        _exportPointPath = xdr.xdrDecodeString();
    }
}
