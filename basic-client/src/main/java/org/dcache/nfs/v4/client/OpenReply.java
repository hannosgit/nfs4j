package org.dcache.nfs.v4.client;

import org.dcache.nfs.v4.xdr.nfs_fh4;
import org.dcache.nfs.v4.xdr.stateid4;

class OpenReply {

    private final nfs_fh4 _fh;
    private final stateid4 _stateid;

    OpenReply(nfs_fh4 fh, stateid4 stateid) {
        _stateid = stateid;
        _fh = fh;
    }

    nfs_fh4 fh() {
        return _fh;
    }

    stateid4 stateid() {
        return _stateid;
    }
}
