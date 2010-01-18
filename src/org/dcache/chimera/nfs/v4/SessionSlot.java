/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */

package org.dcache.chimera.nfs.v4;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.dcache.chimera.nfs.ChimeraNFSException;
import org.dcache.chimera.nfs.v4.xdr.nfs_resop4;
import org.dcache.chimera.nfs.v4.xdr.nfsstat4;

/**
 *
 */
public class SessionSlot {

    private static final Logger _log = Logger.getLogger(SessionSlot.class.getName());

    private int _sequence;
    private List<nfs_resop4> _reply;
    SessionSlot(int sequence) {
        _sequence = sequence;

    }

    public SessionSlot() {
        this(0);
    }

    /**
     *
     * @param sequence
     * @param reply
     * @return true if retransmit is detected and cached reply available.
     * @throws ChimeraNFSException
     */
    boolean update(int sequence, List<nfs_resop4> reply) throws ChimeraNFSException {

        if( sequence == _sequence ) {
            _log.log(Level.INFO, "retransmit detected");
            if( _reply != null ) {
                _log.log(Level.INFO, "using cached reply");
                reply.clear();
                reply.addAll(_reply);
                return true;
            }else{
                throw new ChimeraNFSException(nfsstat4.NFS4ERR_RETRY_UNCACHED_REP,
                        "Uncached reply retry");
            }
        }
        /*
         * According to spec.
         *
         * If the previous sequence id was 0xFFFFFFFF,
         * then the next request for the slot MUST have
         * the sequence id set to zero.
         */

        int validValue;
        if (_sequence == 0xFFFFFFFF) {
            validValue = 0;
        } else {
            validValue = _sequence + 1;
        }

        if (sequence != validValue) {
            throw new ChimeraNFSException(nfsstat4.NFS4ERR_SEQ_MISORDERED,
                    "disordered : v/n : " + Integer.toHexString(validValue) +
                    "/" + Integer.toHexString(sequence));
        }

        _sequence = sequence;
        _reply = reply;
        return false;
    }

    List<nfs_resop4> reply() {
        return _reply;
    }

    int sequence() {
        return _sequence;
    }

}
