package org.dcache.nfs.v3.client;

import com.google.common.net.HostAndPort;
import jakarta.annotation.Nonnull;
import org.dcache.nfs.v3.xdr.*;
import org.dcache.oncrpc4j.rpc.*;
import org.dcache.oncrpc4j.rpc.net.IpProtocolType;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class Nfs3Client implements AutoCloseable {

    private final fhandle3 rootFhandle;

    private final OncRpcClient oncRpcClient;

    private final RpcCall rpcCall;

    public Nfs3Client(@Nonnull String server, int port, @Nonnull String export) {
        HostAndPort hp = HostAndPort.fromParts(server, port).requireBracketsForIPv6();

        InetSocketAddress serverAddress = new InetSocketAddress(hp.getHost(), hp.getPort());
        oncRpcClient = new OncRpcClient(serverAddress.getAddress(), IpProtocolType.TCP, serverAddress.getPort());
        try {
            RpcTransport transport = oncRpcClient.connect();
            RpcAuth credential = new RpcAuthTypeUnix(
                    0, 0, new int[]{0},
                    (int) (System.currentTimeMillis() / 1000),
                    InetAddress.getLocalHost().getHostName());
            var client = new RpcCall(mount_prot.MOUNT_PROGRAM, 3, credential, transport);

            final var response = new mountres3();
            final MOUNT3args mount3args = new MOUNT3args(export);
            client.call(mount_prot.MOUNTPROC3_MNT_3, mount3args, response);

            rootFhandle = response.mountinfo.fhandle;
            rpcCall = new RpcCall(nfs3_prot.NFS_PROGRAM, 3, credential, transport);
        } catch (IOException e) {
            throw new Nfs3TransportException(e);
        }
    }

    public List<entryplus3> readDirPlus(nfs_fh3 fileHandle) {
        try {
            final var args = new READDIRPLUS3args();
            args.cookie = new cookie3(new uint64(0L));
            args.cookieverf = cookieverf3.valueOf(0L);
            args.dircount = new count3(new uint32(16384));
            args.maxcount = new count3(new uint32(16384));
            args.dir = fileHandle;

            final var response = new READDIRPLUS3res();
            rpcCall.call(nfs3_prot.NFSPROC3_READDIRPLUS_3, args, response);

            final var result = new ArrayList<entryplus3>();
            do {
                if (response.resok == null) {
                    throw new Nfs3Exception("readDirPlus", response.status);
                }

                entryplus3 entry = response.resok.reply.entries;
                while (entry != null) {
                    result.add(entry);

                    entry = entry.nextentry;
                }
            } while (!response.resok.reply.eof);

            return result;
        } catch (IOException e) {
            throw new Nfs3TransportException(e);
        }
    }

    public fattr3 getAttr() {
        try {
            final var args = new GETATTR3args();
            args.object = getRootHandle();

            final var response = new GETATTR3res();
            rpcCall.call(nfs3_prot.NFSPROC3_GETATTR_3, args, response);

            if (response.resok == null) {
                throw new Nfs3Exception("getAttr", response.status);
            }

            return response.resok.obj_attributes;
        } catch (IOException e) {
            throw new Nfs3TransportException(e);
        }
    }

    public nfs_fh3 getRootHandle() {
        final nfs_fh3 nfsFh3 = new nfs_fh3();
        nfsFh3.data = rootFhandle.value;

        return nfsFh3;
    }

    @Override
    public void close() throws IOException {
        this.oncRpcClient.close();
    }
}
