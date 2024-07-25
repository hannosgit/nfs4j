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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class Nfs3Client implements AutoCloseable {

    private final fhandle3 rootFhandle;

    private final OncRpcClient oncRpcClient;

    private final RpcCall rpcCall;

    public Nfs3Client(@Nonnull String server, int port, @Nonnull String export) throws IOException {
        HostAndPort hp = HostAndPort.fromParts(server, port)
                .requireBracketsForIPv6();

        InetSocketAddress serverAddress = new InetSocketAddress(hp.getHost(), hp.getPort());
        oncRpcClient = new OncRpcClient(serverAddress.getAddress(), IpProtocolType.TCP, serverAddress.getPort());

        RpcTransport transport = oncRpcClient.connect();
        RpcAuth credential = new RpcAuthTypeUnix(
                0, 0, new int[]{0},
                (int) (System.currentTimeMillis() / 1000),
                InetAddress.getLocalHost().getHostName());
        var client = new RpcCall(mount_prot.MOUNT_PROGRAM, 3, credential, transport);

        final MOUNT3args mount3args = new MOUNT3args(export);
        final CompletableFuture<mountres3> call = client.call(mount_prot.MOUNTPROC3_MNT_3, mount3args, mountres3.class);

        try {
            final mountres3 mountres3 = call.get();
            rootFhandle = mountres3.mountinfo.fhandle;
            rpcCall = new RpcCall(nfs3_prot.NFS_PROGRAM, 3, credential, transport);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public List<entryplus3> readDirPlus() throws IOException {
        final var args = new READDIRPLUS3args();
        args.cookie = new cookie3(new uint64(0L));
        args.cookieverf = cookieverf3.valueOf(0L);
        args.dircount = new count3(new uint32(16384));
        args.maxcount = new count3(new uint32(16384));
        args.dir = getRootHandle();

        final var response = new READDIRPLUS3res();
        rpcCall.call(nfs3_prot.NFSPROC3_READDIRPLUS_3, args, response);

        final var result = new ArrayList<entryplus3>();
        do {
            entryplus3 entry = response.resok.reply.entries;
            while (entry != null) {
                result.add(entry);

                entry = entry.nextentry;
            }
        } while (!response.resok.reply.eof);

        return result;
    }

    public fattr3 getAttr() throws IOException {
        final var args = new GETATTR3args();
        args.object = getRootHandle();

        final var response = new GETATTR3res();
        rpcCall.call(nfs3_prot.NFSPROC3_GETATTR_3, args, response);

        return response.resok.obj_attributes;
    }

    @Override
    public void close() throws IOException {
        this.oncRpcClient.close();
    }

    private nfs_fh3 getRootHandle() {
        final nfs_fh3 nfsFh3 = new nfs_fh3();
        nfsFh3.data = rootFhandle.value;

        return nfsFh3;
    }
}
