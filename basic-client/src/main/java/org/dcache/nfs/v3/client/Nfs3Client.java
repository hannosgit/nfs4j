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

    private final int mountServerPort;

    private final int nfsdPort;

    private Integer localPort;

    private final fhandle3 rootFhandle;

    private final OncRpcClient oncRpcClient;

    private final RpcCall rpcCall;

    public Nfs3Client(@Nonnull String server, @Nonnull String export, int mountServerPort, int nfsdPort, Integer localPort) {
        this.mountServerPort = mountServerPort;
        this.nfsdPort = nfsdPort;
        this.localPort = localPort;

        HostAndPort hp = HostAndPort.fromParts(server, nfsdPort).requireBracketsForIPv6();
        InetSocketAddress serverAddress = new InetSocketAddress(hp.getHost(), hp.getPort());
        try {
            RpcAuth credential = new RpcAuthTypeUnix(
                0, 0, new int[] {0},
                (int) (System.currentTimeMillis() / 1000),
                InetAddress.getLocalHost().getHostName());
            rootFhandle = mount(serverAddress, export, credential);

            oncRpcClient = new OncRpcClient(serverAddress.getAddress(), IpProtocolType.TCP, serverAddress.getPort(), 1020);
            final RpcTransport transport = oncRpcClient.connect();
            rpcCall = new RpcCall(nfs3_prot.NFS_PROGRAM, 3, credential, transport);
        } catch (IOException e) {
            throw new Nfs3TransportException(e);
        }
    }

    public Nfs3Client(@Nonnull String server, @Nonnull String export) {
        this(server, export, 635, 2049, null);
    }

    public List<entryplus3> readDirPlus(nfs_fh3 fileHandle) {
        try {
            final var result = new ArrayList<entryplus3>();

            READDIRPLUS3res response;
            long cookie = 0L;
            do {
                final var args = new READDIRPLUS3args();
                args.cookie = new cookie3(new uint64(cookie));
                args.cookieverf = new cookieverf3(new byte[nfs3_prot.NFS3_COOKIEVERFSIZE]);
                args.dircount = new count3(new uint32(16384));
                args.maxcount = new count3(new uint32(16384));
                args.dir = fileHandle;

                response = new READDIRPLUS3res();
                rpcCall.call(nfs3_prot.NFSPROC3_READDIRPLUS_3, args, response);
                if (response.resok == null) {
                    throw new Nfs3Exception("readDirPlus", response.status);
                }

                args.cookieverf = response.resok.cookieverf;
                entryplus3 entry = response.resok.reply.entries;
                while (entry != null) {
                    cookie = entry.cookie.value.value;
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

    private fhandle3 mount(InetSocketAddress serverAddress, String export, RpcAuth credential) {
        try (var mountRpcClient = buildOncRpcClient(serverAddress)) {
            RpcTransport transport = mountRpcClient.connect();

            var client = new RpcCall(mount_prot.MOUNT_PROGRAM, 3, credential, transport);

            final var response = new mountres3();
            final MOUNT3args mount3args = new MOUNT3args(export);
            client.call(mount_prot.MOUNTPROC3_MNT_3, mount3args, response);

            if (response.mountinfo == null) {
                throw new Nfs3Exception("mount", response.fhs_status);
            }
            return response.mountinfo.fhandle;
        } catch (IOException e) {
            throw new Nfs3TransportException(e);
        }
    }

    private OncRpcClient buildOncRpcClient(InetSocketAddress serverAddress) {
        if (localPort == null) {
            return new OncRpcClient(serverAddress.getAddress(), IpProtocolType.TCP, mountServerPort);
        }
        return new OncRpcClient(serverAddress.getAddress(), IpProtocolType.TCP, mountServerPort, localPort);
    }

}