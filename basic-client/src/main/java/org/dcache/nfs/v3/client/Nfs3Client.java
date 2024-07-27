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

    private final Integer localPort;

    private final fhandle3 rootFhandle;

    private final OncRpcClient oncRpcClient;

    private final RpcCall rpcCall;

    public Nfs3Client(@Nonnull String server, @Nonnull String export, int mountServerPort, int nfsdPort, Integer localPort) {
        this.mountServerPort = mountServerPort;
        this.localPort = localPort;

        HostAndPort hp = HostAndPort.fromParts(server, nfsdPort).requireBracketsForIPv6();
        InetSocketAddress serverAddress = new InetSocketAddress(hp.getHost(), hp.getPort());
        try {
            RpcAuth credential = new RpcAuthTypeUnix(
                    0, 0, new int[]{0},
                    (int) (System.currentTimeMillis() / 1000),
                    InetAddress.getLocalHost().getHostName());
            rootFhandle = mount(serverAddress, export, credential);

            oncRpcClient = buildOncRpcClient(serverAddress, nfsdPort);
            final RpcTransport transport = oncRpcClient.connect();
            rpcCall = new RpcCall(nfs3_prot.NFS_PROGRAM, 3, credential, transport);
        } catch (IOException e) {
            throw new Nfs3TransportException(e);
        }
    }

    public Nfs3Client(@Nonnull String server, @Nonnull String export) {
        this(server, export, 635, 2049, null);
    }

    public LOOKUP3resok lookUp(@Nonnull nfs_fh3 parentDirectoryFileHandle, @Nonnull String fileName) {
        try {
            final var args = new LOOKUP3args();
            final diropargs3 what = new diropargs3();
            what.name = new filename3(fileName);
            what.dir = parentDirectoryFileHandle;
            args.what = what;
            final var response = new LOOKUP3res();

            rpcCall.call(nfs3_prot.NFSPROC3_LOOKUP_3, args, response);
            if (response.resok == null) {
                throw new Nfs3Exception("lookUp", response.status);
            }
            return response.resok;
        } catch (IOException e) {
            throw new Nfs3TransportException(e);
        }
    }

    public void mkdir(@Nonnull nfs_fh3 parentDirectoryFileHandle, @Nonnull String directoryName) {
        try {
            final var args = new MKDIR3args();
            final diropargs3 where = new diropargs3();
            where.name = new filename3(directoryName);
            where.dir = parentDirectoryFileHandle;
            args.where = where;

            final sattr3 attributes = new sattr3();
            attributes.atime = new set_atime();
            attributes.mtime = new set_mtime();
            final set_mode3 mode = new set_mode3();
            mode.set_it = true;
            mode.mode = new mode3(new uint32(0777));
            attributes.mode = mode;
            attributes.size = new set_size3();
            attributes.gid = new set_gid3();
            attributes.uid = new set_uid3();
            args.attributes = attributes;

            final var response = new MKDIR3res();
            rpcCall.call(nfs3_prot.NFSPROC3_MKDIR_3, args, response);
            if (response.resok == null) {
                throw new Nfs3Exception("mkdir", response.status);
            }
        } catch (IOException e) {
            throw new Nfs3TransportException(e);
        }
    }

    public void rmDir(@Nonnull nfs_fh3 parentDirectoryFileHandle, @Nonnull String directoryName){
        try {
            final var args = new RMDIR3args();
            final diropargs3 what = new diropargs3();
            what.name = new filename3(directoryName);
            what.dir = parentDirectoryFileHandle;
            args.object = what;

            final var response = new RMDIR3res();
            rpcCall.call(nfs3_prot.NFSPROC3_RMDIR_3, args, response);
            if (response.resok == null) {
                throw new Nfs3Exception("rmDir", response.status);
            }
        } catch (IOException e) {
            throw new Nfs3TransportException(e);
        }
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

    public fattr3 getAttr(nfs_fh3 fileHandle) {
        try {
            final var args = new GETATTR3args();
            args.object = fileHandle;

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

    public CREATE3resok create(@Nonnull nfs_fh3 parentDirectoryFileHandle, @Nonnull String fileName) {
        try {
            final var args = new CREATE3args();
            final diropargs3 where = new diropargs3();
            where.name = new filename3(fileName);
            where.dir = parentDirectoryFileHandle;
            args.where = where;

            final createhow3 how = new createhow3();
            how.mode = createmode3.GUARDED;
            final sattr3 attributes = new sattr3();
            attributes.atime = new set_atime();
            attributes.mtime = new set_mtime();
            final set_mode3 mode = new set_mode3();
            mode.set_it = true;
            mode.mode = new mode3(new uint32(0777));
            attributes.mode = mode;
            attributes.size = new set_size3();
            attributes.gid = new set_gid3();
            attributes.uid = new set_uid3();
            how.obj_attributes = attributes;
            args.how = how;

            final var response = new CREATE3res();
            rpcCall.call(nfs3_prot.NFSPROC3_CREATE_3, args, response);
            if (response.resok == null) {
                throw new Nfs3Exception("create", response.status);
            }
            return response.resok;
        } catch (IOException e) {
            throw new Nfs3TransportException(e);
        }
    }

    @Override
    public void close() throws IOException {
        this.oncRpcClient.close();
    }

    private fhandle3 mount(InetSocketAddress serverAddress, String export, RpcAuth credential) {
        try (var mountRpcClient = buildOncRpcClient(serverAddress, mountServerPort)) {
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

    private OncRpcClient buildOncRpcClient(InetSocketAddress serverAddress, int port) {
        if (localPort == null) {
            return new OncRpcClient(serverAddress.getAddress(), IpProtocolType.TCP, port);
        }
        return new OncRpcClient(serverAddress.getAddress(), IpProtocolType.TCP, port, localPort);
    }

}
