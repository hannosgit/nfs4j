/*
 * Copyright (c) 2009 - 2021 Deutsches Elektronen-Synchroton,
 * Member of the Helmholtz Association, (DESY), HAMBURG, GERMANY
 *
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
package org.dcache.nfs.v4.client;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.BaseEncoding;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.nfsstat;
import org.dcache.nfs.status.BadLayoutException;
import org.dcache.nfs.status.NotSuppException;
import org.dcache.nfs.v4.AttributeMap;
import org.dcache.nfs.v4.ClientSession;
import org.dcache.nfs.v4.CompoundBuilder;
import org.dcache.nfs.v4.Stateids;
import org.dcache.nfs.v4.xdr.*;
import org.dcache.nfs.vfs.Stat;
import org.dcache.oncrpc4j.rpc.OncRpcException;
import org.dcache.oncrpc4j.rpc.net.IpProtocolType;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.dcache.nfs.v4.client.Fattr4StandardAttributes.STANDARD_ATTRIBUTES;

class NfsClient4Internal {

    private final nfs4_prot_NFS4_PROGRAM_Client _nfsClient;
    private final Map<deviceid4, FileIoDevice> _knowDevices = new HashMap<>();
    private nfs_fh4 _cwd = null;
    private nfs_fh4 _rootFh = null;
    // FIXME:
    private nfs_fh4 _ioFH = null;
    private clientid4 _clientIdByServer = null;
    private sequenceid4 _sequenceID = null;
    private long _lastUpdate = -1;

    private ClientSession _clientSession;

    private boolean _isMDS = false;
    private boolean _isDS = false;

    private final LoadingCache<InetSocketAddress, NfsClient4Internal> _servers =
            CacheBuilder.newBuilder().build(new Connector());

    /**
     * pNFS layout type that client supports.
     * <p>
     * TODO: add flex_files layout support
     */
    private final layouttype4 clientLayoutType = layouttype4.LAYOUT4_NFSV4_1_FILES;

    private final ScheduledExecutorService _executorService = Executors.newScheduledThreadPool(1);

    /**
     * generate set of files and delete them after words
     *
     * @throws IOException
     * @throws OncRpcException
     */
    private void filebomb(int count) throws OncRpcException, IOException {

        List<String> files = new ArrayList<>(count);
        long start = System.currentTimeMillis();
        try {
            for (int i = 0; i < count; i++) {
                String file = UUID.randomUUID().toString();
                write("/etc/profile", file, true);
                files.add(file);
            }
        } finally {
            for (String file : files) {
                System.out.println("Remove: " + file);
                remove(file);
            }
            System.out.println(count + " files in " + (System.currentTimeMillis() - start) / 1000);
        }

    }

    /**
     * send big number of open requests
     *
     * @throws IOException
     * @throws OncRpcException
     */
    private void openbomb(String path, int count) throws OncRpcException, IOException {

        for (int i = 0; i < count; i++) {
            open(path);
        }
    }

    private void gc() throws OncRpcException, IOException {

        exchange_id();
        create_session();
        sequence();

    }

    private boolean needUpdate() {
        // 60 seconds
        return System.currentTimeMillis() - _lastUpdate > 60000;
    }

    private static class LeaseUpdater implements Runnable {

        private final NfsClient4Internal _nfsClient;

        LeaseUpdater(NfsClient4Internal nfsClient) {
            _nfsClient = nfsClient;
        }

        @Override
        public void run() {
            try {
                if (_nfsClient.needUpdate()) {
                    _nfsClient.sequence();
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    NfsClient4Internal(InetSocketAddress address) throws OncRpcException,
            IOException {
        _nfsClient = new nfs4_prot_NFS4_PROGRAM_Client(address.getAddress(),
                address.getPort(), IpProtocolType.TCP, 0, 0);

        _servers.asMap().put(address, this);
    }

    void mount(String root) throws OncRpcException, IOException {
        exchange_id();
        create_session();

        getRootFh(root);
        get_supported_attributes();
        if (_isMDS) {
            getLayoutTypes();
            get_devicelist();
        }
        reclaimComplete();
        _lastUpdate = System.currentTimeMillis();
    }

    private void dsMount() throws OncRpcException, IOException {
        exchange_id();
        create_session();
        _lastUpdate = System.currentTimeMillis();
    }

    void umount() throws OncRpcException, IOException {
        destroy_session();
        destroy_clientid();
    }

    private void exchange_id() throws OncRpcException, IOException {

        String domain = "nairi.desy.de";
        String name = "dCache.ORG java based client";
        String clientid = this.getClass().getCanonicalName() + ": "
                + ProcessHandle.current().info().user().orElse("<nobody>")
                + "-"
                + ProcessHandle.current().pid() + "@" + InetAddress.getLocalHost().getHostName();

        COMPOUND4args args = new CompoundBuilder()
                .withExchangeId(domain, name, clientid, 0, state_protect_how4.SP4_NONE)
                .withTag("exchange_id")
                .build();

        COMPOUND4res compound4res = sendCompound(args);

        if (compound4res.resarray.get(0).opexchange_id.eir_resok4.eir_server_impl_id.length > 0) {
            String serverId = compound4res.resarray.get(0).opexchange_id.eir_resok4.eir_server_impl_id[0].nii_name.toString();
            nfstime4 buildTime = compound4res.resarray.get(0).opexchange_id.eir_resok4.eir_server_impl_id[0].nii_date;
            System.out.println("Connected to: " + serverId + ", built at: "
                    + (buildTime.seconds > 0 ? new Date(buildTime.seconds * 1000) : "<Unknon>"));
        } else {
            System.out.println("Connected to: Mr. X");
        }

        _clientIdByServer = compound4res.resarray.get(0).opexchange_id.eir_resok4.eir_clientid;
        _sequenceID = compound4res.resarray.get(0).opexchange_id.eir_resok4.eir_sequenceid;

        if ((compound4res.resarray.get(0).opexchange_id.eir_resok4.eir_flags.value
                & nfs4_prot.EXCHGID4_FLAG_USE_PNFS_MDS) != 0) {
            _isMDS = true;
        }

        if ((compound4res.resarray.get(0).opexchange_id.eir_resok4.eir_flags.value
                & nfs4_prot.EXCHGID4_FLAG_USE_PNFS_DS) != 0) {
            _isDS = true;
        }

        System.out.println("pNFS MDS: " + _isMDS);
        System.out.println("pNFS  DS: " + _isDS);
    }

    private void create_session() throws OncRpcException, IOException {

        Preconditions.checkState(_clientSession == null, "Session already exists");
        COMPOUND4args args = new CompoundBuilder()
                .withCreatesession(_clientIdByServer, _sequenceID)
                .withTag("create_session")
                .build();

        COMPOUND4res compound4res = sendCompound(args);

        sessionid4 sessionid = compound4res.resarray.get(0).opcreate_session.csr_resok4.csr_sessionid;
        _sequenceID.value = 0;
        int maxRequests = compound4res.resarray.get(0).opcreate_session.csr_resok4.csr_fore_chan_attrs.ca_maxrequests.value;
        System.out.println("Using slots: " + maxRequests);
        _clientSession = new ClientSession(sessionid, maxRequests);
        if (_isMDS) {
            args = new CompoundBuilder()
                    .withPutrootfh()
                    .withGetattr(nfs4_prot.FATTR4_LEASE_TIME)
                    .withTag("get_lease_time")
                    .build();

            compound4res = sendCompoundInSession(args);

            AttributeMap attributeMap = new AttributeMap(compound4res.resarray.get(compound4res.resarray.size() - 1).opgetattr.resok4.obj_attributes);
            Optional<fattr4_lease_time> fattr4_lease_timeAttr = attributeMap.get(nfs4_prot.FATTR4_LEASE_TIME);
            int leaseTimeInSeconds = fattr4_lease_timeAttr.get().value;
            System.out.println("server lease time: " + leaseTimeInSeconds + " sec.");
            _executorService.scheduleAtFixedRate(new LeaseUpdater(this),
                    leaseTimeInSeconds, leaseTimeInSeconds, TimeUnit.SECONDS);
        } else {
            _executorService.scheduleAtFixedRate(new LeaseUpdater(this),
                    90, 90, TimeUnit.SECONDS);
        }
    }

    private void destroy_session() throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withDestroysession(_clientSession.sessionId())
                .withTag("destroy_session")
                .build();

        @SuppressWarnings("unused")
        COMPOUND4res compound4res = sendCompound(args);
        _clientSession = null;
        _executorService.shutdown();
    }

    private void destroy_clientid() throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withDestroyclientid(_clientIdByServer)
                .withTag("destroy_clientid")
                .build();
        @SuppressWarnings("unused")
        COMPOUND4res compound4res = sendCompound(args);
        _nfsClient.close();

    }

    private void getRootFh(String path) throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withPutrootfh()
                .withLookup(path)
                .withGetfh()
                .withTag("get_rootfh")
                .build();

        COMPOUND4res compound4res = sendCompoundInSession(args);

        _rootFh = compound4res.resarray.get(compound4res.resarray.size() - 1).opgetfh.resok4.object;
        _cwd = _rootFh;
        System.out.println("root fh = " + BaseEncoding.base16().lowerCase().encode(_rootFh.value));
    }

    List<NfsDirectoryEntry> readdir(String path) throws IOException {
        return list(_cwd, path);
    }

    private String[] list(nfs_fh4 fh) throws OncRpcException, IOException, ChimeraNFSException {

        boolean done;
        List<String> list = new ArrayList<>();
        long cookie = 0;
        verifier4 verifier = new verifier4(new byte[nfs4_prot.NFS4_VERIFIER_SIZE]);

        do {

            COMPOUND4args args = new CompoundBuilder()
                    .withPutfh(fh)
                    .withReaddir(cookie, verifier, 16384, 16384)
                    .withTag("readdir")
                    .build();

            COMPOUND4res compound4res = sendCompoundInSession(args);

            verifier = compound4res.resarray.get(2).opreaddir.resok4.cookieverf;
            done = compound4res.resarray.get(2).opreaddir.resok4.reply.eof;

            entry4 dirEntry = compound4res.resarray.get(2).opreaddir.resok4.reply.entries;
            while (dirEntry != null) {
                cookie = dirEntry.cookie.value;
                list.add(new String(dirEntry.name.value));
                dirEntry = dirEntry.nextentry;
            }

        } while (!done);

        return list.toArray(new String[list.size()]);
    }

    private List<NfsDirectoryEntry> list(nfs_fh4 fh, String path) throws IOException {
        final List<NfsDirectoryEntry> result = new ArrayList<>();

        boolean done;
        long cookie = 0;
        verifier4 verifier = new verifier4(new byte[nfs4_prot.NFS4_VERIFIER_SIZE]);
        do {

            COMPOUND4args args = new CompoundBuilder()
                    .withPutfh(path.charAt(0) == '/' ? _rootFh : fh)
                    .withLookup(path)
                    .withReaddir(cookie, verifier, 16384, 16384, STANDARD_ATTRIBUTES)
                    .withTag("readdir")
                    .build();

            COMPOUND4res compound4res = sendCompoundInSession(args);

            verifier = compound4res.resarray.get(compound4res.resarray.size() - 1).opreaddir.resok4.cookieverf;
            done = compound4res.resarray.get(compound4res.resarray.size() - 1).opreaddir.resok4.reply.eof;

            entry4 dirEntry = compound4res.resarray.get(compound4res.resarray.size() - 1).opreaddir.resok4.reply.entries;
            while (dirEntry != null) {
                cookie = dirEntry.cookie.value;
                result.add(new NfsDirectoryEntry(new String(dirEntry.name.value),Fattr4StandardAttributes.parse(dirEntry.attrs.attr_vals)));
                dirEntry = dirEntry.nextentry;
            }

        } while (!done);

        return result;
    }

    void mkdir(String path) throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withPutfh(_cwd)
                .withSavefh()
                .withGetattr(nfs4_prot.FATTR4_CHANGE)
                .withMakedir(path)
                .withRestorefh()
                .withGetattr(nfs4_prot.FATTR4_CHANGE)
                .withTag("mkdir")
                .build();
        COMPOUND4res compound4res = sendCompoundInSession(args);
    }

    private void get_fs_locations(String path) throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withPutfh(_cwd)
                .withLookup(path)
                .withGetattr(nfs4_prot.FATTR4_FS_LOCATIONS)
                .withTag("get_fs_locations")
                .build();

        COMPOUND4res compound4res = sendCompoundInSession(args);

        AttributeMap attrs = new AttributeMap(compound4res.resarray.get(compound4res.resarray.size() - 1).opgetattr.resok4.obj_attributes);

        Optional<fattr4_fs_locations> locationsAttr = attrs.get(nfs4_prot.FATTR4_FS_LOCATIONS);
        if (locationsAttr.isPresent()) {
            fattr4_fs_locations locations = locationsAttr.get();
            System.out.println("fs_locations fs_root: " + locations.value.fs_root.value[0]);
            System.out.println("fs_locations locations rootpath: " + locations.value.locations[0].rootpath.value[0]);
            System.out.println("fs_locations locations server: " + new String(locations.value.locations[0].server[0].value.value));

        }
    }

    private nfs_fh4 cwd(String path) throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withPutfh(path.charAt(0) == '/' ? _rootFh : _cwd)
                .withLookup(path)
                .withGetfh()
                .withTag("lookup (cwd)")
                .build();
        COMPOUND4res compound4res = sendCompoundInSession(args);

        _cwd = compound4res.resarray.get(compound4res.resarray.size() - 1).opgetfh.resok4.object;
        System.out.println("CWD fh = " + BaseEncoding.base16().lowerCase().encode(_cwd.value));
        return new nfs_fh4(_cwd.value);
    }

    private Stat stat(nfs_fh4 fh) throws OncRpcException, IOException {

        Stat stat = new Stat();

        COMPOUND4args args = new CompoundBuilder()
                .withPutfh(fh)
                .withGetattr(nfs4_prot.FATTR4_SIZE, nfs4_prot.FATTR4_TYPE)
                .withTag("getattr (stat)")
                .build();
        COMPOUND4res compound4res = sendCompoundInSession(args);

        AttributeMap attrs = new AttributeMap(compound4res.resarray.get(2).opgetattr.resok4.obj_attributes);

        Optional<fattr4_size> size = attrs.get(nfs4_prot.FATTR4_SIZE);
        if (size.isPresent()) {
            stat.setSize(size.get().value);
        }

        Optional<fattr4_type> type = attrs.get(nfs4_prot.FATTR4_TYPE);
        System.out.println("Type is: " + type.get().value);

        return stat;
    }

    private void read(String path, boolean pnfs) throws OncRpcException, IOException {

        OpenReply or = open(path);

        if (pnfs && _isMDS) {
            StripeMap stripeMap = layoutget(or.fh(), or.stateid(), layoutiomode4.LAYOUTIOMODE4_READ);

            List<Stripe> stripes = stripeMap.getStripe(0, 4096);
            Stripe stripe = stripes.get(0);
            deviceid4 device = stripe.getDeviceId();
            FileIoDevice ioDevice = _knowDevices.get(device);
            InetSocketAddress deviceAddr =
                    ioDevice.of(stripe.getPatternOffset(), stripe.getUnit(),
                            0, 4096, stripe.getFirstStripeIndex());
            NfsClient4Internal dsClient = _servers.getUnchecked(deviceAddr);

            dsClient.nfsRead(stripe.getFh(), or.stateid());

            layoutreturn(or.fh(), 0, -1, new byte[0], stripeMap.getStateid());

        } else {
            nfsRead(or.fh(), or.stateid());
        }
        close(or.fh(), or.stateid());

    }

    private void readNoState(String path) throws OncRpcException, IOException {
        OpenReply or = open(path);
        nfsRead(or.fh(), Stateids.ZeroStateId());
        close(or.fh(), or.stateid());
    }

    private void readatonce(String path) throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withPutfh(path.charAt(0) == '/' ? _rootFh : _cwd)
                .withLookup(dirname(path))
                .withOpen(basename(path), _sequenceID.value, _clientIdByServer, nfs4_prot.OPEN4_SHARE_ACCESS_READ)
                .withRead(4096, 0, Stateids.currentStateId())
                .withClose(Stateids.currentStateId(), 1)
                .withTag("open+read+close")
                .build();

        COMPOUND4res compound4res = sendCompoundInSession(args);

        int opss = compound4res.resarray.size();
        byte[] data = new byte[compound4res.resarray.get(opss - 2).opread.resok4.data.remaining()];
        compound4res.resarray.get(opss - 2).opread.resok4.data.get(data);
        System.out.println("[" + new String(data) + "]");
    }

    private void write(String source, String path, boolean pnfs) throws OncRpcException, IOException {

        File f = new File(source);
        if (!f.exists()) {
            System.out.println("file not found: " + f);
        }

        OpenReply or = create(path);

        if (pnfs && _isMDS) {

            StripeMap stripeMap = layoutget(or.fh(), or.stateid(), layoutiomode4.LAYOUTIOMODE4_RW);
            try (RandomAccessFile raf = new RandomAccessFile(source, "r")) {
                byte[] data = new byte[4096];
                long offset = 0;
                while (true) {

                    int n = raf.read(data);
                    if (n == -1) {
                        break;
                    }

                    /* we got less than 4K wipe the tail */
                    if (n < data.length) {
                        byte[] b = new byte[n];
                        System.arraycopy(data, 0, b, 0, n);
                    }

                    List<Stripe> stripes = stripeMap.getStripe(offset, 4096);
                    Stripe stripe = stripes.get(0);
                    deviceid4 device = stripe.getDeviceId();
                    FileIoDevice ioDevice = _knowDevices.get(device);
                    InetSocketAddress deviceAddr = ioDevice.of(
                            stripe.getPatternOffset(), stripe.getUnit(),
                            offset, data.length, stripe.getFirstStripeIndex());
                    NfsClient4Internal dsClient = _servers.getUnchecked(deviceAddr);

                    dsClient.nfsWrite(stripe.getFh(), data, offset, or.stateid());
                    offset += n;

                    // offset points to current file size
                    if (stripe.isCommitThroughMDS()) {
                        layoutCommit(stripe.getFh(), or.stateid(), 0, offset,
                                OptionalLong.of(offset - 1), new byte[0]);
                    }
                }


            } catch (IOException ie) {
                System.out.println("Write failed: " + ie.getMessage());
            } finally {
                layoutreturn(or.fh(), 0, -1, new byte[0], stripeMap.getStateid());
            }

        } else {
            // not a pNFS server
            nfsWrite(or.fh(), "hello world".getBytes(), 0, or.stateid());
        }
        close(or.fh(), or.stateid());
    }

    private OpenReply open(String path) throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withPutfh(path.charAt(0) == '/' ? _rootFh : _cwd)
                .withLookup(dirname(path))
                .withOpen(basename(path), _sequenceID.value, _clientIdByServer, nfs4_prot.OPEN4_SHARE_ACCESS_READ)
                .withGetfh()
                .withTag("open_read")
                .build();
        COMPOUND4res compound4res = sendCompoundInSession(args);

        int opCount = compound4res.resarray.size();

        nfs_fh4 fh = compound4res.resarray.get(opCount - 1).opgetfh.resok4.object;
        stateid4 stateid = compound4res.resarray.get(opCount - 2).opopen.resok4.stateid;
        System.out.println("open_read fh = " + BaseEncoding.base16().lowerCase().encode(fh.value));

        return new OpenReply(fh, stateid);
    }

    OpenReply create(String path) throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withPutfh(path.charAt(0) == '/' ? _rootFh : _cwd)
                .withLookup(dirname(path))
                .withOpenCreate(basename(path), _sequenceID.value, _clientIdByServer, nfs4_prot.OPEN4_SHARE_ACCESS_BOTH)
                .withGetfh()
                .withTag("open_create")
                .build();
        COMPOUND4res compound4res = sendCompoundInSession(args);

        int opCount = compound4res.resarray.size();
        nfs_fh4 fh = compound4res.resarray.get(opCount - 1).opgetfh.resok4.object;
        stateid4 stateid = compound4res.resarray.get(opCount - 2).opopen.resok4.stateid;
        System.out.println("open_read fh = " + BaseEncoding.base16().lowerCase().encode(fh.value));

        return new OpenReply(fh, stateid);
    }

    void close(nfs_fh4 fh, stateid4 stateid) throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withPutfh(fh)
                .withClose(stateid, 1)
                .withTag("close")
                .build();
        COMPOUND4res compound4res = sendCompoundInSession(args);
    }

    private StripeMap layoutget(nfs_fh4 fh, stateid4 stateid, int layoutiomode) throws OncRpcException,
            IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withPutfh(fh)
                .withLayoutget(false,
                        clientLayoutType,
                        layoutiomode, 0, 0xffffffff, 0xff, 4096,
                        stateid)
                .withTag("layoutget")
                .build();
        COMPOUND4res compound4res = sendCompoundInSession(args);

        layout4[] layout = compound4res.resarray.get(2).oplayoutget.logr_resok4.logr_layout;
        System.out.println("Layoutget for fh: " + BaseEncoding.base16().lowerCase().encode(fh.value));
        System.out.println("    roc   : " + compound4res.resarray.get(2).oplayoutget.logr_resok4.logr_return_on_close);

        StripeMap stripeMap = new StripeMap(compound4res.resarray.get(2).oplayoutget.logr_resok4.logr_stateid);

        for (layout4 l : layout) {
            nfsv4_1_file_layout4 fileDevice = LayoutgetStub.decodeLayoutId(l.lo_content.loc_body);
            System.out.println("       sd # "
                    + Arrays.toString(fileDevice.nfl_deviceid.value) + " size "
                    + fileDevice.nfl_deviceid.value.length);

            _ioFH = fileDevice.nfl_fh_list[0];
            System.out.println("     io fh: " + BaseEncoding.base16().lowerCase().encode(_ioFH.value));
            System.out.println("    length: " + l.lo_length.value);
            System.out.println("    offset: " + l.lo_offset.value);
            System.out.println("    type  : " + l.lo_content.loc_type);
            System.out.println("    unit  : " + fileDevice.nfl_util.value);
            System.out.println("    commit: "
                    + ((fileDevice.nfl_util.value & nfs4_prot.NFL4_UFLG_COMMIT_THRU_MDS) == 0 ? "ds" : "mds"));

            deviceid4 deviceID = fileDevice.nfl_deviceid;
            Stripe stripe = new Stripe(deviceID, fileDevice.nfl_fh_list[0],
                    l.lo_length.value, l.lo_offset.value,
                    fileDevice.nfl_pattern_offset.value,
                    fileDevice.nfl_util.value,
                    fileDevice.nfl_first_stripe_index.value);
            stripeMap.addStripe(stripe);

            if (!_knowDevices.containsKey(deviceID)) {
                System.out.println("    new: true");
                get_deviceinfo(deviceID);
            } else {
                System.out.println("    new: false");
            }
            FileIoDevice address = _knowDevices.get(deviceID);
            if (address == null) {
                System.out.println("    address: failed to get");
            } else {
                System.out.println("    address: " + address);
            }

        }

        return stripeMap;
    }

    private void layoutreturn(nfs_fh4 fh, long offset, long len, byte[] body, stateid4 stateid) throws OncRpcException,
            IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withPutfh(fh)
                .withLayoutreturn(offset, len, body, stateid)
                .withTag("layoutreturn")
                .build();

        COMPOUND4res compound4res = sendCompoundInSession(args);
    }

    private void layoutCommit(nfs_fh4 fh, stateid4 stateid, long offset, long len,
                              OptionalLong newOffset, byte[] body) throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withPutfh(fh)
                .withLayoutcommit(offset, len, false, stateid, newOffset, layouttype4.LAYOUT4_NFSV4_1_FILES, body)
                .withTag("layoutcommit")
                .build();

        COMPOUND4res compound4res = sendCompoundInSession(args);
    }

    private COMPOUND4res sendCompound(COMPOUND4args compound4args)
            throws OncRpcException, IOException {

        COMPOUND4res compound4res;
        /*
         * wait if server is in the grace period.
         *
         * TODO: escape if it takes too long
         */
        do {
            compound4res = _nfsClient.NFSPROC4_COMPOUND_4(compound4args);
        } while (canRetry(compound4res.status, compound4args.tag.toString()));

        nfsstat.throwIfNeeded(compound4res.status);
        return compound4res;
    }

    private COMPOUND4res sendCompoundInSession(COMPOUND4args compound4args)
            throws OncRpcException, IOException {

        if (compound4args.argarray[0].argop == nfs_opnum4.OP_SEQUENCE) {
            throw new IllegalArgumentException("The operation sequence should not be included");
        }

        nfs_argop4[] extendedOps = new nfs_argop4[compound4args.argarray.length + 1];
        System.arraycopy(compound4args.argarray, 0, extendedOps, 1, compound4args.argarray.length);
        compound4args.argarray = extendedOps;

        var slot = _clientSession.acquireSlot();
        try {

            COMPOUND4res compound4res;
            /*
             * wait if server is in the grace period.
             *
             * TODO: escape if it takes too long
             */
            do {

                nfs_argop4 op = new nfs_argop4();
                op.argop = nfs_opnum4.OP_SEQUENCE;
                op.opsequence = new SEQUENCE4args();
                op.opsequence.sa_cachethis = false;

                op.opsequence.sa_slotid = slot.getId();
                op.opsequence.sa_highest_slotid = new slotid4(_clientSession.maxRequests() - 1);
                op.opsequence.sa_sequenceid = slot.nextSequenceId();
                op.opsequence.sa_sessionid = _clientSession.sessionId();

                compound4args.argarray[0] = op;

                compound4res = _nfsClient.NFSPROC4_COMPOUND_4(compound4args);
                _lastUpdate = System.currentTimeMillis();

            } while (canRetry(compound4res.status, compound4args.tag.toString()));

            nfsstat.throwIfNeeded(compound4res.status);
            return compound4res;
        } finally {
            _clientSession.releaseSlot(slot);
        }
    }

    private boolean canRetry(int status, String compound) {
        switch (status) {

            case nfsstat.NFSERR_DELAY:
            case nfsstat.NFSERR_LAYOUTTRYLATER:
            case nfsstat.NFSERR_GRACE:
                System.out.println("Retrying " + compound + " on " + nfsstat.toString(status));
                try {
                    TimeUnit.SECONDS.sleep(ThreadLocalRandom.current().nextInt(5));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
                return true;
            default:
                return false;

        }
    }


    private void get_deviceinfo(deviceid4 deviceId) throws OncRpcException,
            IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withGetdeviceinfo(deviceId, clientLayoutType)
                .withTag("get_deviceinfo")
                .build();
        COMPOUND4res compound4res = sendCompoundInSession(args);

        nfsv4_1_file_layout_ds_addr4 addr = GetDeviceListStub
                .decodeFileDevice(compound4res.resarray.get(1).opgetdeviceinfo.gdir_resok4.gdir_device_addr.da_addr_body);

        _knowDevices.put(deviceId, new FileIoDevice(addr));
    }

    private void getLayoutTypes() throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withPutfh(_rootFh)
                .withGetattr(nfs4_prot.FATTR4_FS_LAYOUT_TYPES)
                .withTag("get_supported_layout_types")
                .build();

        COMPOUND4res compound4res = sendCompoundInSession(args);

        AttributeMap attrs = new AttributeMap(compound4res.resarray.get(compound4res.resarray.size() - 1).opgetattr.resok4.obj_attributes);
        Optional<fattr4_fs_layout_types> layoutTypes = attrs.get(nfs4_prot.FATTR4_FS_LAYOUT_TYPES);

        List<layouttype4> serverLayoutTypes = Arrays.stream(layoutTypes.get().value)
                .mapToObj(t -> {
                    try {
                        return layouttype4.valueOf(t);
                    } catch (BadLayoutException e) {
                        throw new IllegalArgumentException(e);
                    }
                })
                .collect(Collectors.toList());
        System.out.println("Server supported layout types: " + serverLayoutTypes);

        serverLayoutTypes.stream()
                .filter(clientLayoutType::equals)
                .findAny()
                .ifPresentOrElse(t -> System.out.println("Using layout type: " + t),
                        () -> {
                            System.out.println("Layout type " + clientLayoutType + " not supported. Disabling pNFS");
                            _isMDS = false;
                        });
    }

    private void get_devicelist() throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withPutfh(_rootFh)
                .withGetdevicelist(clientLayoutType)
                .withTag("get_devicelist")
                .build();

        try {
            COMPOUND4res compound4res = sendCompoundInSession(args);

            deviceid4[] deviceList = compound4res.resarray.get(2).opgetdevicelist.gdlr_resok4.gdlr_deviceid_list;

            System.out.println("Know devices: ");
            for (deviceid4 device : deviceList) {
                System.out.println("      Device: # " + Arrays.toString(device.value));
            }
        } catch (NotSuppException e) {
            // server does not supports deveice list
        }
    }

    private void nfsRead(nfs_fh4 fh, stateid4 stateid)
            throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withPutfh(fh)
                .withRead(4096, 0, stateid)
                .withTag("pNFS read")
                .build();
        COMPOUND4res compound4res = sendCompoundInSession(args);

        byte[] data = new byte[compound4res.resarray.get(2).opread.resok4.data.remaining()];
        compound4res.resarray.get(2).opread.resok4.data.get(data);
        System.out.println("[" + new String(data) + "]");

    }

    void nfsWrite(nfs_fh4 fh, byte[] data, long offset, stateid4 stateid)
            throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withPutfh(fh)
                .withWrite(offset, data, stateid)
                .withTag("pNFS write")
                .build();

        COMPOUND4res compound4res = sendCompoundInSession(args);
    }

    private void sequence() throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withTag("sequence")
                .build();
        COMPOUND4res compound4res = sendCompoundInSession(args);
    }

    private void get_supported_attributes() throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withPutfh(_rootFh)
                .withGetattr(nfs4_prot.FATTR4_CHANGE)
                .withTag("get_supported_attributes")
                .build();

        COMPOUND4res compound4res = sendCompoundInSession(args);
        // TODO:
    }

    private void reclaimComplete() throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withPutfh(_rootFh)
                .withReclaimComplete()
                .withTag("reclaim_complete")
                .build();

        COMPOUND4res compound4res = sendCompoundInSession(args);
    }

    private void remove(String path) throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withPutfh(_cwd)
                .withRemove(path)
                .withTag("remove")
                .build();
        COMPOUND4res compound4res = sendCompoundInSession(args);
    }

    private void lookup(String path) throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withPutfh(_cwd)
                .withSavefh()
                .withLookup(path)
                .withGetfh()
                .withGetattr(nfs4_prot.FATTR4_CHANGE,
                        nfs4_prot.FATTR4_SIZE, nfs4_prot.FATTR4_TIME_MODIFY)
                .withRestorefh()
                .withGetattr(nfs4_prot.FATTR4_CHANGE,
                        nfs4_prot.FATTR4_SIZE, nfs4_prot.FATTR4_TIME_MODIFY)
                .withTag("lookup-sun")
                .build();

        COMPOUND4res compound4res = sendCompoundInSession(args);
    }

    private void lookup(String fh, String path) throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withPutfh(new nfs_fh4(fh.getBytes()))
                .withLookup(path)
                .withGetfh()
                .withTag("lookup-with-id")
                .build();

        COMPOUND4res compound4res = sendCompoundInSession(args);
        System.out.println("fh = " + BaseEncoding.base16().lowerCase().encode(compound4res.resarray.get(compound4res.resarray.size() - 1).opgetfh.resok4.object.value));
    }

    private void getattr(String path) throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withPutfh(_cwd)
                .withLookup(path)
                .withGetattr(nfs4_prot.FATTR4_CHANGE,
                        nfs4_prot.FATTR4_SIZE, nfs4_prot.FATTR4_TIME_MODIFY, nfs4_prot.FATTR4_MODE)
                .withTag("getattr")
                .build();

        COMPOUND4res compound4res = sendCompoundInSession(args);

        AttributeMap attrs = new AttributeMap(compound4res.resarray.get(compound4res.resarray.size() - 1).opgetattr.resok4.obj_attributes);

        Optional<mode4> mode = attrs.get(nfs4_prot.FATTR4_MODE);
        if (mode.isPresent()) {
            System.out.println("mode: 0" + Integer.toOctalString(mode.get().value));
        }
    }

    private static class Connector extends CacheLoader<InetSocketAddress, NfsClient4Internal> {

        @Override
        public NfsClient4Internal load(InetSocketAddress f) {
            try {
                NfsClient4Internal client = new NfsClient4Internal(f);
                client.dsMount();
                return client;
            } catch (Exception e) {
                return null;
            }
        }
    }

    private static String basename(String path) {
        File f = new File(path);
        return f.getName();
    }

    private static String dirname(String path) {
        File f = new File(path);
        String parent = f.getParent();
        return parent == null ? "/" : parent;
    }
}
