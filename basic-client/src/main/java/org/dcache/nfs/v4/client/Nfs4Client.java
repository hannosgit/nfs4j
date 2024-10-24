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

import com.google.common.net.HostAndPort;
import jakarta.annotation.Nonnull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

public class Nfs4Client implements AutoCloseable {

    private final NfsClient4Internal nfsClient;

    public Nfs4Client(@Nonnull String server, int port, @Nonnull String export) throws IOException {
        HostAndPort hp = HostAndPort.fromParts(server, port)
                .requireBracketsForIPv6();

        InetSocketAddress serverAddress = new InetSocketAddress(hp.getHost(), hp.getPort());
        this.nfsClient = new NfsClient4Internal(serverAddress);
        this.nfsClient.mount(export);
    }

    public void mkDir(String path) throws IOException {
        this.nfsClient.mkdir(path);
    }

    public List<NfsDirectoryEntry> readDir(String path) throws IOException {
        return this.nfsClient.readdir(path);
    }

    public void createFile(String path, String content) throws IOException {
        OpenReply or = this.nfsClient.create(path);

        this.nfsClient.nfsWrite(or.fh(), content.getBytes(), 0, or.stateid());
        this.nfsClient.close(or.fh(), or.stateid());
    }

    @Override
    public void close() throws Exception {
        if (nfsClient != null) {
            nfsClient.umount();
        }
    }
}
