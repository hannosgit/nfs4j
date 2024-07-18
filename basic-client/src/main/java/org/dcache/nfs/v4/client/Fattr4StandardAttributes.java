package org.dcache.nfs.v4.client;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.dcache.nfs.v4.xdr.attrlist4;
import org.dcache.nfs.v4.xdr.bitmap4;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;

import static org.dcache.nfs.v4.xdr.nfs4_prot.*;

public record Fattr4StandardAttributes(int type, long size, long inode, long mode, long numLinks, String owner,
                                       String group, long spaceUsed, Instant aTime, Instant crTime, Instant cTime, Instant mTime)
{

    static final bitmap4 STANDARD_ATTRIBUTES = bitmap4.of(
        FATTR4_TYPE,
        FATTR4_SIZE,
        FATTR4_FILEID,
        FATTR4_MODE,
        FATTR4_NUMLINKS,
        FATTR4_OWNER,
        FATTR4_OWNER_GROUP,
        FATTR4_SPACE_USED,
        FATTR4_TIME_ACCESS,
        FATTR4_TIME_CREATE,
        FATTR4_TIME_METADATA,
        FATTR4_TIME_MODIFY
    );

    static Fattr4StandardAttributes parse(attrlist4 attrlist) {
        final byte[] value = attrlist.value;
        int byteIndex = 0;
        int byteOffset = 4;

        final int type = getAnInt(value, byteIndex, (byteIndex += byteOffset));
        byteOffset = 8;
        final long size = getALong(value, byteIndex, (byteIndex += byteOffset));
        byteOffset = 8;
        final long inode = getALong(value, byteIndex, (byteIndex += byteOffset));
        byteOffset = 4;
        final int mode = getAnInt(value, byteIndex, (byteIndex += byteOffset));
        byteOffset = 4;
        final int numLinks = getAnInt(value, byteIndex, (byteIndex += byteOffset));
        byteOffset = 4;
        final int ownerLength = getAnInt(value, byteIndex, (byteIndex += byteOffset));
        byteOffset = ownerLength;
        final String owner = getUTF8String(value, byteIndex, (byteIndex += byteOffset));

        byteOffset = 4;
        byteIndex += calculatePadding(ownerLength);
        final int groupLength = getAnInt(value, byteIndex, (byteIndex += byteOffset));
        byteOffset = groupLength;
        final String group = getUTF8String(value, byteIndex, (byteIndex += byteOffset));

        byteIndex += calculatePadding(ownerLength);

        byteOffset = 8;
        final long spaceUsed = getALong(value, byteIndex, (byteIndex += byteOffset));
        byteOffset = 12;
        final Instant aTime = getTimestamp(value, byteIndex, (byteIndex += byteOffset));
        byteOffset = 12;
        final Instant crTime = getTimestamp(value, byteIndex, (byteIndex += byteOffset));
        byteOffset = 12;
        final Instant cTime = getTimestamp(value, byteIndex, (byteIndex += byteOffset));
        byteOffset = 12;
        final Instant mTime = getTimestamp(value, byteIndex, (byteIndex += byteOffset));

        return new Fattr4StandardAttributes(type, size, inode, mode, numLinks, owner, group, spaceUsed, aTime, crTime, cTime, mTime);
    }

    private static long getALong(byte[] value, int from, int to) {
        return Longs.fromByteArray(Arrays.copyOfRange(value, from, to));
    }

    private static int getAnInt(byte[] value, int from, int to) {
        return Ints.fromByteArray(Arrays.copyOfRange(value, from, to));
    }

    private static Instant getTimestamp(byte[] value, int from, int to) {
        final long seconds = getALong(value, from, to - 4);
        final int nanoSeconds = getAnInt(value, to - 4, to);

        return Instant.ofEpochSecond(seconds, nanoSeconds);
    }

    private static String getUTF8String(byte[] value, int from, int to) {
        return new String(value, from, to - from, StandardCharsets.UTF_8);
    }

    private static int calculatePadding(int slen) {
        return (4 - (slen & 0x03)) & 0x03;
    }

}
