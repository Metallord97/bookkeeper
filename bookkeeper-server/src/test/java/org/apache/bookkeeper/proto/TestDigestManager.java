package org.apache.bookkeeper.proto;

import io.netty.buffer.*;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.crypto.IllegalBlockSizeException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

@RunWith(Parameterized.class)
public class TestDigestManager {
    private static Random random = new Random();
    private DigestManager digestManager;
    private ByteBuf bufferForPackageSendingData;
    private ByteBuf bufferForPackageSendingLac;
    /**
     * Variables used for instantiating DigestManager
     */
    private final long ledgerId;
    private final byte[] passwd;
    private final DataFormats.LedgerMetadataFormat.DigestType digestType;
    private final ByteBufAllocator allocator;
    private final boolean useV2Protocol;
    /**
     * Variables used to test computeDigestAndPackageForSending
     */
    private long entryId;
    private long lastAddConfirmed;
    private long length;
    private ByteBuf entryData;
    private boolean isEntryValid;

    /**
     * Variables used to capture exceptions
     */
    private boolean isExceptionThrownOnConstructor = false;

    private boolean isExpectedExceptionThrownOnConstructor;
    private boolean isExceptionThrownOnComputeDigestAndPackageForSending = false;
    private boolean isExpectedExceptionThrownOnComputeDigestAndPackageForSending;
    private boolean isExceptionThrownOnVerifyDigestAndReturnLac = false;
    private boolean isExpectedExceptionThrownOnVerifyDigestAndReturnLac;
    private boolean isExceptionThrownOnVerifyDigestAndReturnData = false;
    private boolean isExpectedExceptionThrownOnVerifyDigestAndReturnData;
    private boolean isExceptionThrownOnVerifyDigestAndReturnLastConfirmed = false;
    private boolean isExpectedExceptionThrownOnVerifyDigestAndReturnLastConfirmed;


    @Parameterized.Parameters
    public static Collection getParameters() {
        return Arrays.asList(new Object[][]{
                {-1, null, DataFormats.LedgerMetadataFormat.DigestType.HMAC, null, false, -2, -1, 0, null, false, true, true, true, true, true},
               // {0, new byte[] {}, DataFormats.LedgerMetadataFormat.DigestType.DUMMY, UnpooledByteBufAllocator.DEFAULT, false, 0, 0, 7, generateEntry(8), true, true, false, false, false, false},
                //{1, new byte[] {1, 2, 3, 4}, DataFormats.LedgerMetadataFormat.DigestType.CRC32, UnpooledByteBufAllocator.DEFAULT, true, 2, 1, 9, generateEntry(8), true, false, true, true, true, true},
                {1, new byte[] {5, 6, 7, 8}, DataFormats.LedgerMetadataFormat.DigestType.CRC32, UnpooledByteBufAllocator.DEFAULT, true, 2, 1, 8, generateEntry(8), true, false, false, false, false, false},
                {1, new byte[] {5, 6, 7, 8}, DataFormats.LedgerMetadataFormat.DigestType.CRC32C, UnpooledByteBufAllocator.DEFAULT, true, 2, 1, 8, generateEntry(8), false, false, false, true, true, true},
                {1, new byte[] {5, 6, 7, 8}, DataFormats.LedgerMetadataFormat.DigestType.HMAC, UnpooledByteBufAllocator.DEFAULT, true, 2, 1, 8, generateEntry(8), true, false, false, false, false, false},
                {1, new byte[] {5, 6, 7, 8}, DataFormats.LedgerMetadataFormat.DigestType.DUMMY, UnpooledByteBufAllocator.DEFAULT, false, 2, 1, 8, generateEntry(8), true, false, false, false, false, false},
                {1, new byte[] {5, 6, 7, 8}, null, UnpooledByteBufAllocator.DEFAULT, true, 2, 1, 8, generateEntry(8), true, true, false, false, false, false}
        });
    }

    public TestDigestManager(long ledgerId, byte[] passwd, DataFormats.LedgerMetadataFormat.DigestType digestType,
                             ByteBufAllocator allocator, boolean useV2Protocol, long entryId, long lastAddConfirmed,
                             long length, ByteBuf entryData, boolean isEntryValid, boolean isExpectedExceptionThrownOnConstructor,
                             boolean isExpectedExceptionThrownOnComputeDigestAndPackageForSending,
                             boolean isExpectedExceptionThrownOnVerifyDigestAndReturnLac,
                             boolean isExpectedExceptionThrownOnVerifyDigestAndReturnData,
                             boolean isExpectedExceptionThrownOnVerifyDigestAndReturnLastConfirmed) {

        this.ledgerId = ledgerId;
        this.passwd = passwd;
        this.digestType = digestType;
        this.allocator = allocator;
        this.useV2Protocol = useV2Protocol;
        this.entryId = entryId;
        this.lastAddConfirmed = lastAddConfirmed;
        this.length = length;
        this.entryData = entryData;
        this.isEntryValid = isEntryValid;
        this.isExpectedExceptionThrownOnConstructor = isExpectedExceptionThrownOnConstructor;
        this.isExpectedExceptionThrownOnComputeDigestAndPackageForSending = isExpectedExceptionThrownOnComputeDigestAndPackageForSending;
        this.isExpectedExceptionThrownOnVerifyDigestAndReturnLac = isExpectedExceptionThrownOnVerifyDigestAndReturnLac;
        this.isExpectedExceptionThrownOnVerifyDigestAndReturnData = isExpectedExceptionThrownOnVerifyDigestAndReturnData;
        this.isExpectedExceptionThrownOnVerifyDigestAndReturnLastConfirmed = isExpectedExceptionThrownOnVerifyDigestAndReturnLastConfirmed;
    }

    @Before
    public void configureSUT() {
        try {
            digestManager = DigestManager.instantiate(ledgerId, passwd, digestType, allocator, useV2Protocol);
            if(isEntryValid) {
                ByteBufList packetLac = digestManager.computeDigestAndPackageForSendingLac(lastAddConfirmed);
                bufferForPackageSendingLac = ByteBufList.coalesce(packetLac);
            } else {
                bufferForPackageSendingLac = getInvalidPacket(PACKET.LAC);
            }
        } catch (Exception e) {
            isExceptionThrownOnConstructor = true;
        }

        try {
            if(isEntryValid) {
                ByteBufList packet = digestManager.computeDigestAndPackageForSending(entryId, lastAddConfirmed, length, entryData);
                bufferForPackageSendingData = ByteBufList.coalesce(packet);
            } else {
                bufferForPackageSendingData = getInvalidPacket(PACKET.DATA);
            }
        } catch (Exception e) {
            isExceptionThrownOnComputeDigestAndPackageForSending = true;
        }

    }

    @Test
    public void testConstructor() {
        Assert.assertEquals(isExpectedExceptionThrownOnConstructor, isExceptionThrownOnConstructor);
    }

    @Test
    public void testComputeDigestAndPackageForSending() {
        Assume.assumeTrue(!isExceptionThrownOnConstructor);

        ByteBufList packet = digestManager.computeDigestAndPackageForSending(entryId, lastAddConfirmed, length, entryData);
        ByteBuf bufferForPackageSendingData = ByteBufList.coalesce(packet);
        System.out.println(Arrays.toString(bufferForPackageSendingData.array()));
        long actualLedgerId = bufferForPackageSendingData.readLong();
        long actualEntryId = bufferForPackageSendingData.readLong();
        long actualLac = bufferForPackageSendingData.readLong();
        long actualLength = bufferForPackageSendingData.readLong();
        bufferForPackageSendingData.skipBytes(getMacCodeLength(digestType));
        byte[] actualEntryData = new byte[bufferForPackageSendingData.readableBytes()];
        bufferForPackageSendingData.readBytes(actualEntryData);

        System.out.println(Arrays.toString(actualEntryData));
        System.out.println(Arrays.toString(entryData.array()));

        Assert.assertEquals(ledgerId, actualLedgerId);
        Assert.assertEquals(entryId, actualEntryId);
        Assert.assertEquals(lastAddConfirmed, actualLac);
        Assert.assertEquals(length, actualLength);
        Assert.assertArrayEquals(entryData.array(), actualEntryData);
        Assert.assertEquals(isExpectedExceptionThrownOnComputeDigestAndPackageForSending, isExceptionThrownOnComputeDigestAndPackageForSending);
    }

    @Test
    public void testVerifyDigestAndReturnLac() {
        Assume.assumeTrue(!isExceptionThrownOnConstructor );
        long actualLac;
        try {
            actualLac = digestManager.verifyDigestAndReturnLac(bufferForPackageSendingLac);
            Assert.assertEquals(lastAddConfirmed, actualLac);
        } catch (BKException.BKDigestMatchException e) {
            isExceptionThrownOnVerifyDigestAndReturnLac = true;
        }

        Assert.assertEquals(isExpectedExceptionThrownOnVerifyDigestAndReturnLac, isExceptionThrownOnVerifyDigestAndReturnLac);
    }

    @Test(expected = BKException.BKDigestMatchException.class)
    public void testVerifyDigestAndReturnLacWrongLedgerId() throws BKException.BKDigestMatchException, GeneralSecurityException {
        Assume.assumeTrue(!isExceptionThrownOnConstructor );
        ByteBuf packet = getInvalidPacket(PACKET.LAC_WRONG_LEDGER_ID);
        digestManager.verifyDigestAndReturnLac(packet);
    }

    @Test(expected = BKException.BKDigestMatchException.class)
    public void testVerifyDigestAndReturnLacWrongPacketSize() throws BKException.BKDigestMatchException {
        Assume.assumeTrue(!isExceptionThrownOnConstructor );
        bufferForPackageSendingLac.readerIndex(1);
        digestManager.verifyDigestAndReturnLac(bufferForPackageSendingLac);
    }


    @Test(expected = BKException.BKDigestMatchException.class)
    public void testVerifyDigestAndReturnDataWrongLedgerId() throws GeneralSecurityException, BKException.BKDigestMatchException {
        Assume.assumeTrue(!isExceptionThrownOnConstructor );
        ByteBuf packet = getInvalidPacket(PACKET.DATA_WRONG_LEDGER_ID);
        digestManager.verifyDigestAndReturnData(entryId, packet);
    }

    @Test(expected = BKException.BKDigestMatchException.class)
    public void testVerifyDigestAndReturnDataWrongEntryId() throws GeneralSecurityException, BKException.BKDigestMatchException {
        Assume.assumeTrue(!isExceptionThrownOnConstructor );
        ByteBuf packet = getInvalidPacket(PACKET.DATA_WRONG_ENTRY_ID);
        digestManager.verifyDigestAndReturnData(entryId, packet);
    }

    @Test(expected = BKException.BKDigestMatchException.class)
    public void testVerifyDigestAndReturnDataWrongSize() throws GeneralSecurityException, BKException.BKDigestMatchException {
        Assume.assumeTrue(!isExceptionThrownOnConstructor );
        ByteBuf packet = getInvalidPacket(PACKET.DATA);
        packet.readerIndex(1);
        digestManager.verifyDigestAndReturnData(entryId, packet);
    }

    @Test
    public void testVerifyDigestAndReturnData() {
        Assume.assumeTrue(!(isExceptionThrownOnConstructor | isExceptionThrownOnComputeDigestAndPackageForSending));
        ByteBuf actualData;
        try {
            actualData = digestManager.verifyDigestAndReturnData(entryId, bufferForPackageSendingData);
            Assert.assertArrayEquals(bufferForPackageSendingData.array(), actualData.array());
        } catch (BKException.BKDigestMatchException e) {
            isExceptionThrownOnVerifyDigestAndReturnData = true;
        }
        Assert.assertEquals(isExpectedExceptionThrownOnVerifyDigestAndReturnData, isExceptionThrownOnVerifyDigestAndReturnData);

    }

    @Test
    public void testVerifyDigestAndReturnLastConfirmed() {
        Assume.assumeTrue(!(isExceptionThrownOnConstructor | isExceptionThrownOnComputeDigestAndPackageForSending));
        DigestManager.RecoveryData actualLastConfirmed;
        try {
            actualLastConfirmed = digestManager.verifyDigestAndReturnLastConfirmed(bufferForPackageSendingData);
            Assert.assertEquals(lastAddConfirmed, actualLastConfirmed.getLastAddConfirmed());
        } catch (BKException.BKDigestMatchException e) {
            isExceptionThrownOnVerifyDigestAndReturnLastConfirmed = true;
        }
        Assert.assertEquals(isExpectedExceptionThrownOnVerifyDigestAndReturnLastConfirmed, isExceptionThrownOnVerifyDigestAndReturnLastConfirmed);
    }

    private static ByteBuf generateEntry(int length) {
        byte[] data = new byte[length];
        ByteBuf bb = Unpooled.buffer(length);
        random.nextBytes(data);
        System.out.println(Arrays.toString(data));
        bb.writeBytes(data);
        return bb;
    }
    
    private static int getMacCodeLength(DataFormats.LedgerMetadataFormat.DigestType digestType) {
        switch (digestType) {
            case HMAC:
                return 20;
            case CRC32:
                return 8;
            case CRC32C:
                return 4;
            case DUMMY:
                return 0;
            default:
                throw new IllegalArgumentException("Not a correct DigestType");
        }

    }

    public ByteBuf getInvalidPacket(PACKET returnType) throws GeneralSecurityException {

        DigestManager manager = DigestManager.instantiate(ledgerId, passwd, digestType, allocator, useV2Protocol);
        ByteBufList packet;
        ByteBuf data;
        int writerIndex;
        switch (returnType) {
            case LAC:
                packet = manager.computeDigestAndPackageForSendingLac(lastAddConfirmed);
                data = ByteBufList.coalesce(packet);
                data.readLong();
                data.readLong();
                data.writerIndex(data.readerIndex());
                for(int i = 0; i < getMacCodeLength(digestType); i++)
                    data.writeByte(random.nextInt());
                break;

            case DATA:
                packet = manager.computeDigestAndPackageForSending(entryId, lastAddConfirmed, length, entryData);
                data = ByteBufList.coalesce(packet);
                for(int i = 0 ; i < 4; i ++) data.readLong();
                data.writerIndex(data.readerIndex());
                for(int i = 0; i < getMacCodeLength(digestType); i++)
                    data.writeByte(random.nextInt());
                break;

            case LAC_WRONG_LEDGER_ID:
                packet = digestManager.computeDigestAndPackageForSendingLac(lastAddConfirmed);
                data = ByteBufList.coalesce(packet);
                writerIndex = data.writerIndex();
                data.clear();
                data.writeLong(random.nextLong());
                data.writerIndex(writerIndex);
                break;

            case DATA_WRONG_LEDGER_ID:
                packet = digestManager.computeDigestAndPackageForSending(entryId, lastAddConfirmed, length, entryData);
                data = ByteBufList.coalesce(packet);
                writerIndex = data.writerIndex();
                data.writerIndex(0);
                data.writeLong(random.nextLong());
                data.writerIndex(writerIndex);
                break;

            case DATA_WRONG_ENTRY_ID:
                packet = digestManager.computeDigestAndPackageForSending(entryId, lastAddConfirmed, length, entryData);
                data = ByteBufList.coalesce(packet);
                writerIndex = data.writerIndex();
                data.writerIndex(Long.BYTES);
                data.writeLong(random.nextLong());
                data.writerIndex(writerIndex);
                break;

            default:
                throw new IllegalBlockSizeException("PACKET not valid");

        }


        data.readerIndex(0);
        return data;

    }

    public  enum PACKET {
        LAC, DATA, LAC_WRONG_LEDGER_ID, DATA_WRONG_LEDGER_ID, DATA_WRONG_ENTRY_ID;
    }
}
