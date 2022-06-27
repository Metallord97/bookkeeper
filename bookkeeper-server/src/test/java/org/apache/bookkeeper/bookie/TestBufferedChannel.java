package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * 1. Identificazione delle classi di equivalenza per
 *    BufferedChannel(ByteBufAllocator allocator, FileChannel fc, int writeCapacity, int readCapacity, long unpersistedBytesBound)
 *    - allocator: {null}, {"valid_instance"}, {"invalid_instance"}
 *    - fc: {null}, {"valid_instance"}, {"invalid_instance"}
 *    - writeCapacity: { <= 0 , > 0 }
 *    - readCapacity: { <= 0 , > 0}
 *    - unpersistedBytesBound: { <= 0 , > 0}
 *
 *    Scelta dei valori a confine della classe di equivalenza.
 *    - writeCapacity = -1, 0, 1
 *    - readCapacity = -1, 0, 1
 *    - unpersistedBytesBound: -1, 0, 1
 *
 *
 *    {allocator = "valid_instance", fc = "valid_instance", writeCapacity: -1, readCapacity : 0, unpersistedBytesBound : 1} ---> mi aspetto un errore/eccezione
 *    {allocator = "valid_instance", fc = "valid_instance", writeCapacity: 0, readCapacity : 0, unpersistedBytesBound : 1} ---> errore/eccezione
 *    {allocator = "valid_instance", fc = "valid_instance", writeCapacity: 1, readCapacity : -1, unpersistedBytesBound : 1} ---> errore/eccezione
 *    {allocator = "valid_instance", fc = "valid_instance", writeCapacity: -1, readCapacity : 0, unpersistedBytesBound : 1} ---> errore/eccezione
 *    {allocator = "valid_instance", fc = "valid_instance", writeCapacity: 1, readCapacity : 1, unpersistedBytesBound : 0} ---> istanza valida
 *    {allocator = null, fc = "valid_instance", writeCapacity: 1, readCapacity : 0, unpersistedBytesBound : 1} ---> errore/eccezione
 *    {allocator = "valid_instance", fc = null, writeCapacity: 1, readCapacity : 0, unpersistedBytesBound : 1} --->errore/eccezione
 *    {allocator = "invalid_instance", fc = null, writeCapacity: 1, readCapacity : 0, unpersistedBytesBound : 1} ---> errore/eccezione
 *    {allocator = "valid_instance", fc = "invalid_instance", writeCapacity: 1, readCapacity : 0, unpersistedBytesBound : 1} -->errore/eccezione
 *
 *    I metodi da testare sono:
 *    - write(ByteBuf src)
 *    - read(ByteBuf dest, long pos, int length)
 *
 *    Il metodo write deve tornare un'eccezione nel caso in cui writeCapacity sia = 0.
 *    Il metodo read deve tornare un'eccezione nel caso in cui readCapacity = 0, oppure se readCapacity > 0, torna -1 se pos >= della size del file.
 *    Se length è più grande della grandezza del file dovrebbe esserci una eccezione.
 *
 *    Nuovo Tg:
 *    {allocator = "valid_instance", fc = "valid_instance", writeCapacity: -1, readCapacity : 0, unpersistedBytesBound : 1, pos: 0, length: 0} ---> eccezione nel costruttore di conseguenza non posso fare read o write
 *    {allocator = "valid_instance", fc = "valid_instance", writeCapacity: 0, readCapacity : 0, unpersistedBytesBound : 1, pos: 0, length: 0} ---> eccezione
 *    {allocator = "valid_instance", fc = "valid_instance", writeCapacity: 1, readCapacity : -1, unpersistedBytesBound : 1, pos: 0, length: 0} --> eccezione
 *    {allocator = "valid_instance", fc = "valid_instance", writeCapacity: -1, readCapacity : 0, unpersistedBytesBound : 1, pos: 0, length: 0} -->eccezione
 *    {allocator = "valid_instance", fc = "valid_instance", writeCapacity: 1, readCapacity : 1, unpersistedBytesBound : 0, pos: 0, length: 1} --> istanza valida + read e write validi
 *    {allocator = null, fc = "valid_instance", writeCapacity: 1, readCapacity : 0, unpersistedBytesBound : 1, pos: 0, length: 1} --> errore/eccezione
 *    {allocator = "valid_instance", fc = null, writeCapacity: 1, readCapacity : 0, unpersistedBytesBound : 1, pos: 0, length: 1} --->errore/eccezione
 *    {allocator = "invalid_instance", fc = null, writeCapacity: 1, readCapacity : 0, unpersistedBytesBound : 1, pos: 0, length: 1} ---> errore/eccezione
 *    {allocator = "valid_instance", fc = "invalid_instance", writeCapacity: 1, readCapacity : 0, unpersistedBytesBound : 1, pos: 0, length: 1} -->errore/eccezione
 *    {allocator = "valid_instance", fc = "valid_instance", writeCapacity: 1, readCapacity : 1, unpersistedBytesBound : 0, pos: 0, length: 2} ---> eccezione sulla read
 *
 */
@RunWith(Parameterized.class)
public class TestBufferedChannel {
    private static Random random = new Random();
    private static Set<byte[]> usedBuffer = new HashSet<>();
    private ByteBufAllocator allocator;
    private FileChannel fileChannel;
    private final int writeCapacity;
    private final int readCapacity;
    private final long unpersistedBytesBound;
    private ByteBuf srcBuf;
    private ByteBuf dstBuf;
    private BufferedChannel bufferedChannel;
    private BufferedChannel bufferedChannel2;
    private boolean isExceptionThrownOnConstructor;
    private final boolean isExpectedExceptionThrownOnConstructor;
    private boolean isExceptionThrownOnWrite;
    private final boolean isExpectedExceptionThrownOnWrite;
    private boolean isExceptionThrownOnRead;
    private final boolean isExpectedExceptionThrownOnRead;

    private final long positionRead;
    private final int lengthRead;
/*
    @BeforeClass
    public static void configureRandomGenerator() {
        TestBufferedChannel.random = new Random();
        TestBufferedChannel.usedBuffer = new HashSet<>();
    }
*/
    @AfterClass
    public static void cleanAndReport() {
        System.out.println("The following seeds have been generated: ");
        for(byte[] buf: TestBufferedChannel.usedBuffer) {
            System.out.println(Arrays.toString(buf));
        }
        TestBufferedChannel.usedBuffer.clear();
    }

    public TestBufferedChannel(ByteBufAllocator allocator, FileChannel fileChannel, int writeCapacity, int readCapacity,
                               long unpersistedBytesBound, ByteBuf srcBuf, ByteBuf dstBuf, long positionRead,
                               int lengthRead, boolean isExpectedExceptionThrownOnConstructor,
                               boolean isExpectedExceptionThrownOnWrite,
                               boolean isExpectedExceptionThrownOnRead) {

        this.allocator = allocator;
        this.fileChannel = fileChannel;
        this.writeCapacity = writeCapacity;
        this.readCapacity = readCapacity;
        this.unpersistedBytesBound = unpersistedBytesBound;
        this.srcBuf = srcBuf;
        this.dstBuf = dstBuf;
        this.positionRead = positionRead;
        this.lengthRead = lengthRead;
        this.isExpectedExceptionThrownOnConstructor = isExpectedExceptionThrownOnConstructor;
        this.isExpectedExceptionThrownOnWrite = isExpectedExceptionThrownOnWrite;
        this.isExpectedExceptionThrownOnRead = isExpectedExceptionThrownOnRead;
    }

    @Parameterized.Parameters
    public static Collection getParameters() throws IOException {
        return Arrays.asList(new Object[][] {
                {null, null, -1, -1, -1, null, generateEmptyBuf(8), -1, -2, true, true, true},
                //disabled{UnpooledByteBufAllocator.DEFAULT, getFileChannel(), 0, 0, 0, generateEntry(8), generateEmptyBuf(8), 0, 8, false, false, false},
                {UnpooledByteBufAllocator.DEFAULT, getFileChannel(), 1, 1, 1, null, generateEmptyBuf(8), 0, 8, false, true, true},
                {UnpooledByteBufAllocator.DEFAULT, getFileChannel(), 1, 1, 1, generateEntry(8), generateEmptyBuf(8), 0, 8, false, false, false},
                {UnpooledByteBufAllocator.DEFAULT, getFileChannel(), 1, 1, 1, generateEntry(8), null, 0, 8, false, false, true},
                {UnpooledByteBufAllocator.DEFAULT, getFileChannel(), 1, 1, 1, generateEntry(8), generateEmptyBuf(8), 9, 18, false, false, true},
                {UnpooledByteBufAllocator.DEFAULT, getFileChannel(), 8, 0, 1024, generateEntry(16), generateEmptyBuf(16), 0, 7, false, false, true},
                {UnpooledByteBufAllocator.DEFAULT, getFileChannel(), 7, 1024, 9, generateEntry(8), generateEmptyBuf(8), 1, 7, false, false, false},
                {UnpooledByteBufAllocator.DEFAULT, getFileChannel(), 8, 1, 1, generateEntry(10), generateEmptyBuf(10), 1, 7, false, false, false},
                {UnpooledByteBufAllocator.DEFAULT, getFileChannel(), 1024, 1024, 1024, generateEntry(8), generateEmptyBuf(8), 1, 7, false, false, false},
                {UnpooledByteBufAllocator.DEFAULT, getFileChannel(), 1024, 1024, 1024, generateEntry(8), generateEmptyBuf(8), 9, 8, false, false, true},
                {UnpooledByteBufAllocator.DEFAULT, getFileChannel(), 1024, 1024, 0, generateEntry(8), generateEmptyBuf(8), 0, 8, false, false, false}

                /*
                {UnpooledByteBufAllocator.DEFAULT, getFileChannel(), -1, 0, 1, generateEntry(8), generateEmptyBuf(8), 0, 9, true, true, true},
                {null, getFileChannel(), -1, 0, 1, generateEntry(8), generateEmptyBuf(8), 0, 9, true, true, true},
                {UnpooledByteBufAllocator.DEFAULT, null, -1, 0, 1, generateEntry(8), generateEmptyBuf(8), 0, 9, true, true, true},
                {UnpooledByteBufAllocator.DEFAULT, getFileChannel(), 1, 1, -1, generateEntry(8), generateEmptyBuf(8), 0, 8, true, true, true},
                {UnpooledByteBufAllocator.DEFAULT, getFileChannel(), 1, 1, 0, generateEntry(8), generateEmptyBuf(8), 0, 1, false, false, false},
                {UnpooledByteBufAllocator.DEFAULT, getFileChannel(), 1, 1, 0, generateEntry(8), generateEmptyBuf(8), 0, 1, false, false, false},
                {UnpooledByteBufAllocator.DEFAULT, getFileChannel(), 1, 1, 0, generateEntry(8), generateEmptyBuf(8), 0, 9, false, false, true},
                {UnpooledByteBufAllocator.DEFAULT, getFileChannel(), 8, 8, 8, generateEntry(8), generateEmptyBuf(8), 1, 8, false, false, true},
                {UnpooledByteBufAllocator.DEFAULT, getFileChannel(), 8, 0, 8, generateEntry(8), generateEmptyBuf(8), 1, 8, false, false, true},
                {UnpooledByteBufAllocator.DEFAULT, getFileChannel(), 8, 8, 8, generateEntry(16), generateEmptyBuf(16), 0, 17, false, false, true},
                {UnpooledByteBufAllocator.DEFAULT, getFileChannel(), 8, 8, 8, generateEntry(16), generateEmptyBuf(16), -1, 16, false, false, true},
                {UnpooledByteBufAllocator.DEFAULT, getFileChannel(), 8, 0, 8, null, generateEmptyBuf(8), 0, 8, false, true, true},
                {UnpooledByteBufAllocator.DEFAULT, getFileChannel(), 8, 0, 8, generateEntry(8), null, 0, 8, false, false, true},
                {UnpooledByteBufAllocator.DEFAULT, getFileChannel(), 8, 0, 8, generateEntry(8), generateEntry(4), 0, 4, false, false, true},
                {UnpooledByteBufAllocator.DEFAULT, getFileChannel(), 8, 0, 8, generateEntry(8), generateEmptyBuf(7), 0, 7, false, false, false},
                {UnpooledByteBufAllocator.DEFAULT, getFileChannel(), 0, 8, 8, generateEntry(8), generateEmptyBuf(8), 1, 7, false, false, false},
                {UnpooledByteBufAllocator.DEFAULT, getFileChannel(), 0, 0, 0, generateEntry(8), generateEmptyBuf(8), 0, 9, false, false, false}

                 */
        });
    }

    @Before
    public void configureSUT() {
        try {
            this.bufferedChannel = new BufferedChannel(this.allocator, this.fileChannel,this.writeCapacity, this.readCapacity, this.unpersistedBytesBound);
        } catch (Exception e) {
            this.isExceptionThrownOnConstructor = true;
        }
    }

    @Before
    public void configureSUT2() throws IOException {
        this.bufferedChannel2 = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, getFileChannel(), 8, 1, 1);
    }


    @Test
    public void testBufferedChannelConstructor() {
        Assert.assertEquals(this.isExpectedExceptionThrownOnConstructor, this.isExceptionThrownOnConstructor);
    }

    @Test
    public void testWriteAndRead() {
        int expectedWritePosition = 0;
        int actualByteRead = 0;
        try {
            expectedWritePosition = (int) (this.bufferedChannel.position() + srcBuf.readableBytes());
            this.bufferedChannel.write(srcBuf);
        } catch (Exception e) {
            this.isExceptionThrownOnWrite = true;
        }

        try {
            actualByteRead = this.bufferedChannel.read(dstBuf,this.positionRead, this.lengthRead);
        } catch (Exception e) {
            this.isExceptionThrownOnRead = true;
        }

        Assert.assertEquals(this.isExpectedExceptionThrownOnWrite, this.isExceptionThrownOnWrite);
        Assert.assertEquals(this.isExpectedExceptionThrownOnRead, this.isExceptionThrownOnRead);
        if(! this.isExceptionThrownOnRead ) {
            byte[] expectedBytes = new byte[srcBuf.capacity()];
            srcBuf.getBytes(0, expectedBytes);
            expectedBytes = Arrays.copyOfRange(expectedBytes, (int) this.positionRead, (int) (this.positionRead + this.lengthRead));
            byte[] byteRead = new byte[(int) (this.lengthRead)];
            dstBuf.getBytes(0, byteRead);
            Assert.assertEquals(this.lengthRead, actualByteRead);
            Assert.assertArrayEquals(expectedBytes, byteRead);
            Assert.assertEquals(expectedWritePosition, this.bufferedChannel.position());

        }
    }

    @Test
    public void testWritePersistedByteBound() throws IOException {
        int len = 14;
        ByteBuf src = generateEntry(len);
        this.bufferedChannel2.write(src);

        ByteBuffer dst = ByteBuffer.allocate(len);
        this.bufferedChannel2.fileChannel.position(0);
        this.bufferedChannel2.fileChannel.read(dst);

        byte[] expectedBytes = src.array();
        byte[] actualByte = dst.array();
        System.out.println("Expected Bytes: " + Arrays.toString(expectedBytes));
        System.out.println("Actual Bytes: " + Arrays.toString(actualByte));

        Assert.assertArrayEquals(expectedBytes, actualByte);
    }


    private static ByteBuf generateEntry(int length) {
        byte[] data = new byte[length];
        ByteBuf bb = Unpooled.buffer(length);
        TestBufferedChannel.random.nextBytes(data);
        TestBufferedChannel.usedBuffer.add(data);
        bb.writeBytes(data);
        return bb;
    }

    private static ByteBuf generateEmptyBuf(int length) {
        return Unpooled.buffer(length);
    }

    private static File createTempFile() throws IOException {
        File newLogFile = File.createTempFile("test", "log");
        newLogFile.deleteOnExit();
        return newLogFile;
    }

    private static FileChannel getFileChannel() throws IOException {
        return new RandomAccessFile(createTempFile(), "rw").getChannel();
    }
}
