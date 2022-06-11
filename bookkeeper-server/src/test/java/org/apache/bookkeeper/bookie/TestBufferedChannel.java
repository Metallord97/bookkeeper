package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.*;

@RunWith(Parameterized.class)
public class TestBufferedChannel {
    private static Random random = null;
    private static Set<byte[]> usedBuffer = null;
    private FileChannel fileChannel;
    private int writeCapacity;
    private int readCapacity;
    private long unpersistedBytesBound;
    private int byteBufLength;
    private BufferedChannel bufferedChannel;
    private ByteBuf byteBuf;

    @BeforeClass
    public static void configureRandomGenerator() {
        TestBufferedChannel.random = new Random();
        TestBufferedChannel.usedBuffer = new HashSet<>();
    }

    @AfterClass
    public static void cleanAndReport() {
        System.out.println("The following seeds have been generated: ");
        for(byte[] buf: TestBufferedChannel.usedBuffer) {
            System.out.println(Arrays.toString(buf));
        }
        TestBufferedChannel.usedBuffer.clear();
    }

    public TestBufferedChannel(int writeCapacity, int readCapacity, long unpersistedBytesBound, int byteBufLength) {
        this.writeCapacity = writeCapacity;
        this.readCapacity = readCapacity;
        this.unpersistedBytesBound = unpersistedBytesBound;
        this.byteBufLength = byteBufLength;
    }

    @Parameterized.Parameters
    public static Collection getParameters() {
        return Arrays.asList(new Object[][] {
                {512, 512, 0, 128}
        });
    }

    @Before
    public void configureSUT() throws IOException {
        File newLogFile = File.createTempFile("test", "log");
        newLogFile.deleteOnExit();
        this.fileChannel = new RandomAccessFile(newLogFile, "rw").getChannel();
        this.bufferedChannel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, this.fileChannel,
                this.writeCapacity, this.readCapacity, this.unpersistedBytesBound);
        this.byteBuf = generateEntry();
    }

    @After
    public void closeResources() throws IOException {
        this.fileChannel.close();
        this.bufferedChannel.close();
    }

    @Test
    public void testWriteAndRead() throws IOException {
        this.bufferedChannel.write(this.byteBuf);
        ByteBuf dstBuf = Unpooled.buffer(this.byteBufLength);
        this.bufferedChannel.read(dstBuf, 0, this.byteBufLength);
        Assert.assertEquals(this.byteBuf, dstBuf);
    }

    private ByteBuf generateEntry() {
        byte[] data = new byte[this.byteBufLength];
        ByteBuf bb = Unpooled.buffer(this.byteBufLength);
        TestBufferedChannel.random.nextBytes(data);
        TestBufferedChannel.usedBuffer.add(data);
        bb.writeBytes(data);
        return bb;
    }
}
