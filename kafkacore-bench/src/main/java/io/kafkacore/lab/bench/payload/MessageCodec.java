package io.kafkacore.lab.bench.payload;

import java.nio.ByteBuffer;
import java.util.SplittableRandom;
import java.util.zip.CRC32C;

/**
 * Self-verifying benchmark payload (see ADR-0006).
 *
 * <pre>
 *  offset  size  field
 *  0       1     magic (0x4B 'K')
 *  1       4     producerId
 *  5       8     sequence number (per producer, starts at 0, gap-free)
 *  13      8     send timestamp (System.nanoTime of the producing JVM)
 *  21      4     CRC32C over bytes [0,21) and [25,end)
 *  25      n     pseudo-random padding up to the requested message size
 * </pre>
 *
 * The sequence number lets consumers detect loss, duplication and re-ordering; the CRC detects
 * corruption anywhere in the record; the timestamp gives end-to-end latency without clock skew
 * because producer and consumer share a JVM.
 */
public final class MessageCodec {

    public static final byte MAGIC = 0x4B;
    public static final int HEADER_SIZE = 25;
    private static final int CRC_OFFSET = 21;

    private final byte[] padding;

    /**
     * @param messageSize total encoded size in bytes (>= {@link #HEADER_SIZE})
     * @param seed        seed for the padding bytes; random padding defeats compression unrealistically well
     */
    public MessageCodec(int messageSize, long seed) {
        if (messageSize < HEADER_SIZE) {
            throw new IllegalArgumentException("messageSize must be >= " + HEADER_SIZE);
        }
        this.padding = new byte[messageSize - HEADER_SIZE];
        SplittableRandom rnd = new SplittableRandom(seed);
        for (int i = 0; i < padding.length; i++) {
            // Restrict to a 64-symbol alphabet: realistic entropy so compression codecs have something to do.
            padding[i] = (byte) ('0' + rnd.nextInt(64));
        }
    }

    public int messageSize() {
        return padding.length + HEADER_SIZE;
    }

    public byte[] encode(int producerId, long sequence, long sendNanos) {
        byte[] out = new byte[messageSize()];
        ByteBuffer buf = ByteBuffer.wrap(out);
        buf.put(MAGIC).putInt(producerId).putLong(sequence).putLong(sendNanos).putInt(0);
        buf.put(padding);
        ByteBuffer.wrap(out, CRC_OFFSET, 4).putInt(crc(out));
        return out;
    }

    /** Decodes and verifies a payload, throwing {@link CorruptMessageException} on any mismatch. */
    public static Decoded decode(byte[] data) {
        if (data == null || data.length < HEADER_SIZE) {
            throw new CorruptMessageException("payload too short: " + (data == null ? "null" : data.length));
        }
        ByteBuffer buf = ByteBuffer.wrap(data);
        byte magic = buf.get();
        if (magic != MAGIC) {
            throw new CorruptMessageException("bad magic byte: " + magic);
        }
        int producerId = buf.getInt();
        long seq = buf.getLong();
        long sendNanos = buf.getLong();
        int storedCrc = buf.getInt();
        int actualCrc = crc(data);
        if (storedCrc != actualCrc) {
            throw new CorruptMessageException("CRC mismatch for producer " + producerId + " seq " + seq);
        }
        return new Decoded(producerId, seq, sendNanos, data.length);
    }

    private static int crc(byte[] data) {
        CRC32C crc = new CRC32C();
        crc.update(data, 0, CRC_OFFSET);
        crc.update(data, HEADER_SIZE, data.length - HEADER_SIZE);
        return (int) crc.getValue();
    }

    /** A successfully verified payload. */
    public record Decoded(int producerId, long sequence, long sendNanos, int sizeBytes) {
    }

    /** Raised when a payload fails structural or checksum validation. */
    public static final class CorruptMessageException extends RuntimeException {
        public CorruptMessageException(String message) {
            super(message);
        }
    }
}
