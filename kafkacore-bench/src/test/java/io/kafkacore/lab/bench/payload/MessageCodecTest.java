package io.kafkacore.lab.bench.payload;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MessageCodecTest {

    @ParameterizedTest
    @ValueSource(ints = {MessageCodec.HEADER_SIZE, 100, 1024, 65_536})
    void roundTripsHeaderFieldsAtEverySize(int size) {
        MessageCodec codec = new MessageCodec(size, 42);
        byte[] payload = codec.encode(7, 123_456_789L, 987_654_321L);

        assertThat(payload).hasSize(size);
        MessageCodec.Decoded d = MessageCodec.decode(payload);
        assertThat(d.producerId()).isEqualTo(7);
        assertThat(d.sequence()).isEqualTo(123_456_789L);
        assertThat(d.sendNanos()).isEqualTo(987_654_321L);
        assertThat(d.sizeBytes()).isEqualTo(size);
    }

    @Test
    void detectsSingleBitFlipInPadding() {
        byte[] payload = new MessageCodec(512, 1).encode(1, 1, 1);
        payload[400] ^= 0x01;
        assertThatThrownBy(() -> MessageCodec.decode(payload))
                .isInstanceOf(MessageCodec.CorruptMessageException.class)
                .hasMessageContaining("CRC mismatch");
    }

    @Test
    void detectsTamperedSequenceNumber() {
        byte[] payload = new MessageCodec(64, 1).encode(1, 10, 1);
        payload[12] ^= 0x01; // last byte of the sequence field
        assertThatThrownBy(() -> MessageCodec.decode(payload)).isInstanceOf(MessageCodec.CorruptMessageException.class);
    }

    @Test
    void rejectsForeignAndTruncatedPayloads() {
        byte[] foreign = new byte[64];
        assertThatThrownBy(() -> MessageCodec.decode(foreign)).hasMessageContaining("bad magic");
        assertThatThrownBy(() -> MessageCodec.decode(new byte[10])).hasMessageContaining("too short");
        assertThatThrownBy(() -> MessageCodec.decode(null)).hasMessageContaining("too short");
    }

    @Test
    void rejectsSizesSmallerThanHeader() {
        assertThatThrownBy(() -> new MessageCodec(MessageCodec.HEADER_SIZE - 1, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void paddingIsDeterministicPerSeedAndCompressible() {
        byte[] a = new MessageCodec(256, 5).encode(1, 1, 1);
        byte[] b = new MessageCodec(256, 5).encode(1, 1, 1);
        assertThat(a).isEqualTo(b);
        for (int i = MessageCodec.HEADER_SIZE; i < a.length; i++) {
            assertThat(a[i]).isBetween((byte) '0', (byte) ('0' + 63));
        }
    }
}
