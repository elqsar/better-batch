package wal

import (
	"encoding/binary"
	"hash/crc32"
)

// headerSize is the size of the per-record header: a uint32 payload length
// followed by a uint32 CRC.
const headerSize = 8

// crcTable is Castagnoli, which has hardware support on amd64 and arm64.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// checksum covers the length field as well as the payload so that a corrupted
// length is caught rather than used to read garbage.
func checksum(lenField uint32, payload []byte) uint32 {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], lenField)
	h := crc32.New(crcTable)
	h.Write(b[:])
	h.Write(payload)
	return h.Sum32()
}

// appendRecord encodes payload as a framed record and appends it to dst.
func appendRecord(dst, payload []byte) []byte {
	n := uint32(len(payload))
	var hdr [headerSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], n)
	binary.LittleEndian.PutUint32(hdr[4:8], checksum(n, payload))
	dst = append(dst, hdr[:]...)
	return append(dst, payload...)
}

// recordSize is the on-disk size of a record with the given payload length.
func recordSize(payloadLen int) int64 { return int64(headerSize + payloadLen) }

// decodeHeader splits a raw header into the payload length and CRC.
func decodeHeader(hdr []byte) (length uint32, crc uint32) {
	return binary.LittleEndian.Uint32(hdr[0:4]), binary.LittleEndian.Uint32(hdr[4:8])
}
