package blobcache

// On-disk formats. Three of them, and each one exists to answer a different
// question after an unclean shutdown:
//
//   - the record, appended to a segment, is the log. Replaying it in order
//     reconstructs the index, and its checksum is what makes a torn or
//     zero-filled tail a miss instead of a lie.
//   - the segment footer is written when a segment is sealed and fsynced with
//     it. It is a shortcut, not a source of truth: it turns "read 256 MiB to
//     learn what is in here" into two preads, and if it is missing or broken
//     the segment is simply replayed instead.
//   - the blob trailer marks a single-payload file as complete. A file
//     without one never finished being written.
//
// Every multi-byte field is little-endian, and every header carries a
// checksum of itself so its lengths can be trusted before they are used to
// size a read.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// crc32c is the Castagnoli polynomial, which has a hardware instruction on
// every CPU this proxy runs on. Checksumming must not be the reason a write
// is slow.
var crc32c = crc32.MakeTable(crc32.Castagnoli)

func checksum(parts ...[]byte) uint32 {
	var sum uint32
	for _, p := range parts {
		sum = crc32.Update(sum, crc32c, p)
	}
	return sum
}

const (
	formatVersion = 1

	segmentHeaderSize = 32
	recordHeaderSize  = 40
	footerTrailerSize = 32
	blobTrailerSize   = 40
)

var (
	segmentMagic     = [8]byte{'B', 'L', 'O', 'B', 'C', 'S', 'G', '1'}
	footerMagic      = [8]byte{'B', 'L', 'O', 'B', 'C', 'I', 'X', '1'}
	blobMagic        = [8]byte{'B', 'L', 'O', 'B', 'C', 'B', 'L', '1'}
	recordMagic      = uint32(0x52433031) // "RC01"
	errBadRecord     = errors.New("blobcache: record is not intact")
	errBadFooter     = errors.New("blobcache: segment footer is not intact")
	errBadBlobFile   = errors.New("blobcache: blob file is not intact")
	errShortSegment  = errors.New("blobcache: segment header is truncated")
	errWrongSegments = errors.New("blobcache: not a segment file")
)

// recordKind says what a log record does. Every mutation the store makes is
// one of these three, which is why replaying them in order is the whole of
// recovery.
type recordKind uint32

const (
	kindInline    recordKind = 1 // payload follows the header, in this segment
	kindBlobRef   recordKind = 2 // payload lives in blob file BlobID
	kindTombstone recordKind = 3 // the key is deleted as of this point in the log
)

func (k recordKind) valid() bool {
	return k == kindInline || k == kindBlobRef || k == kindTombstone
}

// recordHeader is the fixed part of a log record. The variable part that
// follows it is key, then meta, then — for kindInline — the payload.
//
// Meta rides in the segment even when the payload does not, so reading an
// entry's metadata never has to open the blob file.
type recordHeader struct {
	Kind    recordKind
	KeyLen  uint32
	MetaLen uint32
	DataLen uint64
	BlobID  uint64
	// CRC covers the variable part, in the order it appears on disk.
	CRC uint32
}

func (h recordHeader) encode(dst []byte) {
	_ = dst[recordHeaderSize-1]
	binary.LittleEndian.PutUint32(dst[0:], recordMagic)
	binary.LittleEndian.PutUint32(dst[4:], uint32(h.Kind))
	binary.LittleEndian.PutUint32(dst[8:], h.KeyLen)
	binary.LittleEndian.PutUint32(dst[12:], h.MetaLen)
	binary.LittleEndian.PutUint64(dst[16:], h.DataLen)
	binary.LittleEndian.PutUint64(dst[24:], h.BlobID)
	binary.LittleEndian.PutUint32(dst[32:], h.CRC)
	binary.LittleEndian.PutUint32(dst[36:], checksum(dst[0:36]))
}

// decodeRecordHeader refuses a header before its lengths are used for
// anything. A zero-filled region — the shape a crash leaves behind on a
// filesystem with delayed allocation — fails on the magic; a torn one fails
// on the self-checksum.
func decodeRecordHeader(src []byte) (recordHeader, error) {
	var h recordHeader
	if len(src) < recordHeaderSize {
		return h, errBadRecord
	}
	if binary.LittleEndian.Uint32(src[0:]) != recordMagic {
		return h, errBadRecord
	}
	if binary.LittleEndian.Uint32(src[36:]) != checksum(src[0:36]) {
		return h, errBadRecord
	}
	h.Kind = recordKind(binary.LittleEndian.Uint32(src[4:]))
	h.KeyLen = binary.LittleEndian.Uint32(src[8:])
	h.MetaLen = binary.LittleEndian.Uint32(src[12:])
	h.DataLen = binary.LittleEndian.Uint64(src[16:])
	h.BlobID = binary.LittleEndian.Uint64(src[24:])
	h.CRC = binary.LittleEndian.Uint32(src[32:])
	if !h.Kind.valid() {
		return h, errBadRecord
	}
	// A tombstone carries neither a payload nor metadata. A header that
	// says otherwise decoded cleanly by luck, not because it is one.
	if h.Kind == kindTombstone && (h.DataLen > 0 || h.MetaLen > 0) {
		return h, errBadRecord
	}
	return h, nil
}

// size is how many bytes this record occupies in its segment.
func (h recordHeader) size() int64 {
	n := int64(recordHeaderSize) + int64(h.KeyLen) + int64(h.MetaLen)
	if h.Kind == kindInline {
		n += int64(h.DataLen)
	}
	return n
}

// segment file header.

func encodeSegmentHeader(id uint64) []byte {
	buf := make([]byte, segmentHeaderSize)
	copy(buf, segmentMagic[:])
	binary.LittleEndian.PutUint32(buf[8:], formatVersion)
	binary.LittleEndian.PutUint64(buf[12:], id)
	binary.LittleEndian.PutUint32(buf[segmentHeaderSize-4:], checksum(buf[:segmentHeaderSize-4]))
	return buf
}

func decodeSegmentHeader(src []byte) (id uint64, err error) {
	if len(src) < segmentHeaderSize {
		return 0, errShortSegment
	}
	if [8]byte(src[0:8]) != segmentMagic {
		return 0, errWrongSegments
	}
	if binary.LittleEndian.Uint32(src[segmentHeaderSize-4:]) != checksum(src[:segmentHeaderSize-4]) {
		return 0, errWrongSegments
	}
	if v := binary.LittleEndian.Uint32(src[8:]); v != formatVersion {
		return 0, fmt.Errorf("blobcache: segment format version %d, want %d", v, formatVersion)
	}
	return binary.LittleEndian.Uint64(src[12:]), nil
}

// segment footer.
//
// The footer repeats every record's header fields and key, in log order, so
// that replaying from it is indistinguishable from replaying the records
// themselves — tombstones and overwrites included. Only the payloads are left
// out, which is the whole point.

type footerEntry struct {
	Offset int64 // where the record starts in the segment
	Header recordHeader
	Key    string
}

func encodeFooter(entries []footerEntry) []byte {
	buf := make([]byte, 0, len(entries)*48+footerTrailerSize)
	var scratch [32]byte
	for _, e := range entries {
		binary.LittleEndian.PutUint64(scratch[0:], uint64(e.Offset))
		binary.LittleEndian.PutUint32(scratch[8:], uint32(e.Header.Kind))
		binary.LittleEndian.PutUint32(scratch[12:], e.Header.KeyLen)
		binary.LittleEndian.PutUint32(scratch[16:], e.Header.MetaLen)
		binary.LittleEndian.PutUint64(scratch[20:], e.Header.DataLen)
		binary.LittleEndian.PutUint32(scratch[28:], 0) // reserved
		buf = append(buf, scratch[:]...)
		var blob [8]byte
		binary.LittleEndian.PutUint64(blob[:], e.Header.BlobID)
		buf = append(buf, blob[:]...)
		buf = append(buf, e.Key...)
	}
	trailer := make([]byte, footerTrailerSize)
	copy(trailer, footerMagic[:])
	binary.LittleEndian.PutUint32(trailer[8:], uint32(len(entries)))
	binary.LittleEndian.PutUint64(trailer[12:], uint64(len(buf)))
	binary.LittleEndian.PutUint32(trailer[20:], checksum(buf))
	binary.LittleEndian.PutUint32(trailer[footerTrailerSize-4:], checksum(trailer[:footerTrailerSize-4]))
	return append(buf, trailer...)
}

// decodeFooterTrailer reads the last footerTrailerSize bytes of a sealed
// segment and reports how long the index region before it is.
func decodeFooterTrailer(src []byte) (count int, length int64, sum uint32, err error) {
	if len(src) < footerTrailerSize {
		return 0, 0, 0, errBadFooter
	}
	if [8]byte(src[0:8]) != footerMagic {
		return 0, 0, 0, errBadFooter
	}
	if binary.LittleEndian.Uint32(src[footerTrailerSize-4:]) != checksum(src[:footerTrailerSize-4]) {
		return 0, 0, 0, errBadFooter
	}
	count = int(binary.LittleEndian.Uint32(src[8:]))
	length = int64(binary.LittleEndian.Uint64(src[12:]))
	sum = binary.LittleEndian.Uint32(src[20:])
	if count < 0 || length < 0 {
		return 0, 0, 0, errBadFooter
	}
	return count, length, sum, nil
}

func decodeFooter(src []byte, count int, sum uint32) ([]footerEntry, error) {
	if checksum(src) != sum {
		return nil, errBadFooter
	}
	entries := make([]footerEntry, 0, count)
	for i := 0; i < count; i++ {
		if len(src) < 40 {
			return nil, errBadFooter
		}
		var e footerEntry
		e.Offset = int64(binary.LittleEndian.Uint64(src[0:]))
		e.Header.Kind = recordKind(binary.LittleEndian.Uint32(src[8:]))
		e.Header.KeyLen = binary.LittleEndian.Uint32(src[12:])
		e.Header.MetaLen = binary.LittleEndian.Uint32(src[16:])
		e.Header.DataLen = binary.LittleEndian.Uint64(src[20:])
		e.Header.BlobID = binary.LittleEndian.Uint64(src[32:])
		src = src[40:]
		if !e.Header.Kind.valid() || uint64(len(src)) < uint64(e.Header.KeyLen) {
			return nil, errBadFooter
		}
		e.Key = string(src[:e.Header.KeyLen])
		src = src[e.Header.KeyLen:]
		entries = append(entries, e)
	}
	if len(src) != 0 {
		return nil, errBadFooter
	}
	return entries, nil
}

// blob trailer.
//
// A single-payload file carries its identity and its length at the end,
// because the length is not always known when the first byte is written — an
// upstream response that does not declare one still has to go somewhere. A
// file without an intact trailer never finished, and is removed.

type blobTrailer struct {
	ID      uint64
	DataLen int64
	CRC     uint32 // over the payload
}

func (t blobTrailer) encode() []byte {
	buf := make([]byte, blobTrailerSize)
	copy(buf, blobMagic[:])
	binary.LittleEndian.PutUint32(buf[8:], formatVersion)
	binary.LittleEndian.PutUint64(buf[12:], t.ID)
	binary.LittleEndian.PutUint64(buf[20:], uint64(t.DataLen))
	binary.LittleEndian.PutUint32(buf[28:], t.CRC)
	binary.LittleEndian.PutUint32(buf[blobTrailerSize-4:], checksum(buf[:blobTrailerSize-4]))
	return buf
}

func decodeBlobTrailer(src []byte) (blobTrailer, error) {
	var t blobTrailer
	if len(src) < blobTrailerSize {
		return t, errBadBlobFile
	}
	if [8]byte(src[0:8]) != blobMagic {
		return t, errBadBlobFile
	}
	if binary.LittleEndian.Uint32(src[blobTrailerSize-4:]) != checksum(src[:blobTrailerSize-4]) {
		return t, errBadBlobFile
	}
	if binary.LittleEndian.Uint32(src[8:]) != formatVersion {
		return t, errBadBlobFile
	}
	t.ID = binary.LittleEndian.Uint64(src[12:])
	t.DataLen = int64(binary.LittleEndian.Uint64(src[20:]))
	t.CRC = binary.LittleEndian.Uint32(src[28:])
	if t.DataLen < 0 {
		return t, errBadBlobFile
	}
	return t, nil
}
