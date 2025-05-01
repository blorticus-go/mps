package mps

import (
	"errors"
	"io"
)

var ErrBufferLengthExceeded = errors.New("buffer length exceeded")

// A FixedSlidingbuffer is a fixed-size byte buffer.  When bytes are read from the buffer,
// an internal pointer is moved forward toward the end of the buffer.  When bytes are written,
// they are appended to the end of the buffer, unless the size of write requires more bytes than
// are available between the head pointer and the end of the buffer.  In that case, as long as
// the buffer still has enough space to accommodate the write data and the remaining data in the
// buffer, the existing data are copied to the beginning of the buffer and the write data are
// appended, at which point the head pointer is reset to the beginning of the buffer.  This
// is not thread-safe.
type FixedSlidingBuffer struct {
	buffer              []byte
	head                int
	numberOfActiveBytes int
}

// Allocate a new FixedSlidingBuffer of the specified size.  If the size is 0, it panics.
func NewFixedSlidingBuffer(size uint) *FixedSlidingBuffer {
	if size == 0 {
		panic("size must be greater than 0")
	}

	return &FixedSlidingBuffer{
		buffer:              make([]byte, size),
		head:                0,
		numberOfActiveBytes: 0,
	}
}

// Write copies data into the buffer using the rules described above.  It returns the number
// of bytes actually written.  If the buffer does not have enough space to accommodate the
// full write (which will mean that the return int is smaller than the length of data),
// it returns the error BufferLengthExceededError after writing as much as it can.
func (b *FixedSlidingBuffer) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}

	if b.head+b.numberOfActiveBytes+len(data) > len(b.buffer) {
		if b.head > 0 {
			copy(b.buffer, b.buffer[b.head:b.head+b.numberOfActiveBytes])
			b.head = 0
		}

		numberOfWritableBytes := len(b.buffer) - b.numberOfActiveBytes
		endOfActiveData := b.head + b.numberOfActiveBytes

		if len(data) > numberOfWritableBytes {
			copy(b.buffer[endOfActiveData:], data[:numberOfWritableBytes])
			b.numberOfActiveBytes += numberOfWritableBytes
			return numberOfWritableBytes, ErrBufferLengthExceeded
		} else {
			copy(b.buffer[endOfActiveData:], data)
			b.numberOfActiveBytes += len(data)
			return len(data), nil
		}
	} else {
		copy(b.buffer[b.head+b.numberOfActiveBytes:], data)
		b.numberOfActiveBytes += len(data)
		return len(data), nil
	}
}

// Read copies active bytes from the buffer into data.  It copies len(data) bytes or the
// number of remaining active bytes in the buffer, whichever is smaller.  It returns the number
// of bytes copied.  Those bytes are removed from the buffer.  If all bytes are read out of the
// buffer, the head pointer is reset to the beginning of the buffer.  The error will always be nil.
func (b *FixedSlidingBuffer) Read(data []byte) (int, error) {
	if len(data) == 0 || b.numberOfActiveBytes == 0 {
		return 0, nil
	}

	if len(data) >= b.numberOfActiveBytes {
		copy(data, b.buffer[b.head:b.head+b.numberOfActiveBytes])
		numberOfCopiedBytes := b.numberOfActiveBytes
		b.numberOfActiveBytes = 0
		b.head = 0
		return numberOfCopiedBytes, nil
	} else {
		copy(data, b.buffer[b.head:b.head+len(data)])
		b.numberOfActiveBytes -= len(data)
		b.head += len(data)
		return len(data), nil
	}
}

// ReadN copies up to bytesToCopy bytes from the buffer into data.  It returns the number of bytes copied.
// If bytesToCopy is larger than the size of data, it panics.  If bytesToCopy is larger than the
// number of bytes in the buffer, it copies all bytes in the buffer, returning the number of bytes copied.
// The ReadN copy is always from the head of the buffer, and after the operation, the number of bytes copied are removed
// from the head of the buffer.  If the buffer is empty after the ReadN operation, the head pointer is reset to the beginning of the buffer.
func (b *FixedSlidingBuffer) ReadN(data []byte, bytesToCopy int) (int, error) {
	if len(data) == 0 || bytesToCopy == 0 || b.numberOfActiveBytes == 0 {
		return 0, nil
	}

	if bytesToCopy > len(data) {
		panic("bytesToCopy is larger than the size of data")
	}

	if bytesToCopy >= b.numberOfActiveBytes {
		return b.Read(data)
	}

	copy(data, b.buffer[b.head:b.head+bytesToCopy])
	b.numberOfActiveBytes -= bytesToCopy
	b.head += bytesToCopy
	return bytesToCopy, nil
}

// WriteSingleReadFrom performs a blocking Read() on source, writing the result into the buffer.  It will only read
// up to the size of the unused space in the buffer.  This means, however, that if the head is anywhere but the
// start of the backing buffer, the remaining bytes must be shifted to the start.  This allows this operation
// to copy the bytes without an intermediate buffer.  It returns the number of bytes read (and written).  Any
// error returned by the source Read() is returned here.  This may include EOF.  If the buffer is full before
// a call to WriteSingleReadFrom, it returns the error BufferLengthExceededError.
func (b *FixedSlidingBuffer) ReadOnceFrom(source io.Reader) (int, error) {
	if b.numberOfActiveBytes == len(b.buffer) {
		return 0, ErrBufferLengthExceeded
	}

	if b.head > 0 {
		copy(b.buffer, b.buffer[b.head:b.head+b.numberOfActiveBytes])
		b.head = 0
	}

	bytesRead, err := source.Read(b.buffer[b.head+b.numberOfActiveBytes:])
	b.numberOfActiveBytes += bytesRead
	return bytesRead, err
}

// Length returns the number of active bytes stored in the buffer.
func (b *FixedSlidingBuffer) Length() int {
	return b.numberOfActiveBytes
}

// Unused returns the number of unused bytes in the buffer (i.e., the size of the buffer minus Length()).
func (b *FixedSlidingBuffer) Unused() int {
	return len(b.buffer) - b.numberOfActiveBytes
}

// Clear resets the buffer Length to 0 and moves the head pointer to the beginning of the buffer.
func (b *FixedSlidingBuffer) Clear() {
	b.numberOfActiveBytes = 0
	b.head = 0
}

// Peek the active bytes in the buffer without removing them.  The returned slice points to the
// backing array, so it should not be modified.  The slice is invalid after the next call to any Read
// or Write method, as well as Clear.
func (b *FixedSlidingBuffer) Peek() []byte {
	return b.buffer[b.head : b.head+b.numberOfActiveBytes]
}
