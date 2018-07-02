package sstable

import "unsafe"

func getBytes(ptr unsafe.Pointer, length int) []byte {
	return (*[1 << 30]byte)(ptr)[:length:length]
}

func decodeVarint(ptr unsafe.Pointer) (uint32, unsafe.Pointer) {
	src := (*[5]uint8)(ptr)
	dst := uint32((*src)[0]) & 0x7f
	if (*src)[0] < 128 {
		return dst, unsafe.Pointer(uintptr(ptr) + 1)
	}
	dst |= (uint32((*src)[1]&0x7f) << 7)
	if (*src)[1] < 128 {
		return dst, unsafe.Pointer(uintptr(ptr) + 2)
	}
	dst |= (uint32((*src)[2]&0x7f) << 14)
	if (*src)[2] < 128 {
		return dst, unsafe.Pointer(uintptr(ptr) + 3)
	}
	dst |= (uint32((*src)[3]&0x7f) << 21)
	if (*src)[3] < 128 {
		return dst, unsafe.Pointer(uintptr(ptr) + 4)
	}
	dst |= (uint32((*src)[4]&0x7f) << 28)
	return dst, unsafe.Pointer(uintptr(ptr) + 5)
}
