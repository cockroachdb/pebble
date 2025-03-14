//go:build !cgo

package types

func GetZstdCtx() ZstdCtx {
	return noopZstdCtx{}
}
