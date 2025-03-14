//go:build cgo

package types

import "github.com/DataDog/zstd"

func GetZstdCtx() ZstdCtx {
	return zstd.NewCtx()
}
