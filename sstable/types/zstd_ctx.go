package types

type ZstdCtx interface {
	CompressLevel(dst []byte, src []byte, level int) ([]byte, error)
	Decompress(dst []byte, src []byte) ([]byte, error)
}

var _ ZstdCtx = (*noopZstdCtx)(nil)

type noopZstdCtx struct{}

func (n noopZstdCtx) CompressLevel(dst []byte, src []byte, level int) ([]byte, error) {
	return nil, nil
}
func (n noopZstdCtx) Decompress(dst []byte, src []byte) ([]byte, error) {
	return nil, nil
}
