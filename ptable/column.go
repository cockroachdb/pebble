package ptable

type bitmap []byte

func (b bitmap) get(i int) bool {
	return (b[i/8] & (1 << uint(i%8))) != 0
}

func (b bitmap) set(i int, v bool) bitmap {
	j := i / 8
	if len(b) <= int(j) {
		b = append(b, 0)
	}
	if v {
		b[j] |= 1 << uint(i%8)
	}
	return b
}

type columnType int16

const (
	columnTypeBool    columnType = 1
	columnTypeInt8               = 2
	columnTypeInt16              = 3
	columnTypeInt32              = 4
	columnTypeInt64              = 5
	columnTypeFloat32            = 6
	columnTypeFloat64            = 7
	columnTypeBytes              = 8
	// TODO(peter): decimal, uuid, ipaddr, timestamp, time, timetz, duration,
	// collated string, tuple.
)

func (t columnType) width() int32 {
	switch t {
	case columnTypeBool:
		return 1
	case columnTypeInt8:
		return 1
	case columnTypeInt16:
		return 2
	case columnTypeInt32:
		return 4
	case columnTypeInt64:
		return 8
	case columnTypeFloat32:
		return 4
	case columnTypeFloat64:
		return 8
	case columnTypeBytes:
		return -1
	default:
		panic("not reached")
	}
}

func (t columnType) alignment() int32 {
	switch t {
	case columnTypeBool:
		return 1
	case columnTypeInt8:
		return 1
	case columnTypeInt16:
		return 2
	case columnTypeInt32:
		return 4
	case columnTypeInt64:
		return 8
	case columnTypeFloat32:
		return 4
	case columnTypeFloat64:
		return 8
	case columnTypeBytes:
		return 1
	default:
		panic("not reached")
	}
}

type columnDef struct {
	ID   int32
	Type columnType
}
