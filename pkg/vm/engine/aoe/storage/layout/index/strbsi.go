package index

import (
	"bytes"
	"io"
	"matrixone/pkg/container/types"
	"matrixone/pkg/encoding"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/index/bsi"
	// log "github.com/sirupsen/logrus"
)

func initStringBsi(t types.Type) *bsi.StringBSI {
	var bsiIdx bsi.BitSlicedIndex
	switch t.Oid {
	case types.T_char:
		bsiIdx = bsi.NewStringBSI(int(t.Width), int(t.Size))
	default:
		panic("not supported")
	}
	return bsiIdx.(*bsi.StringBSI)
}

func StringBsiIndexConstructor(capacity uint64, freeFunc buf.MemoryFreeFunc) buf.IMemoryNode {
	return NewStringBsiEmptyNode(capacity, freeFunc)
}

type StringBsiIndex struct {
	bsi.StringBSI
	T         types.Type
	Col       int16
	AllocSize uint64
	FreeFunc  buf.MemoryFreeFunc
}

func NewStringBsiIndex(t types.Type, colIdx int16) *StringBsiIndex {
	bsiIdx := initStringBsi(t)
	return &StringBsiIndex{
		T:         t,
		Col:       colIdx,
		StringBSI: *bsiIdx,
	}
}

func NewStringBsiEmptyNode(capacity uint64, freeFunc buf.MemoryFreeFunc) buf.IMemoryNode {
	return &StringBsiIndex{
		AllocSize: capacity,
		FreeFunc:  freeFunc,
	}
}

func (i *StringBsiIndex) GetCol() int16 {
	return i.Col
}

func (i *StringBsiIndex) Eval(ctx *FilterCtx) error {
	return nil
}

func (i *StringBsiIndex) FreeMemory() {
	if i.FreeFunc != nil {
		i.FreeFunc(i)
	}
}

func (i *StringBsiIndex) Type() base.IndexType {
	return base.FixStrBsi
}

func (i *StringBsiIndex) GetMemorySize() uint64 {
	return i.AllocSize
}

func (i *StringBsiIndex) GetMemoryCapacity() uint64 {
	return i.AllocSize
}

func (i *StringBsiIndex) Reset() {
}

func (i *StringBsiIndex) ReadFrom(r io.Reader) (n int64, err error) {
	data := make([]byte, i.AllocSize)
	nr, err := r.Read(data)
	if err != nil {
		return n, err
	}
	buf := data[2 : 2+encoding.TypeSize]
	i.T = encoding.DecodeType(buf)
	i.StringBSI = *initStringBsi(i.T)
	err = i.Unmarshall(data)
	return int64(nr), err
}

func (i *StringBsiIndex) WriteTo(w io.Writer) (n int64, err error) {
	buf, err := i.Marshall()
	if err != nil {
		return n, err
	}

	nw, err := w.Write(buf)
	return int64(nw), err
}

func (i *StringBsiIndex) Unmarshall(data []byte) error {
	buf := data
	i.Col = encoding.DecodeInt16(buf[:2])
	buf = buf[2:]
	i.T = encoding.DecodeType(buf[:encoding.TypeSize])
	buf = buf[encoding.TypeSize:]
	return i.StringBSI.Unmarshall(buf)
}

func (i *StringBsiIndex) Marshall() ([]byte, error) {
	var bw bytes.Buffer
	bw.Write(encoding.EncodeInt16(i.Col))
	bw.Write(encoding.EncodeType(i.T))
	indexBuf, err := i.StringBSI.Marshall()
	if err != nil {
		return nil, err
	}
	bw.Write(indexBuf)
	return bw.Bytes(), nil
}
