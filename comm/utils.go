package comm

import (
	"encoding/binary"
	"fmt"
)

type ReaderWriter interface {
	Write(b []byte) (n int, err error)
	Read(b []byte) (n int, err error)
	Close() error
}

func NetWrite(conn ReaderWriter, bytes []byte) {

	_, err := conn.Write(bytes)
	if err != nil {
		panic(fmt.Sprintf("Write to conn failed, err:%v\n", err))
	}
}

func NetRead(conn ReaderWriter, bytes []byte) (int, []byte) {
	n, err := conn.Read(bytes)
	if err != nil {
		panic(fmt.Sprintf("Read from conn failed, err:%v\n", err))
	}
	return n, bytes
}

func EncodeUint64(n uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, n)
	return b
}

func PanicIfMarshalError(err error) {
	if err != nil {
		panic(fmt.Sprintf("Marshal failed, err:%v\n", err))
	}
}

func PanicIfUnmarshalError(err error) {
	if err != nil {
		panic(fmt.Sprintf("Unmarshal failed, err:%v\n", err))
	}
}
