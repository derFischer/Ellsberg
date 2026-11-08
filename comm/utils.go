package comm

import (
	"encoding/binary"
	"fmt"
)

type ReaderWriter interface {
	Write(b []byte) (n int, err error)
	Read(b []byte) (n int, err error)
	// Close() error
}

func NetWrite(conn ReaderWriter, bytes []byte) {

	_, err := conn.Write(bytes)
	if err != nil {
		panic(fmt.Sprintf("Write to conn failed, err:%v\n", err))
	}
}

func NetRead(conn ReaderWriter, bytes []byte) (int, []byte) {
	pos := 0
	for {
		n, err := conn.Read(bytes[pos:])
		if err != nil {
			panic(fmt.Sprintf("Read from conn failed, err:%v\n", err))
		}
		pos += n
		if pos == len(bytes) {
			break
		}
	}
	if pos != len(bytes) {
		panic(fmt.Sprintf("Read from conn failed, n:%v, len(bytes):%v\n", pos, len(bytes)))
	}
	return pos, bytes
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

func IntArrayToHexStringArray(arr []uint64) []string {
	res := []string{}
	for _, i := range arr {
		res = append(res, fmt.Sprintf("0x%x", i))
	}
	return res
}
