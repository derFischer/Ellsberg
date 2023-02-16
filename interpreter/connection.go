package interpreter

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
)

type RPCConnection struct {
	mutex     *sync.RWMutex
	cond      *sync.Cond
	nextReqId uint64
	doneIds   map[uint64]interface{}
	netConn   net.Conn
}

func (c *RPCConnection) Init(netConn net.Conn) *RPCConnection {
	if netConn == nil {
		return nil
	}
	c = new(RPCConnection)
	c.mutex = &sync.RWMutex{}
	c.cond = sync.NewCond(c.mutex)
	c.doneIds = make(map[uint64]interface{})
	c.netConn = netConn

	// go func() {
	// 	bytes := make([]byte, 16)
	// 	for {
	// 		// The read will block until there is data in buffer,
	// 		// so this won't occupy too much CPU cycles.
	// 		pos := 0
	// 		for {
	// 			n, err := c.netConn.Read(bytes[pos:])
	// 			if err != nil {
	// 				panic(fmt.Sprintf("Read from conn failed, err:%v\n", err))
	// 			}
	// 			if pos+n == len(bytes) {
	// 				break
	// 			}
	// 		}
	// 		if string(bytes[:8]) != "nyusimul" {
	// 			panic("Read abnormal data from conn failed\n")
	// 		}
	// 		reqId := c.ExtractReqId(bytes)
	// 		c.cond.L.Lock()
	// 		c.doneIds[reqId] = nil
	// 		c.cond.Broadcast()
	// 		c.cond.L.Unlock()
	// 	}
	// }()

	return c
}

func (c *RPCConnection) NextReqId() uint64 {
	// nextReqId := atomic.AddUint64(&c.nextReqId, 1)
	c.nextReqId++
	nextReqId := c.nextReqId
	return nextReqId
}

func (c *RPCConnection) AllocateRequest(length uint64) (uint64, []byte, int) {
	reqbytes := make([]byte, 24+length)
	copy(reqbytes[:8], []byte("nyusimul"))
	reqId := c.NextReqId()
	binary.LittleEndian.PutUint64(reqbytes[8:16], reqId)
	binary.LittleEndian.PutUint64(reqbytes[16:24], length)
	offset := 24
	return reqId, reqbytes, offset
}

func (c *RPCConnection) ExtractReqId(reqbytes []byte) uint64 {
	return binary.LittleEndian.Uint64(reqbytes[8:16])
}

func (c *RPCConnection) Request(reqbytes []byte) {
	_, err := c.netConn.Write(reqbytes)
	if err != nil {
		panic(fmt.Sprintf("Write to conn failed, err:%v\n", err))
	}
}

func (c *RPCConnection) WaitFor(reqId uint64) {
	return
	// c.cond.L.Lock()
	// _, ok := c.doneIds[reqId]
	// for !ok {
	// 	c.cond.Wait()
	// 	_, ok = c.doneIds[reqId]
	// }
	// delete(c.doneIds, reqId) // GoLang uses hashtable for map, so the cost of reading then deleting is low.
	// c.cond.L.Unlock()
}
