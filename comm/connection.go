package comm

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// MODE = [async|sync]
type MODE_T uint64

const (
	NONE  MODE_T = 0 // means don't forward to simulator
	ASYNC MODE_T = 1
	SYNC  MODE_T = 2
	FILE  MODE_T = 3
)

// CONNTYPE = [tcp|unix|pipe]
type CONNTYPE_T uint64

const (
	TCP  CONNTYPE_T = 0
	UNIX CONNTYPE_T = 1
	PIPE CONNTYPE_T = 2
)

var MODE = ASYNC
var CONNTYPE = PIPE

type RPCConnection struct {
	mutex     *sync.RWMutex
	cond      *sync.Cond
	nextReqId uint64
	// doneIds   map[uint64]interface{}
	doneIds *sync.Map
	OutConn ReaderWriter // [net.Conn|*os.File]
	InConn  ReaderWriter // [net.Conn|*os.File]

	// For FILE mode
	bufferedData [][]byte
	file         *os.File
}

func (c *RPCConnection) Init(myId uint64, ids []uint64) *RPCConnection {
	mode := os.Getenv("SIMMODE")
	if mode != "" {
		switch mode {
		case "NONE":
			MODE = NONE
		case "SYNC":
			MODE = SYNC
		case "ASYNC":
			MODE = ASYNC
		case "FILE":
			MODE = FILE
		default:
			panic(fmt.Sprintf("Unknown SIMMODE: %v", mode))
		}
	}
	conntype := os.Getenv("SIMCONNTYPE")
	if conntype != "" {
		switch conntype {
		case "TCP":
			CONNTYPE = TCP
		case "UNIX":
			CONNTYPE = UNIX
		case "PIPE":
			CONNTYPE = PIPE
		default:
			panic(fmt.Sprintf("Unknown SIMCONNTYPE: %v", conntype))
		}
	}
	if MODE == NONE {
		return nil
	}
	fmt.Printf("RPCConnection Setup: MODE=%v, CONNTYPE=%v\n", MODE, CONNTYPE)
	var err error
	c = new(RPCConnection)

	iid := os.Getenv("IID")

	if MODE == FILE {
		c.bufferedData = make([][]byte, 0)
		c.file, err = os.OpenFile(fmt.Sprintf("/tmp/messages.%s.data", iid), os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			panic(fmt.Sprintf("Cannot open file: %v", err))
		}
	}

	if MODE != FILE {
		switch CONNTYPE {
		case UNIX:
			server_sock_path := fmt.Sprintf("/tmp/server.%s.sock", iid)
			c.OutConn, err = net.Dial("unix", server_sock_path)
			c.InConn = c.OutConn
		case TCP:
			c.OutConn, err = net.Dial("tcp", "127.0.0.1:9090")
			c.InConn = c.OutConn
		case PIPE:
			stocPath := fmt.Sprintf("/tmp/tmpPipe_s_to_c.%s.pipe", iid)
			ctosPath := fmt.Sprintf("/tmp/tmpPipe_c_to_s.%s.pipe", iid)
			c.OutConn, err = os.OpenFile(ctosPath, os.O_RDWR, os.ModeNamedPipe)
			if err != nil {
				break
			}
			c.InConn, err = os.OpenFile(stocPath, os.O_RDONLY, os.ModeNamedPipe)
		default:
			panic("Unknown CONNTYPE")
		}
		if err != nil {
			fmt.Printf("\033[1;31mWarning: cannot connect to simulator! (%v)\033[0m\n", err)
			return nil
		}
		fmt.Printf("Connected to simulator, out=%v, in=%v\n", c.OutConn, c.InConn)
		marshalled_cluster_info, err := json.Marshal(ids)
		PanicIfMarshalError(err)
		// write my id
		NetWrite(c.OutConn, EncodeUint64(myId))
		// write cluster info length
		NetWrite(c.OutConn, EncodeUint64(uint64(len(marshalled_cluster_info))))
		// write cluster info
		NetWrite(c.OutConn, marshalled_cluster_info)
	}

	if MODE == SYNC {
		c.mutex = &sync.RWMutex{}
		c.cond = sync.NewCond(c.mutex)
		c.doneIds = new(sync.Map)
		go func() {
			bytes := make([]byte, 16)
			for {
				// The read will block until there is data in buffer,
				// so this won't occupy too much CPU cycles.
				pos := 0
				for {
					n, err := c.InConn.Read(bytes[pos:])
					if err != nil {
						panic(fmt.Sprintf("Read from conn failed, err:%v\n", err))
					}
					if pos+n == len(bytes) {
						break
					}
				}
				if string(bytes[:8]) != "nyusimul" {
					panic("Read abnormal data from conn failed\n")
				}
				reqId := c.ExtractReqId(bytes)
				ch, loaded := c.doneIds.LoadAndDelete(reqId)
				if !loaded {
					panic("Read from conn failed\n")
				}
				ch.(chan bool) <- true
			}
		}()
	} else if MODE == FILE {
		go func() {
			for {
				time.Sleep(10 * time.Second)
				c.file.Sync()
			}
		}()
	}

	return c
}

func (c *RPCConnection) NextReqId() uint64 {
	nextReqId := atomic.AddUint64(&c.nextReqId, 1)
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

func (c *RPCConnection) Request(reqbytes []byte) chan bool {
	var ch chan bool = nil

	if MODE != FILE {
		if MODE == SYNC {
			reqId := c.ExtractReqId(reqbytes)
			ch = make(chan bool)
			c.doneIds.Store(reqId, ch)
		}

		n, err := c.OutConn.Write(reqbytes)
		if err != nil {
			panic(fmt.Sprintf("Write to conn failed, err:%v\n", err))
		}
		if n != len(reqbytes) {
			panic(fmt.Sprintf("Write to conn failed, n:%v, len(reqbytes):%v\n", n, len(reqbytes)))
		}
	} else {
		n, err := c.file.Write(reqbytes)
		if err != nil {
			panic(fmt.Sprintf("Write to file failed, err:%v\n", err))
		}
		if n != len(reqbytes) {
			panic(fmt.Sprintf("Write to conn failed, n:%v, len(reqbytes):%v\n", n, len(reqbytes)))
		}
	}
	return ch
}

func (c *RPCConnection) WaitFor(reqId uint64, ch chan bool) {
	if MODE == SYNC {
		<-ch
	}
}
