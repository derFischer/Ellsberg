package comm

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// MODE = [async|sync]
type MODE_T uint64

const (
	NONE        MODE_T = 0 // means don't forward to simulator
	ASYNC       MODE_T = 1
	SYNC        MODE_T = 2
	FILE        MODE_T = 3
	SYNCANDFILE MODE_T = 4
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
	MyId      uint64
	Ids       []uint64
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
	metaFile     *os.File
	logDir       string
	msgCount     uint64
	reqBytesCh   chan []byte
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
		case "SYNCANDFILE":
			MODE = SYNCANDFILE
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

	c.MyId = myId
	c.Ids = ids

	iid := os.Getenv("IID")

	if MODE == FILE || MODE == SYNCANDFILE {
		c.mutex = &sync.RWMutex{}
		c.msgCount = 0
		c.bufferedData = make([][]byte, 0)
		now := time.Now()
		formatTime := fmt.Sprintf("%04d%02d%02d-%02d%02d%02d", now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second())
		c.logDir = "/tmp/log/log" + formatTime
		err = os.MkdirAll(c.logDir, 0755)
		if err != nil {
			panic(fmt.Sprintf("Cannot create log directory: %v", err))
		}
		c.file, err = os.OpenFile(filepath.Join(c.logDir, fmt.Sprintf("messages.%s.%s.data", iid, formatTime)), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			panic(fmt.Sprintf("Cannot open file: %v", err))
		}
		c.metaFile, err = os.OpenFile(filepath.Join(c.logDir, fmt.Sprintf("messages.%s.%s.meta", iid, formatTime)), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			panic(fmt.Sprintf("Cannot open file: %v", err))
		}
		f, err := os.OpenFile(filepath.Join(c.logDir, "cluster.meta"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			panic(fmt.Sprintf("Cannot open file: %v", err))
		}
		f.WriteString(fmt.Sprintf("%d\n", c.MyId))
		for _, id := range c.Ids {
			f.WriteString(fmt.Sprintf("%d\n", id))
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

	if MODE == SYNC || MODE == SYNCANDFILE {
		c.mutex = &sync.RWMutex{}
		c.cond = sync.NewCond(c.mutex)
		c.doneIds = new(sync.Map)
		go func() {
			bytes := make([]byte, 24)
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
				success := binary.LittleEndian.Uint64(bytes[16:24]) == 1
				reqId := c.ExtractReqId(bytes)
				ch, loaded := c.doneIds.LoadAndDelete(reqId)
				if !loaded {
					panic("Read from conn failed\n")
				}
				ch.(chan bool) <- success
			}
		}()
	}

	if MODE == FILE || MODE == SYNCANDFILE {
		c.reqBytesCh = make(chan []byte, 2048)
		go func() {
			writeToFile := func(f *os.File, reqBytes []byte) {
				c.msgCount++
				n, err := f.Write(reqBytes)
				if err != nil {
					panic(fmt.Sprintf("Write to file failed, err:%v\n", err))
				}
				if n != len(reqBytes) {
					panic(fmt.Sprintf("Write to conn failed, n:%v, len(reqBytes):%v\n", n, len(reqBytes)))
				}
			}

			timeAfter := time.After(1 * time.Second)
			period := 1
			warmupDone := false
			for !warmupDone {
				select {
				case reqBytes := <-c.reqBytesCh:
					writeToFile(c.file, reqBytes)
				case <-timeAfter:
					warmupDone = true
				}
			}
			timeAfter = time.After(time.Duration(period) * time.Second)
			startMsgCount := c.msgCount
			fmt.Printf("\033[1;31mstart timing\033[0m: %v\n", startMsgCount)
			for {
				select {
				case reqBytes := <-c.reqBytesCh:
					writeToFile(c.file, reqBytes)
				case <-timeAfter:
					endMsgCount := c.msgCount
					fmt.Printf("\033[1;31mafter %ds\033[0m: %v\n", period, endMsgCount)
					// save for data file
					c.file.Sync()
					c.file.Close()
					// save for meta file
					c.metaFile.WriteString(fmt.Sprintf("%d\n%d\n", startMsgCount, endMsgCount))
					c.metaFile.Sync()
					c.metaFile.Close()
					// start new data file and meta file
					now := time.Now()
					formatTime := fmt.Sprintf("%04d%02d%02d-%02d%02d%02d", now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second())
					c.file, err = os.OpenFile(filepath.Join(c.logDir, fmt.Sprintf("messages.%s.%s.data", iid, formatTime)), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
					if err != nil {
						panic(fmt.Sprintf("Cannot open file: %v", err))
					}
					c.metaFile, err = os.OpenFile(filepath.Join(c.logDir, fmt.Sprintf("messages.%s.%s.meta", iid, formatTime)), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
					if err != nil {
						panic(fmt.Sprintf("Cannot open file: %v", err))
					}
					timeAfter = time.After(time.Duration(period) * time.Second)
				}
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
	offset := 32
	reqbytes := make([]byte, offset+int(length))
	copy(reqbytes[:8], []byte("nyusimul"))
	reqId := c.NextReqId()
	binary.LittleEndian.PutUint64(reqbytes[8:16], reqId)
	binary.LittleEndian.PutUint64(reqbytes[16:24], length)
	nowNano := time.Now().UnixNano()
	binary.LittleEndian.PutUint64(reqbytes[24:32], uint64(nowNano))
	return reqId, reqbytes, offset
}

func (c *RPCConnection) ExtractReqId(reqbytes []byte) uint64 {
	return binary.LittleEndian.Uint64(reqbytes[8:16])
}

func (c *RPCConnection) Request(reqbytes []byte) chan bool {
	var ch chan bool = nil

	if MODE == FILE || MODE == SYNCANDFILE {
		// Let a goroutine to actually write to file.
		c.reqBytesCh <- reqbytes
	}

	if MODE != FILE {
		if MODE == SYNC || MODE == SYNCANDFILE {
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
	}

	return ch
}

func (c *RPCConnection) WaitFor(reqId uint64, ch chan bool) {
	if MODE == SYNC || MODE == SYNCANDFILE {
		success := <-ch
		if !success {
			panic(fmt.Sprintf("Request %v failed\n", reqId))
		}
	}
}
