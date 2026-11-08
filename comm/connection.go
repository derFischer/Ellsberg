package comm

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"go.nyu.edu/protosim/statistics"
)

// MODE = [async|sync]
type MODE_T uint64

const (
	NONE  MODE_T = 0 // means don't forward to simulator; this is not used in simulator
	ASYNC MODE_T = 1
	SYNC  MODE_T = 2
	FILE  MODE_T = 3
	TLA   MODE_T = 4
)

func ModeString(mode MODE_T) string {
	switch mode {
	case NONE:
		return "NONE"
	case ASYNC:
		return "ASYNC"
	case SYNC:
		return "SYNC"
	case FILE:
		return "FILE"
	case TLA:
		return "TLA"
	default:
		panic("Unknown MODE")
	}
}

// CONNTYPE = [tcp|unix|pipe]
type CONNTYPE_T uint64

const (
	TCP  CONNTYPE_T = 0
	UNIX CONNTYPE_T = 1
	PIPE CONNTYPE_T = 2
)

func ConnTypeString(conntype CONNTYPE_T) string {
	switch conntype {
	case TCP:
		return "TCP"
	case UNIX:
		return "UNIX"
	case PIPE:
		return "PIPE"
	default:
		panic("Unknown CONNTYPE")
	}
}

type RPCConnection struct {
	// config
	App      string
	MyId     uint64
	Ids      []uint64
	Mode     MODE_T
	ConnType CONNTYPE_T

	OutConn ReaderWriter `json:"-"` // [net.Conn|*os.File]
	InConn  ReaderWriter `json:"-"` // [net.Conn|*os.File]

	// MODE == FILE only
	LogDir      string
	UsedFiles   map[string]bool
	AllBytes    []byte `json:"-"`
	Pos         int    // indicating the current position in AllBytes
	FileCounter int
	// ZooKeeper traces: one directory per time slot, one file per stream
	TheLogReader *LogReader

	CurrentFilePath           string
	CurrentFileStartTime      time.Time `json:"-"`
	CurrentFileNumMessage     int
	CurrentFileNumSendMessage int
	CurrentFileNumRecvMessage int

	CurrentMetaPath string
	StartMsgCount   int // will get from log*.meta
	EndMsgCount     int // will get from log*.meta
	NumTimeslots    int // I.e, num files

	FirstReqID uint64
}

var Conn *RPCConnection

func (c *RPCConnection) ReadFirstReqID() {
	if c.Mode != FILE {
		panic("ReadFirstReqID is only for FILE mode")
	}
	// read the first request id
	f, err := os.OpenFile(filepath.Join(c.LogDir, "firstReqID.txt"), os.O_RDONLY, 0)
	if err != nil {
		panic(err)
	}
	fmt.Fscanf(f, "%d\n", &c.FirstReqID)
	f.Close()
}

func (c *RPCConnection) useNextNotUsedFile() {
	var err error
	if c.FileCounter > 0 {
		fmt.Printf("Last file(%v) processed %v(%d+%d) took %v\n", filepath.Base(c.CurrentFilePath), c.CurrentFileNumMessage, c.CurrentFileNumSendMessage, c.CurrentFileNumRecvMessage, time.Since(c.CurrentFileStartTime))
	}
	firstNotUsed := ""
	for firstNotUsed == "" {
		paths, err := os.ReadDir(c.LogDir)
		if err != nil {
			panic(err)
		}
		filteredPaths := []os.DirEntry{}
		for _, path := range paths {
			name := path.Name()
			if strings.HasPrefix(name, "messages") && strings.HasSuffix(name, ".data") {
				filteredPaths = append(filteredPaths, path)
			}
		}
		for i, path := range filteredPaths {
			name := path.Name()
			used, ok := c.UsedFiles[name]
			if !ok {
				c.UsedFiles[name] = false
			}
			if firstNotUsed == "" && (!ok || !used) {
				if i >= len(filteredPaths)-1 {
					firstNotUsed = ""
				} else {
					firstNotUsed = name
				}
				break
			}
		}
		if firstNotUsed == "" {
			time.Sleep(5 * time.Second)
			continue
		}
		c.UsedFiles[firstNotUsed] = true
	}

	firstNotUsed = filepath.Join(c.LogDir, firstNotUsed)
	// setup for this data file
	fileToUse := firstNotUsed
	f, err := os.OpenFile(fileToUse, os.O_RDONLY, 0)
	if err != nil {
		panic(err)
	}
	c.Pos = 0
	c.AllBytes, err = ioutil.ReadAll(f)
	if err != nil {
		panic(err)
	}

	f.Close()

	// setup for this meta file
	metaToUse := firstNotUsed[:len(firstNotUsed)-5] + ".meta"
	c.CurrentMetaPath = metaToUse
	f, err = os.OpenFile(metaToUse, os.O_RDONLY, 0)
	if err != nil {
		panic(err)
	}
	var StartMsgIdx, LastMsgIdx int
	fmt.Fscanf(f, "%d\n%d\n", &StartMsgIdx, &LastMsgIdx)
	if c.FileCounter == c.NumTimeslots-1 {
		// statistics.Stats.StartMsgIdx = StartMsgIdx
		statistics.Stats.LastMsgIdx = LastMsgIdx
	}
	f.Close()
	fmt.Printf("Using file: %+v (%d - %d)\n", filepath.Base(firstNotUsed), StartMsgIdx, LastMsgIdx)

	// os.Remove(fileToUse)
	// os.Remove(metaToUse)

	c.CurrentFilePath = fileToUse
	c.CurrentFileStartTime = time.Now()
	c.CurrentFileNumMessage = 0
	c.CurrentFileNumSendMessage = 0
	c.CurrentFileNumRecvMessage = 0
	c.FileCounter = c.FileCounter + 1
}

func (c *RPCConnection) SetupEnv() {
	mode := os.Getenv("SIMMODE")
	switch mode {
	case "NONE":
		c.Mode = NONE
	case "SYNC":
		c.Mode = SYNC
	case "ASYNC":
		c.Mode = ASYNC
	case "FILE":
		c.Mode = FILE
	case "TLA":
		c.Mode = TLA
	case "":
		c.Mode = ASYNC
	default:
		panic(fmt.Sprintf("Unknown SIMMODE: %v", mode))
	}
	if c.Mode != FILE && c.Mode != TLA {
		conntype := os.Getenv("SIMCONNTYPE")
		switch conntype {
		case "TCP":
			c.ConnType = TCP
			fmt.Printf("\tConnType=\033[1mTCP\033[0m\n")
		case "UNIX":
			c.ConnType = UNIX
			fmt.Printf("\tConnType=\033[1mUNIX\033[0m\n")
		case "PIPE":
			c.ConnType = PIPE
			fmt.Printf("\tConnType=\033[1mPIPE\033[0m\n")
		case "":
			c.ConnType = PIPE
			fmt.Printf("\tConnTyp=\033[1mPIPE\033[0m\n")
		default:
			panic(fmt.Sprintf("Unknown SIMCONNTYPE: %v", conntype))
		}
	}
	fmt.Printf("RPCConnection Setup: Mode=%s, ConnType=%s\n", ModeString(c.Mode), ConnTypeString(c.ConnType))
}

func (c *RPCConnection) ReorderEtcdIds() {
	// FIXME: I temporally replace with the following cluster ids
	if c.MyId == 0xc7650337f7e099ff || c.MyId == 0x26ded800b662066a || c.MyId == 0xb5aa32b3a17abb1c || c.MyId == 0x1ba5146ccb4c35de || c.MyId == 0x39f5b69282aeb766 {
		// These are for 10.10.1.1, 10.10.1.2, 10.10.1.3, 10.10.1.4, 10.10.1.5
		if len(c.Ids) == 3 {
			c.Ids = []uint64{0xc7650337f7e099ff, 0x26ded800b662066a, 0xb5aa32b3a17abb1c}
		} else if len(c.Ids) == 5 {
			c.Ids = []uint64{0xc7650337f7e099ff, 0x26ded800b662066a, 0xb5aa32b3a17abb1c, 0x1ba5146ccb4c35de, 0x39f5b69282aeb766}
		}
	} else if c.MyId == 0xc21aed40f3975fc3 || c.MyId == 0x76409a1fe3d4f4cb || c.MyId == 0x57766e4340673b8c || c.MyId == 0x7c2014fd331959d0 || c.MyId == 0x3623ed51995d4e6 {
		// These are for 172.17.1.2, 172.17.1.3, 172.17.1.4, 172.17.1.5, 172.17.1.6
		if len(c.Ids) == 3 {
			c.Ids = []uint64{0xc21aed40f3975fc3, 0x76409a1fe3d4f4cb, 0x57766e4340673b8c}
		} else if len(c.Ids) == 5 {
			c.Ids = []uint64{0xc21aed40f3975fc3, 0x76409a1fe3d4f4cb, 0x57766e4340673b8c, 0x7c2014fd331959d0, 0x3623ed51995d4e6}
		}
	}
}

func (c *RPCConnection) Init(app string, id string, logDir string) *RPCConnection {
	var err error
	c.App = app
	c.SetupEnv()

	iid := os.Getenv("IID")
	if iid == "" {
		panic("Environment Variable `IID` is not set")
	}
	if c.Mode == FILE {
		// find the newest directory in /mydata
		c.FileCounter = 0
		c.CurrentFileNumMessage = 0
		c.UsedFiles = make(map[string]bool)
		if logDir != "" {
			c.LogDir = logDir
		} else {
			dirs, err := os.ReadDir("/mydata/")
			if err != nil {
				panic(err)
			}
			lastLogDir := ""
			for _, dir := range dirs {
				name := dir.Name()
				if strings.HasPrefix(name, "log") {
					lastLogDir = name
				}
			}
			c.LogDir = filepath.Join("/mydata", lastLogDir)
		}
		fmt.Printf("\033[1mLog dir\033[0m: %+v\n", c.LogDir)
		if c.App == "etcd" {
			// get cluster info
			f, err := os.OpenFile(filepath.Join(c.LogDir, "cluster.meta"), os.O_RDONLY, 0)
			if err != nil {
				panic(err)
			}
			cluster_info, err := ioutil.ReadAll(f)
			if err != nil {
				panic(err)
			}
			f.Close()
			cluster_info_id_strs := strings.Split(string(cluster_info), "\n")
			cluster_info_id_strs = cluster_info_id_strs[:len(cluster_info_id_strs)-1]
			cluster_info_ids := make([]uint64, len(cluster_info_id_strs))
			for i := 0; i < len(cluster_info_id_strs); i++ {
				cluster_info_ids[i], err = strconv.ParseUint(cluster_info_id_strs[i], 10, 64)
				if err != nil {
					panic(err)
				}
			}
			c.MyId = cluster_info_ids[0]
			c.Ids = cluster_info_ids[1:]

			// FIXME: I temporally replace with the following cluster ids
			c.ReorderEtcdIds()

			// get first reqid
			// c.ReadFirstReqID()
		}

		fmt.Printf("\033[1mMe\033[0m: 0x%x\n", c.MyId)
		fmt.Printf("\033[1mIds\033[0m: %+v\n\n", IntArrayToHexStringArray(c.Ids))

		if c.App == "zk" {
			c.TheLogReader = new(LogReader)
			c.TheLogReader.init(c.LogDir)
		} else {
			c.useNextNotUsedFile()
		}
	} else if c.Mode == TLA {
		fmt.Printf("\033[1mMe\033[0m: 0x%x\n", c.MyId)
		fmt.Printf("\033[1mIds\033[0m: %+v\n\n", IntArrayToHexStringArray(c.Ids))
	} else {
		switch c.ConnType {
		case UNIX:
			server_sock_path := fmt.Sprintf("/tmp/server.%s.sock", iid)
			os.Remove(server_sock_path)
			listen, err := net.Listen("unix", server_sock_path)
			if err != nil {
				fmt.Printf("listen failed, err:%v\n", err)
				return nil
			}
			c.OutConn, err = listen.Accept()
			if err != nil {
				fmt.Printf("accept failed, err:%v\n", err)
				return nil
			}
			c.InConn = c.OutConn
		case TCP:
			listen, err := net.Listen("tcp", "127.0.0.1:9090")
			if err != nil {
				fmt.Printf("listen failed, err:%v\n", err)
				return nil
			}
			c.OutConn, err = listen.Accept()
			if err != nil {
				fmt.Printf("accept failed, err:%v\n", err)
				return nil
			}
			c.InConn = c.OutConn
		case PIPE:
			stocPath := fmt.Sprintf("/tmp/tmpPipe_s_to_c.%s.pipe", iid)
			ctosPath := fmt.Sprintf("/tmp/tmpPipe_c_to_s.%s.pipe", iid)
			os.Remove(stocPath)
			os.Remove(ctosPath)
			syscall.Mkfifo(stocPath, 0666)
			syscall.Mkfifo(ctosPath, 0666)
			c.OutConn, err = os.OpenFile(stocPath, os.O_RDWR, os.ModeNamedPipe)
			if err != nil {
				panic(fmt.Sprintf("Failed to open pipe: %v", err))
			}
			c.InConn, err = os.OpenFile(ctosPath, os.O_RDONLY, os.ModeNamedPipe)
			if err != nil {
				panic(fmt.Sprintf("Failed to open pipe: %v", err))
			}
		default:
			panic("Unknown CONNTYPE")
		}
		fmt.Printf("accept success\n")
	}
	return c
}

func (c *RPCConnection) Close() {
	// c.OutConn.Close()
	// c.InConn.Close()
}

func (c *RPCConnection) Ack(data []byte) {
	if c.Mode == SYNC {
		NetWrite(c.OutConn, data)
	}
}

func (c *RPCConnection) Read(n int) (int, []byte) {
	if c.Mode != FILE {
		return NetRead(c.InConn, make([]byte, n))
	} else {
		// fmt.Printf("c.Pos=%d, n=%d, len(c.AllBytes)=%d\n", c.Pos, n, len(c.AllBytes))
		if c.Pos+n <= len(c.AllBytes) {
			c.Pos += n
			return n, c.AllBytes[c.Pos-n : c.Pos]
		} else {
			c.useNextNotUsedFile()
			return c.Read(n)
		}
	}
}
