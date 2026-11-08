//go:build etcd || redis

package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.nyu.edu/protosim/comm"
	"go.nyu.edu/protosim/engine"
	"go.nyu.edu/protosim/raft/interpreter"
	"go.nyu.edu/protosim/raft/messagepb"
	"go.nyu.edu/protosim/raft/protocol"
	"go.nyu.edu/protosim/raft/spec"
	"go.nyu.edu/protosim/statistics"
)

func ParseArgsAndSetup() {
	err := parser.Parse(os.Args)
	if err != nil {
		fmt.Print(parser.Usage(err))
		os.Exit(1)
	}
	// Setup evaluation mode
	switch *MergeMode {
	case "nomerge":
		engine.MergeMode = engine.NOMERGE
	case "exact":
		engine.MergeMode = engine.EXACT
	case "functional":
		engine.MergeMode = engine.FUNCTIONAL
	default:
		panic("wrong merge mode")
	}
	switch *AsapMode {
	case "off":
		engine.ASAPMode = engine.OFF
	case "on":
		engine.ASAPMode = engine.ON
	default:
		panic("wrong asap mode")
	}

	// Setup config
	var ids = []uint64{}
	IdStrs := strings.Split(*IdsStr, ",")
	for _, idStr := range IdStrs {
		idInt, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil {
			panic(err)
		}
		ids = append(ids, idInt)
	}
	MyId, err := strconv.ParseUint(*IdStr, 10, 64)
	if err != nil {
		panic(err)
	}

	// fmt.Printf("Using app=%s, id=%s\n", *App, *IdStr)
	// spec.APP = *App

	fmt.Printf("Parallelizing Interpreting=%v\n", !*NoParallelizeInterpreting)

	// Setup Connection
	comm.Conn = new(comm.RPCConnection)
	comm.Conn.NumTimeslots = *NumTimeslots
	comm.Conn.MyId = MyId
	comm.Conn.Ids = ids
	comm.Conn.Init(*App, *IdStr, *LogDir)
	if *NumTimeslots < 0 {
		numTimeslotsStr, err := exec.Command("/bin/bash", "-c", "ls "+comm.Conn.LogDir+" | grep data | wc -l").Output()
		if err != nil {
			panic(err)
		}
		numTimeslots, err := strconv.Atoi(strings.TrimSpace(string(numTimeslotsStr)))
		if err != nil {
			panic(err)
		}
		comm.Conn.NumTimeslots = numTimeslots + *NumTimeslots
		if comm.Conn.NumTimeslots < 0 {
			comm.Conn.NumTimeslots = -1
		}
	}

	// Setup Statistics
	getStat := func(exprname string, reg string) int {
		f, err := os.OpenFile(filepath.Join(comm.Conn.LogDir, exprname, "status.data"), os.O_RDONLY, 0644)
		if err != nil {
			panic(err)
		}
		statusbytes, err := ioutil.ReadAll(f)
		if err != nil {
			panic(err)
		}
		re := regexp.MustCompile(reg)
		match := re.FindStringSubmatch(string(statusbytes))
		SendMsgProcessedStr := match[1]
		SendMsgProcessed, err := strconv.Atoi(SendMsgProcessedStr)
		return SendMsgProcessed
	}
	if *HackStartMode != "default" {
		// If start mode is specified we always use this for HackStart
		*HackStart = getStat(*HackStartMode, ".*, HackStart: ([0-9]+)")
	} else if *AsapMode != "on" || *MergeMode != "functional" {
		// If optimizations are not in default mode, we must specify a start hack point.
		*HackStart = getStat("default", ".*, MsgProcessed: ([0-9]+)") / 2
	}
	if *HackEndMode != "default" {
		*HackEnd = getStat(*HackEndMode, ".*, MsgProcessed: ([0-9]+)")
	}

	fmt.Printf("merge mode: %v, asap mode: %v, HackStart: %v, HackEnd: %v\n", *MergeMode, *AsapMode, *HackStart, *HackEnd)

	statistics.Init(*StatsEnabled, *HackStart, *HackEnd, *OnlyPerformance, comm.Conn.LogDir, *ExprName)
	if *StatsEnabled {
		fmt.Printf("Stats enabled, exprname=%s\n", *ExprName)
	} else {
		fmt.Printf("Stats disabled\n")
	}

}

func GetClusterInfoAndStartSimulationServer() {
	conn := comm.Conn
	defer conn.Close()

	spec.INIT_NUM_PEERS = uint64(len(conn.Ids))
	spec.Look_ahead_index = make(map[messagepb.MessageType]int)
	sl := protocol.StartSimulationServer(conn.MyId, conn.Ids, *App)

	for _, mt := range messagepb.Send_Message_Types {
		spec.Look_ahead_index[mt] = -1
	}

	type MAndMsg struct {
		M      *interpreter.Msg
		Header []byte
		Msg    *messagepb.Message
	}

	var GetNext func() *MAndMsg
	if *App == "etcd" {
		GetNext = func() *MAndMsg {
			conn.CurrentFileNumMessage += 1
			_, header := conn.Read(32)
			if string(header[:8]) != "nyusimul" {
				panic("wrong protocol")
			}
			lengths := binary.LittleEndian.Uint64(header[16:24])
			_, msgBytes := conn.Read(int(lengths))
			m := interpreter.ParsePbMsgBytesToMsg(msgBytes)
			msg := interpreter.MsgInterpreter(m)
			if m.Send {
				conn.CurrentFileNumSendMessage++
			} else {
				conn.CurrentFileNumRecvMessage++
			}
			return &MAndMsg{M: &m, Header: header, Msg: &msg}
		}
	} else if *App == "redis" {
		GetNext = func() *MAndMsg {
			conn.CurrentFileNumMessage += 1
			_, header := conn.Read(8)
			hackHeader := make([]byte, 32)
			copy(hackHeader, header)
			lengths := binary.LittleEndian.Uint64(hackHeader[:8])
			_, msgBytes := conn.Read(int(lengths))
			m := interpreter.ParsePbMsgBytesToMsg(msgBytes)
			msg := interpreter.MsgInterpreter(m)
			if m.Send {
				conn.CurrentFileNumSendMessage++
			} else {
				conn.CurrentFileNumRecvMessage++
			}
			return &MAndMsg{M: &m, Header: hackHeader, Msg: &msg}
		}
	} else {
		panic(fmt.Sprintf("Unkown app: %s\n", *App))
	}

	var msgChan chan *MAndMsg
	if !*NoParallelizeInterpreting {
		msgChan = make(chan *MAndMsg, 1024)
		go func() {
			for {
				msgChan <- GetNext()
			}
		}()
	}

	msg_buffer := make([]*MAndMsg, 0)
	buffer_pop_idx := 0

	for {
		MsgStartTime := time.Now()

		var mAndMsg *MAndMsg

		if len(msg_buffer) != 0 {
			mAndMsg = msg_buffer[0]
			msg_buffer = msg_buffer[1:]
			buffer_pop_idx = buffer_pop_idx + 1
		} else {
			if !*NoParallelizeInterpreting {
				mAndMsg = <-msgChan
			} else {
				mAndMsg = GetNext()
			}
		}

		m := mAndMsg.M
		header := mAndMsg.Header
		msg := *mAndMsg.Msg

		// if m.Send {
		// 	fmt.Printf("Send(%d): %+v\n", statistics.Stats.MsgProcessed, msg)
		// } else {
		// 	fmt.Printf("Recv(%d): %+v\n", statistics.Stats.MsgProcessed, msg)
		// }

		if *DontProcessPtr {
			continue
		}
		var msgProcessedTime int64 = 0
		if m.Send {
			var next_mAndMsg *MAndMsg
			look_ahead_type := spec.Look_ahead(msg.Type)
			if look_ahead_type != messagepb.NONE {
				buffer_idx := spec.Look_ahead_index[msg.Type]
				if buffer_idx >= buffer_pop_idx {
					next_mAndMsg = msg_buffer[buffer_idx-buffer_pop_idx]
				} else {
					var tmp_next_mAndMsg *MAndMsg
					for {
						if !*NoParallelizeInterpreting {
							tmp_next_mAndMsg = <-msgChan
						} else {
							tmp_next_mAndMsg = GetNext()
						}
						msg_buffer = append(msg_buffer, tmp_next_mAndMsg)
						if tmp_next_mAndMsg.M.Send && (look_ahead_type&tmp_next_mAndMsg.Msg.Type != 0) {
							next_mAndMsg = tmp_next_mAndMsg
							spec.Look_ahead_index[msg.Type] = len(msg_buffer) - 1 + buffer_pop_idx
							break
						}
					}
				}
			}
			// if msg.Type == messagepb.ClientNormalResp || msg.Type == messagepb.ClientROResp {
			// 	buffer_length := len(msg_buffer)
			// 	if buffer_length != 0 {
			// 		next_mAndMsg = msg_buffer[buffer_length-1]
			// 	} else {
			// 		var tmp_next_mAndMsg *MAndMsg
			// 		for i := buffer_length; i < *MaxAhead; i++ {
			// 			if !*NoParallelizeInterpreting {
			// 				tmp_next_mAndMsg = <-msgChan
			// 			} else {
			// 				tmp_next_mAndMsg = GetNext()
			// 			}
			// 			msg_buffer = append(msg_buffer, tmp_next_mAndMsg)
			// 			if tmp_next_mAndMsg.M.Send && tmp_next_mAndMsg.Msg.Mterm != 0 {
			// 				next_mAndMsg = tmp_next_mAndMsg
			// 				break
			// 			}
			// 		}
			// 	}
			// }
			var next_message *messagepb.Message
			if next_mAndMsg != nil {
				next_message = next_mAndMsg.Msg
			}
			success := sl.SendMsg(msg, next_message)
			msgProcessedTime = time.Since(MsgStartTime).Microseconds()
			statistics.Stats.RecordSendMsgProcessTime(msgProcessedTime)
			statistics.Stats.RecordSendMsgTimestamp(binary.LittleEndian.Uint64(header[24:32]))
			statistics.Stats.AfterProcessedOneSendMsg()
			if !success {
				binary.LittleEndian.PutUint64(header[16:24], 0)
				conn.Ack(header[:24])
				statistics.Stats.PrintBasicInfo(time.Now())
				panic("send failed")
			}
		} else {
			sl.ReceiveMsg(msg)
			msgProcessedTime = time.Since(MsgStartTime).Microseconds()
			statistics.Stats.AfterProcessedOneRecvMsg()
		}
		if statistics.Stats.Enabled &&
			(engine.ASAPMode == engine.OFF || engine.MergeMode == engine.NOMERGE) &&
			statistics.Stats.MsgProcessed >= statistics.Stats.HackStart {
			fmt.Printf("MsgProcessed: %v, time: %v, PendingMsgCount=%v\n", statistics.Stats.MsgProcessed, msgProcessedTime, statistics.Stats.PendingMsgCount[len(statistics.Stats.PendingMsgCount)-1])
		}
		if statistics.Stats.Enabled && msgProcessedTime >= 10_000_000 || statistics.Stats.MsgProcessed == statistics.Stats.HackEnd {
			fmt.Printf("------------\nTaking too long, or reached HackEnd=%d(%d)\n", statistics.Stats.HackEnd, statistics.Stats.MsgProcessed)
			statistics.Stats.DumpAllStats()
			os.Exit(0)
		}
		if statistics.Stats.MsgProcessed == statistics.Stats.LastMsgIdx {
			fmt.Printf("------------\nReached LastMsgIdx=%d\n", statistics.Stats.LastMsgIdx)
			statistics.Stats.DumpAllStats()
			os.Exit(0)
		}
		binary.LittleEndian.PutUint64(header[16:24], 1)
		conn.Ack(header[:24])
	}
}

func main() {
	ParseArgsAndSetup()

	iid, err := strconv.ParseInt(os.Getenv("IID"), 10, 64)
	if err != nil {
		panic(err)
	}
	fmt.Printf("main: my iid:%v\n", iid)

	if *App == "etcd" {
		if comm.Conn.Mode != comm.FILE {
			b := make([]byte, 8)
			_, myIdBytes := comm.NetRead(comm.Conn.InConn, b)
			comm.Conn.MyId = uint64(binary.LittleEndian.Uint64(myIdBytes))

			_, cluster_info_lengths_bytes := comm.NetRead(comm.Conn.InConn, b)
			cluster_info_lengths := uint64(binary.LittleEndian.Uint64(cluster_info_lengths_bytes))
			_, cluster_info_bytes := comm.NetRead(comm.Conn.InConn, make([]byte, cluster_info_lengths))
			comm.Conn.Ids = make([]uint64, cluster_info_lengths/8)
			err := json.Unmarshal(cluster_info_bytes, &comm.Conn.Ids)
			if err != nil {
				panic(err)
			}
			comm.Conn.ReorderEtcdIds()
		}
	} else if *App == "redis" {
		// Redis's id are from arguments, will be setup in `ParseArgsAndSetup()`
	}

	GetClusterInfoAndStartSimulationServer()
}
