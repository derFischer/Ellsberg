//go:build zk

package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"go.nyu.edu/protosim/comm"
	"go.nyu.edu/protosim/engine"
	"go.nyu.edu/protosim/statistics"
	"go.nyu.edu/protosim/zab/interpreter"
	"go.nyu.edu/protosim/zab/messagepb"
	"go.nyu.edu/protosim/zab/protocol"
	"go.nyu.edu/protosim/zab/spec"
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
	var idInt uint64
	var ids = []uint64{}
	IdStrs := strings.Split(*IdsStr, ",")
	for _, IdStr := range IdStrs {
		idInt, err = strconv.ParseUint(IdStr, 10, 64)
		if err != nil {
			panic(err)
		}
		ids = append(ids, idInt)
	}

	fmt.Printf("Using app=%s, id=%s\n", *App, *IdStr)

	fmt.Printf("Parallelizing Interpreting=%v\n", !*NoParallelizeInterpreting)

	// Setup Connection
	comm.Conn = new(comm.RPCConnection)
	comm.Conn.MyId = idInt
	comm.Conn.Ids = ids
	comm.Conn.Init(*App, *IdStr, *LogDir)
	if comm.Conn.Mode == comm.FILE {
		comm.Conn.TheLogReader.NumTimeslots = *NumTimeslots
		if *NumTimeslots < 0 {
			numTimeslotsStr, err := exec.Command("/bin/bash", "-c", "ls "+comm.Conn.LogDir+" | grep 1970 | wc -l").Output()
			if err != nil {
				panic(err)
			}
			numTimeslots, err := strconv.Atoi(strings.TrimSpace(string(numTimeslotsStr)))
			if err != nil {
				panic(err)
			}
			comm.Conn.TheLogReader.NumTimeslots = numTimeslots + *NumTimeslots
			if comm.Conn.TheLogReader.NumTimeslots < 0 {
				comm.Conn.TheLogReader.NumTimeslots = -1
			}
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
		if err != nil {
			panic(err)
		}
		return SendMsgProcessed
	}
	if *HackStartMode != "default" {
		// If start mode is specified we always use this for HackStart
		*HackStart = getStat(*HackStartMode, ".*, HackStart: ([0-9]+)")
	} else if *AsapMode != "on" || *MergeMode != "functional" {
		// If optimizations are not in default mode, we must specify a start hack point.
		*HackStart = getStat("default", ".*, MsgProcessed: ([0-9]+)") / 5
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

func ZKGetClusterInfoAndStartSimulationServer() {
	conn := comm.Conn
	defer conn.Close()

	intp := interpreter.Interpreter{}
	spec.Look_ahead_index = make(map[messagepb.MessageType]int)
	sl := protocol.StartSimulationServer(conn.MyId, conn.Ids)

	for _, mt := range messagepb.Send_Message_Types {
		spec.Look_ahead_index[mt] = -1
	}
	var GetNext func() *interpreter.SimConnMessage
	if conn.Mode == comm.FILE {
		GetNext = func() *interpreter.SimConnMessage {
			reader := conn.TheLogReader.NextSimConnMessageReader()
			m := intp.GetNextMessage(reader)
			conn.TheLogReader.CurrentTimeslotNumMessage++
			if m.IsSend {
				conn.TheLogReader.CurrentTimeslotNumSendMessage++
			} else {
				conn.TheLogReader.CurrentTimeslotNumRecvMessage++
			}
			return &m
		}
	} else {
		GetNext = func() *interpreter.SimConnMessage {
			conn.CurrentFileNumMessage += 1
			_, header := conn.Read(8)
			lengths := binary.BigEndian.Uint64(header)
			_, msgBytes := conn.Read(int(lengths))
			reader := bufio.NewReader(bytes.NewReader(msgBytes))
			m := intp.GetNextMessage(reader)
			return &m
		}
	}

	var msgChan chan *interpreter.SimConnMessage = make(chan *interpreter.SimConnMessage, 2048)
	go func() {
		for {
			msgChan <- GetNext()
		}
	}()

	msg_buffer := make([]*interpreter.SimConnMessage, 0)

	// f, _ := CreateFile("", "cpu.prof")
	for {
		MsgStartTime := time.Now()
		var msgProcessedTime int64 = 0
		var mm *interpreter.SimConnMessage
		if len(msg_buffer) != 0 {
			mm = msg_buffer[0]
			msg_buffer = msg_buffer[1:]
			for k, v := range spec.Look_ahead_index {
				spec.Look_ahead_index[k] = v - 1
			}
		} else {
			mm = <-msgChan
		}
		statistics.Stats.AfterReadOneMsg()

		if mm.Skip {
			if statistics.Stats.MsgReaded == statistics.Stats.LastMsgIdx {
				fmt.Printf("------------\nReached LastMsgIdx=%d\n", statistics.Stats.LastMsgIdx)
				statistics.Stats.DumpAllStats()
				os.Exit(0)
			}
			continue
		}
		// if mm.IsSend {
		// 	fmt.Printf("[%d] send: %+v\n", statistics.Stats.MsgProcessed, mm)
		// } else {
		// 	fmt.Printf("[%d] recv: %+v\n", statistics.Stats.MsgProcessed, mm)
		// }

		statistics.Stats.RecordMsgTimestamp(uint64(mm.Timestamp))
		timeouted := false
		if !statistics.Stats.Enabled {
			if mm.IsSend {
				var next_simconnmessage *interpreter.SimConnMessage
				look_ahead_type := spec.Look_ahead(mm.M.Type)
				if look_ahead_type != messagepb.NONE {
					buffer_idx := spec.Look_ahead_index[mm.M.Type]
					if buffer_idx >= 0 {
						next_simconnmessage = msg_buffer[buffer_idx]
					} else {
						var tmp_next_message *interpreter.SimConnMessage
						for {
							tmp_next_message = <-msgChan
							if tmp_next_message.IsSend && (look_ahead_type&tmp_next_message.M.Type) != 0 {
								next_simconnmessage = tmp_next_message
								spec.Look_ahead_index[mm.M.Type] = len(msg_buffer) - 1
								break
							}
						}
					}
				}

				var next_message *messagepb.Message
				if next_simconnmessage != nil {
					next_message = &next_simconnmessage.M
				}
				success := sl.SendMsg(&mm.M, next_message)
				msgProcessedTime = time.Since(MsgStartTime).Microseconds()
				statistics.Stats.RecordSendMsgProcessTime(msgProcessedTime)
				statistics.Stats.RecordSendMsgTimestamp(uint64(mm.Timestamp))
				statistics.Stats.AfterProcessedOneSendMsg()
				if !success {
					// TODO: add ack if trying SYNC in the future.
					statistics.Stats.PrintBasicInfo(time.Now())
					panic("send failed")
				}
			} else {
				sl.ReceiveMsg(mm.M)
				msgProcessedTime = time.Since(MsgStartTime).Microseconds()
				statistics.Stats.AfterProcessedOneRecvMsg()
			}
		} else {
			// This is because we need a timeout (instead of using the processtime,
			// which can be as large as 20 minutes).
			doneCh := make(chan bool)
			executeFunc := func() {
				if mm.IsSend {
					success := sl.SendMsg(&mm.M, nil)
					msgProcessedTime = time.Since(MsgStartTime).Microseconds()
					statistics.Stats.RecordSendMsgProcessTime(msgProcessedTime)
					statistics.Stats.RecordSendMsgTimestamp(uint64(mm.Timestamp))
					statistics.Stats.AfterProcessedOneSendMsg()
					if !success {
						// TODO: add ack if trying SYNC in the future.
						statistics.Stats.PrintBasicInfo(time.Now())
						panic("send failed")
					}
				} else {
					sl.ReceiveMsg(mm.M)
					msgProcessedTime = time.Since(MsgStartTime).Microseconds()
					statistics.Stats.AfterProcessedOneRecvMsg()
				}
				if statistics.Stats.Enabled {
					doneCh <- true
				}
			}
			go executeFunc()
			select {
			case <-doneCh:
			case <-time.After(30 * time.Second):
				msgProcessedTime = time.Since(MsgStartTime).Microseconds()
				fmt.Printf("timeout\n")
				timeouted = true
			}
		}

		if statistics.Stats.Enabled &&
			(engine.ASAPMode == engine.OFF || engine.MergeMode == engine.NOMERGE) &&
			statistics.Stats.MsgProcessed >= statistics.Stats.HackStart && msgProcessedTime >= 10_000 {
			fmt.Printf("MsgProcessed: %v, time: %v, PendingMsgCount=%v, RemainingStates=%v\n", statistics.Stats.MsgProcessed, msgProcessedTime, statistics.Stats.PendingMsgCount[len(statistics.Stats.PendingMsgCount)-1], statistics.Stats.RemainingStates[len(statistics.Stats.RemainingStates)-1])
		}
		if statistics.Stats.Enabled && msgProcessedTime >= 30_000_000 || timeouted || statistics.Stats.MsgProcessed == statistics.Stats.HackEnd {
			fmt.Printf("------------\nTaking too long, or reached HackEnd=%d(%d)\n", statistics.Stats.HackEnd, statistics.Stats.MsgProcessed)
			statistics.Stats.DumpAllStats()
			os.Exit(0)
		}
		if statistics.Stats.MsgReaded == statistics.Stats.LastMsgIdx {
			fmt.Printf("------------\nReached LastMsgIdx=%d\n", statistics.Stats.LastMsgIdx)
			statistics.Stats.DumpAllStats()
			os.Exit(0)
		}
		// TODO: add ack if trying SYNC in the future.
	}
}

func main() {
	ParseArgsAndSetup()
	// Setup config
	var ids = []uint64{}
	IdStrs := strings.Split(*IdsStr, ",")
	for _, IdStr := range IdStrs {
		idInt, err := strconv.ParseUint(IdStr, 10, 64)
		if err != nil {
			panic(err)
		}
		ids = append(ids, idInt)
	}
	idInt, err := strconv.ParseUint(*IdStr, 10, 64)
	if err != nil {
		panic(err)
	}
	comm.Conn.MyId = idInt
	comm.Conn.Ids = ids

	ZKGetClusterInfoAndStartSimulationServer()
}

func CreateFile(dir, name string) (*os.File, error) {
	if dir == "" {
		var err error
		dir, err = os.Getwd()
		if err != nil {
			return nil, err
		}
	}
	path := filepath.Join(dir, name)
	return os.Create(path)
}

func startCPUProfile(f *os.File) error {
	if f == nil {
		return errors.New("nil file")
	}
	return pprof.StartCPUProfile(f)
}

func stopCPUProfile() {
	pprof.StopCPUProfile()
}
