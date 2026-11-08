package statistics

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"
)

type Statistics struct {
	// config
	Enabled         bool
	OnlyPerformance bool
	LogDir          string
	ExprName        string

	// starting from this messageCount, we will hack the simulator
	// to disable optimization.
	HackStart int
	HackEnd   int

	// processed information
	StartMsgIdx                 int
	LastMsgIdx                  int
	MsgProcessed                int
	MsgReaded                   int
	SendMsgProcessed            int
	SendMsgInRangeProcessed     int
	SendMsgProcessedBeforeRange int

	// time
	InitTime           time.Time `json:"-"`
	StartTime          time.Time `json:"-"`
	SendMsgProcessTime []int64   `json:"-"`

	// stats
	MsgTimestamps      []uint64  `json:"-"`
	SendMsgTimestamps  []uint64  `json:"-"`
	EliminatedStates   []uint64  `json:"-"`
	StateCountThisSend uint64    `json:"-"`
	StateCountPerSend  []uint64  `json:"-"`
	PendingRatio       []float64 `json:"-"`
	SendMsgType        []int8    `json:"-"`
	MsgType            []int8    `json:"-"`

	RemainingStates []uint64  `json:"-"`
	PendingMsgCount []uint64  `json:"-"`
	PrunedRatio     []float64 `json:"-"`
	Pruned1Ratio    []float64 `json:"-"`
	Pruned2Ratio    []float64 `json:"-"`
	IsSendList      []bool    `json:"-"`
}

var Stats Statistics = Statistics{
	StartMsgIdx:             0,
	LastMsgIdx:              math.MaxInt64,
	MsgProcessed:            0,
	MsgReaded:               0,
	SendMsgProcessed:        0,
	SendMsgInRangeProcessed: 0,
}

func Init(enabled bool, HackStart int, HackEnd int, onlyPerformance bool, logDir string, exprName string) {
	Stats.StartMsgIdx = 0
	Stats.LastMsgIdx = math.MaxInt64
	Stats.MsgProcessed = 0
	Stats.SendMsgProcessed = 0
	Stats.SendMsgInRangeProcessed = 0
	Stats.Enabled = enabled
	Stats.OnlyPerformance = onlyPerformance
	Stats.LogDir = logDir
	Stats.ExprName = exprName
	Stats.HackStart = HackStart
	Stats.HackEnd = HackEnd
	Stats.InitTime = time.Now()
}

func (stats *Statistics) AfterReadOneMsg() {
	stats.MsgReaded++
}

func (stats *Statistics) AfterProcessOneMsg() {
	stats.MsgProcessed++
	if stats.MsgProcessed == stats.StartMsgIdx {
		fmt.Printf("stats start\n")
		stats.StartTime = time.Now()
		stats.SendMsgProcessedBeforeRange = stats.SendMsgProcessed
	}
}

func (stats *Statistics) AfterProcessedOneSendMsg() {
	stats.RecordIsSend(true)
	stats.AfterProcessOneMsg()
	stats.SendMsgProcessed++
	if stats.StartMsgIdx <= stats.MsgProcessed && stats.MsgProcessed <= stats.LastMsgIdx {
		stats.SendMsgInRangeProcessed++
	}
}

func (stats *Statistics) AfterProcessedOneRecvMsg() {
	stats.RecordIsSend(false)
	stats.AfterProcessOneMsg()
}

func (stats *Statistics) StatsCondition() bool {
	return stats.Enabled && !stats.OnlyPerformance && stats.StartMsgIdx <= stats.MsgProcessed && stats.MsgProcessed <= stats.LastMsgIdx
}

func (stats *Statistics) PerfCondition() bool {
	return stats.Enabled && stats.StartMsgIdx <= stats.MsgProcessed && stats.MsgProcessed <= stats.LastMsgIdx
}

func (stats *Statistics) RecordEliminatedStates(value uint64) {
	if stats.StatsCondition() {
		stats.EliminatedStates = append(stats.EliminatedStates, value)
	}
}

func (stats *Statistics) RecordRemainingStates(value uint64) {
	if stats.StatsCondition() {
		stats.RemainingStates = append(stats.RemainingStates, value)
	}
}

func (stats *Statistics) RecordPendingMsgCount(value uint64) {
	if stats.StatsCondition() {
		stats.PendingMsgCount = append(stats.PendingMsgCount, value)
	}
}

func (stats *Statistics) RecordStateCountPerSend(value uint64) {
	if stats.StatsCondition() {
		stats.StateCountPerSend = append(stats.StateCountPerSend, value)
	}
}

func (stats *Statistics) RecordPrunedRatio(value float64) {
	if stats.StatsCondition() {
		stats.PrunedRatio = append(stats.PrunedRatio, value)
	}
}

func (stats *Statistics) RecordPruned1Ratio(value float64) {
	if stats.StatsCondition() {
		stats.Pruned1Ratio = append(stats.Pruned1Ratio, value)
	}
}

func (stats *Statistics) RecordPruned2Ratio(value float64) {
	if stats.StatsCondition() {
		stats.Pruned2Ratio = append(stats.Pruned2Ratio, value)
	}
}

func (stats *Statistics) RecordIsSend(value bool) {
	if stats.StatsCondition() {
		stats.IsSendList = append(stats.IsSendList, value)
	}
}

func (stats *Statistics) RecordPendingRatio(value float64) {
	if stats.StatsCondition() {
		stats.PendingRatio = append(stats.PendingRatio, value)
	}
}

func (stats *Statistics) RecordMsgTimestamp(value uint64) {
	if stats.StatsCondition() {
		stats.MsgTimestamps = append(stats.MsgTimestamps, value)
	}
}

func (stats *Statistics) RecordSendMsgTimestamp(value uint64) {
	if stats.StatsCondition() {
		stats.SendMsgTimestamps = append(stats.SendMsgTimestamps, value)
	}
}

func (stats *Statistics) RecordSendMsgProcessTime(value int64) {
	if stats.PerfCondition() {
		stats.SendMsgProcessTime = append(stats.SendMsgProcessTime, value)
	}
}

func (stats *Statistics) RecordSendMsgType(value int8) {
	if stats.PerfCondition() {
		stats.SendMsgType = append(stats.SendMsgType, value)
	}
}

func (stats *Statistics) RecordMsgType(value int8) {
	if stats.PerfCondition() {
		stats.MsgType = append(stats.MsgType, value)
	}
}

func (stats *Statistics) PrintBasicInfo(now time.Time) {
	fmt.Printf(
		"time: %v, timeFromInit: %v, MsgProcessed: %v, SendMsgProcessedBeforeRange: %v, SendMsgProcessed: %v, SendMsgInRangeProcessed: %v, The Msg Range: %v - %v, HackStart: %v\n",
		now.Sub(stats.StartTime), now.Sub(stats.InitTime), stats.MsgProcessed, stats.SendMsgProcessedBeforeRange, stats.SendMsgProcessed, stats.SendMsgInRangeProcessed,
		stats.StartMsgIdx, stats.LastMsgIdx, stats.HackStart,
	)
}

func (stats *Statistics) DumpAllStats() {
	now := time.Now()
	stats.PrintBasicInfo(now)
	fmt.Printf("Dumping all stats...")
	os.MkdirAll(filepath.Join(stats.LogDir, stats.ExprName), 0755)
	stats.DumpConfig()
	stats.DumpStatus(now)
	stats.DumpStatsInt64(filepath.Join(stats.LogDir, stats.ExprName, "SendMsgProcessTime.data"), stats.SendMsgProcessTime)
	if !stats.OnlyPerformance {
		// stats.DumpStatsUint64(filepath.Join(stats.LogDir, stats.ExprName, "EliminatedStates.data"), stats.EliminatedStates)
		stats.DumpStatsUint64(filepath.Join(stats.LogDir, stats.ExprName, "RemainingStates.data"), stats.RemainingStates)
		stats.DumpStatsFloat64(filepath.Join(stats.LogDir, stats.ExprName, "PrunedRatio.data"), stats.PrunedRatio)
		stats.DumpStatsFloat64(filepath.Join(stats.LogDir, stats.ExprName, "Pruned1Ratio.data"), stats.Pruned1Ratio)
		stats.DumpStatsFloat64(filepath.Join(stats.LogDir, stats.ExprName, "Pruned2Ratio.data"), stats.Pruned2Ratio)
		stats.DumpStatsFloat64(filepath.Join(stats.LogDir, stats.ExprName, "PendingRatio.data"), stats.PendingRatio)
		stats.DumpStatsUint64(filepath.Join(stats.LogDir, stats.ExprName, "PendingMsgCount.data"), stats.PendingMsgCount)
		// stats.DumpStatsUint64(filepath.Join(stats.LogDir, stats.ExprName, "StateCountPerSend.data"), stats.StateCountPerSend)
		// stats.DumpStatsUint64(filepath.Join(stats.LogDir, stats.ExprName, "SendMsgTimestamps.data"), stats.SendMsgTimestamps)
		stats.DumpStatsUint64(filepath.Join(stats.LogDir, stats.ExprName, "MsgTimestamps.data"), stats.MsgTimestamps)
		stats.DumpStatsInt8(filepath.Join(stats.LogDir, stats.ExprName, "SendMsgType.data"), stats.SendMsgType)
		stats.DumpStatsInt8(filepath.Join(stats.LogDir, stats.ExprName, "MsgType.data"), stats.MsgType)
		stats.DumpStatsBool(filepath.Join(stats.LogDir, stats.ExprName, "IsSend.data"), stats.IsSendList)
	}
	fmt.Printf("Done!\n")
}

func (stats *Statistics) DumpStatus(now time.Time) {
	file, err := os.OpenFile(filepath.Join(stats.LogDir, stats.ExprName, "status.data"), os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	status := fmt.Sprintf(
		"time: %v, MsgProcessed: %v, SendMsgProcessedBeforeRange: %v, SendMsgProcessed: %v, SendMsgInRangeProcessed: %v, The Msg Range: %v - %v, HackStart: %v\n",
		now.Sub(stats.StartTime), stats.MsgProcessed, stats.SendMsgProcessedBeforeRange, stats.SendMsgProcessed, stats.SendMsgInRangeProcessed,
		stats.StartMsgIdx, stats.LastMsgIdx, stats.HackStart,
	)
	file.WriteString(status)
	file.Close()
}

func (stats *Statistics) DumpConfig() {
	file, err := os.OpenFile(filepath.Join(stats.LogDir, stats.ExprName, "config"), os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	file.WriteString(fmt.Sprintf("%d\n%d\n%d\n%d\n%d\n", stats.StartMsgIdx, stats.LastMsgIdx, stats.HackStart, stats.HackEnd, stats.SendMsgProcessedBeforeRange))
	file.Close()
}

func (stats *Statistics) DumpStatsInt8(filename string, array []int8) {
	file, err := os.OpenFile(filename, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0644)
	writer := bufio.NewWriter(file)
	if err != nil {
		panic(err)
	}
	for _, t := range array {
		writer.WriteString(fmt.Sprintf("%d\n", t))
	}
	writer.Flush()
	file.Close()
}

func (stats *Statistics) DumpStatsBool(filename string, array []bool) {
	file, err := os.OpenFile(filename, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0644)
	writer := bufio.NewWriter(file)
	if err != nil {
		panic(err)
	}
	for _, t := range array {
		if t {
			writer.WriteString("1\n")
		} else {
			writer.WriteString("0\n")
		}
	}
	writer.Flush()
	file.Close()
}

func (stats *Statistics) DumpStatsInt64(filename string, array []int64) {
	file, err := os.OpenFile(filename, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0644)
	writer := bufio.NewWriter(file)
	if err != nil {
		panic(err)
	}
	for _, t := range array {
		writer.WriteString(fmt.Sprintf("%d\n", t))
	}
	writer.Flush()
	file.Close()
}

func (stats *Statistics) DumpStatsUint64(filename string, array []uint64) {
	file, err := os.OpenFile(filename, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0644)
	writer := bufio.NewWriter(file)
	if err != nil {
		panic(err)
	}
	for _, t := range array {
		writer.WriteString(fmt.Sprintf("%d\n", t))
	}
	writer.Flush()
	file.Close()
}

func (stats *Statistics) DumpStatsFloat64(filename string, array []float64) {
	file, err := os.OpenFile(filename, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0644)
	writer := bufio.NewWriter(file)
	if err != nil {
		panic(err)
	}
	for _, t := range array {
		writer.WriteString(fmt.Sprintf("%f\n", t))
	}
	writer.Flush()
	file.Close()
}
