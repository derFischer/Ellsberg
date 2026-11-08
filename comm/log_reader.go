/*
 * Assumtions:
 *   1. directory structure:
 *      /mydata/20000101-000000/
 *      	  tag1.data
 *				  tag1.meta
 *      	  tag2.data
 *				  tag2.meta
 *      /mydata/20000101-000100/
 *      	  tag1.data
 *				  tag1.meta
 *      	  tag2.data
 *				  tag2.meta
 *      ...
 *   2. Whenever we see a new directory, we assume no files
 *      will be added to the older directories. Thus, we
 *      can safely consume files in the older dir.
 *   3. We assume the app-end logger should log messages
 *      into the directory corresponding to the correct
 *      timeslot. This can be guaranteed because the logger
 *      is fast enough
 *   4. Two circumstance that the sendQueue won't be got by
 *      writer: 1. the master thread died at half of
 *      timeslot, 2.the master thread didn't get any more
 *      messages in the timeslot. For 1, we will just flush
 *      everything including current sendQueue when shutdown.
 *      For 2, let the writer notify master for timeslot ending
 *
 */
package comm

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.nyu.edu/protosim/statistics"
)

const TimestampPeekBegin = 4 // The first 4 bytes for SimConnMessageType

type SingleFileLogReader struct {
	FilePath string
	File     *os.File
	Reader   *bufio.Reader

	NextUnusedTimestamp int64
	Done                bool
	StartMsgIdx         int
	LastMsgIdx          int
}

func (sflr *SingleFileLogReader) init(filePath string) {
	var err error
	sflr.FilePath = filePath
	sflr.File, err = os.OpenFile(sflr.FilePath, os.O_RDONLY, 0)
	if err != nil {
		panic(err)
	}
	sflr.Reader = bufio.NewReader(sflr.File)
	sflr.NextUnusedTimestamp = 0
}

func (sflr *SingleFileLogReader) nextTimestamp() int64 {
	if sflr.NextUnusedTimestamp != 0 {
		return sflr.NextUnusedTimestamp
	} else {
		peekBytes, err := sflr.Reader.Peek(TimestampPeekBegin + 8)
		if err == io.EOF {
			return math.MaxInt64
		} else if err != nil {
			panic(err)
		}
		peekBytes = peekBytes[TimestampPeekBegin:]
		timestamp := int64(binary.BigEndian.Uint64(peekBytes))
		sflr.NextUnusedTimestamp = timestamp
		return timestamp
	}
}

func (sflr *SingleFileLogReader) useNext() *bufio.Reader {
	if sflr.NextUnusedTimestamp == 0 {
		panic("LastTimestamp is 0")
	}
	sflr.NextUnusedTimestamp = 0
	return sflr.Reader
}

type LogReader struct {
	LogDir              string
	SingleFileLogReader []*SingleFileLogReader

	CurrentDirPath string
	CurrentDirName string

	DirCounter                    int
	DirStartTime                  time.Time
	CurrentTimeslotNumMessage     int
	CurrentTimeslotNumSendMessage int
	CurrentTimeslotNumRecvMessage int

	NumTimeslots int
}

func (lr *LogReader) init(LogDir string) {
	lr.LogDir = LogDir
	lr.useNextDir()
}

func (lr *LogReader) useNextDir() {
	if lr.DirCounter > 0 {
		fmt.Printf("Last timeslot (%v) processed %v(%d+%d) took %v, %v\n",
			filepath.Base(lr.CurrentDirName), lr.CurrentTimeslotNumMessage, lr.CurrentTimeslotNumSendMessage, lr.CurrentTimeslotNumRecvMessage,
			time.Since(lr.DirStartTime), statistics.Stats.MsgReaded,
		)
	}
	lr.SingleFileLogReader = []*SingleFileLogReader{}
	var err error
	dirToUse := ""
	for dirToUse == "" {
		dirEntries, err := os.ReadDir(lr.LogDir)
		if err != nil {
			panic(err)
		}
		filteredPaths := []string{}
		for _, dirEntry := range dirEntries {
			path := dirEntry.Name()
			if len(path) == 15 && path[8] == '-' && strings.Compare(path, lr.CurrentDirName) == 1 {
				filteredPaths = append(filteredPaths, path)
			}
		}
		if len(filteredPaths) > 1 {
			dirToUse = filteredPaths[0]
			break
		} else {
			time.Sleep(3 * time.Second)
		}
	}

	lr.CurrentDirName = dirToUse
	lr.CurrentDirPath = filepath.Join(lr.LogDir, dirToUse)
	if err != nil {
		panic(err)
	}
	fileEntries, err := os.ReadDir(lr.CurrentDirPath)
	if err != nil {
		panic(err)
	}

	StartMsgIdx := 0
	LastMsgIdx := 0
	for _, fileEntry := range fileEntries {
		if strings.HasSuffix(fileEntry.Name(), "meta") {
			continue
		}
		sflr := new(SingleFileLogReader)
		sflr.init(filepath.Join(lr.CurrentDirPath, fileEntry.Name()))
		lr.SingleFileLogReader = append(lr.SingleFileLogReader, sflr)
		metaName := string(fileEntry.Name()[:len(fileEntry.Name())-5]) + ".meta"
		f, err := os.OpenFile(filepath.Join(lr.CurrentDirPath, metaName), os.O_RDONLY, 0)
		if err != nil {
			panic(err)
		}
		fmt.Fscanf(f, "%d\n%d\n", &sflr.StartMsgIdx, &sflr.LastMsgIdx)
		f.Close()
		StartMsgIdx += sflr.StartMsgIdx
		LastMsgIdx += sflr.LastMsgIdx
	}

	if lr.DirCounter == lr.NumTimeslots-1 {
		// Start using last timeslot
		fmt.Printf("Reached numTimeslots limit (%d), setting LastMsgIdx to %d\n", lr.NumTimeslots, LastMsgIdx)
		statistics.Stats.LastMsgIdx = LastMsgIdx
	}

	fmt.Printf("Using timeslot: %+v (%d - %d)\n", lr.CurrentDirName, StartMsgIdx, LastMsgIdx)
	lr.CurrentTimeslotNumMessage = 0
	lr.CurrentTimeslotNumSendMessage = 0
	lr.CurrentTimeslotNumRecvMessage = 0
	lr.DirCounter++
	lr.DirStartTime = time.Now()
}

func (lr *LogReader) NextSimConnMessageReader() *bufio.Reader {
	var minTs int64 = math.MaxInt64
	var minReader *SingleFileLogReader = nil
	for minReader == nil {
		for _, reader := range lr.SingleFileLogReader {
			if reader.Done {
				continue
			}
			ts := reader.nextTimestamp()
			if ts < minTs {
				minTs = ts
				minReader = reader
			}
		}
		if minReader == nil {
			lr.useNextDir()
		} else {
			break
		}
	}
	return minReader.useNext()
}
