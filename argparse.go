package main

import (
	"math"

	"github.com/akamensky/argparse"
)

var parser = argparse.NewParser("ProtoSim", "Protocol Simulatior")

// simulator setup
var App = parser.Selector("a", "app", []string{"etcd", "redis", "redis-no-reconfig", "zk"}, &argparse.Options{Required: true, Help: "Application name"})
var IdStr = parser.String("i", "id", &argparse.Options{Required: true, Help: "The node id for redis*; The id for socket file for etcd."})
var IdsStr = parser.String("", "ids", &argparse.Options{Required: false, Default: "1,2,3", Help: "All node ids in the cluster, split by ,"})
var DontProcessPtr = parser.Flag("d", "dontprocess", &argparse.Options{Required: false, Help: "If specified, we just provide a server to receive messages without processing messages."})
var LogDir = parser.String("l", "logdir", &argparse.Options{Required: false, Help: "The directory storing logs", Default: ""})
var NoParallelizeInterpreting = parser.Flag("", "no_pi", &argparse.Options{Required: false, Help: "Do not Parallelize Interpreting"})
var NumTimeslots = parser.Int("n", "numtimeslots", &argparse.Options{Required: false, Help: "Number of timeslots", Default: -1})
var MaxAhead = parser.Int("", "maxahead", &argparse.Options{Required: false, Help: "Max number of timeslots ahead", Default: math.MaxInt64})

// stats
var MergeMode = parser.Selector("m", "mergemode", []string{"nomerge", "exact", "functional"}, &argparse.Options{Help: "Merge mode: nomerge|exact|functional", Default: "functional"})
var AsapMode = parser.Selector("", "asapmode", []string{"on", "off"}, &argparse.Options{Help: "Asap mode: on|off", Default: "on"})
var HackStart = parser.Int("f", "from", &argparse.Options{Help: "starting disabling optimiztion when sendMsgCount reaches this", Default: 0})
var HackEnd = parser.Int("t", "to", &argparse.Options{Help: "end of optimization disabling tests", Default: 0})
var HackStartMode = parser.String("", "frommode", &argparse.Options{Required: false, Help: "default|nomerge|asap|<exprname>", Default: "default"})
var HackEndMode = parser.String("", "tomode", &argparse.Options{Required: false, Help: "default|nomerge|asap|<exprname>", Default: "default"})
var OnlyPerformance = parser.Flag("p", "onlyperf", &argparse.Options{Required: false, Help: "Only record performance statistics"})
var OnlyCheckTime = parser.Flag("", "onlychecktime", &argparse.Options{Required: false, Help: "Only record time of checking"})
var ExprName = parser.String("", "exprname", &argparse.Options{Required: false, Default: "default", Help: "Experiment name. LogDir/ExprName will be the directory to store output stats."})
var StatsEnabled = parser.Flag("s", "stats", &argparse.Options{Required: false, Help: "Enable statistics"})

// explore tree
var ExploreTreeEnabled = parser.Flag("z", "exploretree", &argparse.Options{Required: false, Help: "Enable recording explore tree"})
