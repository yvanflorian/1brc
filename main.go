package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"time"
)

// main
func main() {
	profCmd := flag.String("profile", "", "Run CPU Profiling")
	flag.Parse()

	if *profCmd != "" {
		cpuFile, err := os.Create(*profCmd)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(cpuFile)
		defer pprof.StopCPUProfile()
	}

	start := time.Now()

	// It2BulkProcess("1B.txt")
	// It1SimpleImplementation("1M.txt")
	// It3Concurrent("1B.txt")
	It4ReadChunks("1B.txt")

	fmt.Println("")
	fmt.Println("")
	log.Println("Elapsed Seconds:", time.Since(start).Seconds())
}
