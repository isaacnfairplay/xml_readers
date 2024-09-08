package main

import (
	"archive/zip"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
)

// Profiling setup and teardown
func setupProfiling(cpuProfile, memProfile string) (*os.File, *os.File) {
	var cpuFile, memFile *os.File
	if cpuProfile != "" {
		var err error
		cpuFile, err = os.Create(cpuProfile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		pprof.StartCPUProfile(cpuFile)
	}
	if memProfile != "" {
		var err error
		memFile, err = os.Create(memProfile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
	}
	return cpuFile, memFile
}

func stopProfiling(cpuFile, memFile *os.File) {
	if cpuFile != nil {
		pprof.StopCPUProfile()
		cpuFile.Close()
	}
	if memFile != nil {
		runtime.GC()
		pprof.WriteHeapProfile(memFile)
		memFile.Close()
	}
}

func main() {
	// Parse command-line arguments
	cpuProfile := flag.String("cpuprofile", "", "write CPU profile to `file`")
	memProfile := flag.String("memprofile", "", "write memory profile to `file`")
	flag.Parse()

	if flag.NArg() < 2 {
		fmt.Println("Usage: go run main.go <xlsx_file> <targetFile>")
		return
	}
	fileName := flag.Arg(0)
	targetPath := flag.Arg(1)

	// Profiling setup
	cpuFile, memFile := setupProfiling(*cpuProfile, *memProfile)
	defer stopProfiling(cpuFile, memFile)

	// Open the XLSX file
	r, err := zip.OpenReader(fileName)
	if err != nil {
		fmt.Println("Failed to open file:", err)
		return
	}
	defer r.Close()

	// Read the workbook and shared strings
	workbook, err := ReadWorkbook(r)
	if err != nil {
		fmt.Println("Failed to read workbook:", err)
		return
	}

	sharedStrings, err := ReadSharedStrings(r)
	if err != nil {
		fmt.Println("Failed to read shared strings:", err)
		return
	}

	// Process sheets concurrently
	var data []CellData
	var wg sync.WaitGroup
	processSheetsConcurrently(r, workbook, sharedStrings, &data, &wg)

	// Determine output format and write data
	outputFormat := strings.Split(filepath.Base(targetPath), ".")[1]
	switch outputFormat {
	case "csv":
		writeCSV(data, targetPath)
	case "json":
		writeJSON(data, targetPath)
	case "parquet":
		writeParquet(data, targetPath)
	default:
		fmt.Println("Unknown output format. Use 'csv', 'json', or 'parquet'.")
	}
}
