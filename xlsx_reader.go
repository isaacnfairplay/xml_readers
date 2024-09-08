package main

import (
	"archive/zip"
	"bufio"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
)

// Define Workbook, Cell, and SharedStrings

// Workbook represents the workbook.xml structure, containing sheet names
type Workbook struct {
	Sheets struct {
		Sheet []struct {
			Name string `xml:"name,attr"`
			ID   string `xml:"sheetId,attr"`
			RID  string `xml:"r:id,attr"`
		} `xml:"sheet"`
	} `xml:"sheets"`
}

// Cell represents a single cell in a sheet
type Cell struct {
	R string `xml:"r,attr"` // Reference (e.g., "A1")
	T string `xml:"t,attr"` // Type (e.g., "s" for shared string, "n" for number)
	V string `xml:"v"`      // Value (content of the cell)
}

// SharedStrings represents shared strings in the workbook
type SharedStrings struct {
	Items []string `xml:"si>t"`
}

// Struct to hold cell data information
type CellData struct {
	SheetName    string `json:"sheet_name"`
	RowNumber    int32  `json:"row_number"`
	ColumnNumber int32  `json:"column_number"`
	SheetValue   string `json:"sheet_value"`
	Merged       bool   `json:"merged,omitempty"`
	MergedRange  string `json:"merged_range,omitempty"`
}

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

// Generalized XML reading helper
func readXMLFromZip(zipReader *zip.ReadCloser, filePath string, data interface{}) error {
	for _, file := range zipReader.File {
		if file.Name == filePath {
			f, err := file.Open()
			if err != nil {
				return err
			}
			defer f.Close()
			decoder := xml.NewDecoder(bufio.NewReaderSize(f, 128*1024))
			return decoder.Decode(data)
		}
	}
	return fmt.Errorf("%s not found", filePath)
}

// Read the workbook structure
func ReadWorkbook(zipReader *zip.ReadCloser) (*Workbook, error) {
	var workbook Workbook
	err := readXMLFromZip(zipReader, "xl/workbook.xml", &workbook)
	return &workbook, err
}

// Read shared strings from sharedStrings.xml
func ReadSharedStrings(zipReader *zip.ReadCloser) (*SharedStrings, error) {
	var sharedStrings SharedStrings
	err := readXMLFromZip(zipReader, "xl/sharedStrings.xml", &sharedStrings)
	return &sharedStrings, err
}

// Read sheet data and return parsed cell data
func ReadSheetData(zipReader *zip.ReadCloser, fileName string, sharedStrings *SharedStrings) ([]CellData, error) {
	var cellData []CellData
	for _, file := range zipReader.File {
		if file.Name == fileName {
			f, err := file.Open()
			if err != nil {
				return nil, err
			}
			defer f.Close()

			decoder := xml.NewDecoder(bufio.NewReaderSize(f, 128*1024))
			var currentRow int32

			for {
				t, err := decoder.Token()
				if err != nil {
					if err == io.EOF {
						break
					}
					return nil, err
				}
				if startElem, ok := t.(xml.StartElement); ok {
					switch startElem.Name.Local {
					case "row":
						for _, attr := range startElem.Attr {
							if attr.Name.Local == "r" {
								rowInt, _ := strconv.ParseInt(attr.Value, 10, 32)
								currentRow = int32(rowInt)
							}
						}
					case "c":
						var cell Cell
						decoder.DecodeElement(&cell, &startElem)
						col, _ := parseCellReference(cell.R)
						val := getCellValue(cell, sharedStrings)
						cellData = append(cellData, CellData{
							RowNumber:    currentRow,
							ColumnNumber: col,
							SheetValue:   val,
						})
					}
				}
			}
			return cellData, nil
		}
	}
	return nil, fmt.Errorf("sheet %s not found", fileName)
}

// Utility: Get cell value, handles shared strings
func getCellValue(cell Cell, sharedStrings *SharedStrings) string {
	if cell.T == "s" {
		idx, _ := strconv.Atoi(cell.V)
		if idx < len(sharedStrings.Items) {
			return sharedStrings.Items[idx]
		}
	}
	return cell.V
}

// Concurrent sheet processing
func processSheetsConcurrently(zipReader *zip.ReadCloser, workbook *Workbook, sharedStrings *SharedStrings, data *[]CellData, wg *sync.WaitGroup) {
	for _, sheet := range workbook.Sheets.Sheet {
		wg.Add(1)
		go func(sheetName, sheetID string) {
			defer wg.Done()
			sheetFile := fmt.Sprintf("xl/worksheets/sheet%s.xml", sheetID)
			sheetData, err := ReadSheetData(zipReader, sheetFile, sharedStrings)
			if err != nil {
				fmt.Printf("Failed to read data for sheet %s: %v\n", sheetName, err)
				return
			}
			*data = append(*data, sheetData...)
		}(sheet.Name, sheet.ID)
	}
	wg.Wait()
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
