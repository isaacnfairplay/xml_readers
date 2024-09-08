package main

import (
	"archive/zip"
	"bufio"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
)

// Struct to hold cell data information
type CellData struct {
	SheetName    string `json:"sheet_name"`
	RowNumber    int32  `json:"row_number"`
	ColumnNumber int32  `json:"column_number"`
	SheetValue   string `json:"sheet_value"`
	Merged       bool   `json:"merged,omitempty"`
	MergedRange  string `json:"merged_range,omitempty"`
}

// BufferedXMLDecoder wraps the xml.Decoder and adds buffered byte reading
type BufferedXMLDecoder struct {
	decoder *xml.Decoder
	buf     []byte // The buffer to hold pre-read data
	pos     int    // Current reading position in the buffer
	size    int    // The size of the buffered data
}

// Initialize the buffered decoder
func NewBufferedXMLDecoder(r io.Reader) *BufferedXMLDecoder {
	decoder := xml.NewDecoder(bufio.NewReaderSize(r, 128*1024)) // Using a larger buffer size
	return &BufferedXMLDecoder{
		decoder: decoder,
		buf:     make([]byte, 4096), // Buffer 4 KB at a time
		pos:     0,
		size:    0,
	}
}

// Read the next byte from the buffer, refilling it when necessary
func (bxd *BufferedXMLDecoder) getc() (byte, bool) {
	if bxd.pos >= bxd.size {
		n, err := bxd.decoder.InputOffset()
		if err != nil || n == 0 {
			return 0, false
		}
		bxd.size = n
		bxd.pos = 0
	}
	char := bxd.buf[bxd.pos]
	bxd.pos++
	return char, true
}

// ReadWorkbook extracts the list of sheets from the workbook.xml file.
func ReadWorkbook(zipReader *zip.ReadCloser) (*Workbook, error) {
	for _, file := range zipReader.File {
		if file.Name == "xl/workbook.xml" {
			f, err := file.Open()
			if err != nil {
				return nil, err
			}
			defer f.Close()

			bufferedReader := bufio.NewReaderSize(f, 128*1024) // Increased buffer size
			decoder := xml.NewDecoder(bufferedReader)
			var workbook Workbook
			inSheets := false

			for {
				t, err := decoder.Token()
				if err != nil {
					break
				}
				switch se := t.(type) {
				case xml.StartElement:
					if se.Name.Local == "sheets" {
						inSheets = true
					} else if se.Name.Local == "sheet" && inSheets {
						var sheet struct {
							Name string `xml:"name,attr"`
							ID   string `xml:"sheetId,attr"`
							RID  string `xml:"r:id,attr"`
						}
						if err := decoder.DecodeElement(&sheet, &se); err == nil {
							workbook.Sheets.Sheet = append(workbook.Sheets.Sheet, sheet)
						}
					}
				case xml.EndElement:
					if se.Name.Local == "sheets" {
						inSheets = false
					}
				}
			}
			return &workbook, nil
		}
	}
	return nil, fmt.Errorf("workbook.xml not found")
}

// Streamlined ReadSheetData function with buffered XML reader
func ReadSheetData(zipReader *zip.ReadCloser, fileName string, sharedStrings *SharedStrings) ([]CellData, error) {
	var cellData []CellData

	for _, file := range zipReader.File {
		if file.Name != fileName {
			continue
		}

		f, err := file.Open()
		if err != nil {
			return nil, err
		}
		defer f.Close()

		// Using the buffered XML decoder for performance optimization
		bxd := NewBufferedXMLDecoder(f)
		var currentRow int32

		for {
			t, err := bxd.decoder.Token()
			if err != nil {
				if err == io.EOF {
					break
				}
				return nil, err
			}

			switch se := t.(type) {
			case xml.StartElement:
				switch se.Name.Local {
				case "row":
					for _, attr := range se.Attr {
						if attr.Name.Local == "r" {
							currentRow, _ = Atoi32(attr.Value)
						}
					}
				case "c":
					var cell Cell
					bxd.decoder.DecodeElement(&cell, &se)

					column, _ := parseCellReference(cell.R)
					value := getCellValue(cell, sharedStrings)

					cellData = append(cellData, CellData{
						RowNumber:    currentRow,
						ColumnNumber: column,
						SheetValue:   value,
					})
				}
			}
		}
	}
	return cellData, nil
}

// Pre-allocate space for merged cell ranges and map them efficiently
func PreAllocateMergedCells(sheetData []CellData, mergedCells []MergedCell) map[string]string {
	mergedMap := make(map[string]string, len(mergedCells)) // Pre-allocated based on the number of merged cells
	for _, mc := range mergedCells {
		startCell := cellReferenceFromCoordinates(mc.StartColumn, mc.StartRow)
		endCell := cellReferenceFromCoordinates(mc.EndColumn, mc.EndRow)
		mergedRange := fmt.Sprintf("%s:%s", startCell, endCell)

		for col := mc.StartColumn; col <= mc.EndColumn; col++ {
			for row := mc.StartRow; row <= mc.EndRow; row++ {
				cellRef := cellReferenceFromCoordinates(col, row)
				mergedMap[cellRef] = mergedRange
			}
		}
	}
	return mergedMap
}

// Helper function to parse integer from string to int32
func Atoi32(s string) (int32, error) {
	i, err := strconv.Atoi(s)
	return int32(i), err
}

// Helper function to get the cell value, supports shared strings
func getCellValue(cell Cell, sharedStrings *SharedStrings) string {
	if cell.T == "s" {
		if idx, err := strconv.Atoi(cell.V); err == nil && idx < len(sharedStrings.Items) {
			return sharedStrings.Items[idx]
		}
	}
	return cell.V
}

// Profiling setup and teardown
func setupProfiling(cpuProfile string, memProfile string) (*os.File, *os.File) {
	var cpuFile, memFile *os.File
	var err error

	if cpuProfile != "" {
		cpuFile, err = os.Create(cpuProfile)
		if err != nil {
			fmt.Println("Failed to create CPU profile file:", err)
		}
		if err := pprof.StartCPUProfile(cpuFile); err != nil {
			fmt.Println("Failed to start CPU profile:", err)
		}
	}

	if memProfile != "" {
		memFile, err = os.Create(memProfile)
		if err != nil {
			fmt.Println("Failed to create memory profile file:", err)
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
		runtime.GC() // Get up-to-date statistics for the memory profile
		if err := pprof.WriteHeapProfile(memFile); err != nil {
			fmt.Println("Failed to write memory profile:", err)
		}
		memFile.Close()
	}
}

// Main Program Logic

func main() {
	// Command-line argument parsing
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

	// Open the XLSX file (ZIP format)
	r, err := zip.OpenReader(fileName)
	if err != nil {
		fmt.Println("Failed to open file:", err)
		return
	}
	defer r.Close()

	// Read the workbook (sheet metadata)
	workbook, err := ReadWorkbook(r)
	if err != nil {
		fmt.Println("Failed to read workbook:", err)
		return
	}

	// Read shared strings
	sharedStrings, err := ReadSharedStrings(r)
	if err != nil {
		fmt.Println("Failed to read shared strings:", err)
		return
	}

	var data []CellData
	var wg sync.WaitGroup

	// Process each sheet concurrently
	for _, sheet := range workbook.Sheets.Sheet {
		wg.Add(1)
		go func(sheetName, sheetID string) {
			defer wg.Done()

			sheetFile := fmt.Sprintf("xl/worksheets/sheet%s.xml", sheetID)
			sheetData, err := ReadSheetData(r, sheetFile, sharedStrings)
			if err != nil {
				fmt.Printf("Failed to read data for sheet %s: %v\n", sheetName, err)
				return
			}

			mergedCells, err := ReadMergedCells(r, sheetFile)
			if err != nil {
				fmt.Printf("Failed to read merged cells for sheet %s: %v\n", sheetName, err)
				return
			}

			mergedMap := PreAllocateMergedCells(sheetData, mergedCells)

			for _, cell := range sheetData {
				cellData := CellData{
					SheetName:    sheetName,
					RowNumber:    cell.RowNumber,
					ColumnNumber: cell.ColumnNumber,
					SheetValue:   cell.SheetValue,
				}

				if mergedRange, ok := mergedMap[cellReferenceFromCoordinates(cell.ColumnNumber, cell.RowNumber)]; ok {
					cellData.Merged = true
					cellData.MergedRange = mergedRange
				}

				data = append(data, cellData)
			}
		}(sheet.Name, sheet.ID)
	}
	wg.Wait()

	// Output format determination and writing
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
