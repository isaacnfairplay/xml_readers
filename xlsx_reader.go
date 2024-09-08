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

// Struct to hold cell data information
type CellData struct {
	SheetName    string `json:"sheet_name"`
	RowNumber    int32  `json:"row_number"`
	ColumnNumber int32  `json:"column_number"`
	SheetValue   string `json:"sheet_value"`
	Merged       bool   `json:"merged,omitempty"`
	MergedRange  string `json:"merged_range,omitempty"`
}

// MergedCell represents a merged cell range
type MergedCell struct {
	StartRow    int32
	StartColumn int32
	EndRow      int32
	EndColumn   int32
}

// Cell represents a single cell in a sheet
type Cell struct {
	R string `xml:"r,attr"` // Reference (e.g., "A1")
	T string `xml:"t,attr"` // Type (e.g., "s" for shared string, "n" for number)
	V string `xml:"v"`      // Value (content of the cell)
}

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

// SharedStrings represents shared strings in the workbook
type SharedStrings struct {
	Items []string `xml:"si>t"`
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
					if err == io.EOF {
						break
					}
					return nil, err
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

// ReadSharedStrings extracts shared strings from an XLSX file.
func ReadSharedStrings(zipReader *zip.ReadCloser) (*SharedStrings, error) {
	for _, file := range zipReader.File {
		if file.Name == "xl/sharedStrings.xml" {
			f, err := file.Open()
			if err != nil {
				return nil, err
			}
			defer f.Close()

			bufferedReader := bufio.NewReaderSize(f, 64*1024) // Buffer for performance
			decoder := xml.NewDecoder(bufferedReader)

			var sharedStrings SharedStrings
			for {
				t, err := decoder.Token()
				if err != nil {
					if err == io.EOF {
						break
					}
					return nil, err
				}
				switch se := t.(type) {
				case xml.StartElement:
					if se.Name.Local == "si" {
						var text struct {
							T string `xml:"t"`
						}
						if err := decoder.DecodeElement(&text, &se); err == nil {
							sharedStrings.Items = append(sharedStrings.Items, text.T)
						}
					}
				}
			}
			return &sharedStrings, nil
		}
	}
	return nil, fmt.Errorf("shared strings file not found")
}

// ReadSheetData extracts sheet data from an XLSX file for a specific sheet file.
func ReadSheetData(zipReader *zip.ReadCloser, fileName string, sharedStrings *SharedStrings) ([]CellData, error) {
	for _, file := range zipReader.File {
		if file.Name == fileName {
			f, err := file.Open()
			if err != nil {
				return nil, err
			}
			defer f.Close()

			// Buffered reader for performance
			decoder := xml.NewDecoder(bufio.NewReaderSize(f, 128*1024))
			var cellData []CellData
			var currentRow int32

			for {
				t, err := decoder.Token()
				if err != nil {
					if err == io.EOF {
						break
					}
					return nil, err
				}

				switch se := t.(type) {
				case xml.StartElement:
					if se.Name.Local == "row" {
						for _, attr := range se.Attr {
							if attr.Name.Local == "r" {
								currentRow, _ = Atoi32(attr.Value)
							}
						}
					}

					if se.Name.Local == "c" {
						var cell Cell
						decoder.DecodeElement(&cell, &se)

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
			return cellData, nil
		}
	}
	return nil, fmt.Errorf("sheet file not found")
}

// ReadMergedCells extracts merged cell ranges from an XLSX file for a specific sheet.
func ReadMergedCells(zipReader *zip.ReadCloser, fileName string) ([]MergedCell, error) {
	for _, file := range zipReader.File {
		if file.Name == fileName {
			f, err := file.Open()
			if err != nil {
				return nil, err
			}
			defer f.Close()

			var mergeCells struct {
				Cells []struct {
					Ref string `xml:"ref,attr"`
				} `xml:"mergeCells>mergeCell"`
			}
			decoder := xml.NewDecoder(f)
			if err := decoder.Decode(&mergeCells); err != nil {
				return nil, err
			}

			var mergedCells []MergedCell
			for _, cell := range mergeCells.Cells {
				startCol, startRow, endCol, endRow := parseMergedCellRange(cell.Ref)
				mergedCells = append(mergedCells, MergedCell{
					StartRow:    startRow,
					StartColumn: startCol,
					EndRow:      endRow,
					EndColumn:   endCol,
				})
			}
			return mergedCells, nil
		}
	}
	return nil, nil
}

// Utility: Parse cell reference like "A1" and return column and row as int32
func parseCellReference(ref string) (int32, int32) {
	col := int32(0)
	row := int32(0)
	for i := 0; i < len(ref); i++ {
		if ref[i] >= 'A' && ref[i] <= 'Z' {
			col = int32(col*26 + int32(ref[i]-'A'+1))
		} else {
			rowPart, _ := strconv.Atoi(ref[i:])
			row = int32(rowPart)
			break
		}
	}
	return col, row
}

// Utility: Parse merged cell range like "A1:B2" and return start and end rows/columns
func parseMergedCellRange(ref string) (int32, int32, int32, int32) {
	parts := strings.Split(ref, ":")
	if len(parts) == 2 {
		startCol, startRow := parseCellReference(parts[0])
		endCol, endRow := parseCellReference(parts[1])
		return startCol, startRow, endCol, endRow
	}
	return 0, 0, 0, 0
}

// Utility: Create cell reference from column and row (e.g., 1, 1 becomes "A1")
func cellReferenceFromCoordinates(col int32, row int32) string {
	colRef := ""
	for col > 0 {
		colRef = string('A'+(col-1)%26) + colRef
		col = (col - 1) / 26
	}
	return fmt.Sprintf("%s%d", colRef, row)
}

// Utility: Get cell value, handles shared strings
func getCellValue(cell Cell, sharedStrings *SharedStrings) string {
	if cell.T == "s" { // Shared string
		if idx, err := strconv.Atoi(cell.V); err == nil && idx < len(sharedStrings.Items) {
			return sharedStrings.Items[idx]
		}
	}
	return cell.V // Numeric or other type
}

// Utility: Convert string to int32
func Atoi32(s string) (int32, error) {
	i, err := strconv.Atoi(s)
	return int32(i), err
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
		if err := pprof.StartCPUProfile(cpuFile); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
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
		runtime.GC() // get up-to-date statistics
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

			// Map merged cell ranges for quick lookup
			mergedMap := make(map[string]string)
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
