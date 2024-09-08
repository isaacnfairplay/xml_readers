package main

import (
	"archive/zip"
	"bufio"
	"encoding/csv"
	"encoding/json"
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

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

// Struct to hold cell data information
type CellData struct {
	SheetName    string `json:"sheet_name"`
	RowNumber    int32  `json:"row_number"`    // Optimized to int32 for smaller storage
	ColumnNumber int32  `json:"column_number"` // Optimized to int32
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

// SheetData represents a sheet's data structure
type SheetData struct {
	Row []struct {
		R int32  `xml:"r,attr"`
		C []Cell `xml:"c"`
	} `xml:"sheetData>row"`
}

// MergeCells represents the merged cell ranges in a sheet
type MergeCells struct {
	Cells []struct {
		Ref string `xml:"ref,attr"`
	} `xml:"mergeCells>mergeCell"`
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

			// Use a larger buffer for reading the file
			bufferedReader := bufio.NewReaderSize(f, 64*1024)

			// Parse only the relevant portions of the XML
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

// Optimized ReadSheetData function
func ReadSheetData(zipReader *zip.ReadCloser, fileName string, sharedStrings *SharedStrings) ([]CellData, error) {
	for _, file := range zipReader.File {
		if file.Name == fileName {
			f, err := file.Open()
			if err != nil {
				return nil, err
			}
			defer f.Close()

			// Buffer size can be increased depending on file size, but 64KB is a good start
			bufferedReader := bufio.NewReaderSize(f, 64*1024)
			decoder := xml.NewDecoder(bufferedReader)

			var cellData []CellData
			var currentSheet string // Set the correct sheet name externally
			var currentRow int32

			for {
				// Get the next XML token
				t, err := decoder.Token()
				if err != nil {
					if err == io.EOF {
						break
					}
					return nil, err
				}

				switch se := t.(type) {
				case xml.StartElement:
					// Handle <row> elements
					if se.Name.Local == "row" {
						for _, attr := range se.Attr {
							if attr.Name.Local == "r" {
								// Parse row number and convert to int32
								rowNum, parseErr := strconv.Atoi(attr.Value)
								if parseErr == nil {
									currentRow = int32(rowNum) // Use currentRow for the row level
								}
							}
						}
					}

					// Handle <c> elements (cells)
					if se.Name.Local == "c" {
						var cell Cell
						// Decode the cell element
						if err := decoder.DecodeElement(&cell, &se); err != nil {
							return nil, err
						}

						// Parse the cell reference (like "A1")
						column, _ := parseCellReference(cell.R)

						// Determine the value of the cell
						var value string
						if cell.T == "s" { // Shared string
							idx, parseErr := strconv.Atoi(cell.V)
							if parseErr == nil && idx < len(sharedStrings.Items) {
								value = sharedStrings.Items[idx]
							}
						} else { // Numeric or other type
							value = cell.V
						}

						// Collect the cell data with currentRow now actively used
						cellData = append(cellData, CellData{
							SheetName:    currentSheet,
							RowNumber:    currentRow, // Use the currentRow value
							ColumnNumber: column,
							SheetValue:   value,
						})
					}

				case xml.EndElement:
					// Reset row data at the end of each <row> element
					if se.Name.Local == "row" {
						currentRow = 0
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

			var mergeCells MergeCells
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

// ReadSharedStrings extracts shared strings from an XLSX file.
func ReadSharedStrings(zipReader *zip.ReadCloser) (*SharedStrings, error) {
	for _, file := range zipReader.File {
		if file.Name == "xl/sharedStrings.xml" {
			f, err := file.Open()
			if err != nil {
				return nil, err
			}
			defer f.Close()

			bufferedReader := bufio.NewReaderSize(f, 64*1024)
			decoder := xml.NewDecoder(bufferedReader)

			var sharedStrings SharedStrings
			for {
				t, err := decoder.Token()
				if err != nil {
					break
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

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	if *memProfile != "" {
		f, err := os.Create(*memProfile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date memory stats
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}

	nameparts := strings.Split(filepath.Base(targetPath), ".")
	if len(nameparts) < 2 {
		fmt.Println("targetPath must have a file extension")
	}
	outputFormat := nameparts[1]

	// Open the Excel file
	r, err := zip.OpenReader(fileName)
	if err != nil {
		fmt.Println("Failed to open file:", err)
		return
	}
	defer r.Close()

	// Read the workbook (sheet metadata)
	workbook, err := ReadWorkbook(r) // Pass r instead of &r
	if err != nil {
		fmt.Println("Failed to read workbook:", err)
		return
	}

	// Read shared strings
	sharedStrings, err := ReadSharedStrings(r) // Pass r instead of &r
	if err != nil {
		fmt.Println("Failed to read shared strings:", err)
		return
	}

	var data []CellData

	// Process each sheet
	for _, sheet := range workbook.Sheets.Sheet {
		sheetFile := fmt.Sprintf("xl/worksheets/sheet%s.xml", sheet.ID)

		// Read sheet data
		sheetData, err := ReadSheetData(r, sheetFile, sharedStrings) // Pass sharedStrings to help with cell decoding
		if err != nil {
			fmt.Printf("Failed to read data for sheet %s: %v\n", sheet.Name, err)
			continue
		}

		// Read merged cells data
		mergedCells, err := ReadMergedCells(r, sheetFile) // Pass r instead of &r
		if err != nil {
			fmt.Printf("Failed to read merged cells for sheet %s: %v\n", sheet.Name, err)
			continue
		}

		// Map merged cell ranges for quick lookup
		mergedMap := map[string]string{}
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

		// Iterate through the rows and cells to collect the data
		for _, cell := range sheetData {
			// Populate cell data and check for merged ranges
			cellData := CellData{
				SheetName:    sheet.Name,
				RowNumber:    cell.RowNumber,
				ColumnNumber: cell.ColumnNumber,
				SheetValue:   cell.SheetValue,
			}

			// Check if the cell is part of a merged range
			if mergedRange, ok := mergedMap[cellReferenceFromCoordinates(cell.ColumnNumber, cell.RowNumber)]; ok {
				cellData.Merged = true
				cellData.MergedRange = mergedRange
			}

			data = append(data, cellData)
		}
	}

	// Choose output format
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

// writeCSV outputs the data in CSV format to the specified targetPath
func writeCSV(data []CellData, targetPath string) {
	file, err := os.Create(targetPath)
	if err != nil {
		fmt.Println("Error creating CSV file:", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write the header
	writer.Write([]string{"SheetName", "RowNumber", "ColumnNumber", "SheetValue", "Merged", "MergedRange"})

	// Write the data
	for _, d := range data {
		writer.Write([]string{d.SheetName, strconv.Itoa(int(d.RowNumber)), strconv.Itoa(int(d.ColumnNumber)), d.SheetValue, strconv.FormatBool(d.Merged), d.MergedRange})
	}
	fmt.Println("CSV output written to", targetPath)
}

// writeJSON outputs the data in JSON format to the specified targetPath
func writeJSON(data []CellData, targetPath string) {
	file, err := os.Create(targetPath)
	if err != nil {
		fmt.Println("Error creating JSON file:", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	err = encoder.Encode(data)
	if err != nil {
		fmt.Println("Error encoding JSON:", err)
		return
	}
	fmt.Println("JSON output written to", targetPath)
}

// writeParquet outputs the data in Parquet format using parquet-go library
func writeParquet(data []CellData, targetPath string) error {
	// Create the target file
	file, err := os.Create(targetPath)
	if err != nil {
		return fmt.Errorf("error creating Parquet file: %w", err)
	}
	defer file.Close()

	// Create a new ZSTD codec instance with strong compression
	zstdCodec := &zstd.Codec{
		Level:       zstd.SpeedBestCompression, // Set to best compression level
		Concurrency: 4,                         // Number of cores to use for encoding
	}

	// Define the Parquet writer with strong ZSTD compression, dictionary encoding, and row group size
	writer := parquet.NewGenericWriter[CellData](file,
		parquet.Compression(zstdCodec),            // Use the ZSTD codec with strong compression
		parquet.MaxRowsPerRowGroup(128*1024*1024), // Reduce row group size to 8 MB for better compression
	)
	defer writer.Close()

	// Write data to the Parquet file
	if _, err := writer.Write(data); err != nil {
		return fmt.Errorf("error writing data to Parquet file: %w", err)
	}

	// Ensure the writer is properly closed (flushes buffers and writes the footer)
	if err := writer.Close(); err != nil {
		return fmt.Errorf("error closing Parquet writer: %w", err)
	}

	fmt.Println("Parquet output written to", targetPath)
	return nil
}

// parseCellReference takes a cell reference like "A1" and returns the column and row numbers.
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

// parseMergedCellRange takes a reference like "A1:B2" and returns start and end row/column.
func parseMergedCellRange(ref string) (int32, int32, int32, int32) {
	parts := strings.Split(ref, ":")
	if len(parts) == 2 {
		startCol, startRow := parseCellReference(parts[0])
		endCol, endRow := parseCellReference(parts[1])
		return startCol, startRow, endCol, endRow
	}
	return 0, 0, 0, 0
}

// cellReferenceFromCoordinates creates a cell reference from column and row numbers (e.g., A1).
func cellReferenceFromCoordinates(col int32, row int32) string {
	colRef := ""
	for col > 0 {
		colRef = string('A'+(col-1)%26) + colRef
		col = (col - 1) / 26
	}
	return fmt.Sprintf("%s%d", colRef, row)
}
