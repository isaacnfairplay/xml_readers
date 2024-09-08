package main

import (
	"archive/zip"
	"bufio"
	"encoding/xml"
	"fmt"
	"io"
	"strconv"
	"sync"
)

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

// parseCellReference takes a cell reference like "A1" and returns the column and row numbers.
func parseCellReference(ref string) (int32, int32) {
	var col int32 = 0
	var row int32 = 0
	for i := 0; i < len(ref); i++ {
		if ref[i] >= 'A' && ref[i] <= 'Z' { // Process the column letters
			// Convert letter to a column number (A = 1, B = 2, ..., Z = 26, AA = 27, etc.)
			col = col*26 + int32(ref[i]-'A'+1)
		} else {
			// Process the row part by slicing the remaining string and converting it to an integer
			rowPart, _ := strconv.Atoi(ref[i:])
			row = int32(rowPart)
			break
		}
	}
	return col, row
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

// Read sheet data and return parsed cell data using xml.RawToken for performance
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
			var currentCol int32
			var currentValue string
			var cell Cell // Define cell variable here

			// RawToken will return tokens without unnecessary overhead
			for {
				t, err := decoder.RawToken()
				if err != nil {
					if err == io.EOF {
						break
					}
					return nil, err
				}

				switch token := t.(type) {
				case xml.StartElement:
					switch token.Name.Local {
					case "row":
						// Capture row number from the attributes
						for _, attr := range token.Attr {
							if attr.Name.Local == "r" {
								rowInt, _ := strconv.ParseInt(attr.Value, 10, 32)
								currentRow = int32(rowInt)
							}
						}
					case "c":
						// Capture cell reference (e.g., A1) and type (e.g., "s" for shared string)
						cell = Cell{} // Reinitialize cell variable for each <c> element
						for _, attr := range token.Attr {
							switch attr.Name.Local {
							case "r":
								currentCol, _ = parseCellReference(attr.Value)
							case "t":
								cell.T = attr.Value
							}
						}
					case "v":
						// Capture the cell value (this is a RawToken, so we may get just the content)
						t, err := decoder.RawToken() // Capture text between <v>...</v>
						if err != nil {
							return nil, err
						}
						if charData, ok := t.(xml.CharData); ok {
							currentValue = string(charData)
						}
					}

				case xml.EndElement:
					if token.Name.Local == "c" {
						// Finished processing a cell, get the value
						val := getCellValue(Cell{T: cell.T, V: currentValue}, sharedStrings)
						cellData = append(cellData, CellData{
							RowNumber:    currentRow,
							ColumnNumber: currentCol,
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

			// Debugging statement to print shared string size
			sharedStringCount := len(sharedStrings.Items)

			// Optional: warn if shared string count exceeds a threshold
			if sharedStringCount > 1000_000 {
				fmt.Println("Warning: Large shared strings dataset detected, consider optimizing lookup.")
			}

			return &sharedStrings, nil
		}
	}
	return nil, fmt.Errorf("shared strings file not found")
}

// Read the workbook structure
func ReadWorkbook(zipReader *zip.ReadCloser) (*Workbook, error) {
	var workbook Workbook
	err := readXMLFromZip(zipReader, "xl/workbook.xml", &workbook)
	return &workbook, err
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
