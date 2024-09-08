package main

import "strconv"

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
