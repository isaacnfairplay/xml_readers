package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

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
