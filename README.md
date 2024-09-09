# XLSX to CSV/JSON/Parquet Converter

Welcome to the **XLSX to CSV/JSON/Parquet Converter**! This project is a flexible and efficient tool that helps you extract data from `.xlsx` files and export it into various formats such as CSV, JSON, or Parquet. With support for concurrent sheet processing and strong data compression using Parquet's ZSTD compression, this tool is built to handle large datasets with ease. Whether you're working with simple spreadsheets or complex workbooks, this tool ensures your data is processed quickly and outputted in the format you need.

## Features

- **Multi-Format Output**: Export your `.xlsx` data into CSV, JSON, or Parquet formats based on your needs.
- **Concurrent Sheet Processing**: Leverage multi-core processors for faster sheet data extraction by processing multiple sheets simultaneously.
- **Efficient Data Compression**: Use ZSTD compression for Parquet files to optimize storage and processing times.
- **Profiling Support**: Optional CPU and memory profiling to identify bottlenecks in performance.
- **Easy-to-Use**: Simple command-line interface for file conversion and flexible output options.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Command Line Options](#command-line-options)
- [Formats Supported](#formats-supported)
- [Profiling](#profiling)
- [License](#license)

## Installation

To get started, clone this repository and install the necessary Go dependencies.

```bash
git clone https://github.com/yourusername/xlsx-to-parquet-converter.git
cd xlsx-to-parquet-converter
go mod tidy
```

Make sure you have Go installed on your machine. You can download Go from [here](https://golang.org/dl/).

## Usage

Convert an `.xlsx` file into CSV, JSON, or Parquet by running the following command:

```bash
go run main.go <xlsx_file> <target_file>
```

- `<xlsx_file>`: Path to the source `.xlsx` file.
- `<target_file>`: Path to the output file, including the desired format (`csv`, `json`, or `parquet`).

### Example:

```bash
go run main.go sample.xlsx output.csv
```

This command will read `sample.xlsx` and export the data to `output.csv`.

## Command Line Options

- `-cpuprofile=<file>`: Generate a CPU profile and save it to the specified file.
- `-memprofile=<file>`: Generate a memory profile and save it to the specified file.

### Example with Profiling:

```bash
go run main.go -cpuprofile=cpu.prof -memprofile=mem.prof sample.xlsx output.parquet
```

This will generate both CPU and memory profiles while processing the file.

## Formats Supported

The tool supports exporting `.xlsx` data into three formats:

- **CSV**: A standard and widely-used format for tabular data.
- **JSON**: A structured format that works well with modern web APIs and applications.
- **Parquet**: An efficient, columnar storage format optimized for large datasets with ZSTD compression for space saving and better I/O performance.

### Output File Naming:
The tool automatically detects the format based on the target file extension (e.g., `.csv`, `.json`, or `.parquet`).

## Profiling

To optimize performance, you can enable CPU and memory profiling. These profiles can help you diagnose performance bottlenecks or memory leaks in large-scale data conversions.

- **CPU Profiling**: Captures how much time is spent on various operations during execution.
- **Memory Profiling**: Provides insights into memory usage, garbage collection, and potential memory leaks.

Use the `-cpuprofile` and `-memprofile` options to generate these profiles. Analyze the profiles using tools such as `go tool pprof`.

## License

This project is open-source and licensed under the MIT License. Feel free to contribute and improve the code!

---

Enjoy seamless data conversion with XLSX to CSV/JSON/Parquet Converter! 🚀

