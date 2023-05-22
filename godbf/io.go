// Package godbf offers functionality for loading and saving  "dBASE Version 5" dbf formatted files.
// (https://en.wikipedia.org/wiki/.dbf#File_format_of_Level_5_DOS_dBASE) file structure.
// For the definitive source, see http://www.dbase.com/manuals/57LanguageReference.zip
package godbf

import (
//	"bufio"
	"fmt"
	"os"
	"strings"
	"crypto/md5"
	"encoding/csv"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

// NewFromFile creates a DbfTable, reading it from a file with the given file name, expecting the supplied encoding.
func NewFromFile(fileName string, fileEncoding string) (table *DbfTable, newErr error) {
	defer func() {
		if e := recover(); e != nil {
			newErr = fmt.Errorf("%v", e)
		}
	}()

	data, readErr := readFile(fileName)
	if readErr != nil {
		return nil, readErr
	}
	return NewFromByteArray(data, fileEncoding)
}

// SaveToFile saves the supplied DbfTable to a file of the specified filename
func SaveToFile(dt *DbfTable, filename string, saveType string) (saveErr error) {
	defer func() {
		if e := recover(); e != nil {
			saveErr = fmt.Errorf("%v", e)
		}
	}()

	f, createErr := fsWrapper.Create(filename)
	if createErr != nil {
		return createErr
	}

	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			saveErr = closeErr
		}
	}()

	if saveType == "bin" {
		writeErr := writeContent(dt, f)
		if writeErr != nil {
			return writeErr
		}
	} else if saveType == "SQL"{
		tabname, defn := SQLTableDef(dt, filename)
		//convert string to bytes
		bdefn := []byte(defn)
		n2, err := f.Write(bdefn)
		check(err)
		err = SQLTableInserts(dt, tabname, f) 
		check(err)

		fmt.Printf("\n-- Write Table: %s, File: %s, Bytes: %d", tabname, filename, n2)
	} else {
		fmt.Printf("SaveToFile: Unrecognized file type: %s", saveType)
	}


	return saveErr
}

func writeContent(dt *DbfTable, f *os.File) error {
	if _, dsErr := f.Write(dt.dataStore); dsErr != nil {
		return dsErr
	}
	return nil
}

// Given the DbfTable return a SQL table create.
//
func SQLTableDef(dt *DbfTable, file string) (string, string) {

	SQL_tbl := fmt.Sprintf("tmp_%x", md5.Sum([]byte(file)))   // filenames may be too long for table names
	SQLdef := fmt.Sprintf("\n\nCREATE TABLE %s (\n", SQL_tbl)  //create unique strings for tables

	for _, df := range dt.Fields() {
		SQLdef += sQLTableClauseDef(df)
	}

	SQLdef = SQLdef[:len(SQLdef)-2] //lose last ,
	SQLdef += "\n)\n"

	return SQL_tbl, SQLdef
}

//Takes df  and return SQL create table clause
//
func sQLTableClauseDef(df FieldDescriptor) string {

	nm := df.Name()
	typ := df.FieldType()
	len := df.Length()
	places := df.DecimalPlaces()
	retval := ""

	switch typ {
		case 'C', 'V':
			retval = fmt.Sprintf("\t %s NVARCHAR(%d),\n", nm, len)
		case 'N':
			if (places == 0) {
				retval = fmt.Sprintf("\t %s INT,\n", nm)
			} else{
				retval = fmt.Sprintf("\t %s DECIMAL(%d,%d),\n", nm, len, places)
			}
		case 'M':
			fmt.Printf("\n\t: Field: %s, Type: %c, Len: %d, (%d)dp (SQLTableDef)", nm, typ, len, places)
		case 'F', 'B', 'O':  //Floating point number, stored as string, padded with spaces if shorter than the field length
			retval = fmt.Sprintf("\t %s FLOAT(%d),\n", nm, len)
		case 'L':  //Logical variable A boolean value, stored as one of YyNnTtFf. May be set to ? if not initialized
					//NB no boolean type in SQLServer
			retval = fmt.Sprintf("\t %s BIT DEFAULT 0,\n", nm)
		case '@', 'D':
			retval = fmt.Sprintf("\t %s DATE,\n", nm)
		case 'T':  //DateTime	459599234239	A date and time, stored as a number (see http://www.independent-software.com/dbase-dbf-dbt-file-format.html, under record reading)
			//unpackErr = dt.AddDateField(fieldName)
			fmt.Printf("\n\t: Field: %s, Type: %c, Len: %d, (%d)dp (SQLTableDef)", nm, typ, len, places)
		case 'I':     // Integer value, stored as a little endian 32-bit value, with the highest bit used to negate numbers
			retval = fmt.Sprintf("\t %s INT,\n", nm)
		case '0', 0x0:    // no idea what this is :(
			fmt.Printf("\n\t: Field: %s, Type: %c, Len: %d, (%d)dp (SQLTableDef)", nm, typ, len, places)
		default:
			//typechar := s[offset+11]
			fmt.Printf("SQLTableDef: Unrecognized type: %c, (%x hex)\n", typ, typ)
	}
	
	return retval
}

func SQLTableInserts(dt *DbfTable, SQL_tbl string, f *os.File) error {
	thHeaders := dt.Fields()
	//headerLength := len(thHeaders)
	text_fields := "MDCV@" //text fields
	//number_fields := "NFLTI" //numeric fields
	err := error(nil)

	for i := 0; i < dt.NumberOfRecords(); i++ {
		row := dt.GetRowAsSlice(i)
		//fmt.Printf("\nRow: %d: Items: %d (Hdrs: %d)", i, len(row), headerLength) //Todo panic in not same
		
		for x := 0; x < len(row); x++ {
			df := thHeaders[x]
			typ := df.FieldType()

			//fmt.Printf("\nType: %c", typ)
			if  (strings.Index(text_fields, string(typ)) >= 0){   
				row[x] = strings.Replace(row[x], "\"", "'", -1)   //replace old " with '
				row[x] = fmt.Sprintf("\"%s\"", row[x])             //embed strings in quotes
			} 
		}
		_, err := fmt.Fprintf(f, "\nINSERT INTO %s \n VALUES\n(%s)", SQL_tbl, strings.Join(row, ", "))
		check(err)
	}

	return err
}


func SaveToCSV(dt *DbfTable, filename string) (saveErr error)  {

	// Create a new CSV file
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Create a new CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	var cols []string

	//Collect and Write the column headers
	for _, df := range dt.Fields() {
		//cols += df.Name()
		cols = append(cols, df.Name())
	}
	
	err = writer.Write(cols)
	check(err)

	// Write the data rows
	for i := 0; i < dt.NumberOfRecords(); i++ {
		row := dt.GetRowAsSlice(i)
		err = writer.Write(row)
		check(err)
	}

	// Flush any buffered data to the underlying writer
	writer.Flush()

	// Check for any errors during the write
	if err := writer.Error(); err != nil {
		panic(err)
	}

	return err
}

