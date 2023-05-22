package main
import (
    "fmt"
    "log"
    "os"
    "path/filepath"
    "time"
    "strings"

    "github.com/IntegrityAnalytics/go-dbf/godbf"
)

func main() {
    start := time.Now()         //to get runtime

    // generate  log files names based on time
    short_time := start.Format("2006-1-2_15_4_5")  //gives time template
    fmt.Printf("--  Time: %s", short_time)
    logFilePath := "c:\\temp" + "\\" + short_time + "_goDBFs.txt"

    lfile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY, 0666)  //os.O_APPEND|
    if err != nil {
        log.Fatal(err)
    }

    defer lfile.Close()  //defer to close when you're done with it
    log.SetOutput(lfile)
    log.Printf("Starting Run: %s\n", start)

    // Replace "path/to/directory" with the actual directory you want to read
    // e.g. C:\\GoDBF\\data\\spec\\fixtures\\

    files, err := filepath.Glob(`C:\jeremy\go_execute\IA\godbf\testdata\*.dbf`)

    if err != nil {
        panic(err)
    }

    fmt.Printf("\n-- Found %d files\n", len(files))
    fileProcess(files)
    fmt.Printf("\n-- Finished in %v @%s\n", time.Since(start), time.Now())
}

func fileProcess(files []string ){
    max_count := 20  //limit nos for debug 

    for _, file := range files {

        max_count--        // just for debug
        if max_count < 0 {
            break
        }
        
        dbfTable, err := godbf.NewFromFile(file, "CP437") //CP437 utf8 cp1252
        if err != nil {
            log.Printf("\nFile: %s (%d), Godbf error: %s ",  file, max_count, err)
            continue
            
            // quick fix -- ignore off by ones on hdr
            //if !(strings.Contains(err.Error(), "but header expected")){
            //    continue
            //}
            // panic(err)
        }

        // Do something with the DBF table here
        fmt.Printf("--  File: %s, Records: %d, Updated: %s\n", file, dbfTable.NumberOfRecords(), dbfTable.LastUpdated())

        SQLfile := strings.TrimSuffix(file, ".dbf") + ".sql"   //change ext to .sql as output for testing
        err = godbf.SaveToFile(dbfTable, SQLfile, "SQL")

        CSVfile :=  strings.TrimSuffix(file, ".dbf") + ".csv"  // ditto for csv
        err = godbf.SaveToCSV(dbfTable, CSVfile)
        
        if err != nil {
            panic(err)
        }
        // tabname, defn := godbf.SQLTableDef(dbfTable, file)
       // fmt.Printf("\n--  Table: %s\n%s", tabname, defn)
       // _ = godbf.SQLTableInserts(dbfTable, tabname)
        

    }
}


