package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	_ "github.com/mattn/go-sqlite3"
	"github.com/maxmind/mmdbwriter"
	"github.com/maxmind/mmdbwriter/inserter"
	"github.com/maxmind/mmdbwriter/mmdbtype"
	"github.com/schollz/progressbar/v3"
	"log"
	"net"
	"os"
	"strconv"
)

func readCsvFile(filePath string) [][]string {
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Unable to read input file "+filePath, err)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	records, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal("Unable to parse file as CSV for "+filePath, err)
	}
	return records
}

// change only country and registerd country
func MMDBTemplate(cgid, rgid int, c_iso, r_iso, c_name, r_name string) mmdbtype.Map {
	tmp := mmdbtype.Map{
		"country": mmdbtype.Map{
			"geoname_id": mmdbtype.Int32(cgid),
			"iso_code":   mmdbtype.String(c_iso),
			"names": mmdbtype.Map{
				"en": mmdbtype.String(c_name),
			},
		},
		"registered_country": mmdbtype.Map{
			"geoname_id": mmdbtype.Int32(rgid),
			"iso_code":   mmdbtype.String(r_iso),
			"names": mmdbtype.Map{
				"en": mmdbtype.String(r_name),
			},
		},
		"modifyed": mmdbtype.Bool(true),
	}
	return tmp
}

func insertToTree(CIDR string, cgid, rgid int, tree *mmdbwriter.Tree, db *sql.DB) error {
	_, p_cidr, err := net.ParseCIDR(CIDR)
	if err != nil {
		log.Fatal(err)
	}

	c_iso, c_name, err := getFromDB(cgid, db)
	if err != nil {
		println(CIDR)
		return err
	}
	r_iso, r_name, err := getFromDB(rgid, db)
	if err != nil {
		println(CIDR)
		return err
	}

	data := MMDBTemplate(cgid, rgid, c_iso, r_iso, c_name, r_name)

	if err := tree.InsertFunc(p_cidr, inserter.DeepMergeWith(data)); err != nil {
		log.Fatal(err)
	}

	return nil
}

func writeToMMDB(filename string, tree *mmdbwriter.Tree) {
	fh, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	_, err = tree.WriteTo(fh)
	if err != nil {
		log.Fatal(err)
	}
}

// return ISO code and Name of country by geo name id
func getFromDB(gid int, db *sql.DB) (string, string, error) {
	var iso, name string
	query := "SELECT ISO, name_eng FROM countries WHERE gid = $1;"

	err := db.QueryRow(query, gid).Scan(&iso, &name)

	if err != nil {
		println(gid)
		return "", "", err
	}

	return iso, name, nil
}

func main() {
	// vars
	var mmdb_in, mmdb_out, country_database, csv_for_patching string

	flag.StringVar(&mmdb_in, "i", "GeoLite2-City.mmdb", "Input mmdb database")
	flag.StringVar(&mmdb_out, "o", "GeoLite2-City_patched.mmdb", "Output mmdb database")
	flag.StringVar(&country_database, "db", "country.db", "Database with country code")
	flag.StringVar(&csv_for_patching, "csv", "GeoLite2-Country-Blocks-IPv4.csv", "Subnets for patching")

	flag.Parse()

	// Load mmdb for enrich
	println("Loading mmdb DataBase..")

	writer, err := mmdbwriter.Load(mmdb_in, mmdbwriter.Options{})

	if err != nil {
		log.Fatal(err)
	}

	// Load databases with country codes
	println("Connect to local database")
	db, err := sql.Open("sqlite3", country_database)

	if err != nil {
		log.Fatal(err)
	}

	// Load subnets for patching
	println("Load subnets for patching..")
	lines := readCsvFile(csv_for_patching)

	// Start inserting
	csv_length := len(lines)
	bar := progressbar.Default(int64(csv_length) - 1)

	for i := 1; i < csv_length; i++ {
		cgid, _ := strconv.Atoi(lines[i][1]) // country geo name id
		rgid, _ := strconv.Atoi(lines[i][2]) // registered country geo name id

		err := insertToTree(lines[i][0], cgid, rgid, writer, db)
		if err != nil {
			println("Error")
		}
		bar.Add(1)
	}
	defer db.Close()

	println("Start writing")

	writeToMMDB(mmdb_out, writer)
	println("OK")
}
