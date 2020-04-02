package main

import (
	"database/sql"
	"fmt"
	"os"
)

func openDatabase(path string) (*sql.DB, error) {
	// the path to the database--this could be an absolute path

	options :=
		"?" + "_busy_timeout=10000" +
			"&" + "_case_sensitive_like=OFF" +
			"&" + "_foreign_keys=ON" +
			"&" + "_journal_mode=OFF" +
			"&" + "_locking_mode=NORMAL" +
			"&" + "mode=rw" +
			"&" + "_synchronous=OFF"
	db, err := sql.Open("sqlite3", path+options)
	if err != nil {
		return nil, fmt.Errorf("Error opening db: %v", err)
	}
	return db, nil
}

func createDatabase(path string) (*sql.DB, error) {
	var db *sql.DB

	if _, err := os.Stat(path); err == nil {
		//remove db if exists
		os.Remove(path)
		db, err = sql.Open("sqlite3", path)
		if err != nil {
			return nil, fmt.Errorf("Error creating db: %v", err)
		}
	}
	if _, err := db.Exec("create table pairs (key text, value text);"); err != nil {
		db.Close()
		return nil, fmt.Errorf("Error creating table pairs: %v", err)
	}
	return db, nil
}

func splitDatabase(source, outputPattern string, m int) ([]string, error){
	db, err := openDatabase(source)
	defer db.Close()
	if err != nil{
		return nil, fmt.Errorf("In splitDatabase: Could not open %v", err)
	}
	rows, err := db.Query("Select key, value from pairs;")
	if err != nil{
		db.Close()
		return nil, fmt.Errorf("In splitdatabase: Error selecting key and value %v", err)
	}
	defer rows.Close()

	for rows.Next(){
		var key string
		var val string

		if err := rows.Scan(&key, &val); err != nil{
			db.Close()
			return nil, fmt.Errorf("Scan error %v", err)
		}
		if _, err := db

	}
}

func main() {

}
