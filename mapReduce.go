package main

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

func openDatabase(path string) (*sql.DB, error) {
	// the path to the database--this could be an absolute path

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("Error opening db: %v", err)
	}
	return db, nil
}
func createDatabase(path string) (*sql.DB, error) {
	var db *sql.DB
	if _, err := os.Stat(path); err == nil {
		// database exists
		//log.Printf("createDatabase called on database that  %v already exists, deleting it.", path)
		os.Remove(path)
		db, err = sql.Open("sqlite3", path)
		if err != nil {
			return nil, fmt.Errorf("createDatabase: sql.Open: %v", err)
		}
	}

	if _, err := db.Exec("create table pairs (key text, value text);"); err != nil {
		db.Close()
		return nil, fmt.Errorf("createDatabase: db.Exec(create table): %v", err)
	}
	return db, nil
}

func splitDatabase(source, outputPattern string, m int) ([]string, error) {
	db, err := openDatabase(source)
	defer db.Close()
	if err != nil {
		return nil, fmt.Errorf("splitDatabase: sql.Open: %v", err)
	}
	pairsc, err := db.Query("SELECT count(1) as count FROM pairs;")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("splitDatabase: db.Query(count): %v", err)
	}
	var count int
	for pairsc.Next() {
		if err := pairsc.Scan(&count); err != nil {
			return nil, fmt.Errorf("splitDatabase: scan-count: %v", err)
		}
	}

	if count < m {
		err := errors.New("splitDatabase: count less than m")
		db.Close()
		return nil, err
	}
	var pairsperpartition float64
	pairsperpartition = float64(count) / float64(m)
	math.Ceil(pairsperpartition)
	log.Printf("PairsperPartition: %v", pairsperpartition)
	rows, err := db.Query("SELECT key, value FROM pairs;")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("splitDatabase: db.Query(Select): %v", err)
	}
	defer rows.Close()
	i := 0
	j := 0
	var pathTitles []string
	dbsplit := fmt.Sprintf(outputPattern, j)
	pathTitles = append(pathTitles, dbsplit)
	finalDB, err := createDatabase(dbsplit)
	if err != nil {
		return nil, fmt.Errorf("splitDatabase: sql.Open: (split) %v", err)
	}
	_, err = finalDB.Exec("CREATE TABLE IF NOT EXISTS pairs (key text, value text);")
	if err != nil {
		return nil, fmt.Errorf("splitDatabase: db.Exec(CREATE TABLE): %v", err)
	}
	for rows.Next() {
		i++
		if i > int(pairsperpartition) {
			i = 0
			finalDB.Close()
			j++
			dbsplit = fmt.Sprintf(outputPattern, j)
			pathTitles = append(pathTitles, dbsplit)
			finalDB, err = createDatabase(dbsplit)
			if err != nil {
				return nil, fmt.Errorf("splitDatabase: sql.Open: (split) %v", err)
			}
			_, err := finalDB.Exec("CREATE TABLE IF NOT EXISTS pairs (key text, value text);")
			if err != nil {
				return nil, fmt.Errorf("splitDatabase: db.Exec(CREATE TABLE): %v", err)
			}
		}
		var key string
		var value string
		if err := rows.Scan(&key, &value); err != nil {
			finalDB.Close()
			return nil, fmt.Errorf("splitDatabase: rows.Scan: %v", err)
		}
		if _, err := finalDB.Exec("INSERT INTO pairs (key,value) VALUES (?,?);", key, value); err != nil {
			finalDB.Close()
			return nil, fmt.Errorf("splitDatabase: db.Exec(INSERT INTO): %v", err)
		}
		//log.Printf("key,value: %v,%v", key, value)
	}
	finalDB.Close()
	return pathTitles, nil
}

func mergeDatabases(urls []string, path string, temp string) (*sql.DB, error) {
	var finalDataBase *sql.DB
	var err error
	if finalDataBase, err = createDatabase(path); err != nil {
		return nil, fmt.Errorf("Error in merge db: ", err)
	}
	for _, url := range urls {
		in, err := download(url, temp)
		if err != nil {
			finalDataBase.Close()
			return nil, fmt.Errorf("Err in merge db: ", err)
		}
		err = gatherInto(finalDataBase, in)
		if err != nil {
			finalDataBase.Close()
			return nil, fmt.Errorf("Error in mergedb: ", err)
		}
	}
	return finalDataBase, nil
}

func download(douwnloadUrl, path string) (string, error) {
	res, err := http.Get(douwnloadUrl)
	if err != nil {
		return "", fmt.Errorf("error in download", err)
	}
	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("err in download", err)
	}
	defer res.Body.Close()
	fmt.Println("URL:", douwnloadUrl)
	fmt.Println("body:", res.Body)

	file, err := url.Parse(douwnloadUrl)
	if err != nil {
		return "", fmt.Errorf("Err in download parsing: ", err)
	}
	filePath := file.Path
	paths := strings.Split(filePath, "/")
	newFile := paths[len(paths)-1]
	finalFile := path + "/" + newFile
	out, err := os.Create(finalFile)
	if err != nil {
		return "", fmt.Errorf("err in download: ", err)
	}
	defer out.Close()

	_, err = io.Copy(out, res.Body)
	if err != nil {
		return "", fmt.Errorf("err in download", err)
	}
	return finalFile, err

}

func gatherInto(db *sql.DB, path string) error {
	_, err := db.Exec("attach ? as merge;", path)
	if err != nil {
		return fmt.Errorf("err in gatherInto attach", err)
	}
	_, err = db.Exec("insert into pairs select * from merge.pairs;")
	if err != nil {
		return fmt.Errorf("err in gatherInto, insert", err)
	}
	_, err = db.Exec("detach merge;")
	if err != nil {
		return fmt.Errorf("err in gatherInto , detach", err)
	}
	if err = os.Remove(path); err != nil {
		return fmt.Errorf("err in gatherInto, remove", err)
	}
	return err

}

func main() {

	// myaddress := ":8080"
	// tempdir := "/tmp"

	// go func() {
	// 	http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))
	// 	if err := http.ListenAndServe(myaddress, nil); err != nil {
	// 		log.Printf("Error in HTTP server for %s: %v", myaddress, err)
	// 	}
	// }()
	//openDatabase("austen.sqlite3")
	//createDatabase("austen.sqlite3")

	splitDatabase("./austen.sqlite3", "output-%d.sqlite3", 50)
}
