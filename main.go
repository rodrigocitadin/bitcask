package main

import (
	"fmt"
	"rodrigocitadin/bitcask/components"
)

func main() {
	record := components.NewRecord("abc", []byte("abc"))
	fmt.Println(*record)
	// datafile, err := components.NewDatafile("datafile", 2)
	// if err != nil {
	// 	fmt.Println(err)
	// }

	// datafile.Write([]byte("abcefg"))
	// result, err := datafile.Read(6, 6)
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// fmt.Println(string(result))

	// datafile.Sync()
	// datafile.Close()
}
