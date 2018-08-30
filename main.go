package main

import "github.com/erician/gpdDB/logmanager"

func main() {
	/*
		file, err := os.Create("./disk-test1")
		if err != nil {
			log.Fatal(err)
		}
		bs := make([]byte, 100)

		start := time.Now()
		old := start
		for i := 0; i < 1024*1024; i++ {
			file.Write(bs)
			file.Sync()

				new := time.Now()
				if new.Sub(old).Seconds() > (1.0) {
					old = new
					file.Sync()
				}

		}
		file.Close()
		t := time.Now()
		elapsed := t.Sub(start)
		fmt.Println(elapsed)
		os.Remove("./disk-test1")
	*/

	r := logmanager.LogRecord{1}
	r.Oparation = 3
	logmanager.A.Oparation = 4

}
