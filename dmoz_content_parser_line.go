package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"gopkg.in/olivere/elastic.v3"
)

type Dmoz struct {
	Cateid        string   `json:"cateid"`
	Topic         string   `json:"topic"`
	Link          []string `json:"link"`
	GeneratedDate string   `json:"generated_date"`
}

func main() {
	var dmozs []Dmoz
	var obj Dmoz
	starttime := time.Now()
	//open file
	if file, err := os.Open("/tmp/content.rdf.u8"); err == nil {
		//make sure if gets closed
		defer file.Close()

		topictxt := ""
		catid := ""
		var links []string
		var isTopic bool
		var timestamp string
		//create a new scanner and read the file line by line
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			txt := scanner.Text()
			txt = strings.Trim(txt, " ")
			txt = strings.ToLower(txt)

			if strings.Index(txt, "<!-- generated at") == 0 {
				timetxt := strings.TrimPrefix(txt, "<!-- generated at ")
				timetxt = strings.TrimSuffix(timetxt, " est from dmoz 2.0 -->")
				targ := fmt.Sprintf("%s-04:00", timetxt)
				t, err := time.Parse("2006-01-02 15:04:05-07:00 ", targ)
				if err != nil {
					fmt.Println(err)
				}
				timestamp = t.Format(time.RFC3339)
			}

			if strings.Index(txt, "<topic r:id=") == 0 {
				topictxt = strings.TrimPrefix(txt, "<topic r:id=\"")
				topictxt = strings.TrimSuffix(topictxt, "\">")
				//log.Println(topictxt)
				isTopic = true
				obj = Dmoz{}
				obj.Topic = topictxt
			}
			if strings.Index(txt, "<catid>") == 0 {
				catid = strings.TrimPrefix(txt, "<catid>")
				catid = strings.TrimSuffix(catid, "</catid>")
				//log.Println("catid:", catid)
				obj.Cateid = catid
			}
			if strings.Index(txt, "<link") == 0 {
				linktxt := strings.TrimPrefix(txt, "<link1 r:resource=\"")
				linktxt = strings.TrimSuffix(linktxt, "\"></link1>")
				linktxt = strings.TrimPrefix(linktxt, "<link r:resource=\"")
				linktxt = strings.TrimSuffix(linktxt, "\"></link>")
				//log.Println("link:", linktxt)
				links = append(links, linktxt)
			}

			if strings.Index(txt, "</topic>") == 0 {
				isTopic = false
				obj.Link = links
				obj.GeneratedDate = timestamp
				//log.Println(obj)
				dmozs = append(dmozs, obj)
				topictxt = ""
				catid = ""
				links = []string{}
			}
		}

		//check for errors
		if err = scanner.Err(); err != nil {
			log.Fatal(err)
		}

		log.Println("isTopic:", isTopic)

	} else {
		log.Fatal(err)
	}

	log.Println("Finish Parsing RDF xml, time period:", time.Now().Sub(starttime))

	elk := "http://localhost:9200"
	//setup elastic client for bulk update
	client, err := elastic.NewClient(
		elastic.SetSniff(false),
		elastic.SetURL(elk),
	)
	if err != nil {
		log.Println("Elasticsearch Server is dead.")
		panic(err)
	}

	//initialize bulk update service
	bulkUpdateRequest := client.Bulk()
	cnt := 0
	for _, v := range dmozs {

		updateRequest := elastic.NewBulkUpdateRequest().
			Index("dmoz").
			Type("content").
			Id(v.Cateid).
			Doc(v).
			DocAsUpsert(true).
			RetryOnConflict(3)

		bulkUpdateRequest = bulkUpdateRequest.Add(updateRequest)

		cnt++
		if t := cnt % 1000; t == 0 {
			log.Println("")
			log.Println("Number of actions:", bulkUpdateRequest.NumberOfActions())
			log.Println("")

			//execute Bulk Update
			_, brErr := bulkUpdateRequest.Do()
			//time.Sleep(1 * time.Second)
			if brErr != nil {
				log.Println("Bulk Update to ELK failed.", brErr)
			} else {
				bulkUpdateRequest = client.Bulk()
			}
		}
	}

	log.Println("")
	log.Println("Number of actions:", bulkUpdateRequest.NumberOfActions())
	log.Println("")

	//execute Bulk Update to the last data
	_, brErr := bulkUpdateRequest.Do()
	if brErr != nil {
		log.Println("Bulk Update to ELK failed.", brErr)
	}
	log.Println("Finish all time period:", time.Now().Sub(starttime))
}
