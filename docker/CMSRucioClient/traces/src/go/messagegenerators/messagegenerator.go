package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	// load-balanced stomp manager
	lbstomp "github.com/vkuznet/lb-stomp"
	// stomp library
)

// Configuration stores server configuration parameters
type Configuration struct {

	// HTTP server configuration options
	Interval int `json:"interval"` // interval of server
	Verbose  int `json:"verbose"`  // verbose level

	// Stomp configuration options
	StompURI         string `json:"stompURI"`         // StompAMQ URI
	StompLogin       string `json:"stompLogin"`       // StompAQM login name
	StompPassword    string `json:"stompPassword"`    // StompAQM password
	StompIterations  int    `json:"stompIterations"`  // Stomp iterations
	StompSendTimeout int    `json:"stompSendTimeout"` // heartbeat send timeout
	StompRecvTimeout int    `json:"stompRecvTimeout"` // heartbeat recv timeout
	Endpoint         string `json:"endpoint"`         // StompAMQ endpoint
	ContentType      string `json:"contentType"`      // ContentType of UDP packet
}

// Config variable represents configuration object
var Config Configuration

// Record defines general message record
type Record map[string]interface{}

// global stomp manager
var stompMgr *lbstomp.StompManager

// helper function to parse configuration
func parseConfig(configFile string) error {
	//data is a []byte
	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Println("Unable to read", err)
		return err
	}
	err = json.Unmarshal(data, &Config)
	if err != nil {
		log.Println("Unable to parse", err)
		return err
	}
	if Config.StompIterations == 0 {
		Config.StompIterations = 3 // number of Stomp attempts
	}
	if Config.ContentType == "" {
		Config.ContentType = "application/json"
	}
	if Config.StompSendTimeout == 0 {
		Config.StompSendTimeout = 1000 // miliseconds
	}
	if Config.StompRecvTimeout == 0 {
		Config.StompRecvTimeout = 1000 // miliseconds
	}
	log.Println(Config.Endpoint)
	return nil
}

func genUUID() string {
	uuidWithHyphen := uuid.New()
	uuid := strings.Replace(uuidWithHyphen.String(), "-", "", -1)
	return uuid
}

func initStomp() {
	c := lbstomp.Config{
		URI:         Config.StompURI,
		Login:       Config.StompLogin,
		Password:    Config.StompPassword,
		Iterations:  Config.StompIterations,
		SendTimeout: Config.StompSendTimeout,
		RecvTimeout: Config.StompRecvTimeout,
		Endpoint:    Config.Endpoint,
		ContentType: Config.ContentType,
		Verbose:     Config.Verbose,
	}
	stompMgr = lbstomp.New(c)
	log.Println(stompMgr.String())
}

func processMessage(input string, nrec int, producer string) (Record, error) {
	/*
		input string : input data file that contains the original data sample, example wmarchive.json.
		nrec int : number of new doc we will generated. The original data was modified by giving it new timestamp and uuid.
		producer string: The name of the producer, such as wmarchih, cmspop, and xrootd.
	*/

	var rec Record
	var out []Record
	//read the input file
	data, err := ioutil.ReadFile(input)
	if err != nil {
		log.Println("Unable to read", err)
		if Config.Verbose > 0 {
			log.Println("Unable to read file: ", input)
		}
		return nil, err
	}
	if Config.Verbose > 0 {
		log.Println("****************")
		log.Println("Original Record", string(data))
	}
	//conver the message to Record map[string]interface{}
	//in order to process it.
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Println("Unable to parse input data file to map. ", err)
		return nil, err
	}
	//processing the message here.
	var ids []string
	for i := 0; i < nrec; i++ {
		uid := genUUID()
		if producer == "wmarchive" {
			rec["wmats"] = time.Now().Unix()
			v2 := rec["meta_data"].(map[string]interface{})
			v2["ts"] = rec["wmats"]
			rec["wmats_num"] = rec["wmats"]
			rec["wmaid"] = uid
			md := rec["metadata"].(map[string]interface{})
			md["_id"] = uid
			md["uuid"] = uid
		}
		// TODO: add CMSSWPOP and Xrootd here
		data, err := json.Marshal(rec)
		if err != nil {
			if Config.Verbose > 0 {
				log.Printf("Unable to marshal back to JSON string , error: %v, data: %+v\n", err, rec)
			} else {
				log.Printf("Unable to marshal back to JSON string, error: %v \n", err)
			}
			continue
		}
		// dump message to our log
		if Config.Verbose > 0 {
			log.Println("*********************")
			log.Println("New record", string(data))
		}
		// send data to Stomp endpoint
		if Config.Endpoint != "" {
			err := stompMgr.Send(data)
			if err == nil {
				ids = append(ids, uid)
			} else {
				record := make(Record)
				record["status"] = "fail"
				record["reason"] = fmt.Sprintf("Unable to send data to MONIT, error: %v", err)
				record["ids"] = ids
				out = append(out, record)
				r := make(Record)
				r["result"] = out
				return r, err
			}
		} else {
			record := make(Record)
			record["status"] = "fail"
			record["reason"] = "Config.Endpoint is nil"
			record["ids"] = ids
			out = append(out, record)
			r := make(Record)
			r["result"] = out
			return r, nil
		}
	}
	// Everything is fine. Prepare output.
	record := make(Record)
	if len(ids) > 0 {
		record["status"] = "ok"
		record["ids"] = ids
	} else {
		record["status"] = "empty"
		record["ids"] = ids
		record["reason"] = "no data is sent to the endpoint."
	}
	out = append(out, record)
	rec = make(Record)
	rec["result"] = out
	return rec, nil
}

func main() {
	/*
		To run
		./messagegenerator -config messagegeneratorconfig.json -infile wmarchive.json -nrec 2 -producer wmarchive
	*/

	var config string
	var input string
	var producer string
	flag.StringVar(&config, "config", "", "config file name")
	flag.StringVar(&input, "infile", "", "input file for json doc")
	nrecPtr := flag.Int("nrec", 100, "number of docs to send to the endpoint")
	flag.StringVar(&producer, "producer", "", "the name of producer, such as wmarchive, cmspop, xroot")

	flag.Parse()

	err := parseConfig(config)
	if err != nil {
		log.Fatalf("Unable to parse config file %s, error: %v", config, err)
	}
	//initStomp()
	//server()
	if Config.Verbose > 0 {
		log.Printf("%v", Config)
	}
	// init stomp manager and get first connection
	c := lbstomp.Config{
		URI:         Config.StompURI,
		Login:       Config.StompLogin,
		Password:    Config.StompPassword,
		Iterations:  Config.StompIterations,
		SendTimeout: Config.StompSendTimeout,
		RecvTimeout: Config.StompRecvTimeout,
		Endpoint:    Config.Endpoint,
		ContentType: Config.ContentType,
		Verbose:     Config.Verbose,
	}
	stompMgr = lbstomp.New(c)
	log.Println(stompMgr.String())
	t1 := time.Now().Unix()
	out, err := processMessage(input, *nrecPtr, producer)
	log.Printf("Error: %v", err)
	log.Printf("Sent rec: %v", out)
	t2 := time.Now().Unix() - t1
	log.Printf("Total time spend: %d seconds", t2)

}
