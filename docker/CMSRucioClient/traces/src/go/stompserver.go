package main

// stomp-server - Implementation of stomp server to consume, process and produce ActiveMQ messages
// The server will consumer message from below three topics:
// 1.
// 2.
// 3.
// Process them, then produce a Ruci trace message and then it to topic:
//

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync/atomic"
	"time"

	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	// load-balanced stomp manager
	lbstomp "github.com/vkuznet/lb-stomp"
	// stomp library
	"github.com/go-stomp/stomp"
)

// Configuration stores server configuration parameters
type Configuration struct {
	// HTTP server configuration options
	Interval int `json:"interval"` // interval of server
	Verbose  int `json:"verbose"`  // verbose level
	// Stomp configuration options
	StompURI         string `json:"stompURI"`         // StompAMQ URI for consumer and Producer
	StompLogin       string `json:"stompLogin"`       // StompAQM login name
	StompPassword    string `json:"stompPassword"`    // StompAQM password
	StompIterations  int    `json:"stompIterations"`  // Stomp iterations
	StompSendTimeout int    `json:"stompSendTimeout"` // heartbeat send timeout
	StompRecvTimeout int    `json:"stompRecvTimeout"` // heartbeat recv timeout
	EndpointConsumer string `json:"endpointConsumer"` // StompAMQ endpoint Consumer
	EndpointProducer string `json:"endpointProducer"` // StompAMQ endpoint Producer
	ContentType      string `json:"contentType"`      // ContentType of UDP packet
	Protocol         string `json:"Protocol"`         // network protocol tcp4
}

//Trace defines Rucio trace
type Trace struct {
	EventVersion       string `json:"eventVersion"`
	ClientState        string `json:"clientState"`
	Scope              string `json:"scope"`
	EventType          string `json:"eventType"`
	Usrdn              string `json:"usrdn"`
	Account            string `json:"account"`
	Filename           string `json:"filename"`
	RemoteSite         string `json:"remoteSite"`
	DID                string `json:"DID"`
	FileReadts         int64  `json:"file_read_ts"`
	Jobtype            string `json:"jobtype"`
	Wnname             string `json:"wn_name"`
	Timestamp          int64  `json:"timestamp"`
	TraceTimeentryUnix int64  `json:"traceTimeentryUnix"`
}

// Site names from the message may not be what the name Ruci server has. This map matches them
var sitemap map[string]string

// Config variable represents configuration object
var Config Configuration

// Lfnsite for map of lfn and site
type Lfnsite struct {
	site string
	lfn  []string
}

// stompMgr
var stompMgr *lbstomp.StompManager

//rotation logger
type rotateLogger struct {
	RotateLogs *rotatelogs.RotateLogs
}

//for debugging, counting the how many message received when 1k is processed
var msgreceived uint64

//helper function to parse sitemap
func parseSitemap(mapFile string) error {
	data, err := ioutil.ReadFile(mapFile)
	if err != nil {
		log.Println("Unable to read sitemap file", err)
		return err
	}
	//log.Println(string(data))
	err = json.Unmarshal(data, &sitemap)
	if err != nil {
		log.Println("Unable to parse sitemap", err)
		return err
	}
	return nil
}

// helper function to parse configuration
func parseConfig(configFile string) error {
	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Println("Unable to read config file", err)
		return err
	}
	//log.Println(string(data))
	err = json.Unmarshal(data, &Config)
	if err != nil {
		log.Println("Unable to parse config", err)
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
	//log.Printf("%v", Config)
	return nil
}

func initStomp() {
	p := lbstomp.Config{
		URI:         Config.StompURI,
		Login:       Config.StompLogin,
		Password:    Config.StompPassword,
		Iterations:  Config.StompIterations,
		SendTimeout: Config.StompSendTimeout,
		RecvTimeout: Config.StompRecvTimeout,
		Endpoint:    Config.EndpointProducer,
		ContentType: Config.ContentType,
		Protocol:    Config.Protocol,
		Verbose:     Config.Verbose,
	}
	stompMgr = lbstomp.New(p)
	log.Println(stompMgr.String())
}

// FWJRconsumer Consumes for FWJR/WMArchive topic
func FWJRconsumer(msg *stomp.Message) ([]Lfnsite, int64, string, string, error) {
	//first to check to make sure there is something in msg,
	//otherwise we will get error:
	//Failed to continue - runtime error: invalid memory address or nil pointer dereference
	//[signal SIGSEGV: segmentation violation]
	//
	var lfnsite []Lfnsite
	var ls Lfnsite
	atomic.AddUint64(&msgreceived, 1)
	if msg == nil || msg.Body == nil {
		return lfnsite, 0, "", "", errors.New("Empty message")
	}
	//
	if Config.Verbose > 2 {
		log.Println("*****************Source AMQ message of wmarchive*********************")
		log.Println("Source AMQ message of wmarchive: ", string(msg.Body))
		log.Println("*******************End AMQ message of wmarchive**********************")
	}
	// Define FWJR Recod
	type MetaData struct {
		Ts      int64  `json:"ts"`
		JobType string `json:"jobtype"`
		WnName  string `json:"wn_name"`
	}

	type InputLst struct {
		Lfn    int    `json:"lfn"`
		Events int64  `json:"events"`
		GUID   string `json:"guid"`
	}
	type Step struct {
		Input []InputLst `json:"input"`
		Site  string     `json:"site"`
	}
	type FWJRRecord struct {
		LFNArray      []string
		LFNArrayRef   []string
		FallbackFiles []int    `json:"fallbackFiles"`
		Metadata      MetaData `json:"meta_data"`
		Steps         []Step   `json:"steps"`
	}
	var rec FWJRRecord
	err := json.Unmarshal(msg.Body, &rec)
	if err != nil {
		log.Printf("Enable to Unmarchal input message. Error: %v", err)
		return lfnsite, 0, "", "", err
	}
	if Config.Verbose > 2 {
		log.Printf("******PARSED FWJR record******: %+v", rec)
	}
	// process received message, e.g. extract some fields
	var ts int64
	var jobtype string
	var wnname string
	// Check the data
	if rec.Metadata.Ts == 0 {
		ts = time.Now().Unix()
	} else {
		ts = rec.Metadata.Ts
	}

	if len(rec.Metadata.JobType) > 0 {
		jobtype = rec.Metadata.JobType
	} else {
		jobtype = "unknown"
	}

	if len(rec.Metadata.WnName) > 0 {
		wnname = rec.Metadata.WnName
	} else {
		wnname = "unknown"
	}
	//
	for _, v := range rec.Steps {
		ls.site = v.Site
		var goodlfn []string
		for _, i := range v.Input {
			if len(i.GUID) > 0 && i.Events != 0 {
				lfn := i.Lfn
				if !insliceint(rec.FallbackFiles, lfn) {
					if inslicestr(rec.LFNArrayRef, "lfn") {
						if lfn < len(rec.LFNArray) {
							goodlfn = append(goodlfn, rec.LFNArray[lfn])
						}
					}
				}
			}

		}
		if len(goodlfn) > 0 {
			ls.lfn = goodlfn
			lfnsite = append(lfnsite, ls)
		}
	}
	return lfnsite, ts, jobtype, wnname, nil
}

// NewTrace create new instance of Trace
func NewTrace(lfn string, site string, ts int64, jobtype string, wnname string) Trace {
	trc := Trace{}
	trc.Account = "fwjr"
	trc.ClientState = "DONE"
	trc.Filename = lfn
	trc.DID = "cms:" + fmt.Sprintf("%v", trc.Filename)
	trc.EventType = "get"
	trc.EventVersion = "API_1.21.6"
	trc.FileReadts = ts
	trc.Jobtype = jobtype
	trc.RemoteSite = site
	trc.Scope = "cms"
	trc.Timestamp = trc.FileReadts
	trc.TraceTimeentryUnix = trc.FileReadts
	trc.Usrdn = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=yuyi/CN=639751/CN=Yuyi Guo/CN=706639693"
	trc.Wnname = wnname
	return trc
}

//FWJRtrace makes FWJR trace and send it to rucio endpoint
func FWJRtrace(msg *stomp.Message) ([]string, error) {
	var dids []string
	//get trace data
	lfnsite, ts, jobtype, wnname, err := FWJRconsumer(msg)
	if err != nil {
		log.Println("Bad FWJR message.")
		return nil, errors.New("Bad FWJR message")
	}
	for _, ls := range lfnsite {
		goodlfn := ls.lfn
		site := ls.site
		if len(goodlfn) > 0 && len(site) > 0 {
			if s, ok := sitemap[site]; ok {
				site = s
			}
			for _, glfn := range goodlfn {
				trc := NewTrace(glfn, site, ts, jobtype, wnname)
				data, err := json.Marshal(trc)
				if err != nil {
					if Config.Verbose > 0 {
						log.Printf("Unable to marshal back to JSON string , error: %v, data: %v\n", err, trc)
					} else {
						log.Printf("Unable to marshal back to JSON string, error: %v \n", err)
					}
					dids = append(dids, fmt.Sprintf("%v", trc.DID))
					continue
				}
				if Config.Verbose > 2 {
					log.Println("********* Rucio trace record ***************")
					log.Println("Rucio trace record: ", string(data))
					log.Println("******** Done Rucio trace record *************")
				}
				// send data to Stomp endpoint
				if Config.EndpointProducer != "" {
					err := stompMgr.Send(data)
					//totaltrace++
					if err != nil {
						dids = append(dids, fmt.Sprintf("%v", trc.DID))
						log.Printf("Failed to send %s to stomp.", trc.DID)
					}
				} else {
					log.Fatal("*** Config.Enpoint is empty, check config file! ***")
				}
			}
		}
	}
	return dids, nil
}

func server() {
	log.Println("Stomp broker URL: ", Config.StompURI)
	var err error
	var addr string
	var conn *stomp.Conn

	// get connection
	conn, addr, err = stompMgr.GetConnection()
	if err != nil {
		//try again
		conn, addr, err = stompMgr.GetConnection()
	}
	// always close connection
	//defer conn.Disconnect()

	// subscribe to ActiveMQ topic
	sub, err2 := conn.Subscribe(Config.EndpointConsumer, stomp.AckAuto)
	if err2 != nil {
		sub, err = conn.Subscribe(Config.EndpointConsumer, stomp.AckAuto)
	}
	if err != nil {
		log.Fatal(err)
	} else {
		log.Println("stomp connected to", addr)
	}

	var tc uint64
	t1 := time.Now().Unix()
	var t2 int64
	var ok bool
	var msg *stomp.Message
	for {
		// get stomp messages from subscriber channel
		select {
		case msg, ok = <-sub.C:
			if !ok {
				conn, addr, err = stompMgr.GetConnection()
				sub, _ = conn.Subscribe(Config.EndpointConsumer, stomp.AckAuto)
				msg, ok = <-sub.C
			}
			if !ok { //still not get the message, skip this case
				break
			}
			// process stomp messages
			dids, err := FWJRtrace(msg)
			if err == nil {
				atomic.AddUint64(&tc, 1)
				if Config.Verbose > 1 {
					log.Println("The number of traces processed in 1000 group: ", atomic.LoadUint64(&tc))
				}
			}
			//log.Println("processed:", tc)
			if atomic.LoadUint64(&tc) == 1000 {
				atomic.StoreUint64(&tc, 0)
				t2 = time.Now().Unix() - t1
				t1 = time.Now().Unix()
				log.Printf("Processing 1000 messages while total received %d messages.\n", atomic.LoadUint64(&msgreceived))
				log.Printf("Processing 1000 messages took %d seconds.\n", t2)
				atomic.StoreUint64(&msgreceived, 0)
			}
			if err != nil && err.Error() != "Empty message" {
				log.Println("FWJR message processing error", err)
			}
			//got error message "FWJR message processing error unexpected end of JSON input".
			//Code stoped to loop??? YG 2/22/2021
			if len(dids) > 0 {
				log.Printf("DIDS in Error: %v .\n ", dids)
			}
		default:
			sleep := time.Duration(Config.Interval) * time.Millisecond
			if Config.Verbose > 3 {
				log.Println("waiting for ", sleep)
			}
			time.Sleep(sleep)
		}
	}
}

func inslicestr(s []string, v string) bool {
	for i := range s {
		if v == s[i] {
			return true
		}
	}
	return false
}
func insliceint(s []int, v int) bool {
	for i := range s {
		if v == s[i] {
			return true
		}
	}
	return false
}

func main() {
	// usage: ./stompserver -config stompserverconfig.json -sitemap ruciositemap.json

	atomic.StoreUint64(&msgreceived, 0)
	//set up roration logs
	logName := "RucioTrace" + "-%Y%m%d"
	hostname, err := os.Hostname()
	if err == nil {
		logName = "RucioTrace" + "-" + hostname + "-%Y%m%d"
	}
	rlog, err := rotatelogs.New(logName)
	if err == nil {
		log.SetOutput(rlog)
	}
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	var config string
	var fsitemap string
	flag.StringVar(&config, "config", "", "config file name")
	flag.StringVar(&fsitemap, "sitemap", "", "runcio sitemap file")
	flag.Parse()
	err2 := parseConfig(config)
	if err2 != nil {
		log.Fatalf("Unable to parse config file %s, error: %v", config, err2)
	}
	err2 = parseSitemap(fsitemap)
	if err2 != nil {
		log.Fatalf("Unable to parse rucio sitemap file %s, error: %v", fsitemap, err2)
	}
	if Config.Verbose > 0 {
		log.Printf("%v", Config)
		log.Printf("%v", sitemap)
	}
	initStomp()
	server()
}
