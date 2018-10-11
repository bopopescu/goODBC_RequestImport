package main

import (
	_ "bufio"
	_ "encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	_ "github.com/alexbrainman/odbc"
	"github.com/hornbill/color"
	_ "github.com/hornbill/go-mssqldb" //Microsoft SQL Server driver - v2005+
	"github.com/hornbill/goApiLib"
	_ "github.com/hornbill/mysql" //MySQL v4.1 to v5.x and MariaDB driver
	_ "github.com/hornbill/pb"
	"github.com/hornbill/sqlx"
	_ "github.com/jnewmano/mysql320" //MySQL v3.2.0 to v5 driver - Provides SWSQL (MySQL 4.0.16) support
	"html"
	"log"
	"os"
	_ "path/filepath"
	_ "reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	version           = "0.1.1"
	appServiceManager = "com.hornbill.servicemanager"
	//Disk Space Declarations
	sizeKB float64 = 1 << (10 * 1)
	sizeMB float64 = 1 << (10 * 2)
	sizeGB float64 = 1 << (10 * 3)
	sizeTB float64 = 1 << (10 * 4)
	sizePB float64 = 1 << (10 * 5)
)

var (
	appDBDriver          string
	arrCallsLogged       = make(map[string]string)
	arrCallDetailsMaps   = make([]map[string]interface{}, 0)
	arrSWStatus          = make(map[string]string)
	boolConfLoaded       bool
	boolProcessClass     bool
	configFileName       string
	configZone           string
	configDryRun         bool
	configMaxRoutines    string
	connStrAppDB         string
	counters             counterTypeStruct
	mapGenericConf       swCallConfStruct
	analysts             []analystListStruct
	categories           []categoryListStruct
	closeCategories      []categoryListStruct
	customers            []customerListStruct
	priorities           []priorityListStruct
	services             []serviceListStruct
	sites                []siteListStruct
	teams                []teamListStruct
	importFiles          []fileAssocStruct
	sqlCallQuery         string
	swImportConf         swImportConfStruct
	timeNow              string
	callIDcolumn         string
	startTime            time.Time
	endTime              time.Duration
	espXmlmc             *apiLib.XmlmcInstStruct
	xmlmcInstanceConfig  xmlmcConfigStruct
	mutex                = &sync.Mutex{}
	mutexAnalysts        = &sync.Mutex{}
	mutexArrCallsLogged  = &sync.Mutex{}
	mutexBar             = &sync.Mutex{}
	mutexCategories      = &sync.Mutex{}
	mutexCloseCategories = &sync.Mutex{}
	mutexCustomers       = &sync.Mutex{}
	mutexPriorities      = &sync.Mutex{}
	mutexServices        = &sync.Mutex{}
	mutexSites           = &sync.Mutex{}
	mutexTeams           = &sync.Mutex{}
	wgRequest            sync.WaitGroup
	wgAssoc              sync.WaitGroup
	wgFile               sync.WaitGroup
	reqPrefix            string
	maxGoroutines        = 1
)

// ----- Structures -----
type counterTypeStruct struct {
	sync.Mutex
	created        int
	createdSkipped int
}

//----- Config Data Structs
type swImportConfStruct struct {
	HBConf                    hbConfStruct    //Hornbill Instance connection details
	DSNConf                   appDBConfStruct //App Data (swdata) connection details
	CustomerType              string
	SMProfileCodeSeperator    string
	ConfTimelineUpdate        swUpdateConfStruct
	ConfIncident              swCallConfStruct
	ConfServiceRequest        swCallConfStruct
	ConfChangeRequest         swCallConfStruct
	ConfProblem               swCallConfStruct
	ConfKnownError            swCallConfStruct
	PriorityMapping           map[string]interface{}
	TeamMapping               map[string]interface{}
	CategoryMapping           map[string]interface{}
	ResolutionCategoryMapping map[string]interface{}
	ServiceMapping            map[string]interface{}
	StatusMapping             map[string]interface{}
}

type swUpdateConfStruct struct {
	Updatedate    string
	Timespent     string
	Updatetype    string
	Updateindex   string
	Updateby      string
	Updatebyname  string
	Updatebygroup string
	Actiontype    string
	Actionsource  string
	Description   string
}
type hbConfStruct struct {
	APIKey     string
	InstanceID string
	URL        string
}
type sysDBConfStruct struct {
	Driver   string
	UserName string
	Password string
}
type appDBConfStruct struct {
	Driver   string
	Server   string
	UserName string
	Password string
	Port     int
	Database string
	Encrypt  bool
}
type swCallConfStruct struct {
	Import                 bool
	CallIDColumn           string
	CallClass              string
	DefaultTeam            string
	DefaultPriority        string
	DefaultService         string
	SQLStatement           string
	CoreFieldMapping       map[string]interface{}
	AdditionalFieldMapping map[string]interface{}
}

//----- XMLMC Config and Interaction Structs
type xmlmcConfigStruct struct {
	instance string
	url      string
	zone     string
}
type xmlmcResponse struct {
	MethodResult string      `xml:"status,attr"`
	State        stateStruct `xml:"state"`
}

//----- Shared Structs -----
type stateStruct struct {
	Code     string `xml:"code"`
	ErrorRet string `xml:"error"`
}

//----- Data Structs -----

type xmlmcSysSettingResponse struct {
	MethodResult string      `xml:"status,attr"`
	State        stateStruct `xml:"state"`
	Setting      string      `xml:"params>option>value"`
}

//----- Request Logged Structs
type xmlmcRequestResponseStruct struct {
	MethodResult string      `xml:"status,attr"`
	RequestID    string      `xml:"params>primaryEntityData>record>h_pk_reference"`
	SiteCountry  string      `xml:"params>rowData>row>h_country"`
	State        stateStruct `xml:"state"`
}
type xmlmcBPMSpawnedStruct struct {
	MethodResult string      `xml:"status,attr"`
	Identifier   string      `xml:"params>identifier"`
	State        stateStruct `xml:"state"`
}

//----- Site Structs
type siteListStruct struct {
	SiteName string
	SiteID   int
}
type xmlmcSiteListResponse struct {
	MethodResult string      `xml:"status,attr"`
	SiteID       int         `xml:"params>rowData>row>h_id"`
	SiteName     string      `xml:"params>rowData>row>h_site_name"`
	SiteCountry  string      `xml:"params>rowData>row>h_country"`
	State        stateStruct `xml:"state"`
}

//----- Priority Structs
type priorityListStruct struct {
	PriorityName string
	PriorityID   int
}
type xmlmcPriorityListResponse struct {
	MethodResult string      `xml:"status,attr"`
	PriorityID   int         `xml:"params>rowData>row>h_pk_priorityid"`
	PriorityName string      `xml:"params>rowData>row>h_priorityname"`
	State        stateStruct `xml:"state"`
}

//----- Service Structs
type serviceListStruct struct {
	ServiceName          string
	ServiceID            int
	ServiceBPMIncident   string
	ServiceBPMService    string
	ServiceBPMChange     string
	ServiceBPMProblem    string
	ServiceBPMKnownError string
}
type xmlmcServiceListResponse struct {
	MethodResult  string      `xml:"status,attr"`
	ServiceID     int         `xml:"params>rowData>row>h_pk_serviceid"`
	ServiceName   string      `xml:"params>rowData>row>h_servicename"`
	BPMIncident   string      `xml:"params>rowData>row>h_incident_bpm_name"`
	BPMService    string      `xml:"params>rowData>row>h_service_bpm_name"`
	BPMChange     string      `xml:"params>rowData>row>h_change_bpm_name"`
	BPMProblem    string      `xml:"params>rowData>row>h_problem_bpm_name"`
	BPMKnownError string      `xml:"params>rowData>row>h_knownerror_bpm_name"`
	State         stateStruct `xml:"state"`
}

//----- Team Structs
type teamListStruct struct {
	TeamName string
	TeamID   string
}
type xmlmcTeamListResponse struct {
	MethodResult string      `xml:"status,attr"`
	TeamID       string      `xml:"params>rowData>row>h_id"`
	TeamName     string      `xml:"params>rowData>row>h_name"`
	State        stateStruct `xml:"state"`
}

//----- Category Structs
type categoryListStruct struct {
	CategoryCode string
	CategoryID   string
	CategoryName string
}
type xmlmcCategoryListResponse struct {
	MethodResult string      `xml:"status,attr"`
	CategoryID   string      `xml:"params>id"`
	CategoryName string      `xml:"params>fullname"`
	State        stateStruct `xml:"state"`
}

//----- Audit Structs
type xmlmcAuditListResponse struct {
	MethodResult     string      `xml:"status,attr"`
	TotalStorage     float64     `xml:"params>maxStorageAvailble"`
	TotalStorageUsed float64     `xml:"params>totalStorageUsed"`
	State            stateStruct `xml:"state"`
}

//----- Analyst Structs
type analystListStruct struct {
	AnalystID   string
	AnalystName string
}
type xmlmcAnalystListResponse struct {
	MethodResult     string      `xml:"status,attr"`
	AnalystFullName  string      `xml:"params>name"`
	AnalystFirstName string      `xml:"params>firstName"`
	AnalystLastName  string      `xml:"params>lastName"`
	State            stateStruct `xml:"state"`
}

//----- Customer Structs
type customerListStruct struct {
	CustomerID   string
	CustomerName string
}
type xmlmcCustomerListResponse struct {
	MethodResult      string      `xml:"status,attr"`
	CustomerFirstName string      `xml:"params>firstName"`
	CustomerLastName  string      `xml:"params>lastName"`
	State             stateStruct `xml:"state"`
}

//----- Associated Record Struct
type reqRelStruct struct {
	MasterRef string `db:"fk_callref_m"`
	SlaveRef  string `db:"fk_callref_s"`
}

//----- File Attachment Structs
type xmlmcAttachmentResponse struct {
	MethodResult    string      `xml:"status,attr"`
	ContentLocation string      `xml:"params>contentLocation"`
	State           stateStruct `xml:"state"`
	HistFileID      string      `xml:"params>primaryEntityData>record>h_pk_fileid"`
}

//----- Email Attachment Structs
type xmlmcEmailAttachmentResponse struct {
	MethodResult string            `xml:"status,attr"`
	Recipients   []recipientStruct `xml:"params>recipient"`
	Subject      string            `xml:"params>subject"`
	Body         string            `xml:"params>body"`
	HTMLBody     string            `xml:"params>htmlBody"`
	TimeSent     string            `xml:"params>timeSent"`
	State        stateStruct       `xml:"state"`
}
type recipientStruct struct {
	Class   string `xml:"class"`
	Address string `xml:"address"`
	Name    string `xml:"name"`
}

//----- File Attachment Struct
type fileAssocStruct struct {
	ImportRef  int
	SmCallRef  string
	FileID     string  `db:"fileid"`
	CallRef    string  `db:"callref"`
	DataID     string  `db:"dataid"`
	UpdateID   string  `db:"updateid"`
	Compressed string  `db:"compressed"`
	SizeU      float64 `db:"sizeu"`
	SizeC      float64 `db:"sizec"`
	FileName   string  `db:"filename"`
	AddedBy    string  `db:"addedby"`
	TimeAdded  string  `db:"timeadded"`
	FileTime   string  `db:"filetime"`
}

// main package

func validateConf() error {

	//-- Check for API Key
	if swImportConf.HBConf.APIKey == "" {
		err := errors.New("API Key is not set")
		return err
	}
	//-- Check for Instance ID
	if swImportConf.HBConf.InstanceID == "" {
		err := errors.New("InstanceID is not set")
		return err
	}

	//-- Process Config File

	return nil
}

func main() {
	//-- Start Time for Durration
	startTime = time.Now()
	//-- Start Time for Log File
	timeNow = time.Now().Format(time.RFC3339)
	timeNow = strings.Replace(timeNow, ":", "-", -1)

	arrSWStatus["1"] = "status.open"
	arrSWStatus["2"] = "status.open"
	arrSWStatus["3"] = "status.open"
	arrSWStatus["4"] = "status.onHold"
	arrSWStatus["5"] = "status.open"
	arrSWStatus["6"] = "status.resolved"
	arrSWStatus["8"] = "status.new"
	arrSWStatus["9"] = "status.open"
	arrSWStatus["10"] = "status.open"
	arrSWStatus["11"] = "status.open"
	arrSWStatus["16"] = "status.closed"
	arrSWStatus["17"] = "status.cancelled"
	arrSWStatus["18"] = "status.closed"

	//-- Grab and Parse Flags
	flag.StringVar(&configFileName, "file", "conf.json", "Name of the configuration file to load")
	flag.StringVar(&configZone, "zone", "eur", "Override the default Zone the instance sits in")
	flag.BoolVar(&configDryRun, "dryrun", false, "Dump import XML to log instead of creating requests")
	flag.StringVar(&configMaxRoutines, "concurrent", "1", "Maximum number of requests to import concurrently.")
	flag.Parse()

	//-- Output to CLI and Log
	logger(1, "---- Supportworks Call Import Utility V"+fmt.Sprintf("%v", version)+" ----", true)
	logger(1, "Flag - Config File "+fmt.Sprintf("%s", configFileName), true)
	logger(1, "Flag - Zone "+fmt.Sprintf("%s", configZone), true)
	logger(1, "Flag - Dry Run "+fmt.Sprintf("%v", configDryRun), true)
	logger(1, "Flag - Concurrent Requests "+fmt.Sprintf("%v", configMaxRoutines), true)

	//Check maxGoroutines for valid value
	maxRoutines, err := strconv.Atoi(configMaxRoutines)
	if err != nil {
		color.Red("Unable to convert maximum concurrency of [" + configMaxRoutines + "] to type INT for processing")
		return
	}
	maxGoroutines = maxRoutines

	if maxGoroutines < 1 || maxGoroutines > 10 {
		color.Red("The maximum concurrent requests allowed is between 1 and 10 (inclusive).\n\n")
		color.Red("You have selected " + configMaxRoutines + ". Please try again, with a valid value against ")
		color.Red("the -concurrent switch.")
		return
	}

	//-- Load Configuration File Into Struct
	swImportConf, boolConfLoaded = loadConfig()
	if boolConfLoaded != true {
		logger(4, "Unable to load config, process closing.", true)
		return
	}

	errc := validateConf()
	if errc != nil {
		logger(4, fmt.Sprintf("%v", errc), true)
		logger(4, "Please Check your Configuration File: "+fmt.Sprintf("%s", configFileName), true)
		return
	}

	//Set SQL driver ID string for Application Data
	if swImportConf.DSNConf.Driver == "" {
		logger(4, "DSNConf SQL Driver not set in configuration.", true)
		return
	}
	if swImportConf.DSNConf.Driver == "swsql" {
		appDBDriver = "mysql320"
	} else if swImportConf.DSNConf.Driver == "mysql" || swImportConf.DSNConf.Driver == "mssql" || swImportConf.DSNConf.Driver == "mysql320" {
		appDBDriver = swImportConf.DSNConf.Driver
	} else if swImportConf.DSNConf.Driver == "odbc" || swImportConf.DSNConf.Driver == "xls" || swImportConf.DSNConf.Driver == "csv" {
		appDBDriver = swImportConf.DSNConf.Driver
	} else {
		logger(4, "The SQL driver ("+swImportConf.DSNConf.Driver+") for the Supportworks Application Database specified in the configuration file is not valid.", true)
		return
	}

	//-- Set Instance ID
	SetInstance(configZone, swImportConf.HBConf.InstanceID)
	//-- Generate Instance XMLMC Endpoint
	swImportConf.HBConf.URL = getInstanceURL()

	//-- Defer log out of Hornbill instance until after main() is complete
	defer logout()

	//-- Build DB connection strings
	connStrAppDB = buildConnectionString()

	//Process Incidents
	mapGenericConf = swImportConf.ConfIncident
	if mapGenericConf.Import == true {
		reqPrefix = getRequestPrefix("IN")
		processCallData()
	}
	//Process Service Requests
	mapGenericConf = swImportConf.ConfServiceRequest
	if mapGenericConf.Import == true {
		reqPrefix = getRequestPrefix("SR")
		processCallData()
	}
	//Process Change Requests
	mapGenericConf = swImportConf.ConfChangeRequest
	if mapGenericConf.Import == true {
		reqPrefix = getRequestPrefix("CH")
		processCallData()
	}
	//Process Problems
	mapGenericConf = swImportConf.ConfProblem
	if mapGenericConf.Import == true {
		reqPrefix = getRequestPrefix("PM")
		processCallData()
	}
	//Process Known Errors
	mapGenericConf = swImportConf.ConfKnownError
	if mapGenericConf.Import == true {
		reqPrefix = getRequestPrefix("KE")
		processCallData()
	}

	if len(arrCallsLogged) > 0 {
		//We have new calls logged - process associations
		processCallAssociations()

	}

	//-- End output
	logger(1, "Requests Logged: "+fmt.Sprintf("%d", counters.created), true)
	logger(1, "Requests Skipped: "+fmt.Sprintf("%d", counters.createdSkipped), true)
	//-- Show Time Takens
	endTime = time.Now().Sub(startTime)
	logger(1, "Time Taken: "+fmt.Sprintf("%v", endTime), true)
	logger(1, "---- Supportworks Call Import Complete ---- ", true)
}

//getRequestPrefix - gets and returns current maxResultsAllowed sys setting value
func getRequestPrefix(callclass string) string {
	espXmlmc, sessErr := NewEspXmlmcSession()
	if sessErr != nil {
		logger(4, "Unable to attach to XMLMC session to get Request Prefix. Using default ["+callclass+"].", false)
		return callclass
	}
	strSetting := ""
	switch callclass {
	case "IN":
		strSetting = "guest.app.requests.types.IN"
	case "SR":
		strSetting = "guest.app.requests.types.SR"
	case "CH":
		strSetting = "app.requests.types.CH"
	case "PM":
		strSetting = "app.requests.types.PM"
	case "KE":
		strSetting = "app.requests.types.KE"
	}

	espXmlmc.SetParam("appName", appServiceManager)
	espXmlmc.SetParam("filter", strSetting)
	response, err := espXmlmc.Invoke("admin", "appOptionGet")
	if err != nil {
		logger(4, "Could not retrieve System Setting for Request Prefix. Using default ["+callclass+"].", false)
		return callclass
	}
	var xmlRespon xmlmcSysSettingResponse
	err = xml.Unmarshal([]byte(response), &xmlRespon)
	if err != nil {
		logger(4, "Could not retrieve System Setting for Request Prefix. Using default ["+callclass+"].", false)
		return callclass
	}
	if xmlRespon.MethodResult != "ok" {
		logger(4, "Could not retrieve System Setting for Request Prefix: "+xmlRespon.MethodResult, false)
		return callclass
	}
	return xmlRespon.Setting
}

//confirmResponse - prompts user, expects a fuzzy yes or no response, does not continue until this is given
func confirmResponse() bool {
	var cmdResponse string
	_, errResponse := fmt.Scanln(&cmdResponse)
	if errResponse != nil {
		log.Fatal(errResponse)
	}
	if cmdResponse == "y" || cmdResponse == "yes" || cmdResponse == "Y" || cmdResponse == "Yes" || cmdResponse == "YES" {
		return true
	} else if cmdResponse == "n" || cmdResponse == "no" || cmdResponse == "N" || cmdResponse == "No" || cmdResponse == "NO" {
		return false
	} else {
		color.Red("Please enter yes or no to continue:")
		return confirmResponse()
	}
}

//convFloattoSizeStr - takes given float64 value, returns a human readable storage capacity string
func convFloattoSizeStr(floatNum float64) (strReturn string) {
	if floatNum >= sizePB {
		strReturn = fmt.Sprintf("%.2fPB", floatNum/sizePB)
	} else if floatNum >= sizeTB {
		strReturn = fmt.Sprintf("%.2fTB", floatNum/sizeTB)
	} else if floatNum >= sizeGB {
		strReturn = fmt.Sprintf("%.2fGB", floatNum/sizeGB)
	} else if floatNum >= sizeMB {
		strReturn = fmt.Sprintf("%.2fMB", floatNum/sizeMB)
	} else if floatNum >= sizeKB {
		strReturn = fmt.Sprintf("%.2fKB", floatNum/sizeKB)
	} else {
		strReturn = fmt.Sprintf("%vB", int(floatNum))
	}
	return
}

//getInstanceFreeSpace - calculates how much storage is available on the given Hornbill instance
func getInstanceFreeSpace() (int64, int64, string, string) {
	var fltTotalSpace float64
	var fltFreeSpace float64
	var strTotalSpace string
	var strFreeSpace string

	XMLAudit, xmlmcErr := espXmlmc.Invoke("admin", "getInstanceAuditInfo")
	if xmlmcErr != nil {
		logger(4, "Could not return Instance Audit Information: "+fmt.Sprintf("%v", xmlmcErr), true)
		return 0, 0, "0B", "0B"
	}
	var xmlRespon xmlmcAuditListResponse

	err := xml.Unmarshal([]byte(XMLAudit), &xmlRespon)
	if err != nil {
		logger(4, "Could not return Instance Audit Information: "+fmt.Sprintf("%v", err), true)
	} else {
		if xmlRespon.MethodResult != "ok" {
			logger(4, "Could not return Instance Audit Information: "+xmlRespon.State.ErrorRet, true)
		} else {
			//-- Check Response
			if xmlRespon.TotalStorage > 0 && xmlRespon.TotalStorageUsed > 0 {
				fltTotalSpace = xmlRespon.TotalStorage
				fltFreeSpace = xmlRespon.TotalStorage - xmlRespon.TotalStorageUsed
				strTotalSpace = convFloattoSizeStr(fltTotalSpace)
				strFreeSpace = convFloattoSizeStr(fltFreeSpace)
			}
		}
	}
	return int64(fltTotalSpace), int64(fltFreeSpace), strTotalSpace, strFreeSpace
}

//processCallAssociations - Get all records from swdata.cmn_rel_opencall_oc, process accordingly
func processCallAssociations() {
	logger(1, "Processing Request Associations, please wait...", true)
	//Connect to the JSON specified DB
	db, err := sqlx.Open(appDBDriver, connStrAppDB)
	defer db.Close()
	if err != nil {
		logger(4, " [DATABASE] Database Connection Error for Request Associations: "+fmt.Sprintf("%v", err), false)
		return
	}
	//Check connection is open
	err = db.Ping()
	if err != nil {
		logger(4, " [DATABASE] [PING] Database Connection Error for Request Associations: "+fmt.Sprintf("%v", err), false)
		return
	}
	logger(3, "[DATABASE] Connection Successful", false)
	logger(3, "[DATABASE] Running query for Request Associations. Please wait...", false)

	//build query
	sqlDiaryQuery := "SELECT fk_callref_m, fk_callref_s from cmn_rel_opencall_oc "
	logger(3, "[DATABASE] Request Association Query: "+sqlDiaryQuery, false)
	//Run Query
	rows, err := db.Queryx(sqlDiaryQuery)
	if err != nil {
		logger(4, " Database Query Error: "+fmt.Sprintf("%v", err), false)
		return
	}
	//Process each association record, insert in to Hornbill
	//fmt.Println("Maximum Request Association Go Routines:", maxGoroutines)
	maxGoroutinesGuard := make(chan struct{}, maxGoroutines)
	for rows.Next() {
		var requestRels reqRelStruct

		errDataMap := rows.StructScan(&requestRels)
		if errDataMap != nil {
			logger(4, " Data Mapping Error: "+fmt.Sprintf("%v", errDataMap), false)
			return
		}
		smMasterRef, mrOK := arrCallsLogged[requestRels.MasterRef]
		smSlaveRef, srOK := arrCallsLogged[requestRels.SlaveRef]
		maxGoroutinesGuard <- struct{}{}
		wgAssoc.Add(1)
		go func() {
			defer wgAssoc.Done()
			if mrOK == true && smMasterRef != "" && srOK == true && smSlaveRef != "" {
				//We have Master and Slave calls matched in the SM database
				addAssocRecord(smMasterRef, smSlaveRef)
			}
			<-maxGoroutinesGuard
		}()
	}
	wgAssoc.Wait()
	logger(1, "Request Association Processing Complete", true)
}

//addAssocRecord - given a Master Reference and a Slave Refernce, adds a call association record to Service Manager
func addAssocRecord(masterRef, slaveRef string) {
	espXmlmc, err := NewEspXmlmcSession()
	if err != nil {
		return
	}
	espXmlmc.SetParam("application", appServiceManager)
	espXmlmc.SetParam("entity", "RelatedRequests")
	espXmlmc.OpenElement("primaryEntityData")
	espXmlmc.OpenElement("record")
	espXmlmc.SetParam("h_fk_parentrequestid", masterRef)
	espXmlmc.SetParam("h_fk_childrequestid", slaveRef)
	espXmlmc.CloseElement("record")
	espXmlmc.CloseElement("primaryEntityData")
	XMLUpdate, xmlmcErr := espXmlmc.Invoke("data", "entityAddRecord")
	if xmlmcErr != nil {
		//		log.Fatal(xmlmcErr)
		logger(4, "Unable to create Request Association between ["+masterRef+"] and ["+slaveRef+"] :"+fmt.Sprintf("%v", xmlmcErr), false)
		return
	}
	var xmlRespon xmlmcResponse
	errXMLMC := xml.Unmarshal([]byte(XMLUpdate), &xmlRespon)
	if errXMLMC != nil {
		logger(4, "Unable to read response from Hornbill instance for Request Association between ["+masterRef+"] and ["+slaveRef+"] :"+fmt.Sprintf("%v", errXMLMC), false)
		return
	}
	if xmlRespon.MethodResult != "ok" {
		logger(3, "Unable to add Request Association between ["+masterRef+"] and ["+slaveRef+"] : "+xmlRespon.State.ErrorRet, false)
		return
	}
	logger(1, "Request Association Success between ["+masterRef+"] and ["+slaveRef+"]", false)
}

//processCallData - Query Supportworks call data, process accordingly
func processCallData() {

	if mapGenericConf.CallClass == "" || connStrAppDB == "" || mapGenericConf.CallIDColumn == "" {
		return
	}
	//Connect to the JSON specified DB
	db, err := sqlx.Open(appDBDriver, connStrAppDB)
	defer db.Close()
	if err != nil {
		logger(4, " [DATABASE] Database Connection Error: "+fmt.Sprintf("%v", err), true)
		return
	}
	//Check connection is open
	err = db.Ping()
	if err != nil {
		logger(4, " [DATABASE] [PING] Database Connection Error: "+fmt.Sprintf("%v", err), true)
		return
	}
	logger(3, "[DATABASE] Connection Successful", true)
	logger(3, "[DATABASE] Running query for calls of class "+mapGenericConf.CallClass+". Please wait...", true)

	//build query
	sqlCallQuery = mapGenericConf.SQLStatement
	logger(3, "[DATABASE] Query to retrieve "+mapGenericConf.CallClass+" calls using: "+sqlCallQuery, false)

	//Run Query
	rows, err := db.Queryx(sqlCallQuery)
	if err != nil {
		logger(4, " Database Query Error: "+fmt.Sprintf("%v", err), true)
		return
	}
	//Clear down existing Call Details map
	arrCallDetailsMaps = nil
	//Build map full of calls to import
	intCallCount := 0
	intRowCount := 0
	intUpdCount := 0
	callIDcolumn = mapGenericConf.CallIDColumn
	oldCallRef := 0
	hbCallRef := ""
	boolCallLogged := false
	//	espXmlmc = apiLib.NewXmlmcInstance(swImportConf.HBConf.URL)
	//    espXmlmc.SetAPIKey(swImportConf.HBConf.APIKey)

	for rows.Next() {
		results := make(map[string]interface{})
		err = rows.MapScan(results)
		intRowCount++
		//if intRowCount == 1 {
		//    fmt.Println(results)
		//}
		callMap := results
		/*
		   if intRowCount == 1 {
		           fmt.Println(callMap["Call Number"])
		           fmt.Println(reflect.TypeOf(callMap["Call Number"]))
		           vvv := fmt.Sprint("%s", callMap["Call Number"])
		           ppp, _ := strconv.Atoi(vvv)
		           fmt.Sprintln("ABC: %d", ppp)
		           fmt.Print(callMap["Open flag"])
		           vv := fmt.Sprint("%s", callMap["Open flag"])
		           fmt.Println(vv)
		           fmt.Println("aa")
		           //fmt.Println(callMap[callIDcolumn])
		       }
		*/ //really need to work it here.
		// LOG the call if there is a new call number
		bUpdate := true
		if callMap[callIDcolumn] != nil {
			var y int = int(callMap[callIDcolumn].(float64))

			strRef := strconv.Itoa(y)
			callMap[callIDcolumn] = strRef
			if oldCallRef != y { //    CallIDColumn
				//    "CallIDColumn": "Call Number",
				fmt.Println(hbCallRef)
				fmt.Print(y)

				boolCallLogged, hbCallRef = logNewCall(mapGenericConf.CallClass, callMap) //, callID)
				if boolCallLogged {
					logger(3, "[REQUEST LOGGED] Request logged successfully: "+hbCallRef+" from call "+strRef, false)
					intCallCount++
					oldCallRef = y //fmt.Sprintf("%v", callMap[callIDcolumn])
				} else {
					logger(4, mapGenericConf.CallClass+" call log failed: "+strRef, false)
				}

				//oldCallRef = callMap[callIDcolumn]
				bUpdate = false
			} else {
				//same callref so might as well update.

			}
		}

		if bUpdate {
			// callref not set, so part of previous call, hence update call
			if updateCall(hbCallRef, callMap) {
				intUpdCount++
				fmt.Print(".")
			}

		}

		//Stick marshalled data map in to parent slice
		//		arrCallDetailsMaps = append(arrCallDetailsMaps, results)
	}
	fmt.Sprintln("%d Rows Processed", intRowCount)
	fmt.Sprintln("%d New Calls Logged", intCallCount)
	fmt.Sprintln("%d Updates Applied", intUpdCount)
	defer rows.Close()

	/*
				callRecordCallref := callRecord["callref"]

				go func() {
					defer wgRequest.Done()
					time.Sleep(1 * time.Millisecond)
					mutexBar.Lock()
					bar.Increment()
					mutexBar.Unlock()
					//callID := fmt.Sprintf("%s", callRecordCallref)
					callID := ""
					if callInt, ok := callRecordCallref.(int64); ok {
						callID = strconv.FormatInt(callInt, 10)
					} else {
						callID = fmt.Sprintf("%s", callRecordCallref)
					}

					currentCallRef := padCallRef(callID, "F", 7)

					boolCallLogged, hbCallRef := logNewCall(mapGenericConf.CallClass, callRecordArr, callID)
					if boolCallLogged {
						logger(3, "[REQUEST LOGGED] Request logged successfully: "+hbCallRef+" from Supportworks call "+currentCallRef, false)
					} else {
						logger(4, mapGenericConf.CallClass+" call log failed: "+currentCallRef, false)
					}
					<-maxGoroutinesGuard
				}()
			}
			wgRequest.Wait()

			bar.FinishPrint(mapGenericConf.CallClass + " Call Import Complete")
		} else {
			logger(4, "Call Search Failed for Call Class: "+mapGenericConf.CallClass, true)
		}
	*/
}

//logNewCall - Function takes Supportworks call data in a map, and logs to Hornbill
func logNewCall(callClass string, callMap map[string]interface{}) (bool, string) {

	boolCallLoggedOK := false
	strNewCallRef := ""

	strStatus := ""
	boolOnHoldRequest := false
	statusMapping := fmt.Sprintf("%v", mapGenericConf.CoreFieldMapping["h_status"])
	//fmt.Println(statusMapping);
	if statusMapping != "" {
		/*		if statusMapping == "16" || statusMapping == "18" {
					strStatus = arrSWStatus["6"]
				} else {
					strStatus = arrSWStatus[getFieldValue(statusMapping, callMap)]
				}
		*/
		strStatus = fmt.Sprintf("%s", swImportConf.StatusMapping[getFieldValue(statusMapping, callMap)])
	}
	//fmt.Println(strStatus);
	espXmlmc, err := NewEspXmlmcSession()
	if err != nil {
		return false, ""
	}

	espXmlmc.SetParam("application", appServiceManager)
	espXmlmc.SetParam("entity", "Requests")
	espXmlmc.SetParam("returnModifiedData", "true")
	espXmlmc.OpenElement("primaryEntityData")
	espXmlmc.OpenElement("record")
	strAttribute := ""
	strMapping := ""
	strServiceBPM := ""
	boolUpdateLogDate := false
	strLoggedDate := ""
	strClosedDate := ""
	//Loop through core fields from config, add to XMLMC Params
	for k, v := range mapGenericConf.CoreFieldMapping {
		boolAutoProcess := true
		strAttribute = fmt.Sprintf("%v", k)
		strMapping = fmt.Sprintf("%v", v)

		//Owning Analyst Name
		if strAttribute == "h_ownerid" {
			strOwnerID := getFieldValue(strMapping, callMap)
			if strOwnerID != "" {
				boolAnalystExists := doesAnalystExist(strOwnerID)
				if boolAnalystExists {
					//Get analyst from cache as exists
					analystIsInCache, strOwnerName := recordInCache(strOwnerID, "Analyst")
					if analystIsInCache && strOwnerName != "" {
						espXmlmc.SetParam(strAttribute, strOwnerID)
						espXmlmc.SetParam("h_ownername", strOwnerName)
					}
				}
			}
			boolAutoProcess = false
		}

		//Customer ID & Name
		if strAttribute == "h_fk_user_id" {
			strCustID := getFieldValue(strMapping, callMap)
			if strCustID != "" {
				boolCustExists := doesCustomerExist(strCustID)
				if boolCustExists {
					//Get customer from cache as exists
					customerIsInCache, strCustName := recordInCache(strCustID, "Customer")
					if customerIsInCache && strCustName != "" {
						espXmlmc.SetParam(strAttribute, strCustID)
						espXmlmc.SetParam("h_fk_user_name", strCustName)
					}
				}
			}
			boolAutoProcess = false
		}

		//Priority ID & Name
		//-- Get Priority ID
		if strAttribute == "h_fk_priorityid" {
			strPriorityID := getFieldValue(strMapping, callMap)
			strPriorityMapped, strPriorityName := getCallPriorityID(strPriorityID)
			if strPriorityMapped == "" && mapGenericConf.DefaultPriority != "" {
				strPriorityID = getPriorityID(mapGenericConf.DefaultPriority)
				strPriorityName = mapGenericConf.DefaultPriority
			}
			espXmlmc.SetParam(strAttribute, strPriorityMapped)
			espXmlmc.SetParam("h_fk_priorityname", strPriorityName)
			boolAutoProcess = false
		}

		// Category ID & Name
		if strAttribute == "h_category_id" && strMapping != "" {
			//-- Get Call Category ID
			strCategoryID, strCategoryName := getCallCategoryID(callMap, "Request")
			if strCategoryID != "" && strCategoryName != "" {
				espXmlmc.SetParam(strAttribute, strCategoryID)
				espXmlmc.SetParam("h_category", strCategoryName)
			}
			boolAutoProcess = false
		}

		// Closure Category ID & Name
		if strAttribute == "h_closure_category_id" && strMapping != "" {
			strClosureCategoryID, strClosureCategoryName := getCallCategoryID(callMap, "Closure")
			if strClosureCategoryID != "" {
				espXmlmc.SetParam(strAttribute, strClosureCategoryID)
				espXmlmc.SetParam("h_closure_category", strClosureCategoryName)
			}
			boolAutoProcess = false
		}

		// Service ID & Name, & BPM Workflow
		if strAttribute == "h_fk_serviceid" {
			//-- Get Service ID
			swServiceID := getFieldValue(strMapping, callMap)
			strServiceID := getCallServiceID(swServiceID)
			if strServiceID == "" && mapGenericConf.DefaultService != "" {
				strServiceID = getServiceID(mapGenericConf.DefaultService)
			}
			if strServiceID != "" {
				//-- Get record from Service Cache
				strServiceName := ""
				mutexServices.Lock()
				for _, service := range services {
					if strconv.Itoa(service.ServiceID) == strServiceID {
						strServiceName = service.ServiceName
						switch callClass {
						case "Incident":
							strServiceBPM = service.ServiceBPMIncident
						case "Service Request":
							strServiceBPM = service.ServiceBPMService
						case "Change Request":
							strServiceBPM = service.ServiceBPMChange
						case "Problem":
							strServiceBPM = service.ServiceBPMProblem
						case "Known Error":
							strServiceBPM = service.ServiceBPMKnownError
						}
					}
				}
				mutexServices.Unlock()

				if strServiceName != "" {
					espXmlmc.SetParam(strAttribute, strServiceID)
					espXmlmc.SetParam("h_fk_servicename", strServiceName)
				}
			}
			boolAutoProcess = false
		}

		// Team ID and Name
		if strAttribute == "h_fk_team_id" {
			//-- Get Team ID
			swTeamID := getFieldValue(strMapping, callMap)
			strTeamID, strTeamName := getCallTeamID(swTeamID)
			if strTeamID == "" && mapGenericConf.DefaultTeam != "" {
				strTeamName = mapGenericConf.DefaultTeam
				strTeamID = getTeamID(strTeamName)
			}
			if strTeamID != "" && strTeamName != "" {
				espXmlmc.SetParam(strAttribute, strTeamID)
				espXmlmc.SetParam("h_fk_team_name", strTeamName)
			}
			boolAutoProcess = false
		}

		// Site ID and Name
		if strAttribute == "h_site_id" {
			//-- Get site ID
			siteID, siteName := getSiteID(callMap)
			if siteID != "" && siteName != "" {
				espXmlmc.SetParam(strAttribute, siteID)
				espXmlmc.SetParam("h_site", siteName)
			}
			boolAutoProcess = false
		}

		// Resolved Date/Time
		if strAttribute == "h_dateresolved" && strMapping != "" && (strStatus == "status.resolved" || strStatus == "status.closed") {
			resolvedEPOCH := getFieldValue(strMapping, callMap)
			if resolvedEPOCH != "" && resolvedEPOCH != "0" {
				strResolvedDate := epochToDateTime(resolvedEPOCH)
				if strResolvedDate != "" {
					espXmlmc.SetParam(strAttribute, strResolvedDate)
				}
			}
		}

		// Closed Date/Time
		if strAttribute == "h_dateclosed" && strMapping != "" && (strStatus == "status.resolved" || strStatus == "status.closed" || strStatus == "status.onHold") {
			closedEPOCH := getFieldValue(strMapping, callMap)
			if closedEPOCH != "" && closedEPOCH != "0" {
				strClosedDate = epochToDateTime(closedEPOCH)
				if strClosedDate != "" && strStatus != "status.onHold" {
					espXmlmc.SetParam(strAttribute, strClosedDate)
				}
			}
		}

		// Request Status
		if strAttribute == "h_status" {
			if strStatus == "status.onHold" {
				strStatus = "status.open"
				boolOnHoldRequest = true
			}
			espXmlmc.SetParam(strAttribute, strStatus)
			boolAutoProcess = false
		}

		// Log Date/Time - setup ready to be processed after call logged
		if strAttribute == "h_datelogged" && strMapping != "" {
			loggedEPOCH := getFieldValue(strMapping, callMap)
			if loggedEPOCH != "" && loggedEPOCH != "0" {
				strLoggedDate = epochToDateTime(loggedEPOCH)
				if strLoggedDate != "" {
					boolUpdateLogDate = true
				}
			}
		}

		if strAttribute == "h_summary" && strMapping != "" && getFieldValue(strMapping, callMap) != "" {
			q := getFieldValue(strMapping, callMap)
			if len(q) > 253 {
				q = q[0:250] + "..."
			}
			espXmlmc.SetParam(strAttribute, q)
		}
		//Everything Else
		if boolAutoProcess &&
			strAttribute != "h_status" &&
			strAttribute != "h_requesttype" &&
			strAttribute != "h_request_prefix" &&
			strAttribute != "h_category" &&
			strAttribute != "h_closure_category" &&
			strAttribute != "h_fk_servicename" &&
			strAttribute != "h_fk_team_name" &&
			strAttribute != "h_site" &&
			strAttribute != "h_fk_priorityname" &&
			strAttribute != "h_ownername" &&
			strAttribute != "h_fk_user_name" &&
			strAttribute != "h_datelogged" &&
			strAttribute != "h_dateresolved" &&
			strAttribute != "h_summary" &&
			strAttribute != "h_dateclosed" {

			if strMapping != "" && getFieldValue(strMapping, callMap) != "" {
				espXmlmc.SetParam(strAttribute, getFieldValue(strMapping, callMap))
			}
		}

	}

	//Add request class & prefix
	espXmlmc.SetParam("h_requesttype", callClass)
	espXmlmc.SetParam("h_request_prefix", reqPrefix)

	espXmlmc.CloseElement("record")
	espXmlmc.CloseElement("primaryEntityData")

	//Class Specific Data Insert
	espXmlmc.OpenElement("relatedEntityData")
	espXmlmc.SetParam("relationshipName", "Call Type")
	espXmlmc.SetParam("entityAction", "insert")
	espXmlmc.OpenElement("record")
	strAttribute = ""
	strMapping = ""
	//Loop through AdditionalFieldMapping fields from config, add to XMLMC Params if not empty
	for k, v := range mapGenericConf.AdditionalFieldMapping {
		strAttribute = fmt.Sprintf("%v", k)
		strMapping = fmt.Sprintf("%v", v)
		if strMapping != "" && getFieldValue(strMapping, callMap) != "" {
			espXmlmc.SetParam(strAttribute, getFieldValue(strMapping, callMap))
		}
	}

	espXmlmc.CloseElement("record")
	espXmlmc.CloseElement("relatedEntityData")

	//Extended Data Insert
	espXmlmc.OpenElement("relatedEntityData")
	espXmlmc.SetParam("relationshipName", "Extended Information")
	espXmlmc.SetParam("entityAction", "insert")
	espXmlmc.OpenElement("record")
	espXmlmc.SetParam("h_request_type", callClass)
	strAttribute = ""
	strMapping = ""
	//Loop through AdditionalFieldMapping fields from config, add to XMLMC Params if not empty
	for k, v := range mapGenericConf.AdditionalFieldMapping {
		strAttribute = fmt.Sprintf("%v", k)
		strSubString := "h_custom_"
		if strings.Contains(strAttribute, strSubString) {
			strAttribute = convExtendedColName(strAttribute)
			strMapping = fmt.Sprintf("%v", v)
			if strMapping != "" && getFieldValue(strMapping, callMap) != "" {
				espXmlmc.SetParam(strAttribute, getFieldValue(strMapping, callMap))
			}
		}
	}

	espXmlmc.CloseElement("record")
	espXmlmc.CloseElement("relatedEntityData")

	//-- Check for Dry Run
	if configDryRun != true {

		XMLCreate, xmlmcErr := espXmlmc.Invoke("data", "entityAddRecord")
		if xmlmcErr != nil {
			//log.Fatal(xmlmcErr)
			logger(4, "Unable to log request on Hornbill instance:"+fmt.Sprintf("%v", xmlmcErr), false)
			return false, "No"
		}
		var xmlRespon xmlmcRequestResponseStruct

		err := xml.Unmarshal([]byte(XMLCreate), &xmlRespon)
		if err != nil {
			counters.Lock()
			counters.createdSkipped++
			counters.Unlock()
			logger(4, "Unable to read response from Hornbill instance:"+fmt.Sprintf("%v", err), false)
			return false, "No"
		}
		if xmlRespon.MethodResult != "ok" {
			logger(4, "Unable to log request: "+xmlRespon.State.ErrorRet, false)
			counters.Lock()
			counters.createdSkipped++
			counters.Unlock()
		} else {
			strNewCallRef = xmlRespon.RequestID

			mutexArrCallsLogged.Lock()
			//####arrCallsLogged[swCallID] = strNewCallRef
			mutexArrCallsLogged.Unlock()

			counters.Lock()
			counters.created++
			counters.Unlock()
			boolCallLoggedOK = true

			//Now update the request to create the activity stream
			espXmlmc.SetParam("socialObjectRef", "urn:sys:entity:"+appServiceManager+":Requests:"+strNewCallRef)
			espXmlmc.SetParam("content", "Request imported from Supportworks")
			espXmlmc.SetParam("visibility", "public")
			espXmlmc.SetParam("type", "Logged")
			fixed, err := espXmlmc.Invoke("activity", "postMessage")
			if err != nil {
				logger(5, "Activity Stream Creation failed for Request: "+strNewCallRef, false)
			} else {
				var xmlRespon xmlmcResponse
				err = xml.Unmarshal([]byte(fixed), &xmlRespon)
				if err != nil {
					logger(5, "Activity Stream Creation unmarshall failed for Request "+strNewCallRef, false)
				} else {
					if xmlRespon.MethodResult != "ok" {
						logger(5, "Activity Stream Creation was unsuccessful for ["+strNewCallRef+"]: "+xmlRespon.MethodResult, false)
					} else {
						logger(1, "Activity Stream Creation successful for ["+strNewCallRef+"]", false)
					}
				}
			}

			//Now update Logdate
			if boolUpdateLogDate {
				espXmlmc.SetParam("application", appServiceManager)
				espXmlmc.SetParam("entity", "Requests")
				espXmlmc.OpenElement("primaryEntityData")
				espXmlmc.OpenElement("record")
				espXmlmc.SetParam("h_pk_reference", strNewCallRef)
				espXmlmc.SetParam("h_datelogged", strLoggedDate)
				espXmlmc.CloseElement("record")
				espXmlmc.CloseElement("primaryEntityData")
				XMLBPM, xmlmcErr := espXmlmc.Invoke("data", "entityUpdateRecord")
				if xmlmcErr != nil {
					//log.Fatal(xmlmcErr)
					logger(4, "Unable to update Log Date of request ["+strNewCallRef+"] : "+fmt.Sprintf("%v", xmlmcErr), false)
				}
				var xmlRespon xmlmcResponse

				errLogDate := xml.Unmarshal([]byte(XMLBPM), &xmlRespon)
				if errLogDate != nil {
					logger(4, "Unable to update Log Date of request ["+strNewCallRef+"] : "+fmt.Sprintf("%v", errLogDate), false)
				}
				if xmlRespon.MethodResult != "ok" {
					logger(4, "Unable to update Log Date of request ["+strNewCallRef+"] : "+xmlRespon.State.ErrorRet, false)
				}
			}

			//Now do BPM Processing
			if strStatus != "status.resolved" &&
				strStatus != "status.closed" &&
				strStatus != "status.cancelled" {

				logger(1, callClass+" Logged: "+strNewCallRef+". Open Request status, spawing BPM Process "+strServiceBPM, false)
				if strNewCallRef != "" && strServiceBPM != "" {
					espXmlmc.SetParam("application", appServiceManager)
					espXmlmc.SetParam("name", strServiceBPM)
					espXmlmc.OpenElement("inputParams")
					espXmlmc.SetParam("objectRefUrn", "urn:sys:entity:"+appServiceManager+":Requests:"+strNewCallRef)
					espXmlmc.SetParam("requestId", strNewCallRef)
					espXmlmc.CloseElement("inputParams")

					XMLBPM, xmlmcErr := espXmlmc.Invoke("bpm", "processSpawn")
					if xmlmcErr != nil {
						//log.Fatal(xmlmcErr)
						logger(4, "Unable to invoke BPM for request ["+strNewCallRef+"]: "+fmt.Sprintf("%v", xmlmcErr), false)
					}
					var xmlRespon xmlmcBPMSpawnedStruct

					errBPM := xml.Unmarshal([]byte(XMLBPM), &xmlRespon)
					if errBPM != nil {
						logger(4, "Unable to read response from Hornbill instance:"+fmt.Sprintf("%v", errBPM), false)
						return false, "No"
					}
					if xmlRespon.MethodResult != "ok" {
						logger(4, "Unable to invoke BPM: "+xmlRespon.State.ErrorRet, false)
					} else {
						//Now, associate spawned BPM to the new Request
						espXmlmc.SetParam("application", appServiceManager)
						espXmlmc.SetParam("entity", "Requests")
						espXmlmc.OpenElement("primaryEntityData")
						espXmlmc.OpenElement("record")
						espXmlmc.SetParam("h_pk_reference", strNewCallRef)
						espXmlmc.SetParam("h_bpm_id", xmlRespon.Identifier)
						espXmlmc.CloseElement("record")
						espXmlmc.CloseElement("primaryEntityData")

						XMLBPMUpdate, xmlmcErr := espXmlmc.Invoke("data", "entityUpdateRecord")
						if xmlmcErr != nil {
							//log.Fatal(xmlmcErr)
							logger(4, "Unable to associated spawned BPM to request ["+strNewCallRef+"]: "+fmt.Sprintf("%v", xmlmcErr), false)
						}
						var xmlRespon xmlmcResponse

						errBPMSpawn := xml.Unmarshal([]byte(XMLBPMUpdate), &xmlRespon)
						if errBPMSpawn != nil {
							logger(4, "Unable to read response from Hornbill instance:"+fmt.Sprintf("%v", errBPMSpawn), false)
							return false, "No"
						}
						if xmlRespon.MethodResult != "ok" {
							logger(4, "Unable to associate BPM to Request: "+xmlRespon.State.ErrorRet, false)
						}
					}
				}
			}

			// Now handle calls in an On Hold status
			if boolOnHoldRequest {
				espXmlmc.SetParam("requestId", strNewCallRef)
				espXmlmc.SetParam("onHoldUntil", strClosedDate)
				espXmlmc.SetParam("strReason", "Request imported from Supportworks in an On Hold status. See Historical Request Updates for further information.")
				XMLBPM, xmlmcErr := espXmlmc.Invoke("apps/"+appServiceManager+"/Requests", "holdRequest")
				if xmlmcErr != nil {
					//log.Fatal(xmlmcErr)
					logger(4, "Unable to place request on hold ["+strNewCallRef+"] : "+fmt.Sprintf("%v", xmlmcErr), false)
				}
				var xmlRespon xmlmcResponse

				errLogDate := xml.Unmarshal([]byte(XMLBPM), &xmlRespon)
				if errLogDate != nil {
					logger(4, "Unable to place request on hold ["+strNewCallRef+"] : "+fmt.Sprintf("%v", errLogDate), false)
				}
				if xmlRespon.MethodResult != "ok" {
					logger(4, "Unable to place request on hold ["+strNewCallRef+"] : "+xmlRespon.State.ErrorRet, false)
				}
			}
		}
	} else {
		//-- DEBUG XML TO LOG FILE
		var XMLSTRING = espXmlmc.GetParam()
		logger(1, "Request Log XML "+fmt.Sprintf("%s", XMLSTRING), false)
		counters.Lock()
		counters.createdSkipped++
		counters.Unlock()
		espXmlmc.ClearParam()
		return true, "Dry Run"
	}

	//-- If request logged successfully :
	//Get the Call Diary Updates from Supportworks and build the Historical Updates against the SM request
	if boolCallLoggedOK == true && strNewCallRef != "" {
		//####		applyHistoricalUpdates(strNewCallRef, swCallID)
	}

	return boolCallLoggedOK, strNewCallRef
}

func updateCall(newCallRef string, diaryEntry map[string]interface{}) bool {
	espXmlmc, err := NewEspXmlmcSession()
	if err != nil {
		return false
	}
	//#######
	//    swImportConf.ConfTimelineUpdate["h_description"]
	/*
	   diaryTime := ""
	   if diaryEntry["updatetimex"] != nil {
	       diaryTimex := ""
	       if updateTime, ok := diaryEntry["updatetimex"].(int64); ok {
	           diaryTimex = strconv.FormatInt(updateTime, 10)
	       } else {
	           diaryTimex = fmt.Sprintf("%+s", diaryEntry["updatetimex"])
	       }
	       diaryTime = epochToDateTime(diaryTimex)
	   }
	*/

	/*
	   diaryText := ""
	   if diaryEntry["Action Taken"] != nil {
	       diaryText = fmt.Sprintf("%+s", diaryEntry["Action Taken"])
	       diaryText = html.EscapeString(diaryText)
	   }
	*/

	espXmlmc.SetParam("application", appServiceManager)
	espXmlmc.SetParam("entity", "RequestHistoricUpdates")
	espXmlmc.OpenElement("primaryEntityData")
	espXmlmc.OpenElement("record")
	espXmlmc.SetParam("h_fk_reference", newCallRef)
	// fmt.Println(newCallRef)

	diaryText := ""
	q := fmt.Sprintf("%v", swImportConf.ConfTimelineUpdate.Updatedate)
	if q != "" {
		diaryText = getFieldValue(q, diaryEntry)
		diaryText = html.EscapeString(diaryText)
		if diaryText != "" {
			//espXmlmc.SetParam("h_updatedate", diaryText)
			//espXmlmc.SetParam("h_updatedate", "2017-01-01T12:23:34Z+01:00")
			//fmt.Println(diaryText)
			v, e := time.Parse("2006-01-02 15:04:05 -0700 MST", diaryText)
			if e != nil {
				fmt.Println(fmt.Sprintf("%v", e))
				//        fmt.Println(e.Message)
				return false
			}
			//fmt.Println(v)
			//fmt.Println(v.Format(time.RFC3339))
			espXmlmc.SetParam("h_updatedate", v.Format(time.RFC3339))
			//r := v.Format()
		}
	}

	diaryText = ""
	q = fmt.Sprintf("%v", swImportConf.ConfTimelineUpdate.Timespent)
	if q != "" {
		diaryText = getFieldValue(q, diaryEntry)
		diaryText = html.EscapeString(diaryText)
		if diaryText != "" {
			espXmlmc.SetParam("h_timespent", diaryText)
		}
	}

	diaryText = ""
	q = fmt.Sprintf("%v", swImportConf.ConfTimelineUpdate.Updatetype)
	if q != "" {
		diaryText = getFieldValue(q, diaryEntry)
		diaryText = html.EscapeString(diaryText)
		if diaryText != "" {
			espXmlmc.SetParam("h_updatetype", diaryText)
		}
	}

	espXmlmc.SetParam("h_updatebytype", "1")

	diaryText = ""
	q = fmt.Sprintf("%v", swImportConf.ConfTimelineUpdate.Updateindex)
	if q != "" {
		diaryText = getFieldValue(q, diaryEntry)
		diaryText = html.EscapeString(diaryText)
		if diaryText != "" {
			espXmlmc.SetParam("h_updateindex", diaryText)
		}
	}

	diaryText = ""
	q = fmt.Sprintf("%v", swImportConf.ConfTimelineUpdate.Updateby)
	if q != "" {
		diaryText = getFieldValue(q, diaryEntry)
		diaryText = html.EscapeString(diaryText)
		if diaryText != "" {
			espXmlmc.SetParam("h_updateby", diaryText)
		}
	}

	diaryText = ""
	q = fmt.Sprintf("%v", swImportConf.ConfTimelineUpdate.Updatebyname)
	if q != "" {
		diaryText = getFieldValue(q, diaryEntry)
		diaryText = html.EscapeString(diaryText)
		if diaryText != "" {
			espXmlmc.SetParam("h_updatebyname", diaryText)
		}
	}

	diaryText = ""
	q = fmt.Sprintf("%v", swImportConf.ConfTimelineUpdate.Updatebygroup)
	if q != "" {
		diaryText = getFieldValue(q, diaryEntry)
		diaryText = html.EscapeString(diaryText)
		if diaryText != "" {
			espXmlmc.SetParam("h_updatebygroup", diaryText)
		}
	}

	diaryText = ""
	q = fmt.Sprintf("%v", swImportConf.ConfTimelineUpdate.Actiontype)
	if q != "" {
		diaryText = getFieldValue(q, diaryEntry)
		diaryText = html.EscapeString(diaryText)
		if diaryText != "" {
			espXmlmc.SetParam("h_actiontype", diaryText)
		}
	}

	diaryText = ""
	q = fmt.Sprintf("%v", swImportConf.ConfTimelineUpdate.Actionsource)
	if q != "" {
		diaryText = getFieldValue(q, diaryEntry)
		diaryText = html.EscapeString(diaryText)
		if diaryText != "" {
			espXmlmc.SetParam("h_actionsource", diaryText)
		}
	}

	diaryText = ""
	q = fmt.Sprintf("%v", swImportConf.ConfTimelineUpdate.Description)
	if q != "" {
		diaryText = getFieldValue(q, diaryEntry)
		diaryText = html.EscapeString(diaryText)
		if diaryText != "" {
			espXmlmc.SetParam("h_description", diaryText)
		}
	}

	espXmlmc.CloseElement("record")
	espXmlmc.CloseElement("primaryEntityData")
	//fmt.Println(espXmlmc.GetParam())
	//-- Check for Dry Run
	if configDryRun != true {
		XMLUpdate, xmlmcErr := espXmlmc.Invoke("data", "entityAddRecord")
		if xmlmcErr != nil {
			//log.Fatal(xmlmcErr)
			logger(3, "Unable to add Historical Call Diary Update: "+fmt.Sprintf("%v", xmlmcErr), false)
		}
		var xmlRespon xmlmcResponse
		errXMLMC := xml.Unmarshal([]byte(XMLUpdate), &xmlRespon)
		if errXMLMC != nil {
			logger(4, "Unable to read response from Hornbill instance:"+fmt.Sprintf("%v", errXMLMC), false)
		}
		if xmlRespon.MethodResult != "ok" {
			logger(3, "Unable to add Historical Call Diary Update: "+xmlRespon.State.ErrorRet, false)
		}
	} else {
		//-- DEBUG XML TO LOG FILE
		var XMLSTRING = espXmlmc.GetParam()
		logger(1, "Request Historical Update XML "+fmt.Sprintf("%s", XMLSTRING), false)
		counters.Lock()
		counters.createdSkipped++
		counters.Unlock()
		espXmlmc.ClearParam()
		return true
	}

	return true
}

//convExtendedColName - takes old extended column name, returns new one (supply h_custom_a returns h_custom_1 for example)
//Split string in to array with _ as seperator
//Convert last array entry string character to Rune
//Convert Rune to Integer
//Subtract 96 from Integer
//Convert resulting Integer to String (numeric character), append to prefix and pass back
func convExtendedColName(oldColName string) string {
	arrColName := strings.Split(oldColName, "_")
	strNewColID := strconv.Itoa(int([]rune(arrColName[2])[0]) - 96)
	return "h_custom_" + strNewColID
}

//applyHistoricalUpdates - takes call diary records from Supportworks, imports to Hornbill as Historical Updates
func applyHistoricalUpdates(newCallRef, swCallRef string) bool {
	espXmlmc, err := NewEspXmlmcSession()
	if err != nil {
		return false
	}

	//Connect to the JSON specified DB
	db, err := sqlx.Open(appDBDriver, connStrAppDB)
	defer db.Close()
	if err != nil {
		logger(4, " [DATABASE] Database Connection Error for Historical Updates: "+fmt.Sprintf("%v", err), false)
		return false
	}
	//Check connection is open
	err = db.Ping()
	if err != nil {
		logger(4, " [DATABASE] [PING] Database Connection Error for Historical Updates: "+fmt.Sprintf("%v", err), false)
		return false
	}
	logger(3, "[DATABASE] Connection Successful", false)
	mutex.Lock()
	logger(3, "[DATABASE] Running query for Historical Updates of call "+swCallRef+". Please wait...", false)
	//build query
	sqlDiaryQuery := "SELECT updatetimex, repid, groupid, udsource, udcode, udtype, updatetxt, udindex, timespent "
	sqlDiaryQuery = sqlDiaryQuery + " FROM updatedb WHERE callref = " + swCallRef + " ORDER BY udindex DESC"
	logger(3, "[DATABASE} Diary Query: "+sqlDiaryQuery, false)
	mutex.Unlock()
	//Run Query
	rows, err := db.Queryx(sqlDiaryQuery)
	if err != nil {
		logger(4, " Database Query Error: "+fmt.Sprintf("%v", err), false)
		return false
	}
	//Process each call diary entry, insert in to Hornbill
	for rows.Next() {
		diaryEntry := make(map[string]interface{})
		err = rows.MapScan(diaryEntry)
		if err != nil {
			logger(4, "Unable to retrieve data from SQL query: "+fmt.Sprintf("%v", err), false)
		} else {
			//Update Time - EPOCH to Date/Time Conversion
			diaryTime := ""
			if diaryEntry["updatetimex"] != nil {
				diaryTimex := ""
				if updateTime, ok := diaryEntry["updatetimex"].(int64); ok {
					diaryTimex = strconv.FormatInt(updateTime, 10)
				} else {
					diaryTimex = fmt.Sprintf("%+s", diaryEntry["updatetimex"])
				}
				diaryTime = epochToDateTime(diaryTimex)
			}

			//Check for source/code/text having nil value
			diarySource := ""
			if diaryEntry["udsource"] != nil {
				diarySource = fmt.Sprintf("%+s", diaryEntry["udsource"])
			}

			diaryCode := ""
			if diaryEntry["udcode"] != nil {
				diaryCode = fmt.Sprintf("%+s", diaryEntry["udcode"])
			}

			diaryText := ""
			if diaryEntry["updatetxt"] != nil {
				diaryText = fmt.Sprintf("%+s", diaryEntry["updatetxt"])
				diaryText = html.EscapeString(diaryText)
			}

			diaryIndex := ""
			if diaryEntry["udindex"] != nil {
				if updateIndex, ok := diaryEntry["udindex"].(int64); ok {
					diaryIndex = strconv.FormatInt(updateIndex, 10)
				} else {
					diaryIndex = fmt.Sprintf("%+s", diaryEntry["udindex"])
				}
			}

			diaryTimeSpent := ""
			if diaryEntry["timespent"] != nil {
				if updateSpent, ok := diaryEntry["timespent"].(int64); ok {
					diaryTimeSpent = strconv.FormatInt(updateSpent, 10)
				} else {
					diaryTimeSpent = fmt.Sprintf("%+s", diaryEntry["timespent"])
				}
			}

			diaryType := ""
			if diaryEntry["udtype"] != nil {
				if updateType, ok := diaryEntry["udtype"].(int64); ok {
					diaryType = strconv.FormatInt(updateType, 10)
				} else {
					diaryType = fmt.Sprintf("%+s", diaryEntry["udtype"])
				}
			}

			espXmlmc.SetParam("application", appServiceManager)
			espXmlmc.SetParam("entity", "RequestHistoricUpdates")
			espXmlmc.OpenElement("primaryEntityData")
			espXmlmc.OpenElement("record")
			espXmlmc.SetParam("h_fk_reference", newCallRef)
			espXmlmc.SetParam("h_updatedate", diaryTime)
			if diaryTimeSpent != "" && diaryTimeSpent != "0" {
				espXmlmc.SetParam("h_timespent", diaryTimeSpent)
			}
			if diaryType != "" {
				espXmlmc.SetParam("h_updatetype", diaryType)
			}
			espXmlmc.SetParam("h_updatebytype", "1")
			espXmlmc.SetParam("h_updateindex", diaryIndex)
			espXmlmc.SetParam("h_updateby", fmt.Sprintf("%+s", diaryEntry["repid"]))
			espXmlmc.SetParam("h_updatebyname", fmt.Sprintf("%+s", diaryEntry["repid"]))
			espXmlmc.SetParam("h_updatebygroup", fmt.Sprintf("%+s", diaryEntry["groupid"]))
			if diaryCode != "" {
				espXmlmc.SetParam("h_actiontype", diaryCode)
			}
			if diarySource != "" {
				espXmlmc.SetParam("h_actionsource", diarySource)
			}
			if diaryText != "" {
				espXmlmc.SetParam("h_description", diaryText)
			}
			espXmlmc.CloseElement("record")
			espXmlmc.CloseElement("primaryEntityData")

			//-- Check for Dry Run
			if configDryRun != true {
				XMLUpdate, xmlmcErr := espXmlmc.Invoke("data", "entityAddRecord")
				if xmlmcErr != nil {
					//log.Fatal(xmlmcErr)
					logger(3, "Unable to add Historical Call Diary Update: "+fmt.Sprintf("%v", xmlmcErr), false)
				}
				var xmlRespon xmlmcResponse
				errXMLMC := xml.Unmarshal([]byte(XMLUpdate), &xmlRespon)
				if errXMLMC != nil {
					logger(4, "Unable to read response from Hornbill instance:"+fmt.Sprintf("%v", errXMLMC), false)
				}
				if xmlRespon.MethodResult != "ok" {
					logger(3, "Unable to add Historical Call Diary Update: "+xmlRespon.State.ErrorRet, false)
				}
			} else {
				//-- DEBUG XML TO LOG FILE
				var XMLSTRING = espXmlmc.GetParam()
				logger(1, "Request Historical Update XML "+fmt.Sprintf("%s", XMLSTRING), false)
				counters.Lock()
				counters.createdSkipped++
				counters.Unlock()
				espXmlmc.ClearParam()
				return true
			}
		}
	}
	defer rows.Close()
	return true
}

// getFieldValue --Retrieve field value from mapping via SQL record map
func getFieldValue(v string, u map[string]interface{}) string {
	fieldMap := v
	//-- Match $variable from String
	re1, err := regexp.Compile(`\[(.*?)\]`)
	if err != nil {
		color.Red("[ERROR] %v", err)
	}

	result := re1.FindAllString(fieldMap, 100)
	valFieldMap := ""
	//-- Loop Matches
	for _, val := range result {
		valFieldMap = ""
		valFieldMap = strings.Replace(val, "[", "", 1)
		valFieldMap = strings.Replace(valFieldMap, "]", "", 1)
		if valFieldMap == "oldCallRef" {
			valFieldMap = "h_formattedcallref"
			if u[valFieldMap] != nil {

				if valField, ok := u[valFieldMap].(int64); ok {
					valFieldMap = strconv.FormatInt(valField, 10)
				} else {
					valFieldMap = fmt.Sprintf("%+s", u[valFieldMap])
				}

				if valFieldMap != "<nil>" {
					fieldMap = strings.Replace(fieldMap, val, valFieldMap, 1)
				}

			} else {
				valFieldMap = "callref"
				if u[valFieldMap] != nil {

					if valField, ok := u[valFieldMap].(int64); ok {
						valFieldMap = strconv.FormatInt(valField, 10)
					} else {
						valFieldMap = fmt.Sprintf("%+s", u[valFieldMap])
					}

					if valFieldMap != "<nil>" {
						fieldMap = strings.Replace(fieldMap, val, padCallRef(valFieldMap, "F", 7), 1)
					}
				} else {
					fieldMap = strings.Replace(fieldMap, val, "", 1)
				}
			}
		} else {
			if u[valFieldMap] != nil {

				if valField, ok := u[valFieldMap].(int64); ok {
					valFieldMap = strconv.FormatInt(valField, 10)
				} else {
					valFieldMap = fmt.Sprintf("%+s", u[valFieldMap])
				}

				if valFieldMap != "<nil>" {
					fieldMap = strings.Replace(fieldMap, val, valFieldMap, 1)
				}
			} else {
				fieldMap = strings.Replace(fieldMap, val, "", 1)
			}
		}
	}
	return fieldMap
}

//getSiteID takes the Call Record and returns a correct Site ID if one exists on the Instance
func getSiteID(callMap map[string]interface{}) (string, string) {
	siteID := ""
	siteNameMapping := fmt.Sprintf("%v", mapGenericConf.CoreFieldMapping["h_site_id"])
	siteName := getFieldValue(siteNameMapping, callMap)
	if siteName != "" {
		siteIsInCache, SiteIDCache := recordInCache(siteName, "Site")
		//-- Check if we have cached the site already
		if siteIsInCache {
			siteID = SiteIDCache
		} else {
			siteIsOnInstance, SiteIDInstance := searchSite(siteName)
			//-- If Returned set output
			if siteIsOnInstance {
				siteID = strconv.Itoa(SiteIDInstance)
			}
		}
	}
	return siteID, siteName
}

//getCallServiceID takes the Call Record and returns a correct Service ID if one exists on the Instance
func getCallServiceID(swService string) string {
	serviceID := ""
	serviceName := ""
	if swImportConf.ServiceMapping[swService] != nil {
		serviceName = fmt.Sprintf("%s", swImportConf.ServiceMapping[swService])

		if serviceName != "" {
			serviceID = getServiceID(serviceName)
		}
	}
	return serviceID
}

//getServiceID takes a Service Name string and returns a correct Service ID if one exists in the cache or on the Instance
func getServiceID(serviceName string) string {
	serviceID := ""
	if serviceName != "" {
		serviceIsInCache, ServiceIDCache := recordInCache(serviceName, "Service")
		//-- Check if we have cached the Service already
		if serviceIsInCache {
			serviceID = ServiceIDCache
		} else {
			serviceIsOnInstance, ServiceIDInstance := searchService(serviceName)
			//-- If Returned set output
			if serviceIsOnInstance {
				serviceID = strconv.Itoa(ServiceIDInstance)
			}
		}
	}
	return serviceID
}

//getCallPriorityID takes the Call Record and returns a correct Priority ID if one exists on the Instance
func getCallPriorityID(strPriorityName string) (string, string) {
	priorityID := ""
	if swImportConf.PriorityMapping[strPriorityName] != nil {
		strPriorityName = fmt.Sprintf("%s", swImportConf.PriorityMapping[strPriorityName])
		if strPriorityName != "" {
			priorityID = getPriorityID(strPriorityName)
		}
	}
	return priorityID, strPriorityName
}

//getPriorityID takes a Priority Name string and returns a correct Priority ID if one exists in the cache or on the Instance
func getPriorityID(priorityName string) string {
	priorityID := ""
	if priorityName != "" {
		priorityIsInCache, PriorityIDCache := recordInCache(priorityName, "Priority")
		//-- Check if we have cached the Priority already
		if priorityIsInCache {
			priorityID = PriorityIDCache
		} else {
			priorityIsOnInstance, PriorityIDInstance := searchPriority(priorityName)
			//-- If Returned set output
			if priorityIsOnInstance {
				priorityID = strconv.Itoa(PriorityIDInstance)
			}
		}
	}
	return priorityID
}

//getCallTeamID takes the Call Record and returns a correct Team ID if one exists on the Instance
func getCallTeamID(swTeamID string) (string, string) {
	teamID := ""
	teamName := ""
	if swImportConf.TeamMapping[swTeamID] != nil {
		teamName = fmt.Sprintf("%s", swImportConf.TeamMapping[swTeamID])
		if teamName != "" {
			teamID = getTeamID(teamName)
		}
	}
	return teamID, teamName
}

//getTeamID takes a Team Name string and returns a correct Team ID if one exists in the cache or on the Instance
func getTeamID(teamName string) string {
	teamID := ""
	if teamName != "" {
		teamIsInCache, TeamIDCache := recordInCache(teamName, "Team")
		//-- Check if we have cached the Team already
		if teamIsInCache {
			teamID = TeamIDCache
		} else {
			teamIsOnInstance, TeamIDInstance := searchTeam(teamName)
			//-- If Returned set output
			if teamIsOnInstance {
				teamID = TeamIDInstance
			}
		}
	}
	return teamID
}

//getCallCategoryID takes the Call Record and returns a correct Category ID if one exists on the Instance
func getCallCategoryID(callMap map[string]interface{}, categoryGroup string) (string, string) {
	categoryID := ""
	categoryString := ""
	categoryNameMapping := ""
	categoryCode := ""
	if categoryGroup == "Request" {
		categoryNameMapping = fmt.Sprintf("%v", mapGenericConf.CoreFieldMapping["h_category_id"])
		categoryCode = getFieldValue(categoryNameMapping, callMap)
		if swImportConf.CategoryMapping[categoryCode] != nil {
			//Get Category Code from JSON mapping
			categoryCode = fmt.Sprintf("%s", swImportConf.CategoryMapping[categoryCode])
		} else {
			//Mapping doesn't exist - replace hyphens from SW Profile code with another string, and try to use this
			//SMProfileCodeSeperator allows us to specify in the config, the seperator used within Service Manager
			//profile codes
			categoryCode = strings.Replace(categoryCode, "-", swImportConf.SMProfileCodeSeperator, -1)
		}

	} else {
		categoryNameMapping = fmt.Sprintf("%v", mapGenericConf.CoreFieldMapping["h_closure_category_id"])
		categoryCode = getFieldValue(categoryNameMapping, callMap)
		if swImportConf.ResolutionCategoryMapping[categoryCode] != nil {
			//Get Category Code from JSON mapping
			categoryCode = fmt.Sprintf("%s", swImportConf.ResolutionCategoryMapping[categoryCode])
		} else {
			//Mapping doesn't exist - replace hyphens from SW Profile code with colon, and try to use this
			categoryCode = strings.Replace(categoryCode, "-", swImportConf.SMProfileCodeSeperator, -1)
		}
	}
	if categoryCode != "" {
		categoryID, categoryString = getCategoryID(categoryCode, categoryGroup)
	}
	return categoryID, categoryString
}

//getCategoryID takes a Category Code string and returns a correct Category ID if one exists in the cache or on the Instance
func getCategoryID(categoryCode, categoryGroup string) (string, string) {

	categoryID := ""
	categoryString := ""
	if categoryCode != "" {
		categoryIsInCache, CategoryIDCache, CategoryNameCache := categoryInCache(categoryCode, categoryGroup+"Category")
		//-- Check if we have cached the Category already
		if categoryIsInCache {
			categoryID = CategoryIDCache
			categoryString = CategoryNameCache
		} else {
			categoryIsOnInstance, CategoryIDInstance, CategoryStringInstance := searchCategory(categoryCode, categoryGroup)
			//-- If Returned set output
			if categoryIsOnInstance {
				categoryID = CategoryIDInstance
				categoryString = CategoryStringInstance
			} else {
				logger(4, "[CATEGORY] "+categoryGroup+" Category ["+categoryCode+"] is not on instance.", false)
			}
		}
	}
	return categoryID, categoryString
}

//doesAnalystExist takes an Analyst ID string and returns a true if one exists in the cache or on the Instance
func doesAnalystExist(analystID string) bool {
	espXmlmc, err := NewEspXmlmcSession()
	if err != nil {
		return false
	}
	boolAnalystExists := false
	if analystID != "" {
		analystIsInCache, strReturn := recordInCache(analystID, "Analyst")
		//-- Check if we have cached the Analyst already
		if analystIsInCache && strReturn != "" {
			boolAnalystExists = true
		} else {
			//Get Analyst Info
			espXmlmc.SetParam("userId", analystID)

			XMLAnalystSearch, xmlmcErr := espXmlmc.Invoke("admin", "userGetInfo")
			if xmlmcErr != nil {
				logger(4, "Unable to Search for Request Owner ["+analystID+"]: "+fmt.Sprintf("%v", xmlmcErr), true)
			}

			var xmlRespon xmlmcAnalystListResponse
			err := xml.Unmarshal([]byte(XMLAnalystSearch), &xmlRespon)
			if err != nil {
				logger(4, "Unable to Search for Request Owner ["+analystID+"]: "+fmt.Sprintf("%v", err), false)
			} else {
				if xmlRespon.MethodResult != "ok" {
					//Analyst most likely does not exist
					logger(4, "Unable to Search for Request Owner ["+analystID+"]: "+xmlRespon.State.ErrorRet, false)
				} else {
					//-- Check Response
					if xmlRespon.AnalystFullName != "" {
						boolAnalystExists = true
						//-- Add Analyst to Cache
						var newAnalystForCache analystListStruct
						newAnalystForCache.AnalystID = analystID
						newAnalystForCache.AnalystName = xmlRespon.AnalystFullName
						analystNamedMap := []analystListStruct{newAnalystForCache}
						mutexAnalysts.Lock()
						analysts = append(analysts, analystNamedMap...)
						mutexAnalysts.Unlock()
					}
				}
			}
		}
	}
	return boolAnalystExists
}

//doesCustomerExist takes a Customer ID string and returns a true if one exists in the cache or on the Instance
func doesCustomerExist(customerID string) bool {
	espXmlmc, err := NewEspXmlmcSession()
	if err != nil {
		return false
	}
	boolCustomerExists := false
	if customerID != "" {
		customerIsInCache, strReturn := recordInCache(customerID, "Customer")
		//-- Check if we have cached the Analyst already
		if customerIsInCache && strReturn != "" {
			boolCustomerExists = true
		} else {
			//Get Analyst Info
			espXmlmc.SetParam("customerId", customerID)
			espXmlmc.SetParam("customerType", swImportConf.CustomerType)
			XMLCustomerSearch, xmlmcErr := espXmlmc.Invoke("apps/"+appServiceManager, "shrGetCustomerDetails")
			if xmlmcErr != nil {
				logger(4, "Unable to Search for Customer ["+customerID+"]: "+fmt.Sprintf("%v", xmlmcErr), true)
			}

			var xmlRespon xmlmcCustomerListResponse
			err := xml.Unmarshal([]byte(XMLCustomerSearch), &xmlRespon)
			if err != nil {
				logger(4, "Unable to Search for Customer ["+customerID+"]: "+fmt.Sprintf("%v", err), false)
			} else {
				if xmlRespon.MethodResult != "ok" {
					//Customer most likely does not exist
					logger(4, "Unable to Search for Customer ["+customerID+"]: "+xmlRespon.State.ErrorRet, false)
				} else {
					//-- Check Response
					if xmlRespon.CustomerFirstName != "" {
						boolCustomerExists = true
						//-- Add Customer to Cache
						var newCustomerForCache customerListStruct
						newCustomerForCache.CustomerID = customerID
						newCustomerForCache.CustomerName = xmlRespon.CustomerFirstName + " " + xmlRespon.CustomerLastName
						customerNamedMap := []customerListStruct{newCustomerForCache}
						mutexCustomers.Lock()
						customers = append(customers, customerNamedMap...)
						mutexCustomers.Unlock()
					}
				}
			}
		}
	}
	return boolCustomerExists
}

// recordInCache -- Function to check if passed-thorugh record name has been cached
// if so, pass back the Record ID
func recordInCache(recordName, recordType string) (bool, string) {
	boolReturn := false
	strReturn := ""
	switch recordType {
	case "Service":
		//-- Check if record in Service Cache
		mutexServices.Lock()
		for _, service := range services {
			if service.ServiceName == recordName {
				boolReturn = true
				strReturn = strconv.Itoa(service.ServiceID)
			}
		}
		mutexServices.Unlock()
	case "Priority":
		//-- Check if record in Priority Cache
		mutexPriorities.Lock()
		for _, priority := range priorities {
			if priority.PriorityName == recordName {
				boolReturn = true
				strReturn = strconv.Itoa(priority.PriorityID)
			}
		}
		mutexPriorities.Unlock()
	case "Site":
		//-- Check if record in Site Cache
		mutexSites.Lock()
		for _, site := range sites {
			if site.SiteName == recordName {
				boolReturn = true
				strReturn = strconv.Itoa(site.SiteID)
			}
		}
		mutexSites.Unlock()
	case "Team":
		//-- Check if record in Team Cache
		mutexTeams.Lock()
		for _, team := range teams {
			if team.TeamName == recordName {
				boolReturn = true
				strReturn = team.TeamID
			}
		}
		mutexTeams.Unlock()
	case "Analyst":
		//-- Check if record in Analyst Cache
		mutexAnalysts.Lock()
		for _, analyst := range analysts {
			if analyst.AnalystID == recordName {
				boolReturn = true
				strReturn = analyst.AnalystName
			}
		}
		mutexAnalysts.Unlock()
	case "Customer":
		//-- Check if record in Customer Cache
		mutexCustomers.Lock()
		for _, customer := range customers {
			if customer.CustomerID == recordName {
				boolReturn = true
				strReturn = customer.CustomerName
			}
		}
		mutexCustomers.Unlock()
	}
	return boolReturn, strReturn
}

// categoryInCache -- Function to check if passed-thorugh category been cached
// if so, pass back the Category ID and Full Name
func categoryInCache(recordName, recordType string) (bool, string, string) {
	boolReturn := false
	idReturn := ""
	strReturn := ""
	switch recordType {
	case "RequestCategory":
		//-- Check if record in Category Cache
		mutexCategories.Lock()
		for _, category := range categories {
			if category.CategoryCode == recordName {
				boolReturn = true
				idReturn = category.CategoryID
				strReturn = category.CategoryName
			}
		}
		mutexCategories.Unlock()
	case "ClosureCategory":
		//-- Check if record in Category Cache
		mutexCloseCategories.Lock()
		for _, category := range closeCategories {
			if category.CategoryCode == recordName {
				boolReturn = true
				idReturn = category.CategoryID
				strReturn = category.CategoryName
			}
		}
		mutexCloseCategories.Unlock()
	}
	return boolReturn, idReturn, strReturn
}

// seachSite -- Function to check if passed-through  site  name is on the instance
func searchSite(siteName string) (bool, int) {
	espXmlmc, err := NewEspXmlmcSession()
	if err != nil {
		return false, 0
	}

	boolReturn := false
	intReturn := 0
	//-- ESP Query for site
	espXmlmc.SetParam("entity", "Site")
	espXmlmc.SetParam("matchScope", "all")
	espXmlmc.OpenElement("searchFilter")
	espXmlmc.SetParam("column", "h_site_name")
	espXmlmc.SetParam("value", siteName)
	//espXmlmc.SetParam("h_site_name", siteName)
	espXmlmc.CloseElement("searchFilter")
	espXmlmc.SetParam("maxResults", "1")

	XMLSiteSearch, xmlmcErr := espXmlmc.Invoke("data", "entityBrowseRecords2")
	if xmlmcErr != nil {
		logger(4, "Unable to Search for Site: "+fmt.Sprintf("%v", xmlmcErr), false)
		return boolReturn, intReturn
		//log.Fatal(xmlmcErr)
	}
	var xmlRespon xmlmcSiteListResponse

	err = xml.Unmarshal([]byte(XMLSiteSearch), &xmlRespon)
	if err != nil {
		logger(4, "Unable to Search for Site: "+fmt.Sprintf("%v", err), false)
	} else {
		if xmlRespon.MethodResult != "ok" {
			logger(4, "Unable to Search for Site: "+xmlRespon.State.ErrorRet, false)
		} else {
			//-- Check Response
			if xmlRespon.SiteName != "" {
				if strings.ToLower(xmlRespon.SiteName) == strings.ToLower(siteName) {
					intReturn = xmlRespon.SiteID
					boolReturn = true
					//-- Add Site to Cache
					var newSiteForCache siteListStruct
					newSiteForCache.SiteID = intReturn
					newSiteForCache.SiteName = siteName
					siteNamedMap := []siteListStruct{newSiteForCache}
					mutexSites.Lock()
					sites = append(sites, siteNamedMap...)
					mutexSites.Unlock()
				}
			}
		}
	}
	return boolReturn, intReturn
}

// seachPriority -- Function to check if passed-through priority name is on the instance
func searchPriority(priorityName string) (bool, int) {
	boolReturn := false
	intReturn := 0
	//-- ESP Query for Priority
	espXmlmc, err := NewEspXmlmcSession()
	if err != nil {
		return false, 0
	}

	espXmlmc.SetParam("application", appServiceManager)
	espXmlmc.SetParam("entity", "Priority")
	espXmlmc.SetParam("matchScope", "all")
	espXmlmc.OpenElement("searchFilter")
	//espXmlmc.SetParam("h_priorityname", priorityName)
	espXmlmc.SetParam("column", "h_priorityname")
	espXmlmc.SetParam("value", priorityName)
	espXmlmc.CloseElement("searchFilter")
	espXmlmc.SetParam("maxResults", "1")

	XMLPrioritySearch, xmlmcErr := espXmlmc.Invoke("data", "entityBrowseRecords2")
	if xmlmcErr != nil {
		logger(4, "Unable to Search for Priority: "+fmt.Sprintf("%v", xmlmcErr), false)
		return boolReturn, intReturn
		//log.Fatal(xmlmcErr)
	}
	var xmlRespon xmlmcPriorityListResponse

	err = xml.Unmarshal([]byte(XMLPrioritySearch), &xmlRespon)
	if err != nil {
		logger(4, "Unable to Search for Priority: "+fmt.Sprintf("%v", err), false)
	} else {
		if xmlRespon.MethodResult != "ok" {
			logger(4, "Unable to Search for Priority: "+xmlRespon.State.ErrorRet, false)
		} else {
			//-- Check Response
			if xmlRespon.PriorityName != "" {
				if strings.ToLower(xmlRespon.PriorityName) == strings.ToLower(priorityName) {
					intReturn = xmlRespon.PriorityID
					boolReturn = true
					//-- Add Priority to Cache
					var newPriorityForCache priorityListStruct
					newPriorityForCache.PriorityID = intReturn
					newPriorityForCache.PriorityName = priorityName
					priorityNamedMap := []priorityListStruct{newPriorityForCache}
					mutexPriorities.Lock()
					priorities = append(priorities, priorityNamedMap...)
					mutexPriorities.Unlock()
				}
			}
		}
	}
	return boolReturn, intReturn
}

// seachService -- Function to check if passed-through service name is on the instance
func searchService(serviceName string) (bool, int) {
	boolReturn := false
	intReturn := 0
	espXmlmc, err := NewEspXmlmcSession()
	if err != nil {
		return false, 0
	}

	//-- ESP Query for service
	espXmlmc.SetParam("application", appServiceManager)
	espXmlmc.SetParam("entity", "Services")
	espXmlmc.SetParam("matchScope", "all")
	espXmlmc.OpenElement("searchFilter")
	//espXmlmc.SetParam("h_servicename", serviceName)
	espXmlmc.SetParam("column", "h_servicename")
	espXmlmc.SetParam("value", serviceName)
	espXmlmc.CloseElement("searchFilter")
	espXmlmc.SetParam("maxResults", "1")

	XMLServiceSearch, xmlmcErr := espXmlmc.Invoke("data", "entityBrowseRecords2")
	if xmlmcErr != nil {
		logger(4, "Unable to Search for Service: "+fmt.Sprintf("%v", xmlmcErr), false)
		//log.Fatal(xmlmcErr)
		return boolReturn, intReturn
	}
	var xmlRespon xmlmcServiceListResponse

	err = xml.Unmarshal([]byte(XMLServiceSearch), &xmlRespon)
	if err != nil {
		logger(4, "Unable to Search for Service: "+fmt.Sprintf("%v", err), false)
	} else {
		if xmlRespon.MethodResult != "ok" {
			logger(4, "Unable to Search for Service: "+xmlRespon.State.ErrorRet, false)
		} else {
			//-- Check Response
			if xmlRespon.ServiceName != "" {
				if strings.ToLower(xmlRespon.ServiceName) == strings.ToLower(serviceName) {
					intReturn = xmlRespon.ServiceID
					boolReturn = true
					//-- Add Service to Cache
					var newServiceForCache serviceListStruct
					newServiceForCache.ServiceID = intReturn
					newServiceForCache.ServiceName = serviceName
					newServiceForCache.ServiceBPMIncident = xmlRespon.BPMIncident
					newServiceForCache.ServiceBPMService = xmlRespon.BPMService
					newServiceForCache.ServiceBPMChange = xmlRespon.BPMChange
					newServiceForCache.ServiceBPMProblem = xmlRespon.BPMProblem
					newServiceForCache.ServiceBPMKnownError = xmlRespon.BPMKnownError
					serviceNamedMap := []serviceListStruct{newServiceForCache}
					mutexServices.Lock()
					services = append(services, serviceNamedMap...)
					mutexServices.Unlock()
				}
			}
		}
	}
	//Return Service ID once cached - we can now use this in the calling function to get all details from cache
	return boolReturn, intReturn
}

// searchTeam -- Function to check if passed-through support team name is on the instance
func searchTeam(teamName string) (bool, string) {
	boolReturn := false
	strReturn := ""
	//-- ESP Query for team
	espXmlmc, err := NewEspXmlmcSession()
	if err != nil {
		return false, "Unable to create connection"
	}
	//###20181008
	espXmlmc.SetParam("application", appServiceManager)
	espXmlmc.SetParam("entity", "Team")
	//###20181008 espXmlmc.SetParam("entity", "Groups")
	espXmlmc.SetParam("matchScope", "all")
	espXmlmc.OpenElement("searchFilter")
	//espXmlmc.SetParam("h_name", teamName)
	espXmlmc.SetParam("column", "h_name")
	espXmlmc.SetParam("value", teamName)
	espXmlmc.CloseElement("searchFilter")
	espXmlmc.OpenElement("searchFilter")
	espXmlmc.SetParam("column", "h_type")
	espXmlmc.SetParam("value", "1")
	espXmlmc.SetParam("matchType", "exact")
	espXmlmc.CloseElement("searchFilter")
	espXmlmc.SetParam("maxResults", "1")

	XMLTeamSearch, xmlmcErr := espXmlmc.Invoke("data", "entityBrowseRecords2")
	if xmlmcErr != nil {
		logger(4, "Unable to Search for Team: "+fmt.Sprintf("%v", xmlmcErr), true)
		//log.Fatal(xmlmcErr)
		return boolReturn, strReturn
	}
	var xmlRespon xmlmcTeamListResponse

	err = xml.Unmarshal([]byte(XMLTeamSearch), &xmlRespon)
	if err != nil {
		logger(4, "Unable to Search for Team: "+fmt.Sprintf("%v", err), true)
	} else {
		if xmlRespon.MethodResult != "ok" {
			logger(4, "Unable to Search for Team: "+xmlRespon.State.ErrorRet, true)
		} else {
			//-- Check Response
			if xmlRespon.TeamName != "" {
				if strings.ToLower(xmlRespon.TeamName) == strings.ToLower(teamName) {
					strReturn = xmlRespon.TeamID
					boolReturn = true
					//-- Add Team to Cache
					var newTeamForCache teamListStruct
					newTeamForCache.TeamID = strReturn
					newTeamForCache.TeamName = teamName
					teamNamedMap := []teamListStruct{newTeamForCache}
					mutexTeams.Lock()
					teams = append(teams, teamNamedMap...)
					mutexTeams.Unlock()
				}
			}
		}
	}
	return boolReturn, strReturn
}

// seachCategory -- Function to check if passed-through support category name is on the instance
func searchCategory(categoryCode, categoryGroup string) (bool, string, string) {
	espXmlmc, err := NewEspXmlmcSession()
	if err != nil {
		return false, "Unable to create connection", ""
	}

	boolReturn := false
	idReturn := ""
	strReturn := ""
	//-- ESP Query for category
	espXmlmc.SetParam("codeGroup", categoryGroup)
	espXmlmc.SetParam("code", categoryCode)
	var XMLSTRING = espXmlmc.GetParam()
	XMLCategorySearch, xmlmcErr := espXmlmc.Invoke("data", "profileCodeLookup")
	if xmlmcErr != nil {
		logger(4, "XMLMC API Invoke Failed for "+categoryGroup+" Category ["+categoryCode+"]: "+fmt.Sprintf("%v", xmlmcErr), false)
		logger(1, "Category Search XML "+fmt.Sprintf("%s", XMLSTRING), false)
		return boolReturn, idReturn, strReturn
	}
	var xmlRespon xmlmcCategoryListResponse

	err = xml.Unmarshal([]byte(XMLCategorySearch), &xmlRespon)
	if err != nil {
		logger(4, "Unable to unmarshal response for "+categoryGroup+" Category: "+fmt.Sprintf("%v", err), false)
		logger(1, "Category Search XML "+fmt.Sprintf("%s", XMLSTRING), false)
	} else {
		if xmlRespon.MethodResult != "ok" {
			logger(4, "Unable to Search for "+categoryGroup+" Category ["+categoryCode+"]: ["+fmt.Sprintf("%v", xmlRespon.MethodResult)+"] "+xmlRespon.State.ErrorRet, false)
			logger(1, "Category Search XML "+fmt.Sprintf("%s", XMLSTRING), false)
			if xmlRespon.State.ErrorRet == "The specified code does not exist" {
				var newCategoryForCache categoryListStruct
				newCategoryForCache.CategoryID = ""
				newCategoryForCache.CategoryCode = categoryCode
				newCategoryForCache.CategoryName = ""
				categoryNamedMap := []categoryListStruct{newCategoryForCache}
				switch categoryGroup {
				case "Request":
					mutexCategories.Lock()
					categories = append(categories, categoryNamedMap...)
					mutexCategories.Unlock()
				case "Closure":
					mutexCloseCategories.Lock()
					closeCategories = append(closeCategories, categoryNamedMap...)
					mutexCloseCategories.Unlock()
				}
			}
		} else {
			//-- Check Response
			if xmlRespon.CategoryName != "" {
				strReturn = xmlRespon.CategoryName
				idReturn = xmlRespon.CategoryID
				logger(3, "[CATEGORY] [SUCCESS] Methodcall result OK for "+categoryGroup+" Category ["+categoryCode+"] : ["+strReturn+"]", false)
				boolReturn = true
				//-- Add Category to Cache
				var newCategoryForCache categoryListStruct
				newCategoryForCache.CategoryID = idReturn
				newCategoryForCache.CategoryCode = categoryCode
				newCategoryForCache.CategoryName = strReturn
				categoryNamedMap := []categoryListStruct{newCategoryForCache}
				switch categoryGroup {
				case "Request":
					mutexCategories.Lock()
					categories = append(categories, categoryNamedMap...)
					mutexCategories.Unlock()
				case "Closure":
					mutexCloseCategories.Lock()
					closeCategories = append(closeCategories, categoryNamedMap...)
					mutexCloseCategories.Unlock()
				}
			} else {
				logger(3, "[CATEGORY] [FAIL] Methodcall result OK for "+categoryGroup+" Category ["+categoryCode+"] but category name blank: ["+xmlRespon.CategoryID+"] ["+xmlRespon.CategoryName+"]", false)
				logger(3, "[CATEGORY] [FAIL] Category Search XML "+fmt.Sprintf("%s", XMLSTRING), false)
			}
		}
	}
	return boolReturn, idReturn, strReturn
}

//padCalLRef -- Function to pad Call Reference to specified digits, adding an optional prefix
func padCallRef(strIntCallRef, prefix string, length int) (paddedRef string) {
	if len(strIntCallRef) < length {
		padCount := length - len(strIntCallRef)
		strIntCallRef = strings.Repeat("0", padCount) + strIntCallRef
	}
	paddedRef = prefix + strIntCallRef
	return
}

//loadConfig -- Function to Load Configruation File
func loadConfig() (swImportConfStruct, bool) {
	boolLoadConf := true
	//-- Check Config File File Exists
	cwd, _ := os.Getwd()
	configurationFilePath := cwd + "/" + configFileName
	logger(1, "Loading Config File: "+configurationFilePath, false)
	if _, fileCheckErr := os.Stat(configurationFilePath); os.IsNotExist(fileCheckErr) {
		logger(4, "No Configuration File", true)
		os.Exit(102)
	}
	//-- Load Config File
	file, fileError := os.Open(configurationFilePath)
	//-- Check For Error Reading File
	if fileError != nil {
		logger(4, "Error Opening Configuration File: "+fmt.Sprintf("%v", fileError), true)
		boolLoadConf = false
	}

	//-- New Decoder
	decoder := json.NewDecoder(file)
	//-- New Var based on swImportConfStruct
	edbConf := swImportConfStruct{}
	//-- Decode JSON
	err := decoder.Decode(&edbConf)
	//-- Error Checking
	if err != nil {
		logger(4, "Error Decoding Configuration File: "+fmt.Sprintf("%v", err), true)
		boolLoadConf = false
	}
	//-- Return New Config
	return edbConf, boolLoadConf
}

//logout -- XMLMC Logout
//-- Adds details to log file, ends user ESP session
func logout() {
	//-- End output
	espLogger("Requests Logged: "+fmt.Sprintf("%d", counters.created), "debug")
	espLogger("Requests Skipped: "+fmt.Sprintf("%d", counters.createdSkipped), "debug")
	espLogger("Time Taken: "+fmt.Sprintf("%v", endTime), "debug")
	espLogger("---- Supportworks Call Import Complete ---- ", "debug")
	logger(1, "Logout", true)
}

//buildConnectionString -- Build the connection string for the SQL driver
func buildConnectionString() string {
	connectString := ""
	//Build
	if appDBDriver == "" || swImportConf.DSNConf.Driver == "" || swImportConf.DSNConf.Server == "" || swImportConf.DSNConf.Database == "" || swImportConf.DSNConf.UserName == "" || swImportConf.DSNConf.Port == 0 {
		logger(4, "Application Database configuration not set.", true)
		return ""
	}
	switch appDBDriver {
	case "mssql":
		connectString = "server=" + swImportConf.DSNConf.Server
		connectString = connectString + ";database=" + swImportConf.DSNConf.Database
		connectString = connectString + ";user id=" + swImportConf.DSNConf.UserName
		connectString = connectString + ";password=" + swImportConf.DSNConf.Password
		if swImportConf.DSNConf.Encrypt == false {
			connectString = connectString + ";encrypt=disable"
		}
		if swImportConf.DSNConf.Port != 0 {
			var dbPortSetting string
			dbPortSetting = strconv.Itoa(swImportConf.DSNConf.Port)
			connectString = connectString + ";port=" + dbPortSetting
		}
	case "mysql":
		connectString = swImportConf.DSNConf.UserName + ":" + swImportConf.DSNConf.Password
		connectString = connectString + "@tcp(" + swImportConf.DSNConf.Server + ":"
		if swImportConf.DSNConf.Port != 0 {
			var dbPortSetting string
			dbPortSetting = strconv.Itoa(swImportConf.DSNConf.Port)
			connectString = connectString + dbPortSetting
		} else {
			connectString = connectString + "3306"
		}
		connectString = connectString + ")/" + swImportConf.DSNConf.Database

	case "mysql320":
		var dbPortSetting string
		dbPortSetting = strconv.Itoa(swImportConf.DSNConf.Port)
		connectString = "tcp:" + swImportConf.DSNConf.Server + ":" + dbPortSetting
		connectString = connectString + "*" + swImportConf.DSNConf.Database + "/" + swImportConf.DSNConf.UserName + "/" + swImportConf.DSNConf.Password
	case "csv":
		connectString = "DSN=" + swImportConf.DSNConf.Database + ";Extended Properties='text;HDR=Yes;FMT=Delimited'"
		appDBDriver = "odbc"
	case "xls":
		connectString = "DSN=" + swImportConf.DSNConf.Database + ";"
		appDBDriver = "odbc"
	}

	return connectString
}

// logger -- function to append to the current log file
func logger(t int, s string, outputtoCLI bool) {
	cwd, _ := os.Getwd()
	logPath := cwd + "/log"
	logFileName := logPath + "/SW_Call_Import_" + timeNow + ".log"

	//-- If Folder Does Not Exist then create it
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		err := os.Mkdir(logPath, 0777)
		if err != nil {
			color.Red("Error Creating Log Folder %q: %s \r", logPath, err)
			os.Exit(101)
		}
	}

	//-- Open Log File
	f, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0777)
	// don't forget to close it
	defer f.Close()
	if err != nil {
		color.Red("Error Creating Log File %q: %s \n", logFileName, err)
		os.Exit(100)
	}
	// assign it to the standard logger
	log.SetOutput(f)
	var errorLogPrefix string
	//-- Create Log Entry
	switch t {
	case 1:
		errorLogPrefix = "[DEBUG] "
		if outputtoCLI {
			color.Set(color.FgGreen)
			defer color.Unset()
		}
	case 2:
		errorLogPrefix = "[MESSAGE] "
		if outputtoCLI {
			color.Set(color.FgGreen)
			defer color.Unset()
		}
	case 3:
		if outputtoCLI {
			color.Set(color.FgGreen)
			defer color.Unset()
		}
	case 4:
		errorLogPrefix = "[ERROR] "
		if outputtoCLI {
			color.Set(color.FgRed)
			defer color.Unset()
		}
	case 5:
		errorLogPrefix = "[WARNING]"
		if outputtoCLI {
			color.Set(color.FgYellow)
			defer color.Unset()
		}
	case 6:
		if outputtoCLI {
			color.Set(color.FgYellow)
			defer color.Unset()
		}
	}

	if outputtoCLI {
		fmt.Printf("%v \n", errorLogPrefix+s)
	}

	log.Println(errorLogPrefix + s)
}

// espLogger -- Log to ESP
func espLogger(message string, severity string) {
	espXmlmc := apiLib.NewXmlmcInstance(swImportConf.HBConf.URL)
	espXmlmc.SetAPIKey(swImportConf.HBConf.APIKey)
	espXmlmc.SetParam("fileName", "Call_Import")
	espXmlmc.SetParam("group", "general")
	espXmlmc.SetParam("severity", severity)
	espXmlmc.SetParam("message", message)
	espXmlmc.Invoke("system", "logMessage")
}

// SetInstance sets the Zone and Instance config from the passed-through strZone and instanceID values
func SetInstance(strZone string, instanceID string) {
	//-- Set Zone
	SetZone(strZone)
	//-- Set Instance
	xmlmcInstanceConfig.instance = instanceID
	return
}

// SetZone - sets the Instance Zone to Overide current live zone
func SetZone(zone string) {
	xmlmcInstanceConfig.zone = zone
	return
}

// getInstanceURL -- Function to build XMLMC End Point
func getInstanceURL() string {
	xmlmcInstanceConfig.url = "https://"
	xmlmcInstanceConfig.url += xmlmcInstanceConfig.zone
	xmlmcInstanceConfig.url += "api.hornbill.com/"
	xmlmcInstanceConfig.url += xmlmcInstanceConfig.instance
	xmlmcInstanceConfig.url += "/xmlmc/"
	return xmlmcInstanceConfig.url
}

//epochToDateTime - converts an EPOCH value STRING var in to a date-time format compatible with Hornbill APIs
func epochToDateTime(epochDateString string) string {
	dateTime := ""
	i, err := strconv.ParseInt(epochDateString, 10, 64)
	if err != nil {
		logger(5, "EPOCH String to Int conversion FAILED: "+fmt.Sprintf("%v", err), false)
	} else {
		dateTimeStr := fmt.Sprintf("%s", time.Unix(i, 0))
		for i := 0; i < 19; i++ {
			dateTime = dateTime + string(dateTimeStr[i])
		}
	}
	return dateTime
}

//NewEspXmlmcSession - New Xmlmc Session variable (Cloned Session)
func NewEspXmlmcSession() (*apiLib.XmlmcInstStruct, error) {
	time.Sleep(150 * time.Millisecond)
	//	espXmlmcLocal := apiLib.NewXmlmcInstance(swImportConf.HBConf.URL)
	//	espXmlmcLocal.SetSessionID(espXmlmc.GetSessionID())
	espXmlmcLocal := apiLib.NewXmlmcInstance(swImportConf.HBConf.URL)
	espXmlmcLocal.SetAPIKey(swImportConf.HBConf.APIKey)
	return espXmlmcLocal, nil
}
