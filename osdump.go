package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/valyala/fastjson"
)

// Holds the configuration
type Configuration struct {
	Base     string
	User     string
	Password string
	Tls      bool
	Tls_ca   string
	Index    string
	Size     int
	File     string
	Brotli   bool
	Quality  int
}

// Holds the dump context
type Context struct {
	Size     int
	After    string
	Counter  int
	Client   *http.Client
	Parser   *fastjson.Parser
	Template *template.Template
	Tasks    *chan []byte
}

// Line feed "constant"
var ln = []byte{10}

// Query template for search_after
const query_template string = `{
	"size": {{.Size}},
	"query": {"bool": {"must": {"match_all": {}}}},{{if .After}}
	"search_after": ["{{.After}}"],{{end}}
	"sort": [
	  { "_id": "asc" } 
	]
}`

// Default setting for debug log
var debug bool = false

// Helper for checking errs
func check(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

// Helper for debug logging
func debugf(format string, args ...interface{}) {
	// Without this the sprintf will get called even though the final message might be discarded
	if debug {
		log.Printf(format, args...)
	}
}

// Gets configuration from the command line parameters
func get_config() *Configuration {
	var config Configuration
	flag.StringVar(&config.Base, "base", "https://localhost:9200", "opensearch base url")
	flag.StringVar(&config.User, "user", "graylog", "opensearch user")
	flag.StringVar(&config.Password, "password", "password", "opensearch user")
	flag.StringVar(&config.Tls_ca, "ca", "ca.pem", "CA certificate")
	flag.StringVar(&config.Index, "index", "graylog_0", "opensearch index")
	flag.IntVar(&config.Size, "size", 1000, "search window size")
	flag.StringVar(&config.File, "file", "graylog_0.json", "target file for export")
	flag.BoolVar(&config.Brotli, "brotli", false, "compress using brotli")
	flag.IntVar(&config.Quality, "quality", 2, "brotli quality setting")
	flag.BoolVar(&debug, "debug", false, "debug logging")
	flag.Parse()
	if strings.HasPrefix(config.Base, "https") {
		config.Tls = true
	}
	debugf("Configuration: %+v", config)
	return &config
}

// Builds opensearch query template
func build_query_template() *template.Template {
	tmpl, err := template.New("query").Parse(query_template)
	check(err)
	debugf("Query template: %+v", tmpl)
	return tmpl
}

// Builds HTTP or HTTPS client depending on configuration
func build_http_client(conf *Configuration) *http.Client {
	// If configured, build TLS client
	if conf.Tls {
		return build_tls_http_client(conf)
	}
	// Else build non-TLS client
	debugf("Built http client")
	return &http.Client{}
}

// Builds HTTPS client, if requested
func build_tls_http_client(conf *Configuration) *http.Client {
	tlsConfig := &tls.Config{RootCAs: x509.NewCertPool()}
	transport := &http.Transport{TLSClientConfig: tlsConfig}
	client := &http.Client{Transport: transport}
	pemData, err := os.ReadFile(conf.Tls_ca)
	check(err)
	ok := tlsConfig.RootCAs.AppendCertsFromPEM(pemData)
	if !ok {
		log.Fatalf("Parsing CA certificate failed!")
	}
	debugf("Built https client")
	return client
}

// Helper function for opensearch queries
func http_get(uri string, body []byte, config *Configuration, ctx *Context) []byte {
	debugf("URI for HTTP GET: %s", uri)
	br := bytes.NewReader(body)
	req, err := http.NewRequest("GET", uri, br)
	check(err)
	req.Header.Add("Content-Type", "application/json")
	req.SetBasicAuth(config.User, config.Password)
	resp, err := ctx.Client.Do(req)
	check(err)
	defer resp.Body.Close()
	debugf("Response code: %d", resp.StatusCode)
	bodyBytes, err := io.ReadAll(resp.Body)
	check(err)
	debugf("Response body: %s", bodyBytes)
	// Anything besides 200 OK is probably fatal
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("Got invalid HTTP status code: %d", resp.StatusCode)
	}
	return bodyBytes
}

// Queries the opensearch for total amount of data
func query_count_database(config *Configuration, ctx *Context) int {
	count := 0
	// Request
	uri := fmt.Sprintf("%s/%s/_count", config.Base, config.Index)
	body := http_get(uri, nil, config, ctx)
	// Handle results
	json, err := ctx.Parser.ParseBytes(body)
	check(err)
	if json.Exists("count") {
		count = json.GetInt("count")
	}
	debugf("Returning count %d", count)
	return count
}

// Queries the opensearch for one window of data
func query_search_database(config *Configuration, ctx *Context) []byte {
	uri := fmt.Sprintf("%s/%s/_search?request_cache=true", config.Base, config.Index)
	buf := new(bytes.Buffer)
	err := ctx.Template.Execute(buf, ctx)
	check(err)
	bodyBytes := http_get(uri, buf.Bytes(), config, ctx)
	return bodyBytes
}

// Parse search results for single window
func parse_search_results(input []byte, ctx *Context) [][]byte {
	var result [][]byte
	// Parse JSON
	json, err := ctx.Parser.ParseBytes(input)
	check(err)
	// Sanity check
	if !json.Exists("hits") {
		log.Fatalf("JSON result looks incorrect: %s", json)
	}
	results := json.Get("hits").Get("hits").GetArray()
	// If the array is empty, we probably just parsed everything already
	if len(results) == 0 {
		if debug {
			log.Println("Did not get any results, bailing out")
		}

		return [][]byte{}
	}

	// Iterate over results
	for _, v := range results {
		// Update the search_after
		sort := string(v.GetStringBytes("sort", "0"))
		if sort != "" {
			ctx.After = sort
		}
		// Remove sort information
		if v.Exists("sort") {
			v.Del("sort")
		}
		// Increase query counter
		ctx.Counter++
		// Add to results
		result = append(result, v.MarshalTo([]byte{}))
	}
	return result
}

// Loops the search and sends the results to a channel
func producer(ctx *Context, config *Configuration, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		q := query_search_database(config, ctx)
		r := parse_search_results(q, ctx)
		for x := range r {
			*ctx.Tasks <- r[x]
		}
		if len(r) == 0 {
			if debug {
				log.Println("Nothing more to produce, breaking the loop")
			}
			break
		}
	}
	if debug {
		log.Println("Producer done")
	}

}

// Reads results from a channel and writes them
func consumer(ctx *Context, config *Configuration, wg *sync.WaitGroup) {
	defer wg.Done()

	// Prepare the output file for writing
	// Use os.O_CREATE and os.O_EXCL flags to ensure the file is created only if it does not already exist
	f, err := os.OpenFile(config.File, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	check(err)
	w := bufio.NewWriter(f)

	// Build a writer that works both with straight buffering, and brotli's writer
	// Apparently only io.Writer seems to be common with these two writers
	var out io.Writer
	if config.Brotli {
		opts := brotli.WriterOptions{}
		opts.Quality = config.Quality
		cout := brotli.NewWriterOptions(w, opts)
		out = cout
		defer func() {
			cout.Flush()
			cout.Close()
			w.Flush()
			f.Close()
		}()
	} else {
		out = w
		defer func() {
			w.Flush()
			f.Close()
		}()
	}

	// Write received data
	for data := range *ctx.Tasks {
		out.Write(data)
		out.Write(ln) // \n
	}
	if debug {
		log.Println("Consumer done")
	}

}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	config := get_config()
	var ctx Context
	ctx.Size = config.Size
	ctx.Template = build_query_template()
	ctx.Client = build_http_client(config)
	ctx.Parser = &fastjson.Parser{}
	tasksChan := make(chan []byte, 100000)
	ctx.Tasks = &tasksChan

	log.Printf("Starting to dump %s", config.Index)
	start := time.Now()
	// Check the count of documents
	c := query_count_database(config, &ctx)
	log.Printf("Index %s has %d documents to dump", config.Index, c)
	if c == 0 {
		log.Fatal("Nothing to dump!")
	}
	// Set up producer
	var pwg sync.WaitGroup
	pwg.Add(1)
	go producer(&ctx, config, &pwg)
	go func() {
		pwg.Wait()
		close(*ctx.Tasks)
		log.Printf("Closed tasks channel")
	}()
	// Set up consumer
	var cwg sync.WaitGroup
	cwg.Add(1)
	go consumer(&ctx, config, &cwg)
	cwg.Wait()
	// Print statistics
	elapsed := time.Since(start)
	log.Printf("Dumped %d records in %d seconds, average speed %d/second", ctx.Counter, int(elapsed.Seconds()), int(float64(ctx.Counter)/elapsed.Seconds()))
	log.Printf("Finished dumping %s", config.Index)
}
