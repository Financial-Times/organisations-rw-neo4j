package main

import (
	"fmt"
	"os"

	"flag"
	"github.com/Financial-Times/base-ft-rw-app-go/baseftrwapp"
	"github.com/Financial-Times/go-fthealth/v1a"
	queueConsumer "github.com/Financial-Times/message-queue-gonsumer/consumer"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/Financial-Times/organisations-rw-neo4j/organisations"
	log "github.com/Sirupsen/logrus"
	"github.com/jawher/mow.cli"
	"github.com/jmcvetta/neoism"
	"net/http"
	"strings"
)

func main() {
	log.SetLevel(log.InfoLevel)
	log.Println("Application started with args %s", os.Args)
	app := cli.App("organisations-rw-neo4j", "A RESTful API for managing Organisations in neo4j")
	neoURL := app.StringOpt("neo-url", "http://localhost:7474/db/data", "neo4j endpoint URL")
	// neoURL := app.StringOpt("neo-url", "http://ftper58827-law1b-eu-t:8080/db/data", "neo4j endpoint URL")
	port := app.IntOpt("port", 8080, "Port to listen on")
	env := app.StringOpt("env", "local", "environment this app is running in")
	batchSize := app.IntOpt("batchSize", 1024, "Maximum number of statements to execute per batch")
	graphiteTCPAddress := app.StringOpt("graphiteTCPAddress", "",
		"Graphite TCP address, e.g. graphite.ft.com:2003. Leave as default if you do NOT want to output to graphite (e.g. if running locally)")
	graphitePrefix := app.StringOpt("graphitePrefix", "",
		"Prefix to use. Should start with content, include the environment, and the host name. e.g. content.test.organisations.rw.neo4j.ftaps58938-law1a-eu-t")
	logMetrics := app.BoolOpt("logMetrics", false, "Whether to log metrics. Set to true if running locally and you want metrics output")

	// Add consumer group config from command line - what  is the difference between above and using flags???
	consumerAddrs := flag.String("consumer_proxy_addr", "", "Comma separated kafka proxy hosts for message consuming.")
	consumerGroup := flag.String("consumer_group_id", "", "Kafka qroup id used for message consuming.")
	consumerOffset := flag.String("consumer_offset", "", "Kafka read offset.")
	consumerAutoCommitEnable := flag.Bool("consumer_autocommit_enable", false, "Enable autocommit for small messages.")
	consumerAuthorizationKey := flag.String("consumer_authorization_key", "", "The authorization key required to UCS access.")
	topic := flag.String("topic", "", "Kafka topic.")

	// Build up the consumer config struct
	consumerConfig := queueConsumer.QueueConfig{}
	consumerConfig.Addrs = strings.Split(consumerAddrs, ",")
	consumerConfig.Group = consumerGroup
	consumerConfig.Topic = topic
	consumerConfig.Offset = consumerOffset
	consumerConfig.AuthorizationKey = consumerAuthorizationKey
	consumerConfig.AutoCommitEnable = consumerAutoCommitEnable

	app.Action = func() {
		db, err := neoism.Connect(*neoURL)
		db.Session.Client = &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 100}}
		if err != nil {
			log.Errorf("Could not connect to neo4j, error=[%s]\n", err)
		}
		batchRunner := neoutils.NewBatchCypherRunner(neoutils.StringerDb{db}, *batchSize)
		organisationsDriver := organisations.NewCypherOrganisationService(batchRunner, db, *consumerConfig)
		organisationsDriver.Initialise()

		baseftrwapp.OutputMetricsIfRequired(*graphiteTCPAddress, *graphitePrefix, *logMetrics)

		engs := map[string]baseftrwapp.Service{
			"organisations": organisationsDriver,
		}

		var checks []v1a.Check
		for _, e := range engs {
			checks = append(checks, makeCheck(e, batchRunner))
		}

		baseftrwapp.RunServer(engs,
			v1a.Handler("ft-organisations_rw_neo4j ServiceModule", "Writes 'organisations' to Neo4j, usually as part of a bulk upload done on a schedule", checks...),
			*port, "organisations-rw-neo4j", *env)

		organisationsDriver.ConsumeMessages()
	}
	log.SetLevel(log.InfoLevel)
	log.Println("Application started with args %s", os.Args)

	app.Run(os.Args)
}

func makeCheck(service baseftrwapp.Service, cr neoutils.CypherRunner) v1a.Check {
	return v1a.Check{
		BusinessImpact:   "Cannot read/write organisations via this writer",
		Name:             "Check connectivity to Neo4j - neoUrl is a parameter in hieradata for this service",
		PanicGuide:       "TODO - write panic guide",
		Severity:         1,
		TechnicalSummary: fmt.Sprintf("Cannot connect to Neo4j instance %s with at least one organisation loaded in it", cr),
		Checker:          func() (string, error) { return "", service.Check() },
	}
}
