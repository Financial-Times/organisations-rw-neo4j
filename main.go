package main

import (
	"fmt"
	"os"

	"github.com/Financial-Times/base-ft-rw-app-go/baseftrwapp"
	"github.com/Financial-Times/go-fthealth/v1a"
	queueConsumer "github.com/Financial-Times/message-queue-gonsumer/consumer"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	organisations "github.com/Financial-Times/organisations-rw-neo4j/organisations"
	log "github.com/Sirupsen/logrus"
	"github.com/jawher/mow.cli"
	"github.com/jmcvetta/neoism"
	"net/http"
	"strings"
	"os/signal"
	"sync"
	"syscall"
	"encoding/json"
)

func main() {
	log.SetLevel(log.InfoLevel)
	log.Printf("Application started with args %s", os.Args)
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


	consumerAddrs := app.StringOpt("consumer_proxy_addr", "https://kafka-proxy-pr-uk-t-1.glb.ft.com,https://kafka-proxy-pr-uk-t-2.glb.ft.com", "Comma separated kafka proxy hosts for message consuming.")
	consumerGroupID := app.StringOpt("consumer_group_id", "idiOrgs", "Kafka qroup id used for message consuming.")
	consumerOffset := app.StringOpt("consumer_offset", "", "Kafka read offset.")
	consumerAutoCommitEnable := app.BoolOpt("consumer_autocommit_enable", false, "Enable autocommit for small messages.")
	consumerAuthorizationKey := app.StringOpt("consumer_authorization_key", "Basic Y29jbzp2SDRkbjBwWUdTRjRQY2YwRGUvQzRyWXhMSGkwZmEzNzMxa3lBbXlGQW1jPQo=", "The authorization key required to UCS access.")

	topic := app.StringOpt("topic", "IDIOrganisation", "Kafka topic.")

	consumerConfig := queueConsumer.QueueConfig{}
	consumerConfig.Addrs = strings.Split(*consumerAddrs, ",")
	consumerConfig.Group = *consumerGroupID
	consumerConfig.Topic = *topic
	consumerConfig.Offset = *consumerOffset
	consumerConfig.AuthorizationKey = *consumerAuthorizationKey
	consumerConfig.AutoCommitEnable = *consumerAutoCommitEnable

	app.Action = func() {
		db, err := neoism.Connect(*neoURL)

		db.Session.Client = &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 100}}

		if err != nil {
			log.Errorf("Could not connect to neo4j, error=[%s]\n", err)
		}
		batchRunner := neoutils.NewBatchCypherRunner(neoutils.StringerDb{db}, *batchSize)

		organisationsDriver := organisations.NewCypherOrganisationService(batchRunner, db)
		organisationsDriver.Initialise()

		baseftrwapp.OutputMetricsIfRequired(*graphiteTCPAddress, *graphitePrefix, *logMetrics)

		engs := map[string]baseftrwapp.Service{
			"organisations": organisationsDriver,
		}

		var checks []v1a.Check
		for _, e := range engs {
			checks = append(checks, makeCheck(e, batchRunner))
		}

		go baseftrwapp.RunServer(engs,
			v1a.Handler("ft-organisations_rw_neo4j ServiceModule", "Writes 'organisations' to Neo4j, usually as part of a bulk upload done on a schedule", checks...),
			*port, "organisations-rw-neo4j", *env)

	 f := serviceHandler{organisationsDriver}

		consumer := queueConsumer.NewConsumer(consumerConfig, f.writeKafkaMessage, http.Client{})
		consumeKafkaMessages(consumer)
	}
	log.SetLevel(log.InfoLevel)
	log.Println("Application started with args %e", os.Args)

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

func consumeKafkaMessages(consumer queueConsumer.Consumer) {
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		consumer.Start()
		wg.Done()
	}()

	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	consumer.Stop()
	wg.Wait()
}

type serviceHandler struct {
	s organisations.Service
}

// Takes a kafka message and parses it to an organisation and calls write
func (sh serviceHandler) writeKafkaMessage(msg queueConsumer.Message) {
	body := strings.NewReader(msg.Body)
	dec := json.NewDecoder(body)
	inst, _, err := sh.s.DecodeJSON(dec)
	err = sh.s.Write(inst)

	if err == nil {
		log.Infof("Successfully written msg: %s", msg)
	} else {
		log.Errorf("Error processing msg: %s", msg)
	}
}
