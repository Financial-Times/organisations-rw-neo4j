package main

import (
	_ "net/http/pprof"
	"os"

	"github.com/Financial-Times/base-ft-rw-app-go"
	"github.com/Financial-Times/neo-cypher-runner-go"
	"github.com/Financial-Times/neo-utils-go"
	"github.com/Financial-Times/roles-rw-neo4j/roles"
	log "github.com/Sirupsen/logrus"
	"github.com/jawher/mow.cli"
	"github.com/jmcvetta/neoism"
)

func main() {
	log.SetLevel(log.InfoLevel)
	log.Infof("Application started with args %s", os.Args)

	app := cli.App("roles-rw-neo4j", "A RESTful API for managing Roles in neo4j")
	neoURL := app.StringOpt("neo-url", "http://localhost:7474/db/data", "neo4j endpoint URL")
	port := app.IntOpt("port", 8080, "Port to listen on")
	batchSize := app.IntOpt("batchSize", 1024, "Maximum number of statements to execute per batch")
	graphiteTCPAddress := app.StringOpt("graphiteTCPAddress", "",
		"Graphite TCP address, e.g. graphite.ft.com:2003. Leave as default if you do NOT want to output to graphite (e.g. if running locally)")
	graphitePrefix := app.StringOpt("graphitePrefix", "",
		"Prefix to use. Should start with content, include the environment, and the host name. e.g. content.test.roles.rw.neo4j.ftaps58938-law1a-eu-t")
	logMetrics := app.BoolOpt("logMetrics", false, "Whether to log metrics. Set to true if running locally and you want metrics output")

	app.Action = func() {
		db, err := neoism.Connect(*neoURL)
		if err != nil {
			log.Errorf("Could not connect to neo4j, error=[%s]\n", err)
		}

		batchRunner := neocypherrunner.NewBatchCypherRunner(neoutils.StringerDb{db}, *batchSize)
		rolesDriver := roles.NewCypherDriver(batchRunner, db)
		rolesDriver.Initialise()

		baseftrwapp.OutputMetricsIfRequired(*graphiteTCPAddress, *graphitePrefix, *logMetrics)

		engs := map[string]baseftrwapp.Service{
			"roles": rolesDriver,
		}

		baseftrwapp.RunServer(engs,
			"ft-roles_rw_neo4j ServiceModule",
			"Writes 'roles' to Neo4j, usually as part of a bulk upload done on a schedule",
			*port)
	}

	app.Run(os.Args)
}
