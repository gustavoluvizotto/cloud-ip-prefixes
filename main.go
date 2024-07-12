package main

import (
	"flag"
	"github.com/gustavoluvizotto/cloud-ip-prefixes/collect"
	"github.com/gustavoluvizotto/cloud-ip-prefixes/s3upload"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
)

func main() {
	var logFile string
	flag.StringVar(&logFile,
		"log-file",
		"",
		"The log file in JSON format")

	var willCollect bool
	flag.BoolVar(&willCollect,
		"collect",
		false,
		"Collect the IP prefixes")

	var willUpload bool
	flag.BoolVar(&willUpload,
		"upload",
		false,
		"Upload the IP prefixes to S3")

	flag.Parse()

	log.Logger = log.Output(zerolog.NewConsoleWriter())
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if logFile != "" {
		fh, err := os.Create(logFile)
		if err != nil {
			log.Fatal().Err(err).Str("file", logFile).Msg("Error creating log file")
		}
		log.Logger = log.Output(fh)
	}

	if willCollect {
		collect.CloudIpv4Prefixes()
	}
	if willUpload {
		s3upload.CloudIpv4PrefixesIfNecessary()
	}
	if logFile != "" {
		s3upload.UploadLog(logFile)
	}
}
