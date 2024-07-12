package collect

import (
	"crypto/tls"
	"encoding/csv"
	"errors"
	"github.com/PuerkitoBio/goquery"
	"github.com/rs/zerolog/log"
	"github.com/xyproto/unzip"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
)

func CloudIpv4Prefixes() {
	log.Info().Msg("Starting to collect prefixes...")
	err := downloadFile("https://ip-ranges.amazonaws.com/ip-ranges.json", "ipv4_prefixes/aws/ip-ranges.json")
	if err != nil {
		log.Error().Err(err).Msg("Error downloading AWS prefixes")
		err = os.RemoveAll("ipv4_prefixes/aws")
		if err != nil {
			log.Warn().Err(err).Msg("Error removing AWS folder")
		}
	}
	err = downloadFile("https://www.cloudflare.com/ips-v4", "ipv4_prefixes/cloudflare/ips-v4.txt")
	if err != nil {
		log.Error().Err(err).Msg("Error downloading Cloudflare prefixes")
		err = os.RemoveAll("ipv4_prefixes/cloudflare")
		if err != nil {
			log.Warn().Err(err).Msg("Error removing Cloudflare folder")
		}
	}
	err = downloadFile("https://www.gstatic.com/ipranges/cloud.json", "ipv4_prefixes/google/cloud.json")
	if err != nil {
		log.Error().Err(err).Msg("Error downloading Google prefixes")
		err = os.RemoveAll("ipv4_prefixes/google")
		if err != nil {
			log.Warn().Err(err).Msg("Error removing Google folder")
		}
	}

	msPrefixUrl, err := getMsDownloadLink("https://www.microsoft.com/en-us/download/details.aspx?id=56519")
	if err != nil {
		log.Error().Err(err).Msg("Error getting Azure download link")
	}
	err = downloadFile(msPrefixUrl, "ipv4_prefixes/microsoft/azure.json")
	if err != nil {
		log.Error().Err(err).Msg("Error downloading Azure prefixes")
		err = os.RemoveAll("ipv4_prefixes/microsoft")
		if err != nil {
			log.Warn().Err(err).Msg("Error removing Microsoft folder")
		}
	}
	storeAzureSpecialPrefix("ipv4_prefixes/microsoft/")

	err = downloadFile("https://docs.oracle.com/iaas/tools/public_ip_ranges.json", "ipv4_prefixes/oracle/public_ip_ranges.json")
	if err != nil {
		log.Error().Err(err).Msg("Error downloading Oracle prefixes")
		err = os.RemoveAll("ipv4_prefixes/oracle")
		if err != nil {
			log.Warn().Err(err).Msg("Error removing Oracle folder")
		}
	}
	err = downloadFile("https://geoip.linode.com/", "ipv4_prefixes/linode/linode.csv")
	if err != nil {
		log.Error().Err(err).Msg("Error downloading Linode prefixes")
		err = os.RemoveAll("ipv4_prefixes/linode")
		if err != nil {
			log.Warn().Err(err).Msg("Error removing Linode folder")
		}
	}
	err = downloadFile("https://api.fastly.com/public-ip-list", "ipv4_prefixes/fastly/public-ip-list.json")
	if err != nil {
		log.Error().Err(err).Msg("Error downloading Fastly prefixes")
		err = os.RemoveAll("ipv4_prefixes/fastly")
		if err != nil {
			log.Warn().Err(err).Msg("Error removing Fastly folder")
		}
	}
	err = downloadFile("https://digitalocean.com/geo/google.csv", "ipv4_prefixes/digitalocean/google.csv")
	if err != nil {
		log.Error().Err(err).Msg("Error downloading DigitalOcean prefixes")
		err = os.RemoveAll("ipv4_prefixes/digitalocean")
		if err != nil {
			log.Warn().Err(err).Msg("Error removing DigitalOcean folder")
		}
	}

	err = downloadFile("https://techdocs.akamai.com/property-manager/pdfs/akamai_ipv4_ipv6_CIDRs-txt.zip", "ipv4_prefixes/akamai/akamai_ipv4_ipv6_CIDRs-txt.zip")
	if err != nil {
		log.Error().Err(err).Msg("Error downloading Akamai prefixes")
		err = os.RemoveAll("ipv4_prefixes/akamai")
		if err != nil {
			log.Warn().Err(err).Msg("Error removing Akamai folder")
		}
	} else {
		err = unzip.Extract("ipv4_prefixes/akamai/akamai_ipv4_ipv6_CIDRs-txt.zip", "ipv4_prefixes/akamai/")
		if err != nil {
			log.Error().Err(err).Msg("Error unzipping Akamai prefixes")
		} else {
			err = os.Remove("ipv4_prefixes/akamai/akamai_ipv4_ipv6_CIDRs-txt.zip")
			if err != nil {
				log.Error().Err(err).Msg("Error removing Akamai zip file")
			}
			err = os.RemoveAll("ipv4_prefixes/akamai/__MACOSX")
			if err != nil {
				log.Warn().Err(err).Msg("Error removing __MACOSX folder from akamai zip file")
			}
		}
	}
	log.Info().Msg("Finished collecting prefixes")
}

func downloadFile(link string, path string) error {
	dir := filepath.Dir(path)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return err
		}
	}

	out, err := os.Create(path)
	if err != nil {
		return err
	}
	defer out.Close()
	req, err := http.NewRequest("GET", link, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
	client := http.Client{}
	//resp, err := http.Get(link)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.New("HTTP response error: " + resp.Status)
	}

	// Writer the body to file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	return nil
}

func getMsDownloadLink(link string) (string, error) {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			MaxVersion: tls.VersionTLS12,
		},
	}
	client := &http.Client{Transport: tr}
	req, err := http.NewRequest("GET", link, nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", errors.New("Error: status code " + resp.Status)
	}

	var downloadLink string
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return "", err
	}

	doc.Find("a").Each(func(i int, s *goquery.Selection) {
		href, exists := s.Attr("href")
		if exists && href != "" && hrefContains(href, "download.microsoft.com") && hrefEndsWith(href, ".json") {
			downloadLink = href
			return
		}
	})
	return downloadLink, nil
}

func storeAzureSpecialPrefix(dir string) {
	ipv4Path := path.Join(dir, "special_ipv4_prefixes.csv")
	ipv4Prefixes := []string{"168.63.129.16/32"}
	store(ipv4Path, ipv4Prefixes)
	ipv6Path := path.Join(dir, "special_ipv6_prefixes.csv")
	ipv6Prefixes := []string{"FE80::1234:5678:9ABC/128"}
	store(ipv6Path, ipv6Prefixes)
}

func store(ipPath string, content []string) {
	fh, err := os.Create(ipPath)
	if err != nil {
		log.Error().Err(err).Msg("Error downloading Azure special prefixes " + err.Error())
	}
	defer fh.Close()

	writer := csv.NewWriter(fh)
	defer writer.Flush()

	// Write the header
	header := []string{"ip_prefix"}
	if err := writer.Write(header); err != nil {
		log.Fatal().Err(err).Msg("Failed to write header to file")
	}

	// Write the records
	for _, record := range content {
		row := []string{record}
		if err := writer.Write(row); err != nil {
			log.Fatal().Err(err).Msg("Failed to write record to file")
		}
	}
}

func hrefContains(href, substr string) bool {
	return len(href) >= len(substr) && strings.Contains(href, substr)
}

func hrefEndsWith(href, suffix string) bool {
	if len(href) < len(suffix) {
		return false
	}
	return strings.HasSuffix(href, suffix) && path.Ext(href) == suffix
}
