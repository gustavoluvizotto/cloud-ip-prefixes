package s3upload

import (
	"context"
	"fmt"
	"github.com/codingsince1985/checksum"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/rs/zerolog/log"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	endpoint = "localhost:8080"
	bucket   = "catrin"
)

func CloudIpv4PrefixesIfNecessary() {
	mc, err := getMinioClient("upload")
	if err != nil {
		log.Fatal().Err(err).Msg("Cannot retrieve minio client")
	}
	files, err := filePathWalkDir("ipv4_prefixes")
	if err != nil {
		log.Fatal().Err(err).Msg("Cannot list all files to be uploaded")
	}
	fmt.Println(files)

	for _, localFile := range files {
		dirs := path.Dir(localFile)
		provider := strings.Split(dirs, "/")[1]
		if shallUpload(mc, localFile, provider) {
			remoteFile := getRemoteFilePath(provider)
			err = uploadS3(mc, localFile, remoteFile)
		}
	}

}

func filePathWalkDir(root string) ([]string, error) {
	var files []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

func getRemoteFilePath(provider string) string {
	timestamp := time.Now().UTC()
	yearMonthDay := fmt.Sprintf("year=%04d/month=%02d/day=%02d", timestamp.Year(), timestamp.Month(), timestamp.Day())
	remoteFile := fmt.Sprintf("cloud-ip-prefixes/format=raw/provider=%s/%s", provider, yearMonthDay)
	return remoteFile
}

func shallUpload(mc *minio.Client, localFile string, provider string) bool {
	localMd5Sum, err := checksum.MD5sum(localFile)
	if err != nil {
		log.Error().Err(err).Msg("could not calculate local md5sum")
		return false
	}
	remoteMd5Sum, err := getETagFromLatest(mc, provider)
	if err != nil {
		log.Warn().Err(err).Msg("could not get latest etag for provider: " + provider)
		return true
	}
	// do not upload if checksums are equal
	return !strings.EqualFold(localMd5Sum, remoteMd5Sum)
}

func getETagFromLatest(mc *minio.Client, provider string) (string, error) {
	re := regexp.MustCompile(`.*(year=(\d{4})/month=(\d{2})/day=(\d{2})).*`)
	prefix := fmt.Sprintf("cloud-ip-prefixes/format=raw/provider=%s", provider)
	listOpts := minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}
	tsMap := make(map[string]string)
	ctx := context.Background()
	for obj := range mc.ListObjects(ctx, bucket, listOpts) {
		if obj.Err != nil {
			return "", obj.Err
		}
		matches := re.FindStringSubmatch(obj.Key)
		if matches == nil || len(matches) < 5 {
			continue
		}
		year := matches[2]
		month := matches[3]
		day := matches[4]
		tsMap[fmt.Sprintf("%s%s%s", year, month, day)] = obj.ETag
	}

	timestamps := make([]string, 0)
	for k := range tsMap {
		timestamps = append(timestamps, k)
	}
	sort.Strings(timestamps)
	latestTs := timestamps[len(timestamps)-1]
	return tsMap[latestTs], nil
}

func getMinioClient(profile string) (*minio.Client, error) {
	cred := credentials.NewFileAWSCredentials("credentials", profile)
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  cred,
		Secure: false,
	})
	if err != nil {
		return nil, err
	}
	return minioClient, nil
}

func uploadS3(minioClient *minio.Client, localFile string, remoteFile string) error {
	file, err := os.Open(localFile)
	if err != nil {
		return err
	}
	defer file.Close()

	fileStat, err := file.Stat()
	if err != nil {
		return err
	}

	ctx := context.Background()
	opts := minio.PutObjectOptions{ContentType: "application/octet-stream", DisableMultipart: true}
	uploadInfo, err := minioClient.PutObject(ctx, bucket, remoteFile, file, fileStat.Size(), opts)
	if err != nil {
		return err
	}

	log.Info().Str("ETag", uploadInfo.ETag).Msg("Successfully uploaded")

	return nil
}

func UploadLog(logFile string) {
	mc, err := getMinioClient("upload")
	if err != nil {
		log.Fatal().Msg("Cannot retrieve minio client")
	}
	timestamp := time.Now().UTC()
	yearMonthDay := fmt.Sprintf("year=%04d/month=%02d/day=%02d", timestamp.Year(), timestamp.Month(), timestamp.Day())
	timestampStr := fmt.Sprintf("%04d%02d%02d", timestamp.Year(), timestamp.Month(), timestamp.Day())
	remoteLogFile := fmt.Sprintf("artefacts/tool=cloud-ip-prefixes/%s/%s_log.json", yearMonthDay, timestampStr)
	err = uploadS3(mc, logFile, remoteLogFile)
}
