package s3upload

import (
	"context"
	"errors"
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

type s3Obj struct {
	Etag      string
	Timestamp time.Time
}

func CloudIpv4PrefixesIfNecessary() {
	log.Info().Msg("Starting upload files to S3...")
	mc, err := getMinioClient("upload")
	if err != nil {
		log.Fatal().Err(err).Msg("Cannot retrieve minio client")
	}
	files, err := filePathWalkDir("ip_prefixes")
	if err != nil {
		log.Fatal().Err(err).Msg("Cannot list all files to be uploaded")
	}

	for _, localFile := range files {
		dirs := path.Dir(localFile)
		provider := strings.Split(dirs, "/")[1]
		if shallUpload(mc, localFile, provider) {
			remoteFile := getRemoteFilePath(provider, localFile)
			err = uploadS3(mc, localFile, remoteFile)
		}
	}
	log.Info().Msg("Finished upload files to S3")
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

func getRemoteFilePath(provider string, localFile string) string {
	timestamp := time.Now().UTC()
	yearMonthDay := fmt.Sprintf("year=%04d/month=%02d/day=%02d", timestamp.Year(), timestamp.Month(), timestamp.Day())
	filename := filepath.Base(localFile)
	remoteFile := fmt.Sprintf("cloud-ip-prefixes/format=raw/provider=%s/%s/%s", provider, yearMonthDay, filename)
	return remoteFile
}

func shallUpload(mc *minio.Client, localFile string, provider string) bool {
	empty, err := isFileEmpty(localFile)
	if err != nil || empty {
		log.Warn().Err(err).Str("filename", localFile).Msg("Empty file")
		return false
	}
	localMd5Sum, err := checksum.MD5sum(localFile)
	if err != nil {
		log.Warn().Err(err).Msg("could not calculate local md5sum")
		return false
	}
	remoteFilesMap, err := getETagFromLatest(mc, provider)
	if err != nil {
		log.Warn().Err(err).Msg("could not get latest etag for provider: " + provider)
		return true
	}
	remoteMd5Sum := remoteFilesMap[path.Base(localFile)]
	// do not upload if checksums are equal
	return !strings.EqualFold(localMd5Sum, remoteMd5Sum)
}

func isFileEmpty(filename string) (bool, error) {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return false, err
	}
	return fileInfo.Size() == 0, nil
}

func getETagFromLatest(mc *minio.Client, provider string) (map[string]string, error) {
	re := regexp.MustCompile(`.*(year=(\d{4})/month=(\d{2})/day=(\d{2})).*`)
	prefix := fmt.Sprintf("cloud-ip-prefixes/format=raw/provider=%s", provider)
	listOpts := minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}

	tsMap := make(map[string][]s3Obj)
	ctx := context.Background()
	for obj := range mc.ListObjects(ctx, bucket, listOpts) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		matches := re.FindStringSubmatch(obj.Key)
		if matches == nil || len(matches) < 5 {
			continue
		}
		year := matches[2]
		month := matches[3]
		day := matches[4]
		ts, err := time.Parse("20060102", fmt.Sprintf("%s%s%s", year, month, day))
		if err != nil {
			continue
		}
		s := s3Obj{Etag: obj.ETag, Timestamp: ts}
		filename := path.Base(obj.Key)
		tsMap[filename] = append(tsMap[filename], s)
	}

	for k, slice := range tsMap {
		sort.Slice(slice, func(i, j int) bool {
			return slice[i].Timestamp.Before(slice[j].Timestamp)
		})
		tsMap[k] = slice
	}

	fileTagMap := make(map[string]string)
	if len(tsMap) > 0 {
		for k, slice := range tsMap {
			etag := slice[len(slice)-1].Etag
			fileTagMap[k] = etag
		}
		return fileTagMap, nil
	} else {
		return nil, errors.New("no files")
	}
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

	log.Info().Str("ETag", uploadInfo.ETag).Str("file", remoteFile).Msg("Successfully uploaded")

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
	if err != nil {
		log.Error().Err(err).Msg("Cannot upload log file")
	}
}
