package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/jinzhu/configor"
	"github.com/minio/minio-go/v6"
)

// Config configuration for the application
var Config = struct {
	Endpoint  string `default:"us-east-1.linodeobjects.com"`
	AccessKey string `default:""`
	SecretKey string `default:""`
	UseSSL    bool   `default:"true"`
}{}

func main() {
	// read config
	if err := configor.Load(&Config, "config.yml"); err != nil {
		log.Fatalln(err)
		return
	}

	// parse flags
	bucketName := "luxedigest"
	flag.StringVar(&bucketName, "bucket", "", "provide the bucketname")
	bucketPrefix := ""
	flag.StringVar(&bucketPrefix, "prefix", "", "provide a prefix to act on")
	destinationPath := ""
	flag.StringVar(&destinationPath, "destination", "/tmp", "folder where downloaded items will be placed")

	cmdList := false
	flag.BoolVar(&cmdList, "list", false, "list items matching prefix")
	cmdDownload := false
	flag.BoolVar(&cmdDownload, "download", false, "list items matching prefix")
	cmdUsage := false
	flag.BoolVar(&cmdUsage, "usage", false, "calculate space usage of items matching prefix")

	flag.Parse()

	if len(bucketName) == 0 {
		log.Fatalln("bucket not specified")
		return
	}
	if cmdDownload && len(destinationPath) == 0 {
		log.Fatalln("destination not specified")
		return
	}
	if (cmdUsage || cmdList || cmdDownload) && len(bucketPrefix) == 0 {
		log.Fatalln("prefix not specified")
		return
	}

	// Initialize minio client object.
	minioClient, err := minio.New(Config.Endpoint, Config.AccessKey, Config.SecretKey, Config.UseSSL)
	if err != nil {
		log.Fatalln(err)
	}

	if cmdList {
		err := listDirectories(minioClient, bucketName, bucketPrefix, false)
		if err != nil {
			log.Fatalln(err)
			return
		}
	} else if cmdUsage {
		log.Println("Getting usage...")
		total, err := getUsage(minioClient, bucketName, bucketPrefix)
		if err != nil {
			log.Fatalln(err)
			return
		}

		log.Println("Usage: ", humanize.Bytes(total))
	} else if cmdDownload {
		err := download(minioClient, bucketName, bucketPrefix, destinationPath)
		if err != nil {
			log.Fatalln(err)
			return
		}

		log.Println("Download complete")
	}
}

func getObject(client *minio.Client, bucketName, objectName, destinationPath string) error {

	err := client.FGetObject(bucketName, objectName, destinationPath, minio.GetObjectOptions{})
	if err != nil {
		return err
	}

	return err
}

func listDirectories(client *minio.Client, bucketName, prefix string, isRecursive bool) error {
	var err error

	// Create a done channel to control 'ListObjectsV2' go routine.
	doneCh := make(chan struct{})

	// Indicate to our routine to exit cleanly upon return.
	defer close(doneCh)

	log.Println("listDirectories:", bucketName, prefix)

	if prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}

	// isRecursive := false
	objectCh := client.ListObjectsV2(bucketName, prefix, isRecursive, doneCh)
	for object := range objectCh {
		if object.Err != nil {
			log.Println(object.Err)
			return err
		}
		log.Println(object.Key, " - ", humanize.Bytes(uint64(object.Size)))
	}

	return err
}

func getDirectories(client *minio.Client, bucketName, prefix string) ([]string, error) {
	var (
		fdr []string
		err error
	)

	// Create a done channel to control 'ListObjectsV2' go routine.
	doneCh := make(chan struct{})

	// Indicate to our routine to exit cleanly upon return.
	defer close(doneCh)

	if prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}

	isRecursive := false
	objectCh := client.ListObjectsV2(bucketName, prefix, isRecursive, doneCh)
	for object := range objectCh {
		if object.Err != nil {
			fmt.Println(object.Err)
			return nil, err
		}

		if object.Size == 0 {
			fdr = append(fdr, object.Key)
		}
	}

	return fdr, err
}

func getUsage(client *minio.Client, bucketName, prefix string) (uint64, error) {
	var (
		total uint64
		err   error
	)

	// Create a done channel to control 'ListObjectsV2' go routine.
	doneCh := make(chan struct{})

	// Indicate to our routine to exit cleanly upon return.
	defer close(doneCh)

	if prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}

	isRecursive := true
	objectCh := client.ListObjectsV2(bucketName, prefix, isRecursive, doneCh)
	for object := range objectCh {
		if object.Err != nil {
			log.Println(object.Err)
			return uint64(0), err
		}

		total += uint64(object.Size)
	}

	return total, err
}

func download(client *minio.Client, bucketName, prefix, destination string) error {
	var err error

	// Create a done channel to control 'ListObjectsV2' go routine.
	doneCh := make(chan struct{})

	// Indicate to our routine to exit cleanly upon return.
	defer close(doneCh)

	isRecursive := true
	objectCh := client.ListObjectsV2(bucketName, prefix, isRecursive, doneCh)
	for object := range objectCh {
		if object.Err != nil {
			log.Println(object.Err)
			return err
		}

		log.Println(object.Key, " - ", humanize.Bytes(uint64(object.Size)))
		if object.Size == 0 {
			continue
		}

		path := objToFileName(object.Key, prefix, destination)
		if fileExists(path) {
			log.Println("file exists: ", path)
			continue
		}

		if err := getObject(client, bucketName, object.Key, path); err != nil {
			log.Println(object.Key, " Error: ", err)
			continue
		}
	}

	return err
}

func objToFileName(path, prefix, folder string) string {
	np := strings.Replace(path, prefix, "", 1)
	return filepath.Join(folder, np)
}

func fileExists(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}

	return true
}
