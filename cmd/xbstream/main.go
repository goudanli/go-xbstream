/*
 * Copyright (C) 2017 Sean McGrail
 * Copyright (C) 2011-2017 Percona LLC and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/akamensky/argparse"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/skmcgrail/go-xbstream/xbstream"
)

type UPLoad struct {
	uploadID string
	partNum  int64
}

func init() {
	log.SetOutput(os.Stderr)
}

func createAWSClient() *s3.S3 {
	// 设置 AWS 访问凭证
	accessKey := "root"
	secretKey := "suninfo@123"
	end_point := "https://192.168.216.231:44086" //endpoint设置，不要动

	// 创建自定义的HTTP客户端并加载自签名证书
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // 跳过证书验证
		},
	}
	// 创建 AWS Session
	sess, err := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(accessKey, secretKey, ""),
		Endpoint:         aws.String(end_point),
		Region:           aws.String("test"),
		DisableSSL:       aws.Bool(false),
		S3ForcePathStyle: aws.Bool(true),
		HTTPClient:       httpClient,
	})
	if err != nil {
		log.Fatal("Error creating session:", err)
	}

	// 创建 S3 服务客户端
	svc := s3.New(sess)
	return svc
}

func main() {
	parser := argparse.NewParser("xbstream", "Go implementation of the xbstream archive format")

	createCmd := parser.NewCommand("create", "create xbstream archive")
	createFile := createCmd.File("o", "output", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666, &argparse.Options{})
	createList := createCmd.List("i", "input", &argparse.Options{Required: true})

	extractCmd := parser.NewCommand("extract", "extract xbstream archive")
	extractFile := extractCmd.File("i", "input", os.O_RDONLY, 0600, &argparse.Options{})
	extractOut := extractCmd.String("o", "output", &argparse.Options{})
	bucketName := extractCmd.String("b", "s3-bucket", &argparse.Options{})

	if err := parser.Parse(os.Args); err != nil {
		log.Fatal(err)
	}

	if createCmd.Happened() {
		writeStream(createFile, createList)
	} else if extractCmd.Happened() {
		readStream(extractFile, *bucketName, *extractOut)
	}
}

func readStream(file *os.File, bucketName string, output string) {
	var err error

	if *file == (os.File{}) {
		file = os.Stdin
	}

	if output == "" {
		output, err = os.Getwd()
		if err != nil {
			log.Fatal(err)
		}
	}

	svc := createAWSClient()
	r := xbstream.NewReader(file)

	files := make(map[string]*UPLoad)

	var fileUpload *UPLoad
	var ok bool

	for {
		chunk, err := r.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
			break
		}

		fPath := string(chunk.Path)
		newFPath := filepath.Join(output, fPath)
		if fileUpload, ok = files[fPath]; !ok {
			// 初始化分片上传
			ctx := context.Background()
			initResp, err := svc.CreateMultipartUploadWithContext(ctx, &s3.CreateMultipartUploadInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(newFPath),
			}, func(r *request.Request) {
				r.HTTPRequest.Header.Set("Object-Patch", strconv.FormatBool(true))
			})

			if err != nil {
				log.Fatal("Error initializing multipart upload:", err)
			}
			fileUpload = &UPLoad{
				uploadID: *initResp.UploadId,
				partNum:  1,
			}
			files[fPath] = fileUpload
		}

		if chunk.Type == xbstream.ChunkTypeEOF {
			// 完成分片上传
			_, err = svc.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
				Bucket:   aws.String(bucketName),
				Key:      aws.String(newFPath),
				UploadId: &fileUpload.uploadID,
			})
			if err != nil {
				log.Fatal("Error completing multipart upload:", err)
			}
			delete(files, fPath)
			continue
		}

		// crc32Hash := crc32.NewIEEE()
		// log.Printf("fPath:%s,chunk.PayLen:%d,chunk.PayOffset:%d\n", fPath, chunk.PayLen, chunk.PayOffset)
		// tReader := io.TeeReader(chunk, crc32Hash)
		partBuffer := make([]byte, chunk.PayLen)
		_, err = io.ReadFull(chunk.Reader, partBuffer)
		if err != nil {
			fmt.Println("read:", err)
			return
		}
		_, err = svc.UploadPart(&s3.UploadPartInput{
			Body:          aws.ReadSeekCloser(bytes.NewReader(partBuffer)),
			Bucket:        aws.String(bucketName),
			Key:           aws.String(newFPath),
			UploadId:      &fileUpload.uploadID,
			PartNumber:    aws.Int64(fileUpload.partNum),
			ContentLength: aws.Int64(int64(chunk.PayLen)),
			OffSet:        aws.Int64(int64(chunk.PayOffset)),
		})
		if err != nil {
			log.Fatal("Error uploading part:", err)
			return
		}
		fileUpload.partNum++

		// if chunk.Checksum != binary.BigEndian.Uint32(crc32Hash.Sum(nil)) {
		// 	log.Fatal("chunk checksum did not match")
		// 	break
		// }
	}
}

func writeStream(file *os.File, input *[]string) {
	if *file == (os.File{}) {
		file = os.Stdout
	}

	w := xbstream.NewWriter(file)

	wg := sync.WaitGroup{}

	for _, f := range *input {
		wg.Add(1)
		go func(path string) {
			defer wg.Done()

			b := make([]byte, xbstream.MinimumChunkSize)

			if file, err := os.Open(path); err == nil {
				fw, err := w.Create(path)
				if err != nil {
					log.Fatal(err)
				}

				for {
					n, err := file.Read(b)
					if err != nil {
						if err == io.EOF {
							break
						}
						log.Fatal(err)
					}
					if _, err := fw.Write(b[:n]); err != nil {
						log.Fatal(err)
						break
					}
				}

				err = fw.Close()
				if err != nil {
					log.Fatal(err)
				}
			} else {
				log.Printf("unable to open file %s", file.Name())
			}
		}(f)
	}

	wg.Wait()

	err := w.Close()
	if err != nil {
		log.Fatal(err)
	}
}
