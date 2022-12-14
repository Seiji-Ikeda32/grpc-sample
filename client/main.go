package main

import (
	"context"
	"fmt"
	"grpc-sample/pb"
	"io"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func main() {
	conn, err := grpc.Dial("localhost:8080", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	defer conn.Close()

	client := pb.NewFileserviceClient(conn)
	// callListFiles(client)
	callDownload(client)
	// callUpload(client)
	// callUploadAndNotifyProgress(client)
}

func callListFiles(client pb.FileserviceClient) {
	md := metadata.New(map[string]string{"authorization": "Bearer bad-token"})
	ctx := metadata.NewOutgoingContext(context.Background(), md)
	res, err := client.ListFiles(ctx, &pb.ListFilesRequest{})
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println(res.GetFilenames())
}

func callDownload(client pb.FileserviceClient) {
	ctx, cancell := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancell()
	req := &pb.DownloadRequest{Filename: "name.txt"}
	stream, err := client.Download(ctx, req)
	if err != nil {
		log.Fatalln(err)
	}

	for {
		res, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			resErr, ok := status.FromError(err)
			if ok {
				if resErr.Code() == codes.NotFound {
					log.Fatalf("Error Code: %v, Error Message: %v", resErr.Code(), resErr.Message())
				} else if resErr.Code() == codes.DeadlineExceeded {
					log.Fatalln("deadline exceeded")
				} else {
					log.Fatalln("Unkown grpc error")
				}
			} else {
				log.Fatalln(err)
			}
		}
		log.Printf("Response from Download(bytes): %v", res.GetDate())
		log.Printf("Response from Download(string): %v", string(res.GetDate()))
	}
}

func callUpload(client pb.FileserviceClient) {
	filename := "sports.txt"
	path := "/Users/ikeda-seiji/grpc-sample/strage/" + filename

	file, err := os.Open(path)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()

	stream, err := client.Upload(context.Background())
	if err != nil {
		log.Fatalln(err)
	}

	buf := make([]byte, 10)

	for {
		n, err := file.Read(buf)
		if n == 0 || err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalln(err)
		}

		req := &pb.UploadRequest{
			Date: buf[:n],
		}

		sendErr := stream.Send(req)
		if sendErr != nil {
			log.Fatalln(sendErr)
		}

		time.Sleep(1 * time.Second)
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("recived data size %v", res.GetSize())
}

func callUploadAndNotifyProgress(client pb.FileserviceClient) {
	filename := "sports.txt"
	path := "/Users/ikeda-seiji/grpc-sample/strage/" + filename

	file, err := os.Open(path)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()

	stream, err := client.UploadAndNotifyProgress(context.Background())
	if err != nil {
		log.Fatalln(err)
	}

	// req
	buf := make([]byte, 10)
	go func() {
		for {
			n, err := file.Read(buf)
			if n == 0 || err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalln(err)
			}

			req := &pb.UploadAndNotifyProgressRequest{Date: buf[:n]}
			sendErr := stream.Send(req)
			if sendErr != nil {
				log.Fatalln(sendErr)
			}
		}

		err := stream.CloseSend()
		if err != nil {
			log.Fatalln(err)
		}
	}()

	// res
	ch := make(chan struct{})
	go func() {
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalln(err)
			}

			log.Printf("recived message: %v", res.GetMsg())
		}
		close(ch)
	}()
	<-ch

}
