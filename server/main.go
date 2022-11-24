package main

import (
	"bytes"
	"context"
	"fmt"
	"grpc-sample/pb"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedFileserviceServer
}

func (*server) ListFiles(ctx context.Context, req *pb.ListFilesRequest) (*pb.ListFilesResponse, error) {
	fmt.Println("ListFiles was invoked")

	dir := "/Users/ikeda-seiji/grpc-sample/strage"

	paths, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, path := range paths {
		if !path.IsDir() {
			files = append(files, path.Name())
		}
	}

	res := &pb.ListFilesResponse{
		Filenames: files,
	}

	return res, nil
}

func (*server) Download(req *pb.DownloadRequest, stream pb.Fileservice_DownloadServer) error {
	fmt.Println("Download was invoked")

	filename := req.GetFilename()
	path := "/Users/ikeda-seiji/grpc-sample/strage/" + filename

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	buf := make([]byte, 10)
	for {
		n, err := file.Read(buf)
		if n == 0 || err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		res := &pb.DownloadResponse{Date: buf[:n]}
		sendErr := stream.Send(res)

		if sendErr != nil {
			return err
		}
		time.Sleep(1 * time.Second)
	}

	return nil
}

func (*server) Upload(stream pb.Fileservice_UploadServer) error {
	fmt.Println("Upload was invoked")

	var buf bytes.Buffer

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			res := &pb.UploadResponse{Size: int32(buf.Len())}
			return stream.SendAndClose(res)
		}

		if err != nil {
			return err
		}

		date := req.GetDate()
		log.Printf("received date(bytes): %v", date)
		log.Printf("received date(string): %v", string(date))

		buf.Write(date)

	}
}

func (*server) UploadAndNotifyProgress(stream pb.Fileservice_UploadAndNotifyProgressServer) error {
	fmt.Println("UploadAndNotifyProgress was invoked")
	size := 0

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		date := req.GetDate()
		log.Printf("received date: %v", date)
		size += len(date)

		res := &pb.UploadAndNotifyProgressReaponse{
			Msg: fmt.Sprintf("recived %vbytes", date),
		}

		err = stream.Send(res)
		if err != nil {
			return err
		}
	}
}

func main() {
	lis, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterFileserviceServer(s, &server{})

	fmt.Println("server is running...")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
