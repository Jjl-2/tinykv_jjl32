package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/kv/storage"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	s := server.storage 
	sreader, err := s.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawGetResponse{}, err
	}

	val, err := sreader.GetCF(req.Cf, req.Key)
	
	defer sreader.Close()

	if err != nil {
		return &kvrpcpb.RawGetResponse{}, err
	}

	if val != nil {
		return &kvrpcpb.RawGetResponse{Value : val,
					NotFound : false}, nil
	}

	return &kvrpcpb.RawGetResponse{Value : val,
		NotFound : true}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	var put storage.Put
	put.Key = req.Key
	put.Value = req.Value
	put.Cf = req.Cf

	batch := storage.Modify{Data: put}
	s := server.storage
	err := s.Write(req.Context, []storage.Modify{batch})


	return &kvrpcpb.RawPutResponse{}, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	var delete storage.Delete
	delete.Key = req.Key
	delete.Cf = req.Cf

	batch := storage.Modify{Data: delete}
	s := server.storage
	err := s.Write(req.Context, []storage.Modify{batch})

	return &kvrpcpb.RawDeleteResponse{}, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	s:= server.storage
	sreader, err := s.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawScanResponse{}, err
	}
	defer sreader.Close()
	cfit := sreader.IterCF(req.Cf)
	defer cfit.Close()
	cfit.Seek(req.StartKey)
	var kvs []* kvrpcpb.KvPair
	limit := req.Limit

	for ; cfit.Valid(); cfit.Next() {
		item := cfit.Item()
		key := item.Key()
		value, err := item.Value()
		if err != nil {
			return &kvrpcpb.RawScanResponse{}, err
		}
		kvs = append(kvs, &kvrpcpb.KvPair{Key: key, Value: value})
		limit--
		if limit == 0 {
			break
		}
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}
