package standalone_storage

import (
	"github.com/Connor1996/badger"
	"path/filepath"
	"os"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	Config *config.Config
	Engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")
	raftPath := filepath.Join(dbPath, "raft")

	os.MkdirAll(kvPath, os.ModePerm)
	os.MkdirAll(raftPath, os.ModePerm)

	raftDB := engine_util.CreateDB(raftPath, true)
	kvDB := engine_util.CreateDB(kvPath, false)
	engines := engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath)

	return &StandAloneStorage{Engine: engines, Config: conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.Engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).

	txn := s.Engine.Kv.NewTransaction(false)

	return &StandAloneReader{Txn : txn,}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, m := range batch{
		switch m.Data.(type){
		case storage.Put:
			put := m.Data.(storage.Put)
			err := engine_util.PutCF(s.Engine.Kv, put.Cf, put.Key, put.Value)
			if err != nil {
				return err;
			}
		case storage.Delete:
			delete := m.Data.(storage.Delete)
			err := engine_util.DeleteCF(s.Engine.Kv, delete.Cf, delete.Key)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

type StandAloneReader struct {
	Txn *badger.Txn
}

func (s *StandAloneReader)GetCF(cf string, key []byte) ([]byte, error){
	val, err := engine_util.GetCFFromTxn(s.Txn, cf, key)

	if err == badger.ErrKeyNotFound {
		return nil, nil
	}

	return val, err
}

func (s *StandAloneReader) IterCF(cf string) engine_util.DBIterator{
	return engine_util.NewCFIterator(cf, s.Txn)
}

func (s *StandAloneReader) Close(){
	s.Txn.Discard()
}