package standalone_storage

import (
	"log"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db      *badger.DB
	readers []*StandAloneStorageReader
	conf    *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	options := badger.DefaultOptions
	options.Dir = conf.DBPath

	options.ValueDir = conf.DBPath

	db, err := badger.Open(options)
	if err != nil {
		log.Fatal("Can't open badger database at ", options.Dir, ", ", err)
	}

	return &StandAloneStorage{
		db:   db,
		conf: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	for _, reader := range s.readers {
		reader.Close()
	}

	err := s.db.Close()
	if err != nil {
		println(err)
	}

	return err
}

type StandAloneStorageReader struct {
	txn   *badger.Txn
	iters []*engine_util.BadgerIterator
}

func (s *StandAloneStorageReader) GetCF(cf string, key []byte) (value []byte, err error) {
	item, err := s.txn.Get(append([]byte(cf+"_"), key...))
	if err == badger.ErrKeyNotFound {
		return nil, nil
	} else if err != nil {
		println(err)
		return nil, err
	}

	return item.Value()
}

func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	it := engine_util.NewCFIterator(cf, s.txn)
	s.iters = append(s.iters, it)
	return it
}

func (s *StandAloneStorageReader) Close() {
	for _, it := range s.iters {
		it.Close()
	}

	s.txn.Discard()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)

	reader := &StandAloneStorageReader{
		txn:   txn,
		iters: make([]*engine_util.BadgerIterator, 0),
	}

	s.readers = append(s.readers, reader)

	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	return s.db.Update(func(txn *badger.Txn) error {
		for _, m := range batch {
			real_key := append([]byte(m.Cf()+"_"), m.Key()...)

			var err error = nil

			switch m.Data.(type) {
			case storage.Put:
				err = txn.Set(real_key, m.Value())
			case storage.Delete:
				err = txn.Delete(real_key)
			}

			if err != nil {
				println(err)
				return err
			}

		}
		return nil
	})
}
