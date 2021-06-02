package standalone_storage

import (
	badger "github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	conf *config.Config
	db   *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{conf: conf, db: nil}
}

func (s *StandAloneStorage) Start() error {
	opts := badger.DefaultOptions
	opts.Dir = s.conf.DBPath
	opts.ValueDir = s.conf.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	err := s.db.Close()
	if err != nil {
		return err
	}
	s.db = nil
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &StandAloneStorageReader{txn: s.db.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		for _, m := range batch {
			var err error
			switch action := m.Data.(type) {
			case storage.Put:
				err = txn.Set(engine_util.KeyWithCF(action.Cf, action.Key), action.Value)
			case storage.Delete:
				err = txn.Delete(engine_util.KeyWithCF(action.Cf, action.Key))
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	item, err := r.txn.Get(engine_util.KeyWithCF(cf, key))
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item.Value()
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	opts := badger.DefaultIteratorOptions
	it := r.txn.NewIterator(opts)
	return &StandAloneStorageIterator{it: it, cf: cf}
}

func (r *StandAloneStorageReader) Close() {
	r.txn.Discard()
}

type StandAloneStorageIterator struct {
	it *badger.Iterator
	cf string
}

func (i *StandAloneStorageIterator) Item() engine_util.DBItem {
	item := i.it.Item()
	if item == nil {
		return nil
	}
	return &Item{item: item, cf: i.cf}
}

func (i *StandAloneStorageIterator) Valid() bool {
	return i.it.Valid()
}

func (i *StandAloneStorageIterator) Next() {
	i.it.Next()
}

func (i *StandAloneStorageIterator) Seek(key []byte) {
	i.it.Seek(engine_util.KeyWithCF(i.cf, key))
}

func (i *StandAloneStorageIterator) Close() {
	i.it.Close()
}

type Item struct {
	cf   string
	item *badger.Item
}

func (i *Item) Key() []byte {
	return engine_util.RemoveCFFromKey(i.cf, i.item.Key())
}

func (i *Item) KeyCopy(dst []byte) []byte {
	copy(dst, i.Key())
	return dst
}

func (i *Item) Value() ([]byte, error) {
	return i.item.Value()
}

func (i *Item) ValueSize() int {
	return i.item.ValueSize()
}

func (i *Item) ValueCopy(dst []byte) ([]byte, error) {
	return i.item.ValueCopy(dst)
}
