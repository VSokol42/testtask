// storage
package main

import (
	_ "bytes"
	_ "encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/boltdb/bolt"
)

const (
	BU = "Users"    // collection of users, root for next buckets
	BD = "Deposits" // collection of deposits for each user
	BT = "Txs"      // collection of trasnsactions for each user
)

type User struct {
	Balance      float64 `json:"balance"`
	DepositCount int64   `json:"depositCount"`
	DepositSum   float64 `json:"depositSum"`
	WinCount     int64   `json:"winCount"`
	WinSum       float64 `json:"winSum"`
	BetCount     int64   `json:"betCount"`
	BetSum       float64 `json:"betSum"`
}

type UserToDb struct {
	id    uint64
	bytes []byte
}

type Deposit struct {
	BalanceBefore float64 `json:"balanceBefore"`
	BalanceAfter  float64 `json:"balanceAfter"`
	Time          int64   `json:"time"`
}

type Transaction struct {
	TypeTx        string  `json:"typeTx"`
	Diff          float64 `json:"diff"`
	BalanceBefore float64 `json:"balanceBefore"`
	BalanceAfter  float64 `json:"balanceAfter"`
	Time          int64   `json:"time"`
}

type UserTotal struct {
	m       sync.RWMutex
	changed bool
	u       User
	d       map[int64]Deposit
	t       map[int64]Transaction
}

type UsersCache struct {
	users   map[int64]*UserTotal
	refresh bool
}

//var memCache = UsersCache{}
var memCache *UsersCache

var db *bolt.DB

func IsNewUser(id int64) (r bool) {
	_, r = memCache.users[id]
	return !r
}

func AddUserToStorage(id int64, bal float64) error {
	if !IsNewUser(id) {
		return errors.New("Storage: User already exist")
	}
	memCache.users[id] = new(UserTotal)
	memCache.users[id].m.Lock()
	memCache.users[id].u = User{
		Balance: bal,
	}
	memCache.users[id].changed = true
	memCache.refresh = true
	memCache.users[id].m.Unlock()
	Println("Add User ", memCache.users[id])
	return nil
}

func GetUserFromStorage(id int64, resp *RespGetUser) error {
	if IsNewUser(id) {
		return errors.New("Storage: User isn't exist")
	}
	resp.Id = id
	resp.Balance = memCache.users[id].u.Balance
	resp.BetCount = memCache.users[id].u.BetCount
	resp.BetSum = memCache.users[id].u.BetSum
	resp.DepositCount = memCache.users[id].u.DepositCount
	resp.DepositSum = memCache.users[id].u.DepositSum
	resp.WinCount = memCache.users[id].u.WinCount
	resp.WinSum = memCache.users[id].u.WinSum
	Println("Get user ", memCache.users[id])
	return nil
}

func AddDepositToUser(id int64, depositId int64, add float64, resp *RespAddDeposit) error {
	if IsNewUser(id) {
		return errors.New("Storage: User isn't exist")
	}
	memCache.users[id].m.Lock()
	u := memCache.users[id].u
	u.Balance += add
	memCache.users[id].u = u
	resp.Balance = memCache.users[id].u.Balance
	memCache.users[id].changed = true
	memCache.refresh = true
	memCache.users[id].m.Unlock()
	Println("Add deposit ", memCache.users[id])
	return nil
}

func doEvery(d time.Duration, f func()) {
	for _ = range time.Tick(d) {
		f()
	}
}

func refreshDbHandler() {
	Println("Tick")
	if memCache.refresh {
		Println("Need db refresh")
		RefreshDB()
		memCache.refresh = false
	}
}

func setupDB() (db *bolt.DB, err error) {
	db, err = bolt.Open("testtask.db", 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("cann't open db, %v", err)
	}
	return db, db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists([]byte(BU))
		if err != nil {
			return fmt.Errorf("could not create bucket: %v", err)
		}

		return nil
	})
}

func RefreshDB() (err error) {
	var data = []UserToDb{}
	item := UserToDb{}
	for i, user := range memCache.users {
		if user.changed {
			item.id = uint64(i)
			item.bytes, err = json.Marshal(user.u)
			if err != nil {
				return
			}
			data = append(data, item)
		}
	}

	err = db.Update(func(tx *bolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists([]byte(BU))
		for _, d := range data {
			err = b.Put([]byte(strconv.FormatUint(d.id, 10)), d.bytes)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		Println(err.Error())
	}
	return nil
}

func LoadDB() (err error) {
	return db.View(func(tx *bolt.Tx) (err error) {
		u := User{}
		b := tx.Bucket([]byte(BU))
		key := 0
		if err = b.ForEach(func(k, v []byte) error {
			key, err = strconv.Atoi(string(k))
			if err != nil {
				return err
			}
			err = json.Unmarshal(v, &u)
			if err != nil {
				return err
			}
			memCache.users[int64(key)] = new(UserTotal)
			memCache.users[int64(key)].u = u
			Println(key, u)
			return nil
		}); err != nil {
			return err
		}
		return err
	})
}

func PrintDB() {
	err := db.View(func(tx *bolt.Tx) (err error) {
		b := tx.Bucket([]byte(BU))

		if err = b.ForEach(func(k, v []byte) error {
			fmt.Printf("A %s is %s.\n", k, v)
			return nil
		}); err != nil {
			return err
		}
		return err
	})
	if err != nil {
		Println(err.Error())
	}
}

func CtrlCHandler() {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		Println("Terminated on Ctrl-C")
		db.Close()
		os.Exit(0)
	}()
}

func StorageInit() (err error) {
	memCache = new(UsersCache)
	memCache.users = make(map[int64]*UserTotal)
	db, err = setupDB()
	if err != nil {
		return
	}
	PrintDB()
	err = LoadDB()
	if err != nil {
		return
	}
	CtrlCHandler()

	go doEvery(10*time.Second, refreshDbHandler)

	return nil
}
