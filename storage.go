// storage
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
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
	balance      float64 `json:"balance"`
	depositCount int64   `json:"depositCount"`
	depositSum   float64 `json:"depositSum"`
	winCount     int64   `json:"winCount"`
	winSum       float64 `json:"winSum"`
	betCount     int64   `json:"betCount"`
	betSum       float64 `json:"betSum"`
}

type UserToDb struct {
	id    int64
	bytes []byte
}

type Deposit struct {
	balanceBefore float64 `json:"balanceBefore"`
	balanceAfter  float64 `json:"balanceAfter"`
	time          int64   `json:"time"`
}

type Transaction struct {
	typeTx        string  `json:"typeTx"`
	diff          float64 `json:"diff"`
	balanceBefore float64 `json:"balanceBefore"`
	balanceAfter  float64 `json:"balanceAfter"`
	time          int64   `json:"time"`
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
		balance: bal,
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
	resp.Balance = memCache.users[id].u.balance
	resp.BetCount = memCache.users[id].u.betCount
	resp.BetSum = memCache.users[id].u.betSum
	resp.DepositCount = memCache.users[id].u.depositCount
	resp.DepositSum = memCache.users[id].u.depositSum
	resp.WinCount = memCache.users[id].u.winCount
	resp.WinSum = memCache.users[id].u.winSum
	Println("Get user ", memCache.users[id])
	return nil
}

func AddDepositToUser(id int64, depositId int64, add float64, resp *RespAddDeposit) error {
	if IsNewUser(id) {
		return errors.New("Storage: User isn't exist")
	}
	memCache.users[id].m.Lock()
	u := memCache.users[id].u
	u.balance += add
	memCache.users[id].u = u
	resp.Balance = memCache.users[id].u.balance
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
	data := []UserToDb{}
	item := UserToDb{}
	for i, user := range memCache.users {
		if user.changed {
			item.id = i
			item.bytes, err = json.Marshal(user.u)
			if err != nil {
				return
			}
			data = append(data, item)
		}
	}
	err = db.Update(func(tx *bolt.Tx) error {
		err = tx.Bucket([]byte(BU)).Put([]byte("1"), []byte("wdawe"))
		if err != nil {
			return fmt.Errorf("could not set config: %v", err)
		}
		return nil
	})
	if err != nil {
		Println(err.Error())
	}
	return nil
}

func PrintDB() {

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

	CtrlCHandler()

	go doEvery(10*time.Second, refreshDbHandler)

	return nil
}
