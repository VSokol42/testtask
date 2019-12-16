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
	BD = "Deposit_" // prefix for collection of deposits for each user
	BT = "Tx_"      // prefix for collection of trasnsactions for each user
)

type User struct {
	Balance      float64 `json:"balance"`
	DepositCount uint64  `json:"depositCount"`
	DepositSum   float64 `json:"depositSum"`
	WinCount     uint64  `json:"winCount"`
	WinSum       float64 `json:"winSum"`
	BetCount     uint64  `json:"betCount"`
	BetSum       float64 `json:"betSum"`
}

type UserToDb struct {
	id    uint64
	bytes []byte
}

type DepositToDb struct {
	depositId uint64 // id of deposit
	bytes     []byte // data of deposit
}

type DepositsToDb struct {
	userId   uint64        // user id
	deposits []DepositToDb // array of deposits of user
}

type Deposit struct {
	BalanceBefore float64 `json:"balanceBefore"`
	BalanceAfter  float64 `json:"balanceAfter"`
	Time          string  `json:"time"`
}

type TxToDb struct {
	txId  uint64 // id of transaction
	bytes []byte // data of transaction
}

type TxsToDb struct {
	userId uint64   // user id
	txs    []TxToDb // array of transactions of user
}

type Transaction struct {
	TypeTx        string  `json:"typeTx"`
	Diff          float64 `json:"diff"`
	BalanceBefore float64 `json:"balanceBefore"`
	BalanceAfter  float64 `json:"balanceAfter"`
	Time          string  `json:"time"`
}

type UserTotal struct {
	m       sync.RWMutex // mutex ..
	changed bool         // true if user data changed

	u User                    // user data
	d map[uint64]*Deposit     // deposits
	t map[uint64]*Transaction // transactions
}

type UsersCache struct {
	refresh bool                  // something changed in cache
	users   map[uint64]*UserTotal // users data, deposits, transactions
}

// memory cache of users data, deposits and transactions
var memCache *UsersCache

// datanase
var db *bolt.DB

func IsNewUser(id uint64) (r bool) {
	_, r = memCache.users[id]
	return !r
}

func IsNewDeposit(id uint64, dId uint64) (r bool) {
	// at first, check user existance for avoiding panic on deposit's existance check
	_, r = memCache.users[id]
	if r {
		_, r = memCache.users[id].d[dId]
	}
	return !r
}

func IsNewTransaction(id uint64, txId uint64) (r bool) {
	// at first, check user existance for avoiding panic on transaction's existance check
	_, r = memCache.users[id]
	if r {
		_, r = memCache.users[id].t[txId]
	}
	return !r
}

func IsValidTxType(txType string) (r bool) {
	return txType == "Win" || txType == "Bet"
}

func IsValidTxBet(id uint64, txType string, Amount float64) (r bool) {
	if txType != "Bet" || IsNewUser(id) {
		return !r
	}
	return memCache.users[id].u.Balance-Amount > 0
}

func IsLinkedDeposit(id uint64, depositId uint64) (r bool) {
	if IsNewUser(id) {
		return
	}
	return depositId == memCache.users[id].u.DepositCount+1
}

func IsLinkedTx(id uint64, txId uint64) (r bool) {
	if IsNewUser(id) {
		return
	}
	return txId == memCache.users[id].u.BetCount+memCache.users[id].u.WinCount+1
}

func AddUserToStorage(id uint64, bal float64) error {
	if !IsNewUser(id) {
		return errors.New("Storage: User already exist")
	}
	memCache.users[id] = new(UserTotal)
	memCache.users[id].m.Lock()
	memCache.users[id].d = make(map[uint64]*Deposit)
	memCache.users[id].t = make(map[uint64]*Transaction)
	memCache.users[id].u = User{
		Balance: bal,
	}
	memCache.users[id].changed = true
	memCache.refresh = true
	memCache.users[id].m.Unlock()
	Println("Add User ", memCache.users[id])
	return nil
}

func GetUserFromStorage(id uint64, resp *RespGetUser) error {
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

func AddDepositToUser(id uint64, depositId uint64, add float64, resp *RespAddDeposit) error {
	// additional tests of existances
	if IsNewUser(id) {
		return errors.New("Storage: User isn't exist")
	}
	if !IsNewDeposit(id, depositId) {
		return errors.New("Storage: This deposit is already exist")
	}
	// get timestamp and lock memCache
	currentTime := time.Now()
	memCache.users[id].m.Lock()
	// add balance to user
	u := memCache.users[id].u
	prevBal := u.Balance
	u.Balance += add
	u.DepositCount++
	u.DepositSum += add
	memCache.users[id].u = u
	// add deposit to collection
	memCache.users[id].d[depositId] = new(Deposit)
	d := memCache.users[id].d[depositId]
	d.BalanceBefore = prevBal
	d.BalanceAfter = u.Balance
	d.Time = currentTime.Format("2006-01-02 15:04:05.000000")
	memCache.users[id].d[depositId] = d
	// make response and unlock memCache
	resp.Balance = memCache.users[id].u.Balance
	memCache.users[id].changed = true
	memCache.refresh = true
	memCache.users[id].m.Unlock()
	Println("Add deposit successful ", memCache.users[id].d[depositId])
	return nil
}

func TransactionOfUser(id uint64, txId uint64, txType string, txAmount float64, resp *RespTxUser) error {
	// additional tests of existances
	if IsNewUser(id) {
		return errors.New("Storage: User isn't exist")
	}
	if !IsNewTransaction(id, txId) {
		return errors.New("Storage: This transaction is already exist")
	}
	// get timestamp and lock memCache
	currentTime := time.Now()
	memCache.users[id].m.Lock()
	// add or substruct amount to balance of user
	u := memCache.users[id].u
	prevBal := u.Balance
	if txType == "Win" {
		u.Balance += txAmount
		u.WinCount++
		u.WinSum += txAmount
	} else if txType == "Bet" {
		u.Balance -= txAmount
		u.BetCount++
		u.BetSum += txAmount
	}
	memCache.users[id].u = u
	// add transaction to collection
	memCache.users[id].t[txId] = new(Transaction)
	t := memCache.users[id].t[txId]
	t.TypeTx = txType
	t.Diff = u.Balance - prevBal
	t.BalanceBefore = prevBal
	t.BalanceAfter = u.Balance
	t.Time = currentTime.Format("2006-01-02 15:04:05.000000")
	memCache.users[id].t[txId] = t
	// make response and unlock memCache
	resp.Balance = memCache.users[id].u.Balance
	memCache.users[id].changed = true
	memCache.refresh = true
	memCache.users[id].m.Unlock()
	Println("Add transaction successful ", memCache.users[id].t[txId])
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
		return nil, fmt.Errorf("could not open db, %v", err)
	}
	return db, db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(BU))
		if err != nil {
			return fmt.Errorf("could not create bucket: %v", err)
		}

		return nil
	})
}

func RefreshDB() (err error) {
	var usersList = []UserToDb{}
	var userData = UserToDb{}
	var depositsList = []DepositsToDb{}
	var depositsOfUser = DepositsToDb{}
	var txsList = []TxsToDb{}
	var txsOfUser = TxsToDb{}
	for i, user := range memCache.users {
		if user.changed {
			// add data of user to list for update to db
			userData.id = uint64(i)
			userData.bytes, err = json.Marshal(user.u)
			if err != nil {
				return
			}
			usersList = append(usersList, userData)
			// depotits of user to list for update to db
			var depositList = []DepositToDb{}
			userDep := DepositToDb{}
			for j, deposit := range memCache.users[userData.id].d {
				Println(j, deposit)
				userDep.depositId = uint64(j)
				userDep.bytes, err = json.Marshal(deposit)
				if err != nil {
					return
				}
				Println(userDep)
				depositList = append(depositList, userDep)
			}
			depositsOfUser.userId = userData.id
			depositsOfUser.deposits = depositList
			depositsList = append(depositsList, depositsOfUser)
			// transations of user to list for update to db
			var txList = []TxToDb{}
			userTx := TxToDb{}
			for j, tx := range memCache.users[userData.id].t {
				Println(j, tx)
				userTx.txId = uint64(j)
				userTx.bytes, err = json.Marshal(tx)
				if err != nil {
					return
				}
				Println(userTx)
				txList = append(txList, userTx)
			}
			txsOfUser.userId = userData.id
			txsOfUser.txs = txList
			txsList = append(txsList, txsOfUser)

		}
	}

	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(BU))
		if err != nil {
			return err
		}
		for _, d := range usersList {
			err = b.Put([]byte(strconv.FormatUint(d.id, 10)), d.bytes)
			if err != nil {
				return err
			}
		}
		for _, d := range depositsList {
			bd, err := b.CreateBucketIfNotExists([]byte(BD + strconv.FormatUint(d.userId, 10)))
			if err != nil {
				return err
			}
			for _, d0 := range d.deposits {
				err = bd.Put([]byte(strconv.FormatUint(d0.depositId, 10)), d0.bytes)
				if err != nil {
					return err
				}
			}
		}
		for _, t := range txsList {
			bt, err := b.CreateBucketIfNotExists([]byte(BT + strconv.FormatUint(t.userId, 10)))
			if err != nil {
				return err
			}
			for _, t0 := range t.txs {
				err = bt.Put([]byte(strconv.FormatUint(t0.txId, 10)), t0.bytes)
				if err != nil {
					return err
				}
			}
		}

		return nil

	})

	for _, user := range memCache.users {
		if user.changed {
			user.m.Lock()
			// deleye all deposits
			for key := range memCache.users[userData.id].d {
				delete(memCache.users[userData.id].d, key)
			}
			// delete all transactions
			for key := range memCache.users[userData.id].t {
				delete(memCache.users[userData.id].t, key)
			}
			user.changed = false
			user.m.Unlock()
		}
	}
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
			Println(string(k), string(v))
			key, err = strconv.Atoi(string(k))
			if err == nil {

				err = json.Unmarshal(v, &u)
				if err != nil {
					return err
				}
				memCache.users[uint64(key)] = new(UserTotal)
				memCache.users[uint64(key)].u = u
				memCache.users[uint64(key)].d = make(map[uint64]*Deposit)
				memCache.users[uint64(key)].t = make(map[uint64]*Transaction)

				Println(key, u)
			}
			return nil
		}); err != nil {
			return err
		}
		return err
	})
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
	memCache.users = make(map[uint64]*UserTotal)
	db, err = setupDB()
	if err != nil {
		return
	}
	err = LoadDB()
	if err != nil {
		return
	}
	CtrlCHandler()

	go doEvery(10*time.Second, refreshDbHandler)

	return nil
}
