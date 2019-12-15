package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

type ReqAddUser struct {
	Id      uint64  `json:"id"`
	Balance float64 `json:"balance"`
	Token   string  `json:"token"`
}

type RespCommon struct {
	Error string `json:"error"`
}

type ReqGetUser struct {
	Id    uint64 `json:"id"`
	Token string `json:"token"`
}

type RespGetUser struct {
	Id           uint64  `json:"id"`
	Balance      float64 `json:"balance"`
	DepositCount uint64  `json:"depositCount"`
	DepositSum   float64 `json:"depositSum"`
	BetCount     uint64
	BetSum       float64 `json:"betSum"`
	WinCount     uint64  `json:"winCount"`
	WinSum       float64 `json:"winSum"`
}

type ReqAddDeposit struct {
	UserId    uint64  `json:"userId"`
	DepositId uint64  `json:"depositId"`
	Amount    float64 `json:"amount"`
	Token     string  `json:"token"`
}

type RespAddDeposit struct {
	Error   string  `json:"error"`
	Balance float64 `json:"balance"`
}

type ReqTxUser struct {
	UserId        uint64  `json:"userId"`
	TransactionId uint64  `json:"transactionId"`
	Type          string  `json:"type"`
	Amount        float64 `json:"amount"`
	Token         string  `json:"token"`
}

type RespTxUser struct {
	Error   string  `json:"error"`
	Balance float64 `json:"balance"`
}

type EndpointFunc func([]byte) string

type EndpointMap map[string]EndpointFunc

var endpoints = EndpointMap{}

func Println(v ...interface{}) {
	currentTime := time.Now()
	fmt.Println((currentTime.Format("2006-01-02 15:04:05.000000  ")) + fmt.Sprint(v...))
}

func addUser(body []byte) string {
	var err error
	var req ReqAddUser
	var answer RespCommon
	err = json.Unmarshal(body, &req)
	if err != nil {
		answer.Error = err.Error()
	} else {
		if req.Token != "testtask" {
			answer.Error += "Wrong token value."
		}
		if !IsNewUser(req.Id) {
			answer.Error += "This Id is already used."
		}
		if req.Balance != 0.0 {
			answer.Error += "Wrong balance value."
		}
		if len(answer.Error) == 0 {
			// Here add new user
			err = AddUserToStorage(req.Id, req.Balance)
			if err != nil {
				answer.Error = err.Error()
			}
			bytes, err := json.Marshal(answer)
			if err != nil {
				log.Fatal(err)
			}
			return string(bytes)
		}
	}
	bytes, err := json.Marshal(answer)
	if err != nil {
		log.Fatal(err)
	}
	return string(bytes)
}

func getUser(body []byte) string {
	var err error
	var req ReqGetUser
	var answerErr RespCommon
	var answer RespGetUser
	err = json.Unmarshal(body, &req)
	if err != nil {
		answerErr.Error = err.Error()
	} else {
		if req.Token != "testtask" {
			answerErr.Error += "Wrong token value."
		}
		if IsNewUser(req.Id) {
			answerErr.Error += "User isn't exist."
		}
		if len(answerErr.Error) == 0 {
			// Here get user
			err = GetUserFromStorage(req.Id, &answer)
			if err != nil {
				answerErr.Error = err.Error()
				bytes, err := json.Marshal(answerErr)
				if err != nil {
					log.Fatal(err)
				}
				return string(bytes)
			}
			bytes, err := json.Marshal(answer)
			if err != nil {
				log.Fatal(err)
			}
			return string(bytes)
		}
	}
	bytes, err := json.Marshal(answerErr)
	if err != nil {
		log.Fatal(err)
	}
	return string(bytes)
}

func addDepositUser(body []byte) string {
	var err error
	var req ReqAddDeposit
	var answerErr RespCommon
	var answer RespAddDeposit
	err = json.Unmarshal(body, &req)
	if err != nil {
		answerErr.Error = err.Error()
	} else {
		if req.Token != "testtask" {
			answerErr.Error += "Wrong token value."
		}
		if IsNewUser(req.UserId) {
			answerErr.Error += "User isn't exist."
		}
		if !IsNewDeposit(req.UserId, req.DepositId) {
			answerErr.Error += "This deposit is already exist."
		}
		if !IsLinkedDeposit(req.UserId, req.DepositId) {
			answerErr.Error += "Deposit id is not linked to previous."
		}
		if len(answerErr.Error) == 0 {
			// Here add user deposit
			err = AddDepositToUser(req.UserId, req.DepositId, req.Amount, &answer)

			bytes, err := json.Marshal(answer)
			if err != nil {
				log.Fatal(err)
			}
			return string(bytes)
		}
	}
	bytes, err := json.Marshal(answerErr)
	if err != nil {
		log.Fatal(err)
	}
	return string(bytes)
}

func txUser(body []byte) string {
	var err error
	var req ReqTxUser
	var answerErr RespCommon
	var answer RespTxUser
	err = json.Unmarshal(body, &req)
	if err != nil {
		answerErr.Error = err.Error()
	} else {
		if req.Token != "testtask" {
			answerErr.Error += "Wrong token value."
		}
		if IsNewUser(req.UserId) {
			answerErr.Error += "User isn't exist."
		}
		if !IsValidTxType(req.Type) {
			answerErr.Error += "Invalid transaction type."
		}
		if !IsValidTxBet(req.UserId, req.Type, req.Amount) {
			answerErr.Error += "Invalid bet, insufficient of amount."
		}
		if !IsLinkedTx(req.UserId, req.TransactionId) {
			answerErr.Error += "Transaction id is not linked to previous."
		}
		if req.Amount <= 0.0 {
			answerErr.Error += "Invalid amount."
		}

		if len(answerErr.Error) == 0 {
			// Here perform transaction
			err = TransactionOfUser(req.UserId, req.TransactionId, req.Type, req.Amount, &answer)
			if err != nil {
				bytes, err := json.Marshal(answerErr)
				if err != nil {
					log.Fatal(err)
				}
				return string(bytes)
			}
			bytes, err := json.Marshal(answer)
			if err != nil {
				log.Fatal(err)
			}
			return string(bytes)
		}
	}
	bytes, err := json.Marshal(answerErr)
	if err != nil {
		log.Fatal(err)
	}
	return string(bytes)
}

func worker(w http.ResponseWriter, r *http.Request) {
	Println("url: ", r.URL.Path)

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprintf(w, "expected POST method")
		return
	}

	request, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal(err)
	}

	Println("request: ", string(request))

	if _, ok := endpoints[r.URL.Path]; !ok {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "wrong endpoint name")
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, endpoints[r.URL.Path](request))
	return
}

func main() {
	Println("Service started...")
	err := StorageInit()
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.Close()
	if err != nil {
		log.Fatal(err.Error())
	}
	endpoints = map[string]EndpointFunc{
		"/user/create":  addUser,
		"/user/get":     getUser,
		"/user/deposit": addDepositUser,
		"/transaction":  txUser,
	}
	http.HandleFunc("/", worker)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
