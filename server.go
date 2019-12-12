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
	Id      int     `json:"id"`
	Balance float32 `json:"balance"`
	Token   string  `json:"token"`
}

type RespCommon struct {
	Error string `json:"error"`
}

type ReqGetUser struct {
	Id    int    `json:"id"`
	Token string `json:"token"`
}

type RespGetUser struct {
	Id           int     `json:"id"`
	Balance      float32 `json:"balance"`
	DepositCount int     `json:"depositCount"`
	DepositSum   int     `json:"depositSum"`
	BetSum       int     `json:"betSum"`
	WinCount     int     `json:"winCount"`
	WinSum       int     `json:"winSum"`
}

type ReqAddDeposit struct {
	UserId    int    `json:"userId"`
	DepositId int    `json:"depositId"`
	Amount    int    `json:"amount"`
	Token     string `json:"token"`
}

type RespAddDeposit struct {
	Error   string  `json:"error"`
	Balance float32 `json:"balance"`
}

type ReqTxUser struct {
	UserId        int     `json:"userId"`
	TransactionId int     `json:"transactionId"`
	Type          string  `json:"type"`
	Amount        float32 `json:"amount"`
	Token         string  `json:"token"`
}

type RespTxUser struct {
	Error   string  `json:"error"`
	Balance float32 `json:"balance"`
}

type CmdFunc func([]byte) string

type CmdMap map[string]CmdFunc

var Cmd = CmdMap{}

func Println(v ...interface{}) {
	currentTime := time.Now()
	fmt.Println((currentTime.Format("1999-01-02 03:04:05.000000  ")) + fmt.Sprint(v...))
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
			Println("User added")
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
		if !IsNewUser(req.Id) {
			answerErr.Error += "User isn't exist."
		}
		if len(answerErr.Error) == 0 {
			// Here get user
			Println("User get")
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
		if !IsNewUser(req.UserId) {
			answerErr.Error += "User isn't exist."
		}
		if len(answerErr.Error) == 0 {
			// Here add user deposit
			Println("Add deposit")
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
		if !IsNewUser(req.UserId) {
			answerErr.Error += "User isn't exist."
		}
		if len(answerErr.Error) == 0 {
			// Here
			Println("Transaction")
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

	if _, ok := Cmd[r.URL.Path]; !ok {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "wrong endpoint name")
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, Cmd[r.URL.Path](request))
	return
}

func main() {
	Println("Service started...")
	Cmd = map[string]CmdFunc{
		"/user/create":  addUser,
		"/user/get":     getUser,
		"/user/deposit": addDepositUser,
		"/transaction":  txUser,
	}
	http.HandleFunc("/", worker)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
