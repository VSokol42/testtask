package main

import (
	_ "encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

type Server struct {
	DB     interface{}
	Router interface{}
}

type ReqAddUser struct {
	Id      int     `json:"id"`
	Balance float32 `json:"balance"`
	Token   string  `json:"token"`
}

type RespCommon struct {
	Error string `json:"token"`
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

type CmdFunc func([]byte) string

type CmdMap map[string]CmdFunc

var Cmd = CmdMap{}

func Println(v ...interface{}) {
	currentTime := time.Now()
	fmt.Println((currentTime.Format("1999-01-02 03:04:05.000000  ")) + fmt.Sprintln(v...))
}

func addUser(req []byte) (resp string) {
	resp = "addUser ok"
	return
}

func getUser(req []byte) (resp string) {
	resp = "getUser ok"
	return
}

func addDeposit(req []byte) (resp string) {
	resp = "addDeposit ok "
	return
}

func tx(req []byte) (resp string) {
	resp = "tx ok"
	return
}

func handler(w http.ResponseWriter, r *http.Request) {
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
		"/user/deposit": addDeposit,
		"/transaction":  tx,
	}
	http.HandleFunc("/", handler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
