// storage
package main

type User struct {
	id           int64
	balance      float64
	depositCount int64
	depositSum   float64
	winCount     int64
	winSum       float64
	betCount     int64
	betSum       float64
}

type UsersCache map[int64]*User

var Users = UsersCache{}

func IsNewUser(id int) (r bool) {
	return !r
}
