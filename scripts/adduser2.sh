#!/bin/sh
curl -d "@adduser2.json" -X POST http://localhost:8080/user/create