module github.com/bsm/sntable/bench

go 1.12

replace github.com/bsm/sntable => ../

require (
	github.com/bsm/sntable v0.1.1
	github.com/golang/leveldb v0.0.0-20170107010102-259d9253d719
	github.com/syndtr/goleveldb v1.0.0
	golang.org/x/net v0.0.0-20190607181551-461777fb6f67 // indirect
	golang.org/x/sys v0.0.0-20190610081024-1e42afee0f76 // indirect
)
