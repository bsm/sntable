module github.com/bsm/sntable/bench

go 1.12

replace github.com/bsm/sntable => ../

require (
	github.com/bsm/sntable v0.0.0-00010101000000-000000000000
	github.com/golang/leveldb v0.0.0-20170107010102-259d9253d719
	github.com/syndtr/goleveldb v1.0.0
)
