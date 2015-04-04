#!/usr/bin/env python
##
# .test.perf_query_io
##
# Statement I/O: Mass insert and select performance
##
import os
import time
import sys
import decimal
import datetime

def insertSamples(count, insert_records):
	recs = [
		(
			-3, 123, 0xfffffea023,
			decimal.Decimal("90900023123.40031"),
			decimal.Decimal("432.40031"),
			'some_óäæ_thing', 'varying', 'æ',
			datetime.datetime(1982, 5, 18, 12, 0, 0, 100232)
		)
		for x in range(count)
	]
	gen = time.time()
	insert_records.load_rows(recs)
	fin = time.time()
	xacttime = fin - gen
	ats = count / xacttime
	sys.stderr.write(
		"INSERT Summary,\n " \
		"inserted tuples: %d\n " \
		"total time: %f\n " \
		"average tuples per second: %f\n\n" %(
			count, xacttime, ats, 
		)
	)

def timeTupleRead(ps):
	loops = 0
	tuples = 0
	genesis = time.time()
	for x in ps.chunks():
		loops += 1
		tuples += len(x)
	finalis = time.time()
	looptime = finalis - genesis
	ats = tuples / looptime
	sys.stderr.write(
		"SELECT Summary,\n " \
		"looped: {looped}\n " \
		"looptime: {looptime}\n " \
		"tuples: {ntuples}\n " \
		"average tuples per second: {tps}\n ".format(
			looped = loops,
			looptime = looptime,
			ntuples = tuples,
			tps = ats
		)
	)

def main(count):
	sqlexec('CREATE TEMP TABLE samples '
		'(i2 int2, i4 int4, i8 int8, n numeric, n2 numeric, t text, v varchar, c char(2), ts timestamp)')
	insert_records = prepare(
		"INSERT INTO samples VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
	)
	select_records = prepare("SELECT * FROM samples")
	try:
		insertSamples(count, insert_records)
		timeTupleRead(select_records)	
	finally:
		sqlexec("DROP TABLE samples")

def command(args):
	main(int((args + [25000])[1]))

if __name__ == '__main__':
	command(sys.argv)
