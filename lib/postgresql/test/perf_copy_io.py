##
# test.perf_copy_io - Copy I/O: To and From performance
##
import os, sys, random, time

if __name__ == '__main__':
	with open('/usr/share/dict/words', mode='brU') as wordfile:
		Words = wordfile.readlines()
else:
	Words = [b'/usr/share/dict/words', b'is', b'read', b'in', b'__main__']
wordcount = len(Words)
random.seed()

def getWord():
	"extract a random word from ``Words``"
	return Words[random.randrange(0, wordcount)].strip()

def testSpeed(tuples = 50000 * 3):
	sqlexec("CREATE TEMP TABLE _copy "
	"(i int, t text, mt text, ts text, ty text, tx text);")
	try:
		Q = prepare("COPY _copy FROM STDIN")
		size = 0
		def incsize(data):
			'count of bytes'
			nonlocal size
			size += len(data)
			return data
		sys.stderr.write("preparing data(%d tuples)...\n" %(tuples,))

		# Use an LC to avoid the Python overhead involved with a GE
		data = [incsize(b'\t'.join((
			str(x).encode('ascii'), getWord(), getWord(),
			getWord(), getWord(), getWord()
		)))+b'\n' for x in range(tuples)]

		sys.stderr.write("starting copy...\n")
		start = time.time()
		copied_in = Q.load_rows(data)
		duration = time.time() - start
		sys.stderr.write(
			"COPY FROM STDIN Summary,\n " \
			"copied tuples: %d\n " \
			"copied bytes: %d\n " \
			"duration: %f\n " \
			"average tuple size(bytes): %f\n " \
			"average KB per second: %f\n " \
			"average tuples per second: %f\n" %(
				tuples, size, duration,
				size / tuples,
				size / 1024 / duration,
				tuples / duration, 
			)
		)
		Q = prepare("COPY _copy TO STDOUT")
		start = time.time()
		c = 0
		for rows in Q.chunks():
			c += len(rows)
		duration = time.time() - start
		sys.stderr.write(
			"COPY TO STDOUT Summary,\n " \
			"copied tuples: %d\n " \
			"duration: %f\n " \
			"average KB per second: %f\n " \
			"average tuples per second: %f\n " %(
				c, duration,
				size / 1024 / duration,
				tuples / duration, 
			)
		)
	finally:
		sqlexec("DROP TABLE _copy")

if __name__ == '__main__':
	testSpeed()
