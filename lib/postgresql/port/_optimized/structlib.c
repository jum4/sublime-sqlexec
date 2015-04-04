/*
 * .port.optimized - pack and unpack int2, int4, and int8.
 */

/*
 * Define the swap functionality for those endians.
 */
#define swap2(CP) do{register char c; \
	c=CP[1];CP[1]=CP[0];CP[0]=c;\
}while(0)
#define swap4(P) do{register char c; \
	c=P[3];P[3]=P[0];P[0]=c;\
	c=P[2];P[2]=P[1];P[1]=c;\
}while(0)
#define swap8(P) do{register char c; \
	c=P[7];P[7]=P[0];P[0]=c;\
	c=P[6];P[6]=P[1];P[1]=c;\
	c=P[5];P[5]=P[2];P[2]=c;\
	c=P[4];P[4]=P[3];P[3]=c;\
}while(0)

#define long_funcs \
	mFUNC(int2_pack, METH_O, "PyInt to serialized, int2") \
	mFUNC(int2_unpack, METH_O, "PyInt from serialized, int2") \
	mFUNC(int4_pack, METH_O, "PyInt to serialized, int4") \
	mFUNC(int4_unpack, METH_O, "PyInt from serialized, int4") \
	mFUNC(swap_int2_pack, METH_O, "PyInt to swapped serialized, int2") \
	mFUNC(swap_int2_unpack, METH_O, "PyInt from swapped serialized, int2") \
	mFUNC(swap_int4_pack, METH_O, "PyInt to swapped serialized, int4") \
	mFUNC(swap_int4_unpack, METH_O, "PyInt from swapped serialized, int4") \
	mFUNC(uint2_pack, METH_O, "PyInt to serialized, uint2") \
	mFUNC(uint2_unpack, METH_O, "PyInt from serialized, uint2") \
	mFUNC(uint4_pack, METH_O, "PyInt to serialized, uint4") \
	mFUNC(uint4_unpack, METH_O, "PyInt from serialized, uint4") \
	mFUNC(swap_uint2_pack, METH_O, "PyInt to swapped serialized, uint2") \
	mFUNC(swap_uint2_unpack, METH_O, "PyInt from swapped serialized, uint2") \
	mFUNC(swap_uint4_pack, METH_O, "PyInt to swapped serialized, uint4") \
	mFUNC(swap_uint4_unpack, METH_O, "PyInt from swapped serialized, uint4") \

#ifdef HAVE_LONG_LONG
#if SIZEOF_LONG_LONG == 8
/*
 * If the configuration is not consistent with the expectations,
 * just use the slower struct.Struct versions.
 */
#define longlong_funcs \
	mFUNC(int8_pack, METH_O, "PyInt to serialized, int8") \
	mFUNC(int8_unpack, METH_O, "PyInt from serialized, int8") \
	mFUNC(swap_int8_pack, METH_O, "PyInt to swapped serialized, int8") \
	mFUNC(swap_int8_unpack, METH_O, "PyInt from swapped serialized, int8") \
	mFUNC(uint8_pack, METH_O, "PyInt to serialized, uint8") \
	mFUNC(uint8_unpack, METH_O, "PyInt from serialized, uint8") \
	mFUNC(swap_uint8_pack, METH_O, "PyInt to swapped serialized, uint8") \
	mFUNC(swap_uint8_unpack, METH_O, "PyInt from swapped serialized, uint8") \

#define include_structlib_functions \
	long_funcs \
	longlong_funcs

#if 0
		Currently not used, so exclude.

static PY_LONG_LONG
return_long_long(PY_LONG_LONG i)
{
	return(i);
}

static PY_LONG_LONG
swap_long_long(PY_LONG_LONG i)
{
	swap8(((char *) &i));
	return(i);
}
#endif

#endif
#endif

#ifndef include_structlib_functions
#define include_structlib_functions \
	long_funcs
#endif

static short
swap_short(short s)
{
	swap2(((char *) &s));
	return(s);
}

static short
return_short(short s)
{
	return(s);
}

static int32_t
swap_int4(int32_t i)
{
	swap4(((char *) &i));
	return(i);
}

static int32_t
return_int4(int32_t i)
{
	return(i);
}

static PyObject *
int2_pack(PyObject *self, PyObject *arg)
{
	long l;
	short s;

	l = PyLong_AsLong(arg);
	if (PyErr_Occurred())
		return(NULL);

	if (l > SHORT_MAX || l < SHORT_MIN)
	{
		PyErr_Format(PyExc_OverflowError,
			"long '%d' overflows int2", l
		);
		return(NULL);
	}

	s = (short) l;
	return(PyBytes_FromStringAndSize((const char *) &s, 2));
}

static PyObject *
swap_int2_pack(PyObject *self, PyObject *arg)
{
	long l;
	short s;

	l = PyLong_AsLong(arg);
	if (PyErr_Occurred())
		return(NULL);
	if (l > SHORT_MAX || l < SHORT_MIN)
	{
		PyErr_SetString(PyExc_OverflowError, "long too big or small for int2");
		return(NULL);
	}

	s = (short) l;
	swap2(((char *) &s));
	return(PyBytes_FromStringAndSize((const char *) &s, 2));
}

static PyObject *
int2_unpack(PyObject *self, PyObject *arg)
{
	char *c;
	short *i;
	long l;
	Py_ssize_t len;
	PyObject *rob;

	if (PyObject_AsReadBuffer(arg, (const void **) &c, &len))
		return(NULL);

	if (len < 2)
	{
		PyErr_SetString(PyExc_ValueError, "not enough data for int2_unpack");
		return(NULL);
	}

	i = (short *) c;
	l = (long) *i;
	rob = PyLong_FromLong(l);
	return(rob);
}

static PyObject *
swap_int2_unpack(PyObject *self, PyObject *arg)
{
	char *c;
	short s;
	long l;
	Py_ssize_t len;
	PyObject *rob;

	if (PyObject_AsReadBuffer(arg, (const void **) &c, &len))
		return(NULL);

	if (len < 2)
	{
		PyErr_SetString(PyExc_ValueError, "not enough data for swap_int2_unpack");
		return(NULL);
	}

	s = *((short *) c);
	swap2(((char *) &s));
	l = (long) s;
	rob = PyLong_FromLong(l);
	return(rob);
}

static PyObject *
int4_pack(PyObject *self, PyObject *arg)
{
	long l;
	int32_t i;

	l = PyLong_AsLong(arg);
	if (PyErr_Occurred())
		return(NULL);
	if (!(l <= (long) 0x7FFFFFFFL && l >= (long) (-0x80000000L)))
	{
		PyErr_Format(PyExc_OverflowError,
			"long '%ld' overflows int4", l
		);
		return(NULL);
	}
	i = (int32_t) l;
	return(PyBytes_FromStringAndSize((const char *) &i, 4));
}

static PyObject *
swap_int4_pack(PyObject *self, PyObject *arg)
{
	long l;
	int32_t i;

	l = PyLong_AsLong(arg);
	if (PyErr_Occurred())
		return(NULL);
	if (!(l <= (long) 0x7FFFFFFFL && l >= (long) (-0x80000000L)))
	{
		PyErr_Format(PyExc_OverflowError,
			"long '%ld' overflows int4", l
		);
		return(NULL);
	}
	i = (int32_t) l;
	swap4(((char *) &i));
	return(PyBytes_FromStringAndSize((const char *) &i, 4));
}

static PyObject *
int4_unpack(PyObject *self, PyObject *arg)
{
	char *c;
	int32_t i;
	Py_ssize_t len;

	if (PyObject_AsReadBuffer(arg, (const void **) &c, &len))
		return(NULL);

	if (len < 4)
	{
		PyErr_SetString(PyExc_ValueError, "not enough data for int4_unpack");
		return(NULL);
	}
	i = *((int32_t *) c);

	return(PyLong_FromLong((long) i));
}

static PyObject *
swap_int4_unpack(PyObject *self, PyObject *arg)
{
	char *c;
	int32_t i;
	Py_ssize_t len;

	if (PyObject_AsReadBuffer(arg, (const void **) &c, &len))
		return(NULL);

	if (len < 4)
	{
		PyErr_SetString(PyExc_ValueError, "not enough data for swap_int4_unpack");
		return(NULL);
	}

	i = *((int32_t *) c);
	swap4(((char *) &i));
	return(PyLong_FromLong((long) i));
}

static PyObject *
uint2_pack(PyObject *self, PyObject *arg)
{
	long l;
	unsigned short s;

	l = PyLong_AsLong(arg);
	if (PyErr_Occurred())
		return(NULL);

	if (l > USHORT_MAX || l < 0)
	{
		PyErr_Format(PyExc_OverflowError,
			"long '%ld' overflows uint2", l
		);
		return(NULL);
	}

	s = (unsigned short) l;
	return(PyBytes_FromStringAndSize((const char *) &s, 2));
}

static PyObject *
swap_uint2_pack(PyObject *self, PyObject *arg)
{
	long l;
	unsigned short s;

	l = PyLong_AsLong(arg);
	if (PyErr_Occurred())
		return(NULL);

	if (l > USHORT_MAX || l < 0)
	{
		PyErr_Format(PyExc_OverflowError,
			"long '%ld' overflows uint2", l
		);
		return(NULL);
	}

	s = (unsigned short) l;
	swap2(((char *) &s));
	return(PyBytes_FromStringAndSize((const char *) &s, 2));
}

static PyObject *
uint2_unpack(PyObject *self, PyObject *arg)
{
	char *c;
	unsigned short *i;
	long l;
	Py_ssize_t len;
	PyObject *rob;

	if (PyObject_AsReadBuffer(arg, (const void **) &c, &len))
		return(NULL);

	if (len < 2)
	{
		PyErr_SetString(PyExc_ValueError, "not enough data for uint2_unpack");
		return(NULL);
	}

	i = (unsigned short *) c;
	l = (long) *i;
	rob = PyLong_FromLong(l);
	return(rob);
}

static PyObject *
swap_uint2_unpack(PyObject *self, PyObject *arg)
{
	char *c;
	unsigned short s;
	long l;
	Py_ssize_t len;
	PyObject *rob;

	if (PyObject_AsReadBuffer(arg, (const void **) &c, &len))
		return(NULL);
	if (len < 2)
	{
		PyErr_SetString(PyExc_ValueError, "not enough data for swap_uint2_unpack");
		return(NULL);
	}

	s = *((short *) c);
	swap2(((char *) &s));
	l = (long) s;
	rob = PyLong_FromLong(l);
	return(rob);
}

static PyObject *
uint4_pack(PyObject *self, PyObject *arg)
{
	uint32_t i;
	unsigned long l;

	l = PyLong_AsUnsignedLong(arg);
	if (PyErr_Occurred())
		return(NULL);
	if (l > 0xFFFFFFFFL)
	{
		PyErr_Format(PyExc_OverflowError,
			"long '%lu' overflows uint4", l
		);
		return(NULL);
	}

	i = (uint32_t) l;
	return(PyBytes_FromStringAndSize((const char *) &i, 4));
}

static PyObject *
swap_uint4_pack(PyObject *self, PyObject *arg)
{
	uint32_t i;
	unsigned long l;

	l = PyLong_AsUnsignedLong(arg);
	if (PyErr_Occurred())
		return(NULL);
	if (l > 0xFFFFFFFFL)
	{
		PyErr_Format(PyExc_OverflowError,
			"long '%lu' overflows uint4", l
		);
		return(NULL);
	}

	i = (uint32_t) l;
	swap4(((char *) &i));
	return(PyBytes_FromStringAndSize((const char *) &i, 4));
}

static PyObject *
uint4_unpack(PyObject *self, PyObject *arg)
{
	char *c;
	uint32_t i;
	Py_ssize_t len;

	if (PyObject_AsReadBuffer(arg, (const void **) &c, &len))
		return(NULL);

	if (len < 4)
	{
		PyErr_SetString(PyExc_ValueError, "not enough data for uint4_unpack");
		return(NULL);
	}

	i = *((uint32_t *) c);
	return(PyLong_FromUnsignedLong((unsigned long) i));
}

static PyObject *
swap_uint4_unpack(PyObject *self, PyObject *arg)
{
	char *c;
	uint32_t i;
	Py_ssize_t len;

	if (PyObject_AsReadBuffer(arg, (const void **) &c, &len))
		return(NULL);
	if (len < 4)
	{
		PyErr_SetString(PyExc_ValueError,
			"not enough data for swap_uint4_unpack");
		return(NULL);
	}

	i = *((uint32_t *) c);
	swap4(((char *) &i));

	return(PyLong_FromUnsignedLong((unsigned long) i));
}

#ifdef longlong_funcs
/*
 * int8 and "uint8" I/O
 */
static PyObject *
int8_pack(PyObject *self, PyObject *arg)
{
	PY_LONG_LONG l;

	l = PyLong_AsLongLong(arg);
	if (l == (PY_LONG_LONG) -1 && PyErr_Occurred())
		return(NULL);

	return(PyBytes_FromStringAndSize((const char *) &l, 8));
}

static PyObject *
swap_int8_pack(PyObject *self, PyObject *arg)
{
	PY_LONG_LONG l;

	l = PyLong_AsLongLong(arg);
	if (l == (PY_LONG_LONG) -1 && PyErr_Occurred())
		return(NULL);

	swap8(((char *) &l));
	return(PyBytes_FromStringAndSize((const char *) &l, 8));
}

static PyObject *
uint8_pack(PyObject *self, PyObject *arg)
{
	unsigned PY_LONG_LONG l;

	l = PyLong_AsUnsignedLongLong(arg);
	if (l == (unsigned PY_LONG_LONG) -1 && PyErr_Occurred())
		return(NULL);

	return(PyBytes_FromStringAndSize((const char *) &l, 8));
}

static PyObject *
swap_uint8_pack(PyObject *self, PyObject *arg)
{
	unsigned PY_LONG_LONG l;

	l = PyLong_AsUnsignedLongLong(arg);
	if (l == (unsigned PY_LONG_LONG) -1 && PyErr_Occurred())
		return(NULL);

	swap8(((char *) &l));
	return(PyBytes_FromStringAndSize((const char *) &l, 8));
}

static PyObject *
uint8_unpack(PyObject *self, PyObject *arg)
{
	char *c;
	Py_ssize_t len;
	unsigned PY_LONG_LONG i;

	if (PyObject_AsReadBuffer(arg, (const void **) &c, &len))
		return(NULL);
	if (len < 8)
	{
		PyErr_SetString(PyExc_ValueError, "not enough data for uint8_unpack");
		return(NULL);
	}

	i = *((unsigned PY_LONG_LONG *) c);
	return(PyLong_FromUnsignedLongLong(i));
}
static PyObject *
swap_uint8_unpack(PyObject *self, PyObject *arg)
{
	char *c;
	Py_ssize_t len;
	unsigned PY_LONG_LONG i;

	if (PyObject_AsReadBuffer(arg, (const void **) &c, &len))
		return(NULL);
	if (len < 8)
	{
		PyErr_SetString(PyExc_ValueError,
			"not enough data for swap_uint8_unpack");
		return(NULL);
	}

	i = *((unsigned PY_LONG_LONG *) c);
	swap8(((char *) &i));
	return(PyLong_FromUnsignedLongLong(i));
}

static PyObject *
int8_unpack(PyObject *self, PyObject *arg)
{
	char *c;
	Py_ssize_t len;
	PY_LONG_LONG i;

	if (PyObject_AsReadBuffer(arg, (const void **) &c, &len))
		return(NULL);
	if (len < 8)
	{
		PyErr_SetString(PyExc_ValueError,
			"not enough data for int8_unpack");
		return(NULL);
	}

	i = *((PY_LONG_LONG *) c);
	return(PyLong_FromLongLong((PY_LONG_LONG) i));
}

static PyObject *
swap_int8_unpack(PyObject *self, PyObject *arg)
{
	char *c;
	Py_ssize_t len;
	PY_LONG_LONG i;

	c = PyBytes_AsString(arg);
	if (PyErr_Occurred())
		return(NULL);

	if (PyObject_AsReadBuffer(arg, (const void **) &c, &len))
		return(NULL);
	if (len < 8)
	{
		PyErr_SetString(PyExc_ValueError,
			"not enough data for swap_int8_unpack");
		return(NULL);
	}

	i = *((PY_LONG_LONG *) c);
	swap8(((char *) &i));
	return(PyLong_FromLongLong(i));
}
#endif /* longlong_funcs */
