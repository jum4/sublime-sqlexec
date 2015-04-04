/*
 * module.c - optimizations for various parts of py-postgresql
 *
 * This module.c file ties together other classified C source.
 * Each filename describing the part of the protocol package that it
 * covers. It merely uses CPP includes to bring them into this
 * file and then uses some CPP macros to expand the definitions
 * in each file.
 */
#include <Python.h>
#include <structmember.h>
/*
 * If Python didn't find it, it won't include it.
 * However, it's quite necessary.
 */
#ifndef HAVE_STDINT_H
#include <stdint.h>
#endif

#define USHORT_MAX ((1<<16)-1)
#define SHORT_MAX ((1<<15)-1)
#define SHORT_MIN (-(1<<15))

#define PyObject_TypeName(ob) \
	(((PyTypeObject *) (ob->ob_type))->tp_name)

/*
 * buffer.c needs the message_types object from .protocol.message_types.
 * Initialized in PyInit_optimized.
 */
static PyObject *message_types = NULL;
static PyObject *serialize_strob = NULL;
static PyObject *msgtype_strob = NULL;

static int32_t (*local_ntohl)(int32_t) = NULL;
static short (*local_ntohs)(short) = NULL;

/*
 * optimized module contents
 */
#include "structlib.c"
#include "functools.c"
#include "buffer.c"
#include "wirestate.c"
#include "element3.c"


/* cpp abuse, read up on X-Macros if you don't understand  */
#define mFUNC(name, typ, doc) \
	{#name, (PyCFunction) name, typ, PyDoc_STR(doc)},
static PyMethodDef optimized_methods[] = {
	include_element3_functions
	include_structlib_functions
	include_functools_functions
	{NULL}
};
#undef mFUNC

static struct PyModuleDef optimized_module = {
   PyModuleDef_HEAD_INIT,
   "optimized",	/* name of module */
   NULL,				/* module documentation, may be NULL */
   -1,				/* size of per-interpreter state of the module,
							or -1 if the module keeps state in global variables. */
   optimized_methods,
};

PyMODINIT_FUNC
PyInit_optimized(void)
{
	PyObject *mod;
	PyObject *msgtypes;
	PyObject *fromlist, *fromstr;
	long l;

	/* make some constants */
	if (serialize_strob == NULL)
	{
		serialize_strob = PyUnicode_FromString("serialize");
		if (serialize_strob == NULL)
			return(NULL);
	}
	if (msgtype_strob == NULL)
	{
		msgtype_strob = PyUnicode_FromString("type");
		if (msgtype_strob == NULL)
			return(NULL);
	}

	mod = PyModule_Create(&optimized_module);
	if (mod == NULL)
		return(NULL);

/* cpp abuse; ready types */
#define mTYPE(name) \
	if (PyType_Ready(&name##_Type) < 0) \
		goto cleanup; \
	if (PyModule_AddObject(mod, #name, \
			(PyObject *) &name##_Type) < 0) \
		goto cleanup;

	/* buffer.c */
	include_buffer_types
	/* wirestate.c  */
	include_wirestate_types
#undef mTYPE

	l = 1;
	if (((char *) &l)[0] == 1)
	{
		/* little */
		local_ntohl = swap_int4;
		local_ntohs = swap_short;
	}
	else
	{
		/* big */
		local_ntohl = return_int4;
		local_ntohs = return_short;
	}

	/*
	 * Get the message_types tuple to type "instantiation".
	 */
	fromlist = PyList_New(1);
	fromstr = PyUnicode_FromString("message_types");
	PyList_SetItem(fromlist, 0, fromstr);
	msgtypes = PyImport_ImportModuleLevel(
		"protocol.message_types",
		PyModule_GetDict(mod),
		PyModule_GetDict(mod),
		fromlist, 2
	);
	Py_DECREF(fromlist);
	if (msgtypes == NULL)
		goto cleanup;
	message_types = PyObject_GetAttrString(msgtypes, "message_types");
	Py_DECREF(msgtypes);

	if (!PyObject_IsInstance(message_types, (PyObject *) (&PyTuple_Type)))
	{
		PyErr_SetString(PyExc_RuntimeError,
			"local protocol.message_types.message_types is not a tuple object");
		goto cleanup;
	}

	return(mod);
cleanup:
	Py_DECREF(mod);
	return(NULL);
}
/*
 * vim: ts=3:sw=3:noet:
 */
