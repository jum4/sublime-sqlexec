/*
 * .port.optimized - functools.c
 *
 *//*
 * optimizations for postgresql.python package modules.
 */
/*
 * process the tuple with the associated callables while
 * calling the third object in cases of failure to generalize the exception.
 */
#define include_functools_functions \
	mFUNC(rsetattr, METH_VARARGS, "rsetattr(attr, val, ob) set the attribute to the value *and* return `ob`.") \
	mFUNC(compose, METH_VARARGS, "given a sequence of callables, and an argument for the first call, compose the result.") \
	mFUNC(process_tuple, METH_VARARGS, "process the items in the second argument with the corresponding items in the first argument.") \
	mFUNC(process_chunk, METH_VARARGS, "process the items of the chunk given as the second argument with the corresponding items in the first argument.")

static PyObject *
_process_tuple(PyObject *procs, PyObject *tup, PyObject *fail)
{
	PyObject *rob;
	Py_ssize_t len, i;

	if (!PyTuple_CheckExact(procs))
	{
		PyErr_SetString(
			PyExc_TypeError,
			"process_tuple requires an exact tuple as its first argument"
		);
		return(NULL);
	}

	if (!PyTuple_Check(tup))
	{
		PyErr_SetString(
			PyExc_TypeError,
			"process_tuple requires a tuple as its second argument"
		);
		return(NULL);
	}

	len = PyTuple_GET_SIZE(tup);

	if (len != PyTuple_GET_SIZE(procs))
	{
		PyErr_Format(
			PyExc_TypeError,
			"inconsistent items, %d processors and %d items in row",
			len,
			PyTuple_GET_SIZE(procs)
		);
		return(NULL);
	}
	/* types check out; consistent sizes */
	rob = PyTuple_New(len);

	for (i = 0; i < len; ++i)
	{
		PyObject *p, *o, *ot, *r;
		/* p = processor,
		 * o = source object,
		 * ot = o's tuple (temp for application to p),
		 * r = transformed * output
		 */

		/*
		 * If it's Py_None, that means it's NULL. No processing necessary.
		 */
		o = PyTuple_GET_ITEM(tup, i);
		if (o == Py_None)
		{
			Py_INCREF(Py_None);
			PyTuple_SET_ITEM(rob, i, Py_None);
			/* mmmm, cake! */
			continue;
		}

		p = PyTuple_GET_ITEM(procs, i);
		/*
		 * Temp tuple for applying *args to p.
		 */
		ot = PyTuple_New(1);
		PyTuple_SET_ITEM(ot, 0, o);
		Py_INCREF(o);

		r = PyObject_CallObject(p, ot);
		Py_DECREF(ot);
		if (r != NULL)
		{
			/* good, set it and move on. */
			PyTuple_SET_ITEM(rob, i, r);
		}
		else
		{
			/*
			 * Exception caused by >>> p(*ot)
			 *
			 * In this case, the failure callback needs to be called
			 * in order to properly generalize the failure. There are numerous,
			 * and (sometimes) inconsistent reasons why a tuple cannot be
			 * processed and therefore a generalized exception raised in the
			 * context of the original is *very* useful.
			 */
			Py_DECREF(rob);
			rob = NULL;

			/*
			 * Don't trap BaseException's.
			 */
			if (PyErr_ExceptionMatches(PyExc_Exception))
			{
				PyObject *cause, *failargs, *failedat;
				PyObject *exc, *tb;

				/* Store exception to set context after handler. */
				PyErr_Fetch(&exc, &cause, &tb);
				PyErr_NormalizeException(&exc, &cause, &tb);
				Py_XDECREF(exc);
				Py_XDECREF(tb);

				failedat = PyLong_FromSsize_t(i);
				if (failedat != NULL)
				{
					failargs = PyTuple_New(4);
					if (failargs != NULL)
					{
						/* args for the exception "generalizer" */
						PyTuple_SET_ITEM(failargs, 0, cause);
						PyTuple_SET_ITEM(failargs, 1, procs);
						Py_INCREF(procs);
						PyTuple_SET_ITEM(failargs, 2, tup);
						Py_INCREF(tup);
						PyTuple_SET_ITEM(failargs, 3, failedat);

						r = PyObject_CallObject(fail, failargs);
						Py_DECREF(failargs);
						if (r != NULL)
						{
							PyErr_SetString(PyExc_RuntimeError,
								"process_tuple exception handler failed to raise"
							);
							Py_DECREF(r);
						}
					}
					else
					{
						Py_DECREF(failedat);
					}
				}
			}

			/*
			 * Break out of loop to return(NULL);
			 */
			break;
		}
	}

	return(rob);
}

/*
 * process the tuple with the associated callables while
 * calling the third object in cases of failure to generalize the exception.
 */
static PyObject *
process_tuple(PyObject *self, PyObject *args)
{
	PyObject *tup, *procs, *fail;

	if (!PyArg_ParseTuple(args, "OOO", &procs, &tup, &fail))
		return(NULL);

	return(_process_tuple(procs, tup, fail));
}

static PyObject *
_process_chunk_new_list(PyObject *procs, PyObject *tupc, PyObject *fail)
{
	PyObject *rob;
	Py_ssize_t i, len;

	/*
	 * Turn the iterable into a new list.
	 */
	rob = PyObject_CallFunctionObjArgs((PyObject *) &PyList_Type, tupc, NULL);
	if (rob == NULL)
		return(NULL);
	len = PyList_GET_SIZE(rob);

	for (i = 0; i < len; ++i)
	{
		PyObject *tup, *r;
		/*
		 * If it's Py_None, that means it's NULL. No processing necessary.
		 */
		tup = PyList_GetItem(rob, i); /* borrowed ref from list */
		r = _process_tuple(procs, tup, fail);
		if (r == NULL)
		{
			/* process_tuple failed. assume PyErr_Occurred() */
			Py_DECREF(rob);
			return(NULL);
		}
		PyList_SetItem(rob, i, r);
	}

	return(rob);
}

static PyObject *
_process_chunk_from_list(PyObject *procs, PyObject *tupc, PyObject *fail)
{
	PyObject *rob;
	Py_ssize_t i, len;

	len = PyList_GET_SIZE(tupc);
	rob = PyList_New(len);
	if (rob == NULL)
		return(NULL);

	for (i = 0; i < len; ++i)
	{
		PyObject *tup, *r;
		/*
		 * If it's Py_None, that means it's NULL. No processing necessary.
		 */
		tup = PyList_GET_ITEM(tupc, i);
		r = _process_tuple(procs, tup, fail);
		if (r == NULL)
		{
			Py_DECREF(rob);
			return(NULL);
		}
		PyList_SET_ITEM(rob, i, r);
	}

	return(rob);
}

/*
 * process the chunk of tuples with the associated callables while
 * calling the third object in cases of failure to generalize the exception.
 */
static PyObject *
process_chunk(PyObject *self, PyObject *args)
{
	PyObject *tupc, *procs, *fail;

	if (!PyArg_ParseTuple(args, "OOO", &procs, &tupc, &fail))
		return(NULL);

	if (PyList_Check(tupc))
	{
		return(_process_chunk_from_list(procs, tupc, fail));
	}
	else
	{
		return(_process_chunk_new_list(procs, tupc, fail));
	}
}
static PyObject *
rsetattr(PyObject *self, PyObject *args)
{
	PyObject *ob, *attr, *val;

	if (!PyArg_ParseTuple(args, "OOO", &attr, &val, &ob))
		return(NULL);

	if (PyObject_SetAttr(ob, attr, val) < 0)
		return(NULL);

	Py_INCREF(ob);
	return(ob);
}

/*
 * Override the functools.Composition __call__.
 */
static PyObject *
compose(PyObject *self, PyObject *args)
{
	Py_ssize_t i, len;
	PyObject *rob, *argt, *seq, *x;

	if (!PyArg_ParseTuple(args, "OO", &seq, &rob))
		return(NULL);

	Py_INCREF(rob);
	if (PyObject_IsInstance(seq, (PyObject *) &PyTuple_Type))
	{
		len = PyTuple_GET_SIZE(seq);
		for (i = 0; i < len; ++i)
		{
			x = PyTuple_GET_ITEM(seq, i);
			argt = PyTuple_New(1);
			PyTuple_SET_ITEM(argt, 0, rob);
			rob = PyObject_CallObject(x, argt);
			Py_DECREF(argt);
			if (rob == NULL)
				break;
		}
	}
	else if (PyObject_IsInstance(seq, (PyObject *) &PyList_Type))
	{
		len = PyList_GET_SIZE(seq);
		for (i = 0; i < len; ++i)
		{
			x = PyList_GET_ITEM(seq, i);
			argt = PyTuple_New(1);
			PyTuple_SET_ITEM(argt, 0, rob);
			rob = PyObject_CallObject(x, argt);
			Py_DECREF(argt);
			if (rob == NULL)
				break;
		}
	}
	else
	{
		/*
		 * Arbitrary sequence.
		 */
		len = PySequence_Length(seq);
		for (i = 0; i < len; ++i)
		{
			x = PySequence_GetItem(seq, i);
			argt = PyTuple_New(1);
			PyTuple_SET_ITEM(argt, 0, rob);
			rob = PyObject_CallObject(x, argt);
			Py_DECREF(x);
			Py_DECREF(argt);
			if (rob == NULL)
				break;
		}
	}

	return(rob);
}
/*
 * vim: ts=3:sw=3:noet:
 */
