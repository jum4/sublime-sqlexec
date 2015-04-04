/*
 * .port.optimized.WireState - PQ wire state for COPY.
 */
#define include_wirestate_types \
	mTYPE(WireState)

struct wirestate
{
	PyObject_HEAD
	char size_fragment[4];				/* the header fragment; continuation specifies bytes read so far. */
	PyObject *final_view;				/* Py_None unless we reach an unknown message */
	Py_ssize_t remaining_bytes;		/* Bytes remaining in message */
	short continuation;					/* >= 0 when continuing a fragment */
};

static void
ws_dealloc(PyObject *self)
{
	struct wirestate *ws = ((struct wirestate *) self);
	Py_XDECREF(ws->final_view);
	Py_TYPE(self)->tp_free(self);
}

static PyObject *
ws_new(PyTypeObject *subtype, PyObject *args, PyObject *kw)
{
	static char *kwlist[] = {"condition", NULL};
	struct wirestate *ws;
	PyObject *rob;

	if (!PyArg_ParseTupleAndKeywords(args, kw, "|O", kwlist, &rob))
		return(NULL);

	rob = subtype->tp_alloc(subtype, 0);
	ws = ((struct wirestate *) rob);

	ws->continuation = -1;
	ws->remaining_bytes = 0;
	ws->final_view = NULL;

	return(rob);
}

#define CONDITION(MSGTYPE) (MSGTYPE != 'd')

static PyObject *
ws_update(PyObject *self, PyObject *view)
{
	struct wirestate *ws;
	uint32_t remaining_bytes, nmessages = 0;
	unsigned char *buf, msgtype;
	char size_fragment[4];
	short continuation;
	Py_ssize_t position = 0, len;
	PyObject *rob, *final_view = NULL;

	if (PyObject_AsReadBuffer(view, (const void **) &buf, &len))
		return(NULL);

	if (len == 0)
	{
		/*
		 * Nothing changed.
		 */
		return(PyLong_FromUnsignedLong(0));
	}

	ws = (struct wirestate *) self;

	if (ws->final_view)
	{
		PyErr_SetString(PyExc_RuntimeError, "wire state has been terminated");
		return(NULL);
	}

	remaining_bytes = ws->remaining_bytes;
	continuation = ws->continuation;

	if (continuation >= 0)
	{
		short sf_len = continuation, added;
		/*
		 * Continuation of message header.
		 */
		added = 4 - sf_len;
		/*
		 * If the buffer's length does not provide, limit to len.
		 */
		if (len < added)
			added = len;

		Py_MEMCPY(size_fragment, ws->size_fragment, 4);
		Py_MEMCPY(size_fragment + sf_len, buf, added);

		continuation = continuation + added;
		if (continuation == 4)
		{
			/*
			 * Completed the size part of the header.
			 */
			Py_MEMCPY(&remaining_bytes, size_fragment, 4);
			remaining_bytes = (local_ntohl((int32_t) remaining_bytes));
			if (remaining_bytes < 4)
				goto invalid_message_header;

			remaining_bytes = remaining_bytes - sf_len;
			if (remaining_bytes == 0)
				++nmessages;
			continuation = -1;
		}
		else
		{
			/*
			 * Consumed more of the header, but more is still needed.
			 * Jump past the main loop.
			 */
			goto return_nmessages;
		}
	}

	do
	{
		if (remaining_bytes > 0)
		{
			position = position + remaining_bytes;
			if (position > len)
			{
				remaining_bytes = position - len;
				position = len;
			}
			else
			{
				remaining_bytes = 0;
				++nmessages;
			}
		}

		/*
		 * Done with view.
		 */
		if (position >= len)
			break;

		/*
		 * Validate message type.
		 */
		msgtype = *(buf + position);
		if (CONDITION(msgtype))
		{
			final_view = PySequence_GetSlice(view, position, len);
			break;
		}

		/*
		 * Have enough for a complete header?
		 */
		if (len - position < 5)
		{
			/*
			 * Start a continuation. Message type has been verified.
			 */
			continuation = (len - position) - 1;
			Py_MEMCPY(size_fragment, buf + position + 1, (Py_ssize_t) continuation);
			break;
		}

		/*
		 * +1 to include the message type.
		 */
		Py_MEMCPY(&remaining_bytes, buf + position + 1, 4);
		remaining_bytes = local_ntohl((int32_t) remaining_bytes) + 1;
		if (remaining_bytes < 5)
			goto invalid_message_header;
	} while(1);

return_nmessages:
	rob = PyLong_FromUnsignedLong(nmessages);
	if (rob == NULL)
	{
		Py_XDECREF(final_view);
		return(NULL);
	}

	/* Commit new state */
	ws->remaining_bytes = remaining_bytes;
	ws->final_view = final_view;
	ws->continuation = continuation;
	Py_MEMCPY(ws->size_fragment, size_fragment, 4);
	return(rob);

invalid_message_header:
	PyErr_SetString(PyExc_ValueError, "message header contained an invalid size");
	return(NULL);
}

static PyMethodDef ws_methods[] = {
	{"update", ws_update, METH_O,
		PyDoc_STR("update the state of the wire using the given buffer object"),},
	{NULL}
};

PyObject *
ws_size_fragment(PyObject *self, void *closure)
{
	struct wirestate *ws;
	ws = (struct wirestate *) self;

	return(PyBytes_FromStringAndSize(ws->size_fragment,
		ws->continuation <= 0 ? 0 : ws->continuation));
}

PyObject *
ws_remaining_bytes(PyObject *self, void *closure)
{
	struct wirestate *ws;
	ws = (struct wirestate *) self;
	return(PyLong_FromLong(
		ws->continuation == -1 ? ws->remaining_bytes : -1
	));
}

PyObject *
ws_final_view(PyObject *self, void *closure)
{
	struct wirestate *ws;
	PyObject *rob;

	ws = (struct wirestate *) self;
	rob = ws->final_view ? ws->final_view : Py_None;

	Py_INCREF(rob);
	return(rob);
}

static PyGetSetDef ws_getset[] = {
	{"size_fragment", ws_size_fragment, NULL,
		PyDoc_STR("The data acculumated for the continuation."), NULL,},
	{"remaining_bytes", ws_remaining_bytes, NULL,
		PyDoc_STR("Number bytes necessary to complete the current message."), NULL,},
	{"final_view", ws_final_view, NULL,
		PyDoc_STR("A memoryview of the data that triggered the CONDITION()."), NULL,},
	{NULL}
};

PyTypeObject WireState_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"postgresql.port.optimized.WireState",	/* tp_name */
	sizeof(struct wirestate),				/* tp_basicsize */
	0,												/* tp_itemsize */
	ws_dealloc,									/* tp_dealloc */
	NULL,											/* tp_print */
	NULL,											/* tp_getattr */
	NULL,											/* tp_setattr */
	NULL,											/* tp_compare */
	NULL,											/* tp_repr */
	NULL,											/* tp_as_number */
	NULL,											/* tp_as_sequence */
	NULL,											/* tp_as_mapping */
	NULL,											/* tp_hash */
	NULL,											/* tp_call */
	NULL,											/* tp_str */
	NULL,											/* tp_getattro */
	NULL,											/* tp_setattro */
	NULL,											/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT,						/* tp_flags */
	PyDoc_STR("Track the state of the wire."),
		/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	ws_methods,									/* tp_methods */
	NULL,											/* tp_members */
	ws_getset,									/* tp_getset */
	NULL,											/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	ws_new,										/* tp_new */
	NULL,											/* tp_free */
};
/*
 * vim: ts=3:sw=3:noet:
 */
