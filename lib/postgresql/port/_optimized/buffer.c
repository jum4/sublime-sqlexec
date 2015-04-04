/*
 * .port.optimized.pq_message_buffer - PQ message stream
 */
/*
 * PQ messages normally take the form {type, (size), data}
 */
#define include_buffer_types \
	mTYPE(pq_message_stream)

struct p_list
{
	PyObject *data; /* PyBytes pushed onto the buffer */
	struct p_list *next;
};

struct p_place
{
	struct p_list *list;
	uint32_t offset;
};

struct p_buffer
{
	PyObject_HEAD

	struct p_place position;
	struct p_list *last; /* for quick appends */
};

/*
 * Free the list until the given stop
 */
static void
pl_truncate(struct p_list *pl, struct p_list *stop)
{
	while (pl != stop)
	{
		struct p_list *next = pl->next;
		Py_DECREF(pl->data);
		free(pl);
		pl = next;
	}
}

/*
 * Reset the buffer
 */
static void
pb_truncate(struct p_buffer *pb)
{
	struct p_list *pl = pb->position.list;

	pb->position.offset = 0;
	pb->position.list = NULL;
	pb->last = NULL;

	pl_truncate(pl, NULL);
}

/*
 * p_truncate - truncate the buffer
 */
static PyObject *
p_truncate(PyObject *self)
{
	pb_truncate((struct p_buffer *) self);
	Py_INCREF(Py_None);
	return(Py_None);
}


static void
p_dealloc(PyObject *self)
{
	struct p_buffer *pb = ((struct p_buffer *) self);
	pb_truncate(pb);
	self->ob_type->tp_free(self);
}

static PyObject *
p_new(PyTypeObject *subtype, PyObject *args, PyObject *kw)
{
	static char *kwlist[] = {NULL};
	struct p_buffer *pb;
	PyObject *rob;

	if (!PyArg_ParseTupleAndKeywords(args, kw, "", kwlist))
		return(NULL);

	rob = subtype->tp_alloc(subtype, 0);
	pb = ((struct p_buffer *) rob);
	pb->last = pb->position.list = NULL;
	pb->position.offset = 0;
	return(rob);
}

/*
 * p_at_least - whether the position has at least given number of bytes.
 */
static char
p_at_least(struct p_place *p, uint32_t amount)
{
	int32_t current = 0;
	struct p_list *pl;

	pl = p->list;
	if (pl)
		current += PyBytes_GET_SIZE(pl->data) - p->offset;

	if (current >= amount)
		return((char) 1);

	if (pl)
	{
		for (pl = pl->next; pl != NULL; pl = pl->next)
		{
			current += PyBytes_GET_SIZE(pl->data);
			if (current >= amount)
				return((char) 1);
		}
	}

	return((char) 0);
}

static uint32_t
p_seek(struct p_place *p, uint32_t amount)
{
	uint32_t amount_left = amount;
	Py_ssize_t chunk_size;

	/* Can't seek after the end. */
	if (!p->list || p->offset == PyBytes_GET_SIZE(p->list->data))
		return(0);

	chunk_size = PyBytes_GET_SIZE(p->list->data) - p->offset;

	while (amount_left > 0)
	{
		/*
		 * The current list item has the position.
		 * Set the offset and break out.
		 */
		if (amount_left < chunk_size)
		{
			p->offset += amount_left;
			amount_left = 0;
			break;
		}

		amount_left -= chunk_size;
		p->list = p->list->next;
		p->offset = 0;
		if (p->list == NULL)
			break;

		chunk_size = PyBytes_GET_SIZE(p->list->data);
	}

	return(amount - amount_left);
}

static uint32_t
p_memcpy(char *dst, struct p_place *p, uint32_t amount)
{
	struct p_list *pl = p->list;
	uint32_t offset = p->offset;
	uint32_t amount_left = amount;
	char *src;
	Py_ssize_t chunk_size;

	/* Nothing to read */
	if (pl == NULL)
		return(0);

	src = (PyBytes_AS_STRING(pl->data) + offset);
	chunk_size = PyBytes_GET_SIZE(pl->data) - offset;

	while (amount_left > 0)
	{
		uint32_t this_read =
			chunk_size < amount_left ? chunk_size : amount_left;

		memcpy(dst, src, this_read);
		dst = dst + this_read;
		amount_left = amount_left - this_read;

		pl = pl->next;
		if (pl == NULL)
			break;

		src = PyBytes_AS_STRING(pl->data);
		chunk_size = PyBytes_GET_SIZE(pl->data);
	}

	return(amount - amount_left);
}

static Py_ssize_t
p_length(PyObject *self)
{
	char header[5];
	long msg_count = 0;
	uint32_t msg_length;
	uint32_t copy_amount = 0;
	struct p_buffer *pb;
	struct p_place p;

	pb = ((struct p_buffer *) self);
	p.list = pb->position.list;
	p.offset = pb->position.offset;

	while (p.list != NULL)
	{
		copy_amount = p_memcpy(header, &p, 5);
		if (copy_amount < 5)
			break;
		p_seek(&p, copy_amount);

		memcpy(&msg_length, header + 1, 4);
		msg_length = local_ntohl(msg_length);
		if (msg_length < 4)
		{
			PyErr_Format(PyExc_ValueError,
				"invalid message size '%d'", msg_length);
			return(-1);
		}
		msg_length -= 4;

		if (p_seek(&p, msg_length) < msg_length)
			break;

		++msg_count;
	}

	return(msg_count);
}

static PySequenceMethods pq_ms_as_sequence = {
	(lenfunc) p_length, 0
};


/*
 * Build a tuple from the given place.
 */
static PyObject *
p_build_tuple(struct p_place *p)
{
	char header[5];
	uint32_t msg_length;
	PyObject *tuple;
	PyObject *mt, *md;

	char *body = NULL;
	uint32_t copy_amount = 0;

	copy_amount = p_memcpy(header, p, 5);
	if (copy_amount < 5)
		return(NULL);
	p_seek(p, copy_amount);

	memcpy(&msg_length, header + 1, 4);
	msg_length = local_ntohl(msg_length);
	if (msg_length < 4)
	{
		PyErr_Format(PyExc_ValueError,
			"invalid message size '%d'", msg_length);
		return(NULL);
	}
	msg_length -= 4;

	if (!p_at_least(p, msg_length))
		return(NULL);

	/*
	 * Copy out the message body if we need to.
	 */
	if (msg_length > 0)
	{
		body = malloc(msg_length);
		if (body == NULL)
		{
			PyErr_SetString(PyExc_MemoryError,
				"could not allocate memory for message data");
			return(NULL);
		}
		copy_amount = p_memcpy(body, p, msg_length);

		if (copy_amount != msg_length)
		{
			free(body);
			return(NULL);
		}

		p_seek(p, copy_amount);
	}

	mt = PyTuple_GET_ITEM(message_types, (int) header[0]);
	if (mt == NULL)
	{
		/*
		 * With message_types, this is nearly a can't happen.
		 */
		if (body != NULL) free(body);
		return(NULL);
	}
	Py_INCREF(mt);

	md = PyBytes_FromStringAndSize(body, (Py_ssize_t) msg_length);
	if (body != NULL)
		free(body);
	if (md == NULL)
	{
		Py_DECREF(mt);
		return(NULL);
	}


	tuple = PyTuple_New(2);
	if (tuple == NULL)
	{
		Py_DECREF(mt);
		Py_DECREF(md);
	}
	else
	{
		PyTuple_SET_ITEM(tuple, 0, mt);
		PyTuple_SET_ITEM(tuple, 1, md);
	}

	return(tuple);
}

static PyObject *
p_write(PyObject *self, PyObject *data)
{
	struct p_buffer *pb;

	if (!PyBytes_Check(data))
	{
		PyErr_SetString(PyExc_TypeError,
			"pq buffer.write() method requires a bytes object");
		return(NULL);
	}
	pb = ((struct p_buffer *) self);

	if (PyBytes_GET_SIZE(data) > 0)
	{
		struct p_list *pl;

		pl = malloc(sizeof(struct p_list));
		if (pl == NULL)
		{
			PyErr_SetString(PyExc_MemoryError,
				"could not allocate memory for pq message stream data");
			return(NULL);
		}

		pl->data = data;
		Py_INCREF(data);
		pl->next = NULL;

		if (pb->last == NULL)
		{
			/*
			 * First and last.
			 */
			pb->position.list = pb->last = pl;
		}
		else
		{
			pb->last->next = pl;
			pb->last = pl;
		}
	}

	Py_INCREF(Py_None);
	return(Py_None);
}

static PyObject *
p_next(PyObject *self)
{
	struct p_buffer *pb = ((struct p_buffer *) self);
	struct p_place p;
	PyObject *rob;

	p.offset = pb->position.offset;
	p.list = pb->position.list;

	rob = p_build_tuple(&p);
	if (rob != NULL)
	{
		pl_truncate(pb->position.list, p.list);
		pb->position.list = p.list;
		pb->position.offset = p.offset;
		if (p.list == NULL)
			pb->last = NULL;
	}
	return(rob);
}

static PyObject *
p_read(PyObject *self, PyObject *args)
{
	int cur_msg, msg_count = -1, msg_in = 0;
	struct p_place p;
	struct p_buffer *pb;
	PyObject *rob = NULL;

	if (!PyArg_ParseTuple(args, "|i", &msg_count))
		return(NULL);

	pb = (struct p_buffer *) self;
	p.list = pb->position.list;
	p.offset = pb->position.offset;

	msg_in = p_length(self);
	msg_count = msg_count < msg_in && msg_count != -1 ? msg_count : msg_in;

	rob = PyTuple_New(msg_count);
	for (cur_msg = 0; cur_msg < msg_count; ++cur_msg)
	{
		PyObject *msg_tup = NULL;
		msg_tup = p_build_tuple(&p);
		if (msg_tup == NULL)
		{
			if (PyErr_Occurred())
			{
				Py_DECREF(rob);
				return(NULL);
			}
			break;
		}

		PyTuple_SET_ITEM(rob, cur_msg, msg_tup);
	}

	pl_truncate(pb->position.list, p.list);
	pb->position.list = p.list;
	pb->position.offset = p.offset;
	if (p.list == NULL)
		pb->last = NULL;

	return(rob);
}

static PyObject *
p_has_message(PyObject *self)
{
	char header[5];
	uint32_t msg_length;
	uint32_t copy_amount = 0;
	struct p_buffer *pb;
	struct p_place p;
	PyObject *rob;

	pb = ((struct p_buffer *) self);
	p.list = pb->position.list;
	p.offset = pb->position.offset;

	copy_amount = p_memcpy(header, &p, 5);
	if (copy_amount < 5)
	{
		Py_INCREF(Py_False);
		return(Py_False);
	}
	p_seek(&p, copy_amount);
	memcpy(&msg_length, header + 1, 4);

	msg_length = local_ntohl(msg_length);
	if (msg_length < 4)
	{
		PyErr_Format(PyExc_ValueError,
			"invalid message size '%d'", msg_length);
		return(NULL);
	}
	msg_length -= 4;

	rob = p_at_least(&p, msg_length) ? Py_True : Py_False;
	Py_INCREF(rob);
	return(rob);
}

static PyObject *
p_next_message(PyObject *self)
{
	struct p_buffer *pb = ((struct p_buffer *) self);
	struct p_place p;
	PyObject *rob;

	p.offset = pb->position.offset;
	p.list = pb->position.list;

	rob = p_build_tuple(&p);
	if (rob == NULL)
	{
		if (!PyErr_Occurred())
		{
			rob = Py_None;
			Py_INCREF(rob);
		}
	}
	else
	{
		pl_truncate(pb->position.list, p.list);
		pb->position.list = p.list;
		pb->position.offset = p.offset;
		if (p.list == NULL)
			pb->last = NULL;
	}

	return(rob);
}

/*
 * p_getvalue - get the unconsumed data in the buffer
 *
 * Normally used in conjunction with truncate to transfer
 * control of the wire to another state machine.
 */
static PyObject *
p_getvalue(PyObject *self)
{
	struct p_buffer *pb = ((struct p_buffer *) self);
	struct p_list *l;
	uint32_t initial_offset;
	PyObject *rob;

	/*
	 * Don't include data from already read() messages.
	 */
	initial_offset = pb->position.offset;

	l = pb->position.list;
	if (l == NULL)
	{
		/*
		 * Empty list.
		 */
		return(PyBytes_FromString(""));
	}

	/*
	 * Get the first chunk.
	 */
	rob = PyBytes_FromStringAndSize(
		(PyBytes_AS_STRING(l->data) + initial_offset),
		PyBytes_GET_SIZE(l->data) - initial_offset
	);
	if (rob == NULL)
		return(NULL);

	l = l->next;
	while (l != NULL)
	{
		PyBytes_Concat(&rob, l->data);
		if (rob == NULL)
			break;

		l = l->next;
	}

	return(rob);
}

static PyMethodDef p_methods[] = {
	{"write", p_write, METH_O,
		PyDoc_STR("write the string to the buffer"),},
	{"read", p_read, METH_VARARGS,
		PyDoc_STR("read the number of messages from the buffer")},
	{"truncate", (PyCFunction) p_truncate, METH_NOARGS,
		PyDoc_STR("remove the contents of the buffer"),},
	{"has_message", (PyCFunction) p_has_message, METH_NOARGS,
		PyDoc_STR("whether the buffer has a message ready"),},
	{"next_message", (PyCFunction) p_next_message, METH_NOARGS,
		PyDoc_STR("get and remove the next message--None if none."),},
	{"getvalue", (PyCFunction) p_getvalue, METH_NOARGS,
		PyDoc_STR("get the unprocessed data in the buffer")},
	{NULL}
};

PyTypeObject pq_message_stream_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"postgresql.port.optimized.pq_message_stream",	/* tp_name */
	sizeof(struct p_buffer),		/* tp_basicsize */
	0,										/* tp_itemsize */
	p_dealloc,							/* tp_dealloc */
	NULL,									/* tp_print */
	NULL,									/* tp_getattr */
	NULL,									/* tp_setattr */
	NULL,									/* tp_compare */
	NULL,									/* tp_repr */
	NULL,									/* tp_as_number */
	&pq_ms_as_sequence,				/* tp_as_sequence */
	NULL,									/* tp_as_mapping */
	NULL,									/* tp_hash */
	NULL,									/* tp_call */
	NULL,									/* tp_str */
	NULL,									/* tp_getattro */
	NULL,									/* tp_setattro */
	NULL,									/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT,			   /* tp_flags */
	PyDoc_STR(
		"Buffer data on write, return messages on read"
	),										/* tp_doc */
	NULL,									/* tp_traverse */
	NULL,									/* tp_clear */
	NULL,									/* tp_richcompare */
	0,										/* tp_weaklistoffset */
	NULL,									/* tp_iter */
	p_next,								/* tp_iternext */
	p_methods,							/* tp_methods */
	NULL,									/* tp_members */
	NULL,									/* tp_getset */
	NULL,									/* tp_base */
	NULL,									/* tp_dict */
	NULL,									/* tp_descr_get */
	NULL,									/* tp_descr_set */
	0,										/* tp_dictoffset */
	NULL,									/* tp_init */
	NULL,									/* tp_alloc */
	p_new,								/* tp_new */
	NULL,									/* tp_free */
};
/*
 * vim: ts=3:sw=3:noet:
 */
