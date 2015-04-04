/*
 * .port.optimized - .protocol.element3 optimizations
 */
#define include_element3_functions \
	mFUNC(cat_messages, METH_O, "cat the serialized form of the messages in the given list") \
	mFUNC(parse_tuple_message, METH_O, "parse the given tuple data into a tuple of raw data") \
	mFUNC(pack_tuple_data, METH_O, "serialize the give tuple message[tuple of bytes()]") \
	mFUNC(consume_tuple_messages, METH_O, "create a list of parsed tuple data tuples") \

/*
 * Given a tuple of bytes and None objects, join them into a
 * a single bytes object with sizes.
 */
static PyObject *
_pack_tuple_data(PyObject *tup)
{
	PyObject *rob;
	Py_ssize_t natts;
	Py_ssize_t catt;

	char *buf = NULL;
	char *bufpos = NULL;
	Py_ssize_t bufsize = 0;

	if (!PyTuple_Check(tup))
	{
		PyErr_Format(
			PyExc_TypeError,
			"pack_tuple_data requires a tuple, given %s",
			PyObject_TypeName(tup)
		);
		return(NULL);
	}
	natts = PyTuple_GET_SIZE(tup);
	if (natts == 0)
		return(PyBytes_FromString(""));

	/* discover buffer size and valid att types */
	for (catt = 0; catt < natts; ++catt)
	{
		PyObject *ob;
		ob = PyTuple_GET_ITEM(tup, catt);

		if (ob == Py_None)
		{
			bufsize = bufsize + 4;
		}
		else if (PyBytes_CheckExact(ob))
		{
			bufsize = bufsize + PyBytes_GET_SIZE(ob) + 4;
		}
		else
		{
			PyErr_Format(
				PyExc_TypeError,
				"cannot serialize attribute %d, expected bytes() or None, got %s",
				(int) catt, PyObject_TypeName(ob)
			);
			return(NULL);
		}
	}

	buf = malloc(bufsize);
	if (buf == NULL)
	{
		PyErr_Format(
			PyExc_MemoryError,
			"failed to allocate %d bytes of memory for packing tuple data",
			bufsize
		);
		return(NULL);
	}
	bufpos = buf;

	for (catt = 0; catt < natts; ++catt)
	{
		PyObject *ob;
		ob = PyTuple_GET_ITEM(tup, catt);
		if (ob == Py_None)
		{
			uint32_t attsize = 0xFFFFFFFFL; /* Indicates NULL */
			Py_MEMCPY(bufpos, &attsize, 4);
			bufpos = bufpos + 4;
		}
		else
		{
			Py_ssize_t size = PyBytes_GET_SIZE(ob);
			uint32_t msg_size;
			if (size > 0xFFFFFFFE)
			{
				PyErr_Format(PyExc_OverflowError,
					"data size of %d is greater than attribute capacity",
					catt
				);
			}
			msg_size = local_ntohl((uint32_t) size);
			Py_MEMCPY(bufpos, &msg_size, 4);
			bufpos = bufpos + 4;
			Py_MEMCPY(bufpos, PyBytes_AS_STRING(ob), PyBytes_GET_SIZE(ob));
			bufpos = bufpos + PyBytes_GET_SIZE(ob);
		}
	}

	rob = PyBytes_FromStringAndSize(buf, bufsize);
	free(buf);
	return(rob);
}

/*
 * dst must be of PyTuple_Type with at least natts items slots.
 */
static int
_unpack_tuple_data(PyObject *dst, register uint16_t natts, register const char *data, Py_ssize_t data_len)
{
	static const unsigned char null_sequence[4] = {0xFF, 0xFF, 0xFF, 0xFF};
	register PyObject *ob;
	register uint16_t cnatt = 0;
	register uint32_t attsize;
	register const char *next;
	register const char *eod = data + data_len;
	char attsize_buf[4];

	while (cnatt < natts)
	{
		/*
		 * Need enough data for the attribute size.
		 */
		next = data + 4;
		if (next > eod)
		{
			PyErr_Format(PyExc_ValueError,
				"not enough data available for attribute %d's size header: "
				"needed %d bytes, but only %lu remain at position %lu",
				cnatt, 4, eod - data, data_len - (eod - data)
			);
			return(-1);
		}

		Py_MEMCPY(attsize_buf, data, 4);
		data = next;
		if ((*((uint32_t *) attsize_buf)) == (*((uint32_t *) null_sequence)))
		{
			/*
			 * NULL.
			 */
			Py_INCREF(Py_None);
			PyTuple_SET_ITEM(dst, cnatt, Py_None);
		}
		else
		{
			attsize = local_ntohl(*((uint32_t *) attsize_buf));

			next = data + attsize;
			if (next > eod || next < data)
			{
				/*
				 * Increment caused wrap...
				 */
				PyErr_Format(PyExc_ValueError,
					"attribute %d has invalid size %lu",
					cnatt, attsize
				);
				return(-1);
			}

			ob = PyBytes_FromStringAndSize(data, attsize);
			if (ob == NULL)
			{
				/*
				 * Probably an OOM error.
				 */
				return(-1);
			}
			PyTuple_SET_ITEM(dst, cnatt, ob);
			data = next;
		}

		cnatt++;
	}

	if (data != eod)
	{
		PyErr_Format(PyExc_ValueError,
			"invalid tuple(D) message, %lu remaining "
			"bytes after processing %d attributes",
			(unsigned long) (eod - data), cnatt
		);
		return(-1);
	}

	return(0);
}

static PyObject *
parse_tuple_message(PyObject *self, PyObject *arg)
{
	PyObject *rob;
	const char *data;
	Py_ssize_t dlen = 0;
	uint16_t natts = 0;

	if (PyObject_AsReadBuffer(arg, (const void **) &data, &dlen))
		return(NULL);

	if (dlen < 2)
	{
		PyErr_Format(PyExc_ValueError,
			"invalid tuple message: %d bytes is too small", dlen);
		return(NULL);
	}
	Py_MEMCPY(&natts, data, 2);
	natts = local_ntohs(natts);

	rob = PyTuple_New(natts);
	if (rob == NULL)
		return(NULL);

	if (_unpack_tuple_data(rob, natts, data+2, dlen-2) < 0)
	{
		Py_DECREF(rob);
		return(NULL);
	}

	return(rob);
}

static PyObject *
consume_tuple_messages(PyObject *self, PyObject *list)
{
	Py_ssize_t i;
	PyObject *rob; /* builtins.list */

	if (!PyTuple_Check(list))
	{
		PyErr_SetString(PyExc_TypeError,
			"consume_tuple_messages requires a tuple");
		return(NULL);
	}
	rob = PyList_New(PyTuple_GET_SIZE(list));
	if (rob == NULL)
		return(NULL);

	for (i = 0; i < PyTuple_GET_SIZE(list); ++i)
	{
		register PyObject *data;
		PyObject *msg, *typ, *ptm;

		msg = PyTuple_GET_ITEM(list, i);
		if (!PyTuple_CheckExact(msg) || PyTuple_GET_SIZE(msg) != 2)
		{
			Py_DECREF(rob);
			PyErr_SetString(PyExc_TypeError,
				"consume_tuple_messages requires tuples items to be tuples (pairs)");
			return(NULL);
		}

		typ = PyTuple_GET_ITEM(msg, 0);
		if (!PyBytes_CheckExact(typ) || PyBytes_GET_SIZE(typ) != 1)
		{
			Py_DECREF(rob);
			PyErr_SetString(PyExc_TypeError,
				"consume_tuple_messages requires pairs to consist of bytes");
			return(NULL);
		}

		/*
		 * End of tuple messages.
		 */
		if (*(PyBytes_AS_STRING(typ)) != 'D')
			break;

		data = PyTuple_GET_ITEM(msg, 1);
		ptm = parse_tuple_message(NULL, data);
		if (ptm == NULL)
		{
			Py_DECREF(rob);
			return(NULL);
		}
		PyList_SET_ITEM(rob, i, ptm);
	}

	if (i < PyTuple_GET_SIZE(list))
	{
		PyObject *newrob;
		newrob = PyList_GetSlice(rob, 0, i);
		Py_DECREF(rob);
		rob = newrob;
	}

	return(rob);
}

static PyObject *
pack_tuple_data(PyObject *self, PyObject *tup)
{
	return(_pack_tuple_data(tup));
}

/*
 * Check for overflow before incrementing the buffer size for cat_messages.
 */
#define INCSIZET(XVAR, AMT) do { \
	size_t _amt_ = AMT; \
	size_t _newsize_ = XVAR + _amt_; \
	if (_newsize_ >= XVAR) XVAR = _newsize_; else { \
		PyErr_Format(PyExc_OverflowError, \
			"buffer size overflowed, was %zd bytes, but could not add %d more", XVAR, _amt_); \
		goto fail; } \
} while(0)

#define INCMSGSIZE(XVAR, AMT) do { \
	uint32_t _amt_ = AMT; \
	uint32_t _newsize_ = XVAR + _amt_; \
	if (_newsize_ >= XVAR) XVAR = _newsize_; else { \
		PyErr_Format(PyExc_OverflowError, \
			"message size too large, was %u bytes, but could not add %u more", XVAR, _amt_); \
		goto fail; } \
} while(0)

/*
 * cat_messages - cat the serialized form of the messages in the given list
 *
 * This offers a fast way to construct the final bytes() object to be sent to
 * the wire. It avoids re-creating bytes() objects by calculating the serialized
 * size of contiguous, homogenous messages, allocating or extending the buffer
 * to accommodate for the needed size, and finally, copying the data into the
 * newly available space.
 */
static PyObject *
cat_messages(PyObject *self, PyObject *messages_in)
{
	const static char null_attribute[4] = {0xff,0xff,0xff,0xff};
	PyObject *msgs = NULL;
	Py_ssize_t nmsgs = 0;
	Py_ssize_t cmsg = 0;

	/*
	 * Buffer holding the messages' serialized form.
	 */
	char *buf = NULL;
	char *nbuf = NULL;
	size_t bufsize = 0;
	size_t bufpos = 0;

	/*
	 * Get a List object for faster rescanning when dealing with copy data.
	 */
	msgs = PyObject_CallFunctionObjArgs((PyObject *) &PyList_Type, messages_in, NULL);
	if (msgs == NULL)
		return(NULL);

	nmsgs = PyList_GET_SIZE(msgs);

	while (cmsg < nmsgs)
	{
		PyObject *ob;
		ob = PyList_GET_ITEM(msgs, cmsg);

		/*
		 * Choose the path, lots of copy data or more singles to serialize?
		 */
		if (PyBytes_CheckExact(ob))
		{
			Py_ssize_t eofc = cmsg;
			size_t xsize = 0;
			/* find the last of the copy data (eofc) */
			do
			{
				++eofc;
				/* increase in size to allocate for the adjacent copy messages */
				INCSIZET(xsize, PyBytes_GET_SIZE(ob));
				if (eofc >= nmsgs)
					break; /* end of messages in the list? */

				/* Grab the next message. */
				ob = PyList_GET_ITEM(msgs, eofc);
			} while(PyBytes_CheckExact(ob));

			/*
			 * Either the end of the list or `ob` is not a data object meaning
			 * that it's the end of the copy data.
			 */

			/* realloc the buf for the new copy data */
			INCSIZET(xsize, (5 * (eofc - cmsg)));
			INCSIZET(bufsize, xsize);
			nbuf = realloc(buf, bufsize);
			if (nbuf == NULL)
			{
				PyErr_Format(
					PyExc_MemoryError,
					"failed to allocate %lu bytes of memory for out-going messages",
					(unsigned long) bufsize
				);
				goto fail;
			}
			else
			{
				buf = nbuf;
				nbuf = NULL;
			}

			/*
			 * Make the final pass through the copy lines memcpy'ing the data from
			 * the bytes() objects.
			 */
			while (cmsg < eofc)
			{
				uint32_t msg_length = 0;
				char *localbuf = buf + bufpos + 1;
				buf[bufpos] = 'd'; /* COPY data message type */

				ob = PyList_GET_ITEM(msgs, cmsg);
				INCMSGSIZE(msg_length, (uint32_t) PyBytes_GET_SIZE(ob) + 4);

				INCSIZET(bufpos, 1 + msg_length);
				msg_length = local_ntohl(msg_length);
				Py_MEMCPY(localbuf, &msg_length, 4);
				Py_MEMCPY(localbuf + 4, PyBytes_AS_STRING(ob), PyBytes_GET_SIZE(ob));
				++cmsg;
			}
		}
		else if (PyTuple_CheckExact(ob))
		{
			/*
			 * Handle 'D' tuple data from a raw Python tuple.
			 */
			Py_ssize_t eofc = cmsg;
			size_t xsize = 0;

			/* find the last of the tuple data (eofc) */
			do
			{
				Py_ssize_t current_item, nitems;

				nitems = PyTuple_GET_SIZE(ob);
				if (nitems > 0xFFFF)
				{
					PyErr_SetString(PyExc_OverflowError,
						"too many attributes in tuple message");
					goto fail;
				}

				/*
				 * The items take *at least* 4 bytes each.
				 * (The attribute count is considered later)
				 */
				INCSIZET(xsize, (nitems * 4));

				for (current_item = 0; current_item < nitems; ++current_item)
				{
					PyObject *att = PyTuple_GET_ITEM(ob, current_item);

					/*
					 * Attributes *must* be bytes() or None.
					 */
					if (PyBytes_CheckExact(att))
						INCSIZET(xsize, PyBytes_GET_SIZE(att));
					else if (att != Py_None)
					{
						PyErr_Format(PyExc_TypeError,
							"cannot serialize tuple message attribute of type '%s'",
							Py_TYPE(att)->tp_name);
						goto fail;
					}
					/*
					 * else it's Py_None and the size will be included later.
					 */
				}

				++eofc;
				if (eofc >= nmsgs)
					break; /* end of messages in the list? */

				/* Grab the next message. */
				ob = PyList_GET_ITEM(msgs, eofc);
			} while(PyTuple_CheckExact(ob));

			/*
			 * Either the end of the list or `ob` is not a data object meaning
			 * that it's the end of the copy data.
			 */

			/*
			 * realloc the buf for the new tuple data
			 *
			 * Each D message consumes at least 1 + 4 + 2 bytes:
			 *  1 for the message type
			 *  4 for the message size
			 *  2 for the attribute count
			 */
			INCSIZET(xsize, (7 * (eofc - cmsg)));
			INCSIZET(bufsize, xsize);
			nbuf = realloc(buf, bufsize);
			if (nbuf == NULL)
			{
				PyErr_Format(
					PyExc_MemoryError,
					"failed to allocate %zd bytes of memory for out-going messages",
					bufsize
				);
				goto fail;
			}
			else
			{
				buf = nbuf;
				nbuf = NULL;
			}

			/*
			 * Make the final pass through the tuple data memcpy'ing the data from
			 * the bytes() objects.
			 *
			 * No type checks are done here as they should have been done while
			 * gathering the sizes for the realloc().
			 */
			while (cmsg < eofc)
			{
				Py_ssize_t current_item, nitems;
				uint32_t msg_length, out_msg_len;
				uint16_t natts;
				char *localbuf = (buf + bufpos) + 5; /* skipping the header for now */
				buf[bufpos] = 'D'; /* Tuple data message type */

				ob = PyList_GET_ITEM(msgs, cmsg);
				nitems = PyTuple_GET_SIZE(ob);

				/*
				 * 4 bytes for the message length,
				 * 2 bytes for the attribute count and
				 * 4 bytes for each item in 'ob'.
				 */
				msg_length = 4 + 2 + (nitems * 4);

				/*
				 * Set number of attributes.
				 */
				natts = local_ntohs((uint16_t) nitems);
				Py_MEMCPY(localbuf, &natts, 2);
				localbuf = localbuf + 2;

				for (current_item = 0; current_item < nitems; ++current_item)
				{
					PyObject *att = PyTuple_GET_ITEM(ob, current_item);

					if (att == Py_None)
					{
						Py_MEMCPY(localbuf, &null_attribute, 4);
						localbuf = localbuf + 4;
					}
					else
					{
						Py_ssize_t attsize = PyBytes_GET_SIZE(att);
						uint32_t n_attsize;

						n_attsize = local_ntohl((uint32_t) attsize);

						Py_MEMCPY(localbuf, &n_attsize, 4);
						localbuf = localbuf + 4;
						Py_MEMCPY(localbuf, PyBytes_AS_STRING(att), attsize);
						localbuf = localbuf + attsize;

						INCSIZET(msg_length, attsize);
					}
				}

				/*
				 * Summed up the message size while copying the attributes.
				 */
				out_msg_len = local_ntohl(msg_length);
				Py_MEMCPY(buf + bufpos + 1, &out_msg_len, 4);

				/*
				 * Filled in the data while summing the message size, so
				 * adjust the buffer position for the next message.
				 */
				INCSIZET(bufpos, 1 + msg_length);
				++cmsg;
			}
		}
		else
		{
			PyObject *serialized;
			PyObject *msg_type;
			int msg_type_size;
			uint32_t msg_length;

			/*
			 * Call the serialize() method on the element object.
			 * Do this instead of the normal bytes() method to avoid
			 * the type and size packing overhead.
			 */
			serialized = PyObject_CallMethodObjArgs(ob, serialize_strob, NULL);
			if (serialized == NULL)
				goto fail;
			if (!PyBytes_CheckExact(serialized))
			{
				PyErr_Format(
					PyExc_TypeError,
					"%s.serialize() returned object of type %s, expected bytes",
					PyObject_TypeName(ob),
					PyObject_TypeName(serialized)
				);
				goto fail;
			}

			msg_type = PyObject_GetAttr(ob, msgtype_strob);
			if (msg_type == NULL)
			{
				Py_DECREF(serialized);
				goto fail;
			}
			if (!PyBytes_CheckExact(msg_type))
			{
				Py_DECREF(serialized);
				Py_DECREF(msg_type);
				PyErr_Format(
					PyExc_TypeError,
					"message's 'type' attribute was %s, expected bytes",
					PyObject_TypeName(ob)
				);
				goto fail;
			}
			/*
			 * Some elements have empty message types--Startup for instance.
			 * It is important to get the actual size rather than assuming one.
			 */
			msg_type_size = PyBytes_GET_SIZE(msg_type);

			/* realloc the buf for the new copy data */
			INCSIZET(bufsize, 4 + msg_type_size);
			INCSIZET(bufsize, PyBytes_GET_SIZE(serialized));
			nbuf = realloc(buf, bufsize);
			if (nbuf == NULL)
			{
				Py_DECREF(serialized);
				Py_DECREF(msg_type);
				PyErr_Format(
					PyExc_MemoryError,
					"failed to allocate %d bytes of memory for out-going messages",
					bufsize
				);
				goto fail;
			}
			else
			{
				buf = nbuf;
				nbuf = NULL;
			}

			/*
			 * All necessary information acquired, so fill in the message's data.
			 */
			buf[bufpos] = *(PyBytes_AS_STRING(msg_type));
			msg_length = PyBytes_GET_SIZE(serialized);
			INCMSGSIZE(msg_length, 4);
			msg_length = local_ntohl(msg_length);
			Py_MEMCPY(buf + bufpos + msg_type_size, &msg_length, 4);
			Py_MEMCPY(
				buf + bufpos + 4 + msg_type_size,
				PyBytes_AS_STRING(serialized),
				PyBytes_GET_SIZE(serialized)
			);
			bufpos = bufsize;

			Py_DECREF(serialized);
			Py_DECREF(msg_type);
			++cmsg;
		}
	}

	Py_DECREF(msgs);
	if (buf == NULL)
		/* no messages, no data */
		return(PyBytes_FromString(""));
	else
	{
		PyObject *rob;
		rob = PyBytes_FromStringAndSize(buf, bufsize);
		free(buf);

		return(rob);
	}
fail:
	/* pyerr is expected to be set */
	Py_DECREF(msgs);
	if (buf != NULL)
		free(buf);
	return(NULL);
}
