##
# .exceptions - Exception hierarchy for PostgreSQL database ERRORs.
##
"""
PostgreSQL exceptions and warnings with associated state codes.

The primary entry points of this module is the `ErrorLookup` function and the
`WarningLookup` function. Given an SQL state code, they give back the most
appropriate Error or Warning subclass.

For more information on error codes see:
 http://www.postgresql.org/docs/current/static/errcodes-appendix.html

This module is executable via -m: python -m postgresql.exceptions.
It provides a convenient way to look up the exception object mapped to by the
given error code::

	$ python -m postgresql.exceptions XX000
	postgresql.exceptions.InternalError [XX000]

If the exact error code is not found, it will try to find the error class's
exception(The first two characters of the error code make up the class
identity)::

	$ python -m postgresql.exceptions XX400
	postgresql.exceptions.InternalError [XX000]

If that fails, it will return `postgresql.exceptions.Error`
"""
import sys
import os
from functools import partial
from operator import attrgetter
from .message import Message
from . import sys as pg_sys

PythonException = Exception
class Exception(Exception):
	'Base PostgreSQL exception class'
	pass

class LoadError(Exception):
	'Failed to load a library'

class Disconnection(Exception):
	'Exception identifying errors that result in disconnection'

class Warning(Message):
	code = '01000'
	_e_label = property(attrgetter('__class__.__name__'))

class DriverWarning(Warning):
	code = '01-00'
	source = 'CLIENT'
class IgnoredClientParameterWarning(DriverWarning):
	'Warn the user of a valid, but ignored parameter.'
	code = '01-CP'
class TypeConversionWarning(DriverWarning):
	'Report a potential issue with a conversion.'
	code = '01-TP'

class DeprecationWarning(Warning):
	code = '01P01'
class DynamicResultSetsReturnedWarning(Warning):
	code = '0100C'
class ImplicitZeroBitPaddingWarning(Warning):
	code = '01008'
class NullValueEliminatedInSetFunctionWarning(Warning):
	code = '01003'
class PrivilegeNotGrantedWarning(Warning):
	code = '01007'
class PrivilegeNotRevokedWarning(Warning):
	code = '01006'
class StringDataRightTruncationWarning(Warning):
	code = '01004'

class NoDataWarning(Warning):
	code = '02000'
class NoMoreSetsReturned(NoDataWarning):
	code = '02001'

class Error(Message, Exception):
	'A PostgreSQL Error'
	_e_label = 'ERROR'
	code = ''

	def __str__(self):
		'Call .sys.errformat(self)'
		return pg_sys.errformat(self)

	@property
	def fatal(self):
		f = self.details.get('severity')
		return None if f is None else f in ('PANIC', 'FATAL')

class DriverError(Error):
	"Errors originating in the driver's implementation."
	source = 'CLIENT'
	code = '--000'
class AuthenticationMethodError(DriverError, Disconnection):
	"""
	Server requested an authentication method that is not supported by the
	driver.
	"""
	code = '--AUT'
class InsecurityError(DriverError, Disconnection):
	"""
	Error signifying a secure channel to a server cannot be established.
	"""
	code = '--SEC'
class ConnectTimeoutError(DriverError, Disconnection):
	'Client was unable to esablish a connection in the given time'
	code = '--TOE'

class TypeIOError(DriverError):
	"""
	Driver failed to pack or unpack a tuple.
	"""
	code = '--TIO'
class ParameterError(TypeIOError):
	code = '--PIO'
class ColumnError(TypeIOError):
	code = '--CIO'
class CompositeError(TypeIOError):
	code = '--cIO'

class OperationError(DriverError):
	"""
	An invalid operation on an interface element.
	"""
	code = '--OPE'

class TransactionError(Error):
	pass

class SQLNotYetCompleteError(Error):
	code = '03000'

class ConnectionError(Error, Disconnection):
	code = '08000'
class ConnectionDoesNotExistError(ConnectionError):
	"""
	The connection is closed or was never connected.
	"""
	code = '08003'
class ConnectionFailureError(ConnectionError):
	'Raised when a connection is dropped'
	code = '08006'

class ClientCannotConnectError(ConnectionError):
	"""
	Client was unable to establish a connection to the server.
	"""
	code = '08001'

class ConnectionRejectionError(ConnectionError):
	code = '08004'
class TransactionResolutionUnknownError(ConnectionError):
	code = '08007'
class ProtocolError(ConnectionError):
	code = '08P01'

class TriggeredActionError(Error):
	code = '09000'

class FeatureError(Error):
	"Unsupported feature"
	code = '0A000'

class TransactionInitiationError(TransactionError):
	code = '0B000'

class LocatorError(Error):
	code = '0F000'
class LocatorSpecificationError(LocatorError):
	code = '0F001'

class GrantorError(Error):
	code = '0L000'
class GrantorOperationError(GrantorError):
	code = '0LP01'

class RoleSpecificationError(Error):
	code = '0P000'

class CaseNotFoundError(Error):
	code = '20000'

class CardinalityError(Error):
	"Wrong number of rows returned"
	code = '21000'

class TriggeredDataChangeViolation(Error):
	code = '27000'

class AuthenticationSpecificationError(Error, Disconnection):
	code = '28000'

class DPDSEError(Error):
	"Dependent Privilege Descriptors Still Exist"
	code = '2B000'
class DPDSEObjectError(DPDSEError):
	code = '2BP01'

class SREError(Error):
	"SQL Routine Exception"
	code = '2F000'
class FunctionExecutedNoReturnStatementError(SREError):
	code = '2F005'
class DataModificationProhibitedError(SREError):
	code = '2F002'
class StatementProhibitedError(SREError):
	code = '2F003'
class ReadingDataProhibitedError(SREError):
	code = '2F004'

class EREError(Error):
	"External Routine Exception"
	code = '38000'
class ContainingSQLNotPermittedError(EREError):
	code = '38001'
class ModifyingSQLDataNotPermittedError(EREError):
	code = '38002'
class ProhibitedSQLStatementError(EREError):
	code = '38003'
class ReadingSQLDataNotPermittedError(EREError):
	code = '38004'

class ERIEError(Error):
	"External Routine Invocation Exception"
	code = '39000'
class InvalidSQLState(ERIEError):
	code = '39001'
class NullValueNotAllowed(ERIEError):
	code = '39004'
class TriggerProtocolError(ERIEError):
	code = '39P01'
class SRFProtocolError(ERIEError):
	code = '39P02'

class TRError(TransactionError):
	"Transaction Rollback"
	code = '40000'
class DeadlockError(TRError):
	code = '40P01'
class IntegrityConstraintViolationError(TRError):
	code = '40002'
class SerializationError(TRError):
	code = '40001'
class StatementCompletionUnknownError(TRError):
	code = '40003'


class ITSError(TransactionError):
	"Invalid Transaction State"
	code = '25000'
class ActiveTransactionError(ITSError):
	code = '25001'
class BranchAlreadyActiveError(ITSError):
	code = '25002'
class BadAccessModeForBranchError(ITSError):
	code = '25003'
class BadIsolationForBranchError(ITSError):
	code = '25004'
class NoActiveTransactionForBranchError(ITSError):
	code = '25005'
class ReadOnlyTransactionError(ITSError):
	"Occurs when an alteration occurs in a read-only transaction."
	code = '25006'
class SchemaAndDataStatementsError(ITSError):
	"Mixed schema and data statements not allowed."
	code = '25007'
class InconsistentCursorIsolationError(ITSError):
	"The held cursor requires the same isolation."
	code = '25008'

class NoActiveTransactionError(ITSError):
	code = '25P01'
class InFailedTransactionError(ITSError):
	"Occurs when an action occurs in a failed transaction."
	code = '25P02'


class SavepointError(TransactionError):
	"Classification error designating errors that relate to savepoints."
	code = '3B000'
class InvalidSavepointSpecificationError(SavepointError):
	code = '3B001'

class TransactionTerminationError(TransactionError):
	code = '2D000'

class IRError(Error):
	"Insufficient Resource Error"
	code = '53000'
class MemoryError(IRError, MemoryError):
	code = '53200'
class DiskFullError(IRError):
	code = '53100'
class TooManyConnectionsError(IRError):
	code = '53300'

class PLEError(OverflowError):
	"Program Limit Exceeded"
	code = '54000'
class ComplexityOverflowError(PLEError):
	code = '54001'
class ColumnOverflowError(PLEError):
	code = '54011'
class ArgumentOverflowError(PLEError):
	code = '54023'

class ONIPSError(Error):
	"Object Not In Prerequisite State"
	code = '55000'
class ObjectInUseError(ONIPSError):
	code = '55006'
class ImmutableRuntimeParameterError(ONIPSError):
	code = '55P02'
class UnavailableLockError(ONIPSError):
	code = '55P03'


class SEARVError(Error):
	"Syntax Error or Access Rule Violation"
	code = '42000'

class SEARVNameError(SEARVError):
	code = '42602'
class NameTooLongError(SEARVError):
	code = '42622'
class ReservedNameError(SEARVError):
	code = '42939'

class ForeignKeyCreationError(SEARVError):
	code = '42830'

class InsufficientPrivilegeError(SEARVError):
	code = '42501'
class GroupingError(SEARVError):
	code = '42803'

class RecursionError(SEARVError):
	code = '42P19'
class WindowError(SEARVError):
	code = '42P20'

class SyntaxError(SEARVError):
	code = '42601'

class TypeError(SEARVError):
	pass
class CoercionError(TypeError):
	code = '42846'
class TypeMismatchError(TypeError):
	code = '42804'
class IndeterminateTypeError(TypeError):
	code = '42P18'
class WrongObjectTypeError(TypeError):
	code = '42809'

class UndefinedError(SEARVError):
	pass
class UndefinedColumnError(UndefinedError):
	code = '42703'
class UndefinedFunctionError(UndefinedError):
	code = '42883'
class UndefinedTableError(UndefinedError):
	code = '42P01'
class UndefinedParameterError(UndefinedError):
	code = '42P02'
class UndefinedObjectError(UndefinedError):
	code = '42704'

class DuplicateError(SEARVError):
	pass
class DuplicateColumnError(DuplicateError):
	code = '42701'
class DuplicateCursorError(DuplicateError):
	code = '42P03'
class DuplicateDatabaseError(DuplicateError):
	code = '42P04'
class DuplicateFunctionError(DuplicateError):
	code = '42723'
class DuplicatePreparedStatementError(DuplicateError):
	code = '42P05'
class DuplicateSchemaError(DuplicateError):
	code = '42P06'
class DuplicateTableError(DuplicateError):
	code = '42P07'
class DuplicateAliasError(DuplicateError):
	code = '42712'
class DuplicateObjectError(DuplicateError):
	code = '42710'

class AmbiguityError(SEARVError):
	pass
class AmbiguousColumnError(AmbiguityError):
	code = '42702'
class AmbiguousFunctionError(AmbiguityError):
	code = '42725'
class AmbiguousParameterError(AmbiguityError):
	code = '42P08'
class AmbiguousAliasError(AmbiguityError):
	code = '42P09'

class ColumnReferenceError(SEARVError):
	code = '42P10'

class DefinitionError(SEARVError):
	pass
class ColumnDefinitionError(DefinitionError):
	code = '42611'
class CursorDefinitionError(DefinitionError):
	code = '42P11'
class DatabaseDefinitionError(DefinitionError):
	code = '42P12'
class FunctionDefinitionError(DefinitionError):
	code = '42P13'
class PreparedStatementDefinitionError(DefinitionError):
	code = '42P14'
class SchemaDefinitionError(DefinitionError):
	code = '42P15'
class TableDefinitionError(DefinitionError):
	code = '42P16'
class ObjectDefinitionError(DefinitionError):
	code = '42P17'


class CursorStateError(Error):
	code = '24000'

class WithCheckOptionError(Error):
	code = '44000'

class NameError(Error):
	pass
class CatalogNameError(NameError):
	code = '3D000'
class CursorNameError(NameError):
	code = '34000'
class StatementNameError(NameError):
	code = '26000'
class SchemaNameError(NameError):
	code = '3F000'

class ICVError(Error):
	"Integrity Contraint Violation"
	code = '23000'
class RestrictError(ICVError):
	code = '23001'
class NotNullError(ICVError):
	code = '23502'
class ForeignKeyError(ICVError):
	code = '23503'
class UniqueError(ICVError):
	code = '23505'
class CheckError(ICVError):
	code = '23514'


class DataError(Error):
	code = '22000'

class StringRightTruncationError(DataError):
	code = '22001'
class StringDataLengthError(DataError):
	code = '22026'
class ZeroLengthString(DataError):
	code = '2200F'

class EncodingError(DataError):
	code = '22021'
class ArrayElementError(DataError):
	code = '2202E'
class SpecificTypeMismatch(DataError):
	code = '2200G'

class NullValueNotAllowedError(DataError):
	code = '22004'
class NullValueNoIndicatorParameter(DataError):
	code = '22002'

class ZeroDivisionError(DataError):
	code = '22012'
class FloatingPointError(DataError):
	code = '22P01'
class AssignmentError(DataError):
	code = '22005'
class IndicatorOverflowError(DataError):
	code = '22022'
class BadCopyError(DataError):
	code = '22P04'

class TextRepresentationError(DataError):
	code = '22P02'
class BinaryRepresentationError(DataError):
	code = '22P03'
class UntranslatableCharacterError(DataError):
	code = '22P05'
class NonstandardUseOfEscapeCharacterError(DataError):
	code = '22P06'

class NotXMLError(DataError):
	code = '2200L'
class XMLDocumentError(DataError):
	code = '2200M'
class XMLContentError(DataError):
	code = '2200N'
class XMLCommentError(DataError):
	code = '2200S'
class XMLProcessingInstructionError(DataError):
	code = '2200T'

class DateTimeFormatError(DataError):
	code = '22007'
class TimeZoneDisplacementValueError(DataError):
	code = '22009'
class DateTimeFieldOverflowError(DataError):
	code = '22008'
class IntervalFieldOverflowError(DataError):
	code = '22015'

class LogArgumentError(DataError):
	code = '2201E'
class PowerFunctionArgumentError(DataError):
	code = '2201F'
class WidthBucketFunctionArgumentError(DataError):
	code = '2201G'
class CastCharacterValueError(DataError):
	code = '22018'

class EscapeCharacterError(DataError):
	code = '22019'
class EscapeOctetError(DataError):
	code = '2200D'
class EscapeSequenceError(DataError):
	code = '22025'
class EscapeCharacterConflictError(DataError):
	code = '2200B'
class EscapeCharacterError(DataError):
	"Invalid escape character"
	code = '2200C'

class SubstringError(DataError):
	code = '22011'
class TrimError(DataError):
	code = '22027'
class IndicatorParameterValueError(DataError):
	code = '22010'

class LimitValueError(DataError):
	code = '2201W'
	pg_code = '22020'
class OffsetValueError(DataError):
	code = '2201X'

class ParameterValueError(DataError):
	code = '22023'
class RegularExpressionError(DataError):
	code = '2201B'
class NumericRangeError(DataError):
	code = '22003'
class UnterminatedCStringError(DataError):
	code = '22024'


class InternalError(Error):
	code = 'XX000'
class DataCorruptedError(InternalError):
	code = 'XX001'
class IndexCorruptedError(InternalError):
	code = 'XX002'

class SIOError(Error):
	"System I/O"
	code = '58000'
class UndefinedFileError(SIOError):
	code = '58P01'
class DuplicateFileError(SIOError):
	code = '58P02'

class CFError(Error):
	"Configuration File Error"
	code = 'F0000'
class LockFileExistsError(CFError):
	code = 'F0001'

class OIError(Error):
	"Operator Intervention"
	code = '57000'
class QueryCanceledError(OIError):
	code = '57014'
class AdminShutdownError(OIError, Disconnection):
	code = '57P01'
class CrashShutdownError(OIError, Disconnection):
	code = '57P02'
class ServerNotReadyError(OIError, Disconnection):
	'Thrown when a connection is established to a server that is still starting up.'
	code = '57P03'

class PLPGSQLError(Error):
	"Error raised by a PL/PgSQL procedural function"
	code = 'P0000'
class PLPGSQLRaiseError(PLPGSQLError):
	"Error raised by a PL/PgSQL RAISE statement."
	code = 'P0001'
class PLPGSQLNoDataFoundError(PLPGSQLError):
	code = 'P0002'
class PLPGSQLTooManyRowsError(PLPGSQLError):
	code = 'P0003'


# Setup mapping to provide code based exception lookup.
code_to_error = {}
code_to_warning = {}
def map_errors_and_warnings(
	objs : "A iterable of `Warning`s and `Error`'s",
	error_container : "apply the code to error association to this object" = code_to_error,
	warning_container : "apply the code to warning association to this object" = code_to_warning,
):
	"""
	Construct the code-to-error and code-to-warning associations.
	"""
	for obj in objs:
		if not issubclass(type(obj), (type(Warning), type(Error))):
			# It's not object of interest.
			continue
		code = getattr(obj, 'code', None)
		if code is None:
			# It has no code attribute, or the code was set to None.
			# If it's code is None, we don't map it as it's a "container".
			continue

		if issubclass(obj, Error):
			base = Error
			container = error_container
		elif issubclass(obj, Warning):
			base = Warning
			container = warning_container
		else:
			continue

		cur_obj = container.get(code)
		if cur_obj is None or issubclass(cur_obj, obj):
			# There is no object yet, or the object at the code
			# is not the most general class.
			# The latter condition comes into play when
			# there are sub-Class types that share the Class code
			# with the most general type. (See TypeError)
			container[code] = obj
			if hasattr(obj, 'pg_code'):
				# If there's a PostgreSQL version of the code,
				# map it as well for older servers.
				container[obj.pg_code] = obj

def code_lookup(
	default : "The object to return when no code or class is found",
	container : "where to look for the object associated with the code",
	code : "the code to find the exception for"
):
	obj = container.get(code)
	if obj is None:
		obj = container.get(code[:2] + "000", default)
	return obj

map_errors_and_warnings(sys.modules[__name__].__dict__.values())
ErrorLookup = partial(code_lookup, Error, code_to_error)
WarningLookup = partial(code_lookup, Warning, code_to_warning)

if __name__ == '__main__':
	for x in sys.argv[1:]:
		if x.startswith('01'):
			e = WarningLookup(x)
		else:
			e = ErrorLookup(x)
		sys.stdout.write('postgresql.exceptions.%s [%s]%s%s' %(
				e.__name__, e.code, os.linesep, (
					e.__doc__ is not None and os.linesep.join([
						'  ' + x for x in (e.__doc__).split('\n')
					]) + os.linesep or ''
				)
			)
		)
##
# vim: ts=3:sw=3:noet:
