import logging as log
import socket
import sys
from enum import IntEnum
from struct import unpack
from uuid import uuid4


log.basicConfig(
    stream=sys.stdout,
    # format='[%(asctime)s] [%(levelname)s] %(filename)s:%(funcName)s:%(lineno)d @ %(message)s',
    # format='[%(asctime)s] %(message)s',
    format='%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=log.INFO
)


CLIENT_NAME = 'clickhouse-cli'
CLIENT_VERSION = (1, 1, 54060)  # @FIXME: This is a dirty lie.


DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES = 50264
DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS = 51554
DBMS_MIN_REVISION_WITH_BLOCK_INFO = 51903
DBMS_MIN_REVISION_WITH_CLIENT_INFO = 54032
DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE = 54058
DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO = 54060


class ClientInfo(object):
    iface_type = 1  # TCP
    query_kind = 1  # INITIAL_QUERY
    initial_user = ''
    initial_query_id = ''
    quota_key = ''
    os_user = ''
    client_hostname = ''
    client_name = ''
    initial_address = '[::ffff:127.0.0.1]:0'
    client_version_major = 0
    client_version_minor = 0
    client_revision = 0


class QuerySettings(object):
    max_threads = 0
    extremes = False
    skip_unavailable_shards = False
    output_format_write_statistics = True
    use_client_time_zone = False

    # connect_timeout
    # max_block_size
    # distributed_group_by_no_merge
    # strict_insert_defaults
    # network_compression_method
    # priority


class Profile(object):
    rows = 0
    blocks = 0
    bytes = 0
    rows_before_limit = 0
    applied_limit = False
    calculated_rows_before_limit = False


class Progress(object):
    rows = 0
    bytes = 0
    total_rows = 0


class DBException(Exception):

    def __init__(self, code=0, name='', text='', stacktrace=''):
        self.code = code
        self.name = name
        self.text = text
        self.stacktrace = stacktrace

    def __str__(self):
        return "Code: {code}. {text}\n\n{stacktrace}".format(
            code=self.code, text=self.text, stacktrace=self.stacktrace
        )



class ServerCodes(IntEnum):
    Hello = 0
    Data = 1
    Exception = 2
    Progress = 3
    Pong = 4
    EndOfStream = 5
    ProfileInfo = 6
    Totals = 7
    Extremes = 8


class ClientCodes(IntEnum):
    Hello = 0
    Query = 1
    Data = 2
    Cancel = 3
    Ping = 4


def to_varint(number):
    buf = number
    result = []

    for _ in range(8):
        b = buf & 0x7f
        if buf > 0x7f:
            b = b | 0x80

        result.append(b)

        buf = buf >> 7

        if not buf:
            return ''.join(map(chr, result))


def to_string(value):
    return to_varint(len(value)) + value


class Client(object):
    options = None
    query_events = None

    socket = None
    server_info = {
        'name': None,
        'timezone': None,
        'version': (0, 0, 0)
    }

    data = None

    def __init__(self, host='127.0.0.1', port=9000, database='default', user='default', password=''):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((host, port))
        # self.socket.settimeout(5.0)
        # self.socket.setblocking(False)

    def handshake(self):
        if not self.send_hello():
            return False

        return self.receive_packet() == ServerCodes.Hello

    def receive_varint(self):
        value = 0
        for i in range(9):
            buf = self.socket.recv(1)
            b = ord(buf)
            value |= (b & 0x7F) << 7 * i
            if not (b & 0x80):
                return value

    def receive_fixed(self, size):
        return self.socket.recv(size).decode()

    def receive_string(self):
        size = self.receive_varint()
        if size > 0xFFFFFF:
            return False

        return self.receive_fixed(size)

    def receive_bool(self):
        """
        Just a syntax sugar for the UInt8.
        """
        return bool(unpack('<B', self.socket.recv(1)))

    def receive_type(self, type):
        """

        """
        if type == 'String':
            return self.receive_string()

        if 'Int' in type:
            if type.endswith('8'):
                data = self.socket.recv(1)
                fmt = '<b'
            elif type.endswith('16'):
                data = self.socket.recv(2)
                fmt = '<h'
            elif type.endswith('32'):
                data = self.socket.recv(4)
                fmt = '<i'
            elif type.endswith('64'):
                data = self.socket.recv(8)
                fmt = '<q'

            fmt = fmt.upper() if type.startswith('U') else fmt
        elif type == 'Float32':
            data = self.socket.recv(4)
            fmt = '<f'
        elif type == 'Float64':
            data = self.socket.recv(8)
            fmt = '<d'

        result = unpack(fmt, data)[0]
        return result

    def receive_packet(self):
        packet_type = self.receive_varint()
        log.debug('Received packet: %d', packet_type)

        if packet_type == ServerCodes.Hello:
            log.debug("HELLO response received:")
            self.server_info['name'] = self.receive_string()
            self.server_info['version'] = self.receive_varint(), self.receive_varint(), self.receive_varint()

            if self.server_info['version'][2] >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE:
                self.server_info['timezone'] = self.receive_string()

            log.debug(self.server_info)
        elif packet_type == ServerCodes.Data:
            # REVISION >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES
            self.receive_string()  # table_name
            # REVISION >= DBMS_MIN_REVISION_WITH_BLOCK_INFO
            self.receive_varint()  # ???
            is_overflows = self.receive_bool()
            self.receive_varint()  # ???
            bucket_num = self.receive_type('Int32')
            self.receive_varint()  # ???
            log.debug('bucket_num: %d', bucket_num)
            log.debug('is_overflows: %d', is_overflows)

            num_columns = self.receive_varint()
            num_rows = self.receive_varint()

            log.debug('num_columns: %d', num_columns)
            log.debug('num_rows: %d', num_rows)

            header = [None] * num_columns
            data = []

            for col_i in range(num_columns):
                col_name = self.receive_string()
                log.debug('name: %s', col_name)
                col_type = self.receive_string()
                log.debug('type: %s', col_type)
                col_width = len(col_name)
                header[col_i] = [col_name, col_type, col_width]

                if num_rows:
                    column_data = [None] * num_rows
                    for row_i in range(num_rows):
                        column_data[row_i] = self.receive_type(col_type)

                        length = len(str(column_data[row_i]))
                        if length > header[col_i][2]:
                            header[col_i][2] = length

                        log.debug('value(%s): %s', col_type, column_data[row_i])

                    data.append(column_data)

            if data:
                data.insert(0, header)
                self.data = data
            else:
                self.data = None

        elif packet_type == ServerCodes.Exception:
            self.raise_exception()

        elif packet_type == ServerCodes.ProfileInfo:
            rows = self.receive_varint()
            blocks = self.receive_varint()
            bytes = self.receive_varint()
            self.receive_bool()  # applied_limit
            self.receive_varint()  # rows_before_limit
            self.receive_bool()  # calculated_rows_before_limit
            log.debug('Profiling: rows={0}, blocks={1}, bytes={2}'.format(rows, blocks, bytes))
            log.info('%d rows in set.', rows)

        elif packet_type == ServerCodes.Progress:
            rows = self.receive_varint()
            bytes = self.receive_varint()
            # REVISION >= DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS
            total_rows = self.receive_varint()

            log.debug('Progress: rows={0}/{1}, bytes={2}'.format(rows, total_rows, bytes))

        elif packet_type == ServerCodes.EndOfStream:
            pass

        return packet_type

    def raise_exception(self):
        code = self.receive_type('UInt32')
        name = self.receive_string()
        display_text = self.receive_string()
        stacktrace = self.receive_string()
        self.receive_bool()  # has_nested
        raise DBException(code=code, name=name, text=display_text, stacktrace=stacktrace)

    def send_query(self, query):
        query_id = str(uuid4())

        data = ''
        data += to_varint(ClientCodes.Query)
        data += to_string(query_id)

        if self.server_info['version'][2] >= DBMS_MIN_REVISION_WITH_CLIENT_INFO:
            data += to_varint(1)  # query_kind: INITIAL_QUERY
            data += to_string('')  # initial_user
            data += to_string('')  # initial_query_id
            data += to_string('[::ffff:127.0.0.1]:0')  # initial_address
            data += to_varint(1)  # iface_type: TCP

            data += to_string('igor')  # os_user
            data += to_string('localhost')  # client_hostname
            data += to_string(CLIENT_NAME)  # client_name
            data += to_varint(CLIENT_VERSION[0])
            data += to_varint(CLIENT_VERSION[1])
            data += to_varint(CLIENT_VERSION[2])

            if self.server_info['version'][2] >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO:
                data += to_string('')

        data += to_string('')  # per-query settings

        data += to_varint(2)  # Stage: Complete
        data += to_varint(0)  # CompressionState: Disable
        data += to_string(query)

        # empty block of data
        data += to_varint(ClientCodes.Data)

        if self.server_info['version'][2] >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES:
            data += to_string('')

        # block size
        data += to_varint(0)
        data += to_varint(0)
        data += to_varint(0)

        log.debug('sending query:')
        log.debug(repr(data.encode()))
        log.debug('---------')
        self.socket.sendall(data.encode())

    def send_hello(self):
        data = ''

        data += to_varint(ClientCodes.Hello)
        data += to_string(CLIENT_NAME)
        data += to_varint(CLIENT_VERSION[0])
        data += to_varint(CLIENT_VERSION[1])
        data += to_varint(CLIENT_VERSION[2])
        data += to_string('default')  # database
        data += to_string('default')  # user
        data += to_string('')  # password

        log.debug('sending hello:', data)
        self.socket.sendall(data.encode())
        return True

    def display_data(self, fmt='PrettyCompact'):
        """
        This is a great example of how not to code stuff.
        """

        header = self.data.pop(0)

        if fmt == 'CSV':
            print(','.join(col[0] for col in header))
            for row in map(list, zip(*self.data)):
                print(','.join(map(str, row)))

        elif fmt == 'PrettyCompact':
            header_str = '┌─'
            footer_str = '└─'
            for i, col in enumerate(header):
                header_str += '\033[1m' + col[0] + '\033[0m'
                header_str += '─' * (int(col[2]) - len(col[0]))
                footer_str += '─' * int(col[2])
                if i < len(header) - 1:
                    header_str += '─┬─'
                    footer_str += '─┴─'
                else:
                    header_str += '─┐'
                    footer_str += '─┘'

            print(header_str)

            # Just for lulz.
            for row in map(list, zip(*self.data)):
                row_str = map(str, row)
                print('│ ' + ' │ '.join(cell + ' ' * (int(header[col_i][2]) - len(cell)) for (col_i, cell) in enumerate(row_str)) + ' │')

            print(footer_str, end='\n\n')

    def execute_query(self, query):
        log.info('Executing query:\n\n%s\n', query)
        if not self.handshake():
            log.warning("handshake failed :(")
            return False

        self.send_query(query)

        while True:
            try:
                response_code = self.receive_packet()

                if response_code == ServerCodes.Data and self.data:
                    self.display_data()
                if response_code == ServerCodes.EndOfStream:
                    log.debug('Finished receiving the data.')
                    break
            except DBException as e:
                log.error('Caught an exception!', exc_info=True)

        return True


if __name__ == '__main__':
    client = Client()
    client.execute_query("SELECT number AS i, hex(number*16) AS hex_num, 'foo' || toString(number) || 'bar' AS s FROM system.numbers LIMIT 10;")
