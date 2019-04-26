"""Connects to a Decuma database and interacts with it.

This file contains the implementation of the class Client.
The class can be used to communicate with a Decuma database server through the following requests:

  - Operations to browse and manage the database
        echo
        toc
        memory_consumption
        shutdown_server
  - Operations to manage and organize the time series
        new
        delete
        move_to
        get_fields
        rename_fields
        defragment
  - Operations to access the temporal data inside the series
        get
        get_range
        get_all
        insert

"""

import socket
import pickle
import struct
import pandas as pd


class FolderHierarchy(object):
    def __init__(self):
        self.contents = {}

    def _add(self, path, s):
        if len(path) == 1:
            if len(s) > 0:
                self.contents[path[0]] = {'serial': s.serial, 'fields': s.fields,
                                          'length': len(s), 'start': s.start, 'end': s.end}
            else:
                self.contents[path[0]] = {'serial': s.serial, 'fields': s.fields,
                                          'length': len(s), 'start': None, 'end': None}
        else:
            if path[0] not in self.contents:
                self.contents[path[0]] = FolderHierarchy()
            self.contents[path[0]]._add(path[1:], s)

    def find(self, series_name):
        found = [v for v in self.series() if v[-1] == series_name]
        if len(found) == 1:
            return found[0]
        else:
            return found

    def __getitem__(self, item):
        if item in self.contents:
            return self.contents[item]
        else:
            return None

    def __contains__(self, item):
        return item in self.contents

    def series(self, prefix=()):
        results = []
        for k, v in self.contents.items():
            if isinstance(v, FolderHierarchy):
                results += v.series(prefix + (k,))
            else:
                results.append(prefix + (k,))
        return results

    def folders(self, prefix=()):
        results = []
        for k, v in self.contents.items():
            if isinstance(v, FolderHierarchy):
                results += v.folders(prefix + (k,))
            else:
                results.append(prefix)
        return results

    def pretty_print(self, level=None, ellipsis=False, contains=None, indent=0):
        if ellipsis == True:
            ellipsis = 10
        if level == 0:
            return
        else:
            for i, k in enumerate(sorted(self.contents.keys())):
                if contains is None and 1 < ellipsis == i:
                    print(' ' * indent + '...')
                elif contains is None and 1 < ellipsis < i < len(self.contents) - 1:
                    continue
                else:
                    if type(self.contents[k]) == type(self):
                        desc = ' '*indent + k
                        size = len(self.contents[k].contents)
                        unit = 'item' if size <= 1 else 'items'

                        print('\x1b[32m{:30}{:8} {:6}\x1b[0m'.format(desc, size, unit))

                        if level is None:
                            self.contents[k].pretty_print(indent=indent + 4, ellipsis=ellipsis, contains=contains)
                        else:
                            self.contents[k].pretty_print(level=level - 1, ellipsis=ellipsis,
                                                          indent=indent + 4, contains=contains)
                    elif contains is None or contains in k:
                        desc = ' '*indent + k
                        size = self.contents[k]['length']
                        unit = 'point' if size <= 1 else 'points'
                        start = pd.to_datetime(str(self.contents[k]['start'])).strftime('%Y.%m.%d')
                        end = pd.to_datetime(str(self.contents[k]['end'])).strftime('%Y.%m.%d')

                        print('{:30}{:8} {:6}   {} -> {}'.format(desc, size, unit, start, end))

class Folder(object):
    def __init__(self, address, path):
        self._address = address
        self._path = path

    def toc(self):
        return _request(self._address, 'toc', self._path)

    def get_fields(self):
        return _request(self._address, 'get_fields', self._path)

    def get(self, time, fields=None, when='after'):
        t, x = _request(self._address, 'get', (self._path, time, fields, when))
        return (t, x) if len(x) > 1 else (t, x[0])

    def get_range(self, start, end, fields=None):
        t, x = _request(self._address, 'get_range', (self._path, start, end, fields))
        if x.shape[0] == 0:
            return [], []
        elif x.shape[1] <= 1:
            return t, x.flatten()
        else:
            return t, x

    def get_all(self, fields=None):
        t, x = _request(self._address, 'get_all', (self._path, fields))
        return (t, x) if len(x) > 1 else (t, x[0])

    def insert(self, time, data, conflict='keep both'):
        _request(self._address, 'insert', (self._path, time, data, conflict))

    def new(self, fields):
        return _request(self._address, 'create_series', (self._path, fields))

    def delete(self):
        return _request(self._address, 'delete_series', self._path)

    def move_to(self, folder):
        return _request(self._address, 'move_series', (self._path, folder._path))

    def defragment(self):
        return _request(self._address, 'defragment', self._path)

    def rename_fields(self, new_fields):
        return _request(self._address, 'rename_fields', (self._path, new_fields))

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        elif isinstance(attr, tuple):
            return Folder(self._address, self._path + attr)
        else:
            return Folder(self._address, self._path + (attr,))

    def __getitem__(self, item):
        return Folder(self._address, self._path + (item,))


class Client(object):
    def __init__(self, server_address):
        self._address = server_address

    def echo(self, msg):
        return _request(self._address, 'echo', msg)

    def shutdown_server(self):
        return _request(self._address, 'shutdown', None)

    def toc(self):
        return _request(self._address, 'toc', ())

    def memory_consumption(self):
        return _request(self._address, 'memory_consumption', None)

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        elif isinstance(attr, tuple):
            return Folder(self._address, attr)
        else:
            return Folder(self._address, (attr,))

    def __getitem__(self, path):
        if isinstance(path, tuple):
            return Folder(self._address, path)
        else:
            return Folder(self._address, (path,))


def _request(address, command, args):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.connect(address)

        # Send request
        bytes = pickle.dumps((command, args), protocol=2)
        length = struct.pack('>Q', len(bytes))
        s.sendall(length)
        for i in range(0, len(bytes), 4096):
            s.sendall(bytes[i:i+4096])

        # Collect response
        try:
            bs = s.recv(8)
            (length,) = struct.unpack('>Q', bs)
            response = b''
            while len(response) < length:
                response += s.recv(min(4096, length - len(response)))
            result = pickle.loads(response)
            if isinstance(result, BaseException):
                raise result
            else:
                s.shutdown(socket.SHUT_RDWR)
                return result
        except ConnectionResetError as e:
            raise ConnectionResetError('Connection with Decuma server was lost')


