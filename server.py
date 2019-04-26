
import random
import socket
import select
import pickle
import struct
import logging
import queue
import time

import numpy as np

from decuma import Decuma
import config
import memory_manager


class DecumaServer(object):
    def __init__(self):
        logging.info('Initializing the database...')
        self.db = Decuma('decuma.ini')
        logging.info('Decuma indexing complete')

        # Create a non-blocking communication socket, server side

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.setblocking(0)
        self.server_socket.bind((config.host, config.port))
        logging.info('Socket created and bound to {}:{}'.format(config.host, config.port))

        # Create lists to keep track of the sockets of the different clients

        self.inputs = [self.server_socket]
        self.outputs = []
        self.outgoing = {}
        self.outgoing = {}
        self.incoming = {}
        self.server_socket.listen(config.max_clients)
        logging.info('Ready to accept and process client requests')

    def shutdown(self):
        logging.info('Committing all pending changes and shutting down...')
        self.db.shutdown()
        logging.info('Closing server side socket...')
        self.server_socket.close()
        self.inputs.remove(self.server_socket)
        logging.info('Decuma shutdown complete.')

    def serve_forever(self):
        status = 'active'
        loops_doing_nothing = 0
        while self.inputs:
            loops_doing_nothing += 1

            timeout = config.patience if status == 'active' else 0.01
            readable, writable, exceptional = select.select(self.inputs, self.outputs, self.inputs, timeout)

            # Make sure one client cannot monopolize server time
            random.shuffle(readable)
            random.shuffle(writable)

            for client in readable:
                loops_doing_nothing = 0
                if client is self.server_socket:
                    self.accept_connection()
                else:
                    # A readable client socket has data
                    if client not in self.incoming:
                        # New incoming message
                        try:
                            bs = client.recv(8)
                        except (ConnectionAbortedError, ConnectionResetError):
                            self.close_connection(client)
                        else:
                            if bs:
                                (length,) = struct.unpack('>Q', bs)
                                self.incoming[client] = (length, [])
                            else:
                                # A readable socket without data available is from a client that
                                # has disconnected, and the stream is ready to be closed.
                                self.close_connection(client)
                    else:
                        # This is the continuation of a partly delivered message
                        remaining, data = self.incoming[client]
                        bs = client.recv(min(4096, remaining))
                        if bs:
                            remaining -= len(bs)
                            data.append(bs)
                            if remaining == 0:
                                msg = b''.join(data)
                                self.process_request(client, msg)
                                del self.incoming[client]
                            else:
                                self.incoming[client] = remaining, data
                        else:
                            # A readable socket without data available is from a client that
                            # has disconnected, and the stream is ready to be closed.
                            self.close_connection(client)

            for client in writable:
                if client in self.outgoing and not self.outgoing[client].empty():
                    next_msg = self.outgoing[client].get_nowait()
                    try:
                        client.send(next_msg)
                        loops_doing_nothing = 0
                    except ConnectionResetError:
                        self.close_connection(client)
                elif client in self.outputs:
                    self.outputs.remove(client)
                    #client.close()

            # Now handle "exceptional conditions"
            # Sockets with errors are simply closed.
            for client in exceptional:
                self.inputs.remove(client)
                if client in self.outputs:
                    self.outputs.remove(client)
                client.close()
                del self.outgoing[client]
                loops_doing_nothing = 0

            if loops_doing_nothing > 10:
                status = 'idle'
                n_committed = memory_manager.commit(10)
                if n_committed > 0:
                    logging.info('Committed {} segments during idle time'.format(n_committed))
                else:
                    # Server is idle, and there is nothing to commit
                    # Sleep for a while
                    time.sleep(0.1)

    def accept_connection(self):
        # A "readable" server socket is ready to accept a connection
        connection, client_address = self.server_socket.accept()
        connection.setblocking(0)
        self.inputs.append(connection)
        # Give the connection a queue for data we want to send
        self.outgoing[connection] = queue.Queue()

    def close_connection(self, client):
        if client in self.outputs:
            self.outputs.remove(client)
        if client in self.inputs:
            self.inputs.remove(client)
        if client in self.outgoing:
            del self.outgoing[client]
        if client in self.incoming:
            del self.incoming[client]
        client.close()

    def process_request(self, client, data):
        command, args = pickle.loads(data)

        if command == 'echo':
            path = args
            logging.info('echo resquest from ' + str(client.getsockname()) + ' ' + str(client.getpeername()))
            self.send_data(client, path)

        elif command == 'shutdown':
            self.send_data(client, 0)
            self.shutdown()

        elif command == 'toc':
            self.send_data(client, self.db.toc(args))

        elif command == 'memory_consumption':
            self.send_data(client, memory_manager.memory_consumption())

        elif command == 'get_fields':
            path = args
            try:
                result = self.db[path].fields
                logging.info("get_fields: Found {} fields for series '{}'"
                             .format(len(result), '/'.join(map(str, path))))
                self.send_data(client, result)
            except Exception as e:
                logging.error(str(e))
                self.send_data(client, e)

        elif command == 'create_series':
            try:
                path, fields = args
            except ValueError as e:
                logging.error('create_series failed: ' + str(e))
                self.send_data(client, ValueError("'create_series' needs two parameters: path and fields"))
            else:
                if path in self.db.series:
                    self.send_data(client, KeyError("Series '{}' already exists".format('/'.join(map(str, path)))))
                try:
                    self.db.new_series(path, fields)
                    self.send_data(client, 0)
                except KeyError as e:
                    logging.error('create_series failed: ' + str(e))
                    self.send_data(client, KeyError("Could not create series '{}'".format('/'.join(map(str, path)))))

        elif command == 'delete_series':
            path = args
            try:
                self.db.delete_series(path)
                self.send_data(client, 0)
            except KeyError as e:
                logging.error('delete_series failed: ' + str(e))
                self.send_data(client, KeyError("Could not delete series '{}'".format('/'.join(map(str, path)))))

        elif command == 'defragment':
            path = args
            try:
                self.db.defragment_series(path)
                self.send_data(client, 0)
            except KeyError as e:
                logging.error('defragment_series failed: ' + str(e))
                self.send_data(client, KeyError("Could not defragment series '{}'".format('/'.join(map(str, path)))))

        elif command == 'move_series':
            old_path, new_path = args
            try:
                self.db.move_series(old_path, new_path)
                self.send_data(client, 0)
            except KeyError as e:
                logging.error('move_series failed: ' + str(e))
                self.send_data(client, e)

        elif command == 'rename_fields':
            path, fields = args
            try:
                self.db[path].rename_fields(fields)
                self.send_data(client, 0)
            except KeyError as e:
                logging.error('rename_fields failed: ' + str(e))
                self.send_data(client, e)

        elif command == 'get':
            try:
                path, time, fields, when = args
            except ValueError as e:
                logging.error("'get' failed: " + str(e))
                self.send_data(client, ValueError("'get' needs four parameters: folder, series, time, fields"))
            else:
                try:
                    result = self.db[path].get(time, fields, when=when)
                    logging.info("get: Retrieved a data point in series '{}' at time '{}'".format('/'.join(map(str, path)), time))
                    self.send_data(client, result)
                except Exception as e:
                    logging.error("'get' failed: " + str(e))
                    self.send_data(client, e)

        elif command == 'get_range':
            path, start, end, fields = args
            try:
                result = self.db[path].get_range(start, end, fields)
                logging.info("get: Found {} data points in series '{}' between '{}' and '{}'"
                             .format(len(result[0]), '/'.join(map(str, path)), start, end))
                self.send_data(client, result)
            except Exception as e:
                logging.error(str(e))
                self.send_data(client, e)

        elif command == 'get_all':
            path, fields = args
            try:
                result = self.db[path].get_all(fields)
                logging.info("get: Found {} data points in series '{}'"
                             .format(len(result[0]), '/'.join(map(str, path))))
                self.send_data(client, result)
            except Exception as e:
                logging.error(str(e))
                self.send_data(client, e)

        elif command == 'insert':
            try:
                path, t, x, conflict = args
            except ValueError as e:
                logging.error("'insert' failed: " + str(e))
                self.send_data(client,
                               ValueError("'insert' needs three [or four] parameters: path, time, data [and conflict]"))
            else:
                try:
                    if isinstance(t, list) or isinstance(t, np.ndarray):
                        for i, tt in enumerate(t):
                            self.db[path].insert(tt, x[i], conflict=conflict)
                        logging.info("insert: Inserted '{}' data points to series '{}'".format(len(t), path))
                    else:
                        self.db[path].insert(t, x, conflict=conflict)
                        logging.info("insert: Inserted a new data point to series '{}'".format(path))
                    self.send_data(client, 0)
                except Exception as e:
                    logging.error('insert failed: ' + str(e))
                    self.send_data(client, e)

        else:
            logging.error("Unknown command '{}'".format(command))
            self.send_data(client, 0)

    def send_data(self, client, data):
        # use struct to make sure we have a consistent endianness on the length
        bs = pickle.dumps(data, protocol=2)
        length = struct.pack('>Q', len(bs))

        if client not in self.outputs:
            self.outputs.append(client)

        self.outgoing[client].put(length)
        for i in range(0, len(bs), 4096):
            self.outgoing[client].put(bs[i:i + 4096])


if __name__ == '__main__':
    DecumaServer().serve_forever()
