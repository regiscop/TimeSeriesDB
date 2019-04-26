
import os
import pickle

import numpy as np

import config
import memory_manager
import register


class Segment(object):
    def __init__(self, id, data):
        """Creates a new segment

        It is not possible to create an empty segment. The segment must either be a representation of data
        already on the disk, or a new segments initialized with at least some data.

        Args:
            id (int): a number identifying the segment uniquely within the parent series
            data (tuple): data can either be a tuple (index, index, int) or a tuple (np.ndarray, np.ndarray)

                Case 1
                If passed a tuple (start, end, size), it will assume that the segment currently exists on
                the disk, and that it contains 'size' rows with indices going from 'start' to 'end'.
                The created segment will have disk_synced == True but mem_synced == False.

                Case 2
                If passed a tuple (times, data), it will will create a new segment containing the passed data.
                    -> 'times' must be a either an index or a one-dimensional array of indices. If 'times' is an array,
                        then it has to be provided sorted.
                    -> 'data' must then be a one or two-dimensional array of values, depending on wheter 'times' is a
                        index or an array of indices. It must have the same number of rows has len(times).
                A new file will not be created on the disk at this stage.
                The created segment will have disk_synced == False and mem_synced == True.
        """
        self.id = id

        if len(data) == 3:
            self.start, self.end, self.size = data
            self.t, self.x = None, None
            self._disk_synced = True
            self._mem_synced = False
        elif len(data) == 2:
            t, x = data
            if isinstance(t, np.ndarray) or isinstance(t, list):
                self.t, self.x = t, x
                self.start = t[0]
                self.end = t[-1]
                self.size = np.alen(t)
            else:
                self.t = np.array([t])
                self.x = np.reshape(x, (1, np.alen(x)))
                self.start, self.end, self.size = t, t, 1
            self._disk_synced = False
            self._mem_synced = True
            memory_manager.write_op(self)
        else:
            raise ValueError('segment.__init__(): '
                             'data should be a a tuple (index, index, int) or a tuple (np.ndarray, np.ndarray)')

    def __len__(self):
        """Return the number of data points in the segment"""
        return self.size

    def __str__(self):
        return 'Segment n: ' + str(self.id) + \
               '\nStart: ' + str(self.start) + \
               '\nEnd:   ' + str(self.end) + \
               '\n' + str(self.x)

    @property
    def filename(self):
        """Return the complete path to the segment .npz file on the disk"""
        return os.path.join(config.root_dir, str(self.id) + '.npz')

    @property
    def memory_consumption(self):
        if self.x is not None:
            return self.t.nbytes + self.x.nbytes
        else:
            return 0

    def get(self, t, columns=None, find='after'):
        """Find and return a single data point from the segment

        Args:
            t (index): a timestamp indicating the data looked after
            columns (list of int): the columns to be retrieved. Default will retrieve all the data.
            find (str): can be either 'next', 'previous' or 'interp1'
                'after' will look for the first data with timestamp greater than or equal to 't'
                'before' will look for the last data with timestamp less than or equal to 't'
                'exact' will look for data with timestamp exactly equal to 't'

        Returns:
            tuple: a tuple (t, x) where:
                        t is an index
                        x is a one-dimensional of length len(columns) containing the timeseries values

            An IndexError will be raised if no data can be found, for example when:
                t > self.end and find == 'after'
                t < self.start and find == 'before'
                t not in self.t and find == 'exact'
                find not in ['after', 'before', 'exact']
        """
        self.disk_to_mem()

        i = np.searchsorted(self.t, t)

        if find == 'before':
            if i == len(self.t) or self.t[i] != t:
                i -= 1
            if i == -1:
                raise ValueError("There is no data before index {} in the segment".format(t))
        elif find == 'after' and i >= len(self.t):
            raise ValueError("There is no data after index {} in the segment".format(t))
        elif find == 'exact' and self.t[i] != t:
            raise ValueError("There is no data with exact index {} in the segment".format(t))

        if columns is None:
            return self.t[i], self.x[i]
        else:
            return self.t[i], self.x[i, columns]

    def get_range(self, start, end, columns=None):
        """Return all the data in the segment that fall in a certain period of time

        Args:
            start (index): a timestamp indicating the start of the slice
            end (index): a timestamp indicating the end of the slice
            columns (list): the fields to be returned (default None = all the fields)

        Returns:
            tuple: a tuple (t, x) where:
                        t is a one-dimensional numpy array containing the timestamps
                        x is a two-dimensional of shape (len(t), len(columns)) containing the timeseries values
        """
        self.disk_to_mem()

        i0 = np.searchsorted(self.t, start, side='left')
        i1 = np.searchsorted(self.t, end, side='right')
        if columns is None:
            return self.t[i0:i1], self.x[i0:i1]
        else:
            return self.t[i0:i1], self.x[i0:i1, columns]

    def insert(self, t, x, conflict='keep both'):
        """Insert a new data point in the segment

        Args:
            t (index): the timestamp for the data
            x (np.ndarray): a one-dimensional array containing the data.  len(x) should be equal to the number of
                columns in the series.
            conflict (str): determines how collisions are handled (i.e. when some data is already in the segment
                for the same timestamp). Its value can either be 'keep both', 'replace' or 'ignore':
                'keep both' will keep both data and insert the new data *before* the previous one.
                'replace' will remove and replace the already existing data by the new data.
                'skip' will keep the segment as it is, and the new data is not inserted.

        Returns:
            True if the variables self.start or self.end have changed, None otherwise
        """
        self.disk_to_mem()

        x = np.reshape(x, (1, np.alen(x)))
        if t < self.start:
            self.t = np.insert(self.t, 0, t)
            self.x = np.insert(self.x, 0, x, axis=0)
            self.start = t
            self.size = self.size + 1
            self._disk_synced = False
            memory_manager.write_op(self)
            return True

        elif t > self.end:
            self.t = np.append(self.t, t)
            self.x = np.append(self.x, x, axis=0)
            self.end = t
            self.size = self.size + 1
            self._disk_synced = False
            memory_manager.write_op(self)
            return True

        else:
            i = np.searchsorted(self.t, t)
            if self.t[i] > t:
                self.t = np.insert(self.t, i, t)
                self.x = np.insert(self.x, i, x, axis=0)
                self.size = self.size + 1
                self._disk_synced = False
                memory_manager.write_op(self)
            else:
                if conflict == 'keep both':
                    self.t = np.insert(self.t, i, t)
                    self.x = np.insert(self.x, i, x, axis=0)
                    self.size = self.size + 1
                    self._disk_synced = False
                    memory_manager.write_op(self)

                elif conflict == 'replace':
                    self.x[i] = x
                    self._disk_synced = False
                    memory_manager.write_op(self)

                elif conflict == 'skip':
                    pass
                else:
                    raise ValueError("Invalid conflict resolution '{}'. "
                                     "Should be 'keep both', 'overwrite' or 'ignore'.".format(conflict))

    def split(self, new_ids):
        """Split the segment into new segments

        The number of pieces to split the segment into, is determined by the number of id's passed to the function.

        After the end of the split, the splitted segment will be automatically deleted and removed from the disk.

        Notes:
            The data is divided equally, in the sense that the new segments will all contain the same number of
            rows (at one near if not possible).
            If more new id's are passed than there are lines, then some of the returned segments will be empty.

        Args:
            new_ids (list): a list containing the Segment objects between which the data will be splitted.
                There can be as many new_segment as

        Returns:
            The new_segments list, with segments now containing the data.
            All new segments will be mem_synced, but not disk_synced.
        """
        self.disk_to_mem()

        indices = np.linspace(0, self.size, len(new_ids) + 1, dtype=int)
        new_segments = []
        for i, id in enumerate(new_ids):
            i0 = indices[i]
            i1 = indices[i+1]
            new_segments.append(Segment(id, (self.t[i0:i1], self.x[i0:i1, :])))

        self.delete()

        return new_segments

    def delete(self):
        self.start = None
        self.end = None
        self.size = 0
        self.t = np.array([])
        self.x = np.array([[]])
        self._mem_synced = True
        self._disk_synced = False
        memory_manager.write_op(self)

    def mem_to_disk(self):
        """Write changes to the disk

        There is no guarantee that the file on the disk will be up to date after call to the function. The call may
        fail for example is permissions to write to the file is denied by the operating system.

        If the update was successful, subsequent calls to self.is_disk_synced will return True (until more changes
        are made to the segment).
        """
        if not self._disk_synced:
            try:
                if self.size > 0:
                    np.savez(self.filename, t=self.t, x=self.x)
                elif os.path.exists(self.filename):
                    os.remove(self.filename)
                register.record_segment(self)
            except PermissionError as err:
                raise err
            else:
                self._disk_synced = True

    def disk_to_mem(self):
        """Make sure that the version in memory is loaded and up to date"""
        if not self._mem_synced:
            data = np.load(self.filename)
            self.t = data['t']
            self.x = data['x']
            self.start = self.t[0]
            self.end = self.t[-1]
            self.size = len(self.t)
            self._mem_synced = True
            self._disk_synced = True
        memory_manager.read_op(self)

    def load_header(self, file):
        self.id, self.series.id, self.start, self.end, self.size = pickle.load(file)

    def load(self, file):
        self.load_header()
        self.t = np.load(file)
        self.x = np.load(file)

    def save(self, file):
        header = (self.id, self.series.id, self.start, self.end, self.size)
        pickle.dump(header, file, protocol=2)
        np.save(file, self.t)
        np.save(file, self.x)

    def __lt__(self, other):
        return self.end <= other.start
