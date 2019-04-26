
import matplotlib.pyplot as plt

import numpy as np

import config
from segment import Segment


class Series(object):

    n_splits = 2 # number of pieces to split a segment into, once it grows too big

    def __init__(self, serial, fields, segments=None):
        """Creates a series

        Args:
            name (str): the full name of the series
            fields (list): the list of fields (columns)
        """
        self.serial = serial
        self.fields = fields

        if segments:
            self.segments = sorted(segments)
            self._next_free_id = max([s.id for s in self.segments]) + 1
        else:
            self.segments = []
            self._next_free_id = 1

    @property
    def shape(self):
        return len(self), len(self.fields)

    @property
    def start(self):
        return self.segments[0].start

    @property
    def end(self):
        return self.segments[-1].end

    def __len__(self):
        return sum([len(s) for s in self.segments])

    def rename_fields(self, fields):
        if len(self.fields) != len(fields):
            raise IndexError('Number of fields to rename is {}, not {}'.format(len(self.fields), len(fields)))
        else:
            self.fields = fields

    def get(self, t, fields=None, when='after'):
        columns = self._fields_to_columns(fields)
        ind = self._bisect_segment_index(t, when)
        return self.segments[ind].get(t, columns, find=when)

    def get_range(self, start, end, fields=None):
        columns = self._fields_to_columns(fields)
        if start > self.segments[-1].end or end < self.segments[0].start:
            return np.array([]), np.array([])
        else:
            bounds = np.array([[s.start, s.end] for s in self.segments]).flatten()
            i0 = int(np.searchsorted(bounds, start, side='left') / 2)
            i1 = int((np.searchsorted(bounds, end, side='right') - 1) / 2)
            results_t, results_x = [], []
            for i in range(i0, i1 + 1):
                t, x = self.segments[i].get_range(start, end, columns)
                results_t.append(t)
                results_x.append(x)
            return np.concatenate(results_t, axis=0), np.concatenate(results_x, axis=0)

    def get_all(self, fields=None):
        all_t = []
        all_x = []
        if fields is None:
            for seg in self.segments:
                t, x = seg.get_range(seg.start, seg.end)
                all_t.append(t)
                all_x.append(x)
        else:
            columns = self._fields_to_columns(fields)
            for seg in self.segments:
                t, x = seg.get_range(seg.start, seg.end, columns)
                all_t.append(t)
                all_x.append(x)
        return np.concatenate(all_t, axis=0), np.concatenate(all_x, axis=0)

    def insert(self, t, x, conflict='keep both'):
        if not np.alen(x) == len(self.fields):
            raise ValueError('Length of data should be {}, not {}.'.format(len(self.fields), len(x)))

        if not self.segments:
            # Create the very first segment
            self.segments = [Segment(self._generate_segment_id(), (t, x))]
        else:
            ind = self._bisect_segment_index(t)
            self.segments[ind].insert(t, x, conflict)
            if self.segments[ind].memory_consumption > config.max_segment_size:
                # Time for a split !
                ids = [self._generate_segment_id() for _ in range(Series.n_splits)]
                split = self.segments[ind].split(ids)
                self.segments = self.segments[:ind] + split + self.segments[ind+1:]

    def delete(self):
        for seg in self.segments:
            seg.delete()
        self.segments = []
        self.fields = None

    def _find_segment_index(self, t, find=None):
        if find is None:
            if t > self.segments[-1].end:
                return len(self.segments) - 1
            for i, s in enumerate(self.segments):
                if t <= s.end:
                    return i
        elif find == 'after':
            for i, s in enumerate(self.segments):
                if t <= s.end:
                    return i
        elif find == 'before':
            for i, s in reversed(list(enumerate(self.segments))):
                if t >= s.start:
                    return i
        elif find == 'exact':
            for i, s in enumerate(self.segments):
                if s.start <= t <= s.end:
                    return i
        raise ValueError("No data point found at {} 'find={}'".format(t, find))

    def _bisect_segment_index(self, t , find=None):
        if t < self.segments[0].start:
            if find is None or find == 'after':
                return 0
        elif self.segments[0].start <= t <= self.segments[0].end:
            return 0
        elif self.segments[-1].start <= t <= self.segments[-1].end:
            return len(self.segments) - 1
        elif t > self.segments[-1].end:
            if find is None or find == 'before':
                return len(self.segments) - 1
        else:
            lo = 0
            hi = len(self.segments) - 1
            while hi - lo >= 1:
                mid = int((lo + hi) / 2)
                mid_seg = self.segments[mid]
                if t < mid_seg.start:
                    hi = mid
                elif mid_seg.start <= t <= mid_seg.end:
                    return mid
                elif t > mid_seg.end:
                    hi = mid

            if find is None or find == 'after':
                return hi
            elif find == 'before':
                return lo

        raise ValueError("No data point found at {} 'find={}'".format(t, find))


    def _fields_to_columns(self, fields):
        if fields is None:
            return None
        else:
            return [self.fields.index(f) for f in fields]

    def _generate_segment_id(self):
        self._next_free_id += 1
        return self.serial * 100000000 + self._next_free_id - 1



    def print(self):
        for i, seg in enumerate(self.segments):
            print('SEGMENT {} from {} to {}'.format(i, seg.start, seg.end))
            if not seg._mem_synced:
                print('ON DISK'.format(seg.start, seg.end))
            else:
                for t, x in zip(seg.t, seg.x):
                    print('{}:\t{}'.format(t, x))

    def plot(self):
        fig, axes = plt.subplots(2, 1, gridspec_kw={'height_ratios':[1, 1]})

        x = np.array([0.5 * (s.start + s.end) for s in self.segments])
        widths = np.array([s.end - s.start for s in self.segments])
        heights = np.array([s.size for s in self.segments])

        mask = [s._disk_synced for s in self.segments]
        axes[0].bar(x[mask],
                    height=heights[mask],
                    width=widths[mask],
                    color='green')

        mask = np.logical_not(mask)
        axes[0].bar(x[mask],
                    height=heights[mask],
                    width=widths[mask],
                    color='red')

        mask = [s._mem_synced for s in self.segments]
        axes[0].bar(x[mask],
                    height=-heights[mask],
                    width=widths[mask],
                    color='black')


        mng = plt.get_current_fig_manager()
        ### works on Ubuntu??? >> did NOT working on windows
        # mng.resize(*mng.window.maxsize())
        mng.window.state('zoomed')  # works fine on Windows!

        plt.show()
