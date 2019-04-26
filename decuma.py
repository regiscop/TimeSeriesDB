
import numpy as np
import logging
import config
import register
import memory_manager
from series import Series
from client import FolderHierarchy


class Decuma(object):
    def __init__(self, config_filename=config.filename):
        config.load(config_filename)
        self.series = register.parse_index()
        if self.series:
            self._next_free_serial = max([s.serial for s in self.series.values()])
        else:
            self._next_free_serial = 1

    def shutdown(self):
        memory_manager.force_commit_all()
        self.series = {}

    def new_series(self, path, fields):
        if path not in self.series:
            # New segment
            self.series[path] = Series(self._generate_serial(), fields)
            # Unlike segments, for which the register is modified during commits,
            # creation and deletion of series are immediately recorded into the register
            register.record_series(path, self.series[path])
        else:
            raise KeyError('Series already exists')

    def delete_series(self, path):
        # Deleting a series is done ON THE SPOT
        # This may by a slow operation if there are a lot of files
        if path in self.series:
            self.series[path].delete()
            memory_manager.force_commit_all()
            # Unlike segments, for which the register is modified during commits,
            # creation and deletion of series are immediately recorded into the register
            register.record_series(path, self.series[path])
            del self.series[path]
        else:
            raise KeyError('Series does not exist')

    def defragment_series(self, path):
        if len(self.series[path].segments) > 1:
            t, x = self.series[path].get_all()
            order = np.argsort(t)
            fields = self.series[path].fields
            self.delete_series(path)
            self.new_series(path, fields)
            for i in order:
                self.series[path].insert(t[i], x[i])
        logging.info('Series {} defragmented'.format('/'.join(path)))

    def move_series(self, old_path, new_path):
        if old_path == new_path:
            raise KeyError('Destination and origing folders are the same')
        elif old_path not in self.series:
            raise KeyError('Series {} already exists'.format('/'.join(old_path)))
        elif new_path in self.series:
            raise KeyError('Series {} already exists'.format('/'.join(new_path)))
        else:
            self.series[new_path] = self.series[old_path]
            del self.series[old_path]

    def delete_all(self):
        while self.series:
            self.delete_series(next(iter(self.series.keys())))

    def toc(self, path=()):
        toc = FolderHierarchy()
        for k, v in self.series.items():
            if len(path) == 0 or (len(path) <= len(k) and path == k[:len(path)]):
                toc._add(k[len(path):], v)
        return toc

    def __getitem__(self, path):
        return self.series[path]

    def _generate_serial(self):
        self._next_free_serial += 1
        return self._next_free_serial - 1
