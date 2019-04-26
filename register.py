
import struct
import pickle
import logging

import config
from series import Series
from segment import Segment


config.load("decuma.ini")


_series_marker = b'A'
_segment_marker = b'B'
_filename = 'decuma.index'


def record_series(path ,series):
    with open(config.root_dir / _filename, 'ab') as f:
        _write_line(f, _series_marker, (path, series.serial, series.fields))


def record_segment(segment):
    with open(config.root_dir / _filename, 'ab') as f:
        _write_line(f, _segment_marker, (segment.id, segment.start, segment.end, segment.size))


def parse_index():
    parsed_series_data = {}
    parsed_segments_data = {}

    try:
        f = open(config.root_dir / _filename, 'rb')
        while f:
            try:
                marker, data = _read_line(f)
            except TypeError:
                break
            else:
                if marker == _series_marker:
                    path, serial, fields = data
                    if fields is not None:
                        # SERIES CREATED
                        # Record series field name
                        parsed_series_data[serial] = (path, fields)
                    else:
                        # SERIES DELETED
                        # fields_list being None indicates the Series was deleted
                        parsed_series_data.pop(serial, None)
                elif marker == _segment_marker:
                    id, start, end, size = data
                    serial = int(id / 100000000)
                    if size > 0:
                        # SEGMENT COMMITTED
                        # an empty segment can only be a deleted segment
                        try:
                            parsed_segments_data[serial][id] = (start, end, size)
                        except KeyError:
                            parsed_segments_data[serial] = {id: (start, end, size)}
                    else:
                        # SEGMENT DELETED
                        if serial in parsed_segments_data:
                            parsed_segments_data[serial].pop(id, None)
    except FileNotFoundError:
        logging.info("'decuma.index' not found in folder '{}'".format(str(config.root_dir)))
        logging.info("Creating empty index file")
        f = open(config.root_dir / _filename, 'wb')
    finally:
        f.close()

    series = {}
    for serial, _ in parsed_series_data.items():
        path, fields = _
        if serial in parsed_segments_data:
            segments = [Segment(id, bounds) for id, bounds in parsed_segments_data[serial].items()]
        else:
            segments = []
        series[path] = Series(serial, fields, segments)

    # Now purge the register file
    """with open(config.root_dir / _filename, 'wb') as f:
        for path, s in series.items():
            _write_line(f, _series_marker, (path, s.serial, s.fields))
            for seg in s.segments:
                _write_line(f, _segment_marker, (seg.id, seg.start, seg.end, seg.size))"""

    return series


def print_contents():
    with open(config.root_dir / _filename, 'rb') as f:
        while f:
            try:
                marker, data = _read_line(f)
            except TypeError:
                break
            else:
                if marker == _series_marker:
                    print("SERIES", data)
                elif marker == _segment_marker:
                    print("SEGMENT", data)


def _write_line(file, marker, data):
    data = pickle.dumps(data, protocol=2)
    length = struct.pack('>Q', len(data))
    file.write(marker)
    file.write(length)
    file.write(data)


def _read_line(file):
    marker = file.read(1)
    if marker:
        (length,) = struct.unpack('>Q', file.read(8))
        data = pickle.loads(file.read(length))
        return marker, data
