
import gc
from collections import deque

import numpy as np

import config


# write_set will always be a subset of read_set
# last_access.keys() and read_set will always be identical sets
# some elements in queue may not be found in read_set, because they have been deleted
_queue = deque()
_read_set = set([])
_write_set = set([])
_last_access = {}


def memory_consumption():
    mem = 0
    for s in _read_set:
        mem += s.memory_consumption
    return mem


def add_to_queue(segment):
    add_to_queue.counter += 1
    _queue.append((add_to_queue.counter, segment))
    _last_access[segment] = add_to_queue.counter
    _read_set.add(segment)


add_to_queue.counter = 0


def read_op(segment):
    add_to_queue(segment)
    # Remove one segment from memory if the maximum is reached
    if len(_read_set) >= config.max_segments_in_memory:
        # Scan from oldest to newest and pop from the deque
        # Stop when a segment is found that has not been read at a later time.
        # Check if the segment has been modified and release from memory, after committing if necessary
        while _queue and len(_read_set) > config.max_segments_in_memory:
            count, seg = _queue.popleft()
            # check if segment has not been deleted
            if seg in _read_set:
                # check if segment has not been accessed at a later stage
                if _last_access[seg] == count:
                    # To be released from memory
                    if seg in _write_set:
                        # Segment was modified, commit the changes before releasing the memory
                        seg.mem_to_disk()
                        # If syncing has failed (permission denied?), skip the segment
                        if not seg._disk_synced:
                            add_to_queue(seg)
                            continue
                        _write_set.remove(seg)

                    _read_set.remove(seg)
                    del _last_access[seg]

                    # Now release the memory
                    seg.t = None
                    seg.x = None
                    seg._mem_synced = False
                    seg._disk_synced = True
                    gc.collect()
                    break


def write_op(segment):
    _write_set.add(segment)
    read_op(segment)


def commit(n):
    """Commits modified segments by syncing them with the disk

    Args:
        n (int): the number of segments to commit
    """
    n_committed = 0

    candidates = list(_write_set)
    priority = [_last_access[c] for c in candidates]
    segments = [c for _, c in sorted(zip(priority, candidates))]

    for s in segments:
        s.mem_to_disk()
        if s._disk_synced:
            _write_set.remove(s)
            n_committed += 1
            if n_committed >= n:
                break

    return n_committed


def force_commit_all():
    """Commits all modified segments by syncing them with the disk

    Can block if permission to some files is denied by the operating system.
    """
    while _write_set:
        commit(1)
