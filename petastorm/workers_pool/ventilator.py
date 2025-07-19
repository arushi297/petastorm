#  Copyright (c) 2017-2018 Uber Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import numpy as np
import threading
from abc import ABCMeta, abstractmethod
from time import sleep
import sys

import six

_VENTILATION_INTERVAL = 0.01


@six.add_metaclass(ABCMeta)
class Ventilator(object):
    """Manages items to be ventilated to a worker pool."""

    # def __init__(self, ventilate_fn):
    #     self._ventilate_fn = ventilate_fn

    def __init__(self, workers_pool):
        self._workers_pool = workers_pool
        self._ventilate_fn = self._workers_pool.ventilate

    @abstractmethod
    def start(self):
        """Starts the ventilator, beginning to ventilate to the worker pool after this call.
        Therefore the worker pool must be ready to receive ventilated items."""
        return

    @abstractmethod
    def processed_item(self):
        """A callback for the worker pool to tell the ventilator that it has processed an item from the ventilation
        queue. This allows the ventilator to know how many items are currently on the ventilation queue.
        This function should not have a return value."""

    @abstractmethod
    def completed(self):
        """Returns whether the ventilator has completed ventilating all items it expects to ever ventilate."""
        return

    @abstractmethod
    def stop(self):
        """Tells the ventilator to stop ventilating."""
        return


class ConcurrentVentilator(Ventilator):
    """
    A ConcurrentVentilator handles ventilation of a pre-determined list of items to a worker pool and performs
    the ventilation concurrently in a separate thread. It will keep track of how many items are currently in the
    ventilation queue and prevent it from monotonically increasing in order to prevent boundless memory requirements.
    It allows for multiple (or infinite) iterations of ventilating the items, optionally randomizing the order of
    items being ventilated at the start of each iteration.
    """

    def __init__(self,
                 workers_pool,
                 items_to_ventilate,
                 iterations=1,
                 randomize_item_order=False,
                 random_seed=None):
        """
        Constructor for a concurrent ventilator.

        :param ventilate_fn: The function to be called when ventilating. Usually the worker pool ventilate function.
        :param items_to_ventilate: (``list[dict]``) The list of items to ventilate. Each item is a ``dict`` denoting
                the ``**kwargs`` eventually passed to a worker process function
        :param iterations: (int) How many iterations through items_to_ventilate should be done and ventilated to the
                worker pool. For example if set to 2 each item in items_to_ventilate will be ventilated 2 times. If
                ``None`` is passed, the ventilator will continue ventilating forever.
        :param randomize_item_order: (``bool``) Whether to randomize the item order in items_to_ventilate. This will be
                done on every individual iteration.
        :param random_seed: (``int``) If not None: the random seed used for randomize_item_order. Default: None.
        :param max_ventilation_queue_size: (``int``) The maximum number of items to be stored in the ventilation queue.
                The higher this number, the higher potential memory requirements. By default it will use the size
                of items_to_ventilate since that can definitely be held in memory.
        :param ventilation_interval: (``float`` in seconds) How much time passes between checks on whether something
                can be ventilated (when the ventilation queue is considered full).
        """
        super(ConcurrentVentilator, self).__init__(workers_pool)

        if iterations is not None and (not isinstance(iterations, int) or iterations < 1):
            raise ValueError('iterations must be positive integer or None')

        if not isinstance(items_to_ventilate, list) or any(not isinstance(item, dict) for item in items_to_ventilate):
            raise ValueError('items_to_ventilate must be a list of dicts')

        self._items_to_ventilate = items_to_ventilate
        self._iterations_remaining = iterations if iterations else 100000
        self._randomize_item_order = randomize_item_order
        self._random_seed = random_seed
        self._rng = np.random.default_rng(self._random_seed)
        self._iterations = iterations

        self._ventilation_thread = None
        self._stop_requested = False

    def start(self):
        # Start the ventilation thread
        self._ventilation_thread = threading.Thread(target=self._ventilate, args=())
        self._ventilation_thread.daemon = True
        self._ventilation_thread.start()

    def processed_item(self):
        pass

    def completed(self):
        assert self._iterations_remaining is None or self._iterations_remaining >= 0
        return self._stop_requested or self._iterations_remaining == 0 or not self._items_to_ventilate or self._iterations_remaining == 0

    def reset(self):
        """Will restart the ventilation from the beginning. Currently, we may do this only if the ventilator has
        finished ventilating all its items (i.e. ventilator.completed()==True)
        """
        if not self.completed():
            # Might be hard to solve all race conditions, unless no more ventilation is going on.
            raise NotImplementedError('Reseting ventilator while ventilating is not supported.')

        self._iterations_remaining = self._iterations
        self.start()

    def _ventilate(self):
        # if self._randomize_item_order:
        #     if self._random_seed is not None and self._random_seed != 0:
        #         # Deterministic randomization: use provided seed
        #         self._items_to_ventilate = list(self._rng.permutation(self._items_to_ventilate))
        #     else:
        #         # Non-deterministic randomization: use np.random 
        #         self._items_to_ventilate = list(np.random.permutation(self._items_to_ventilate))

        # while True:
        #     # Stop condition is when no iterations are remaining or there are no items to ventilate
        #     if self.completed():
        #         break

        #     self._ventilate_fn(self._items_to_ventilate)

        #     # print(f"_ventilate _iterations_remaining{self._iterations_remaining} self._items_to_ventilate:{len(self._items_to_ventilate)}")
        #     if self._iterations_remaining is not None:
        #         self._iterations_remaining -= 1
        #     # elif self._iterations_remaining is None:
        #     #     self._iterations_remaining = 0
            
        #     while True:
        #         # sleep(7.31)
        #         if self._workers_pool.completed():
        #             break

        for i in range(self._iterations_remaining):
            if self._randomize_item_order:
                if self._random_seed is not None and self._random_seed != 0:
                    # Deterministic randomization: use provided seed
                    self._items_to_ventilate = list(self._rng.permutation(self._items_to_ventilate))
                else:
                    # Non-deterministic randomization: use np.random 
                    self._items_to_ventilate = list(np.random.permutation(self._items_to_ventilate))

            self._ventilate_fn(self._items_to_ventilate)
            # if self._workers_pool.completed():
            #     break


    def stop(self):
        self._stop_requested = True
        if self._ventilation_thread:
            self._ventilation_thread.join()
            self._ventilation_thread = None
