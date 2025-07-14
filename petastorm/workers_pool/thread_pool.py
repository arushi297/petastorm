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
# import pdb; pdb.set_trace()

import cProfile
import logging
import pstats
import random
import sys
import os
from threading import Thread, Event
from traceback import format_exc

from six.moves import queue

from petastorm.workers_pool import EmptyResultError, VentilatedItemProcessedMessage

# Defines how frequently will we check the stop event while waiting on a blocking queue
IO_TIMEOUT_INTERVAL_S = 0.001
# Amount of time we will wait on a the queue to get the next result. If no results received until then, we will
# recheck if no more items are expected to be ventilated
_VERIFY_END_OF_VENTILATION_PERIOD = 0.1


class WorkerTerminationRequested(Exception):
    """This exception will be raised if a thread is being stopped while waiting to write to the results queue."""


class WorkerThread(Thread):
    """Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition."""

    def __init__(self, worker_impl, stop_event, ventilator_queue, results_queue, profiling_enabled=False):
        super(WorkerThread, self).__init__()
        self._stop_event = stop_event
        self._worker_impl = worker_impl
        self._ventilator_queue = ventilator_queue
        self._results_queue = results_queue
        self._profiling_enabled = profiling_enabled
        if profiling_enabled:
            self.prof = cProfile.Profile()

    def run(self):
        if self._profiling_enabled:
            self.prof.enable()
        # Loop and accept messages from both channels, acting accordingly
        while True:
            # Check for stop event first to prevent erroneous reuse
            if self._stop_event.is_set():
                break
            # If the message came from work_receiver channel
            try:
                #print(f'Arushi: Get from Ventilator Queue: {self._ventilator_queue.queue}')
                item = self._ventilator_queue.get(block=True, timeout=IO_TIMEOUT_INTERVAL_S)
                # Mark worker as busy when processing
                self._worker_impl.thread_pool._set_worker_busy(self._worker_impl.worker_id)
                self._worker_impl.process(**item)
                self._worker_impl.publish_func(VentilatedItemProcessedMessage())
                # Mark worker as idle after processing
                self._worker_impl.thread_pool._set_worker_idle(self._worker_impl.worker_id)
            except queue.Empty:
                # Mark worker as idle when waiting
                self._worker_impl.thread_pool._set_worker_idle(self._worker_impl.worker_id)
                pass
            except WorkerTerminationRequested:
                pass
            except Exception as e:  # pylint: disable=broad-except
                stderr_message = 'Worker %d terminated: unexpected exception:\n' % self._worker_impl.worker_id
                stderr_message += format_exc()
                sys.stderr.write(stderr_message)
                self._results_queue.put(e)
                break
        if self._profiling_enabled:
            self.prof.disable()


class ThreadPool(object):
    def __init__(self, workers_count, results_queue_size=50, profiling_enabled=False, shuffle_rows=False, seed=None):
        """Initializes a thread pool.

        TODO: consider using a standard thread pool
        (e.g. http://elliothallmark.com/2016/12/23/requests-with-concurrent-futures-in-python-2-7/ as an implementation)

        Originally implemented our own pool to match the interface of ProcessPool (could not find a process pool
        implementation that would not use fork)

        :param workers_count: Number of threads
        :param profiling_enabled: Whether to run a profiler on the threads
        :param shuffle_rows: Whether to shuffle rows (affects round-robin behavior)
        :param seed: Random seed for deterministic behavior
        """
        #print(f"Arushi: ThreadPool.__init__ called with workers_count={workers_count}, shuffle_rows={shuffle_rows}, seed={seed}")
        # sys.stdout.flush()

        self._workers = []
        self._ventilator_queues = None
        self.workers_count = workers_count
        self._results_queue_size = results_queue_size
        # Worker threads will watch this event and gracefully shutdown when the event is set
        self._stop_event = Event()
        self._profiling_enabled = profiling_enabled
        self._shuffle_rows = shuffle_rows
        self._seed = seed

        self._ventilated_items = 0
        self._ventilated_items_processed = 0
        self._ventilator = None
        
        # Round-robin consumer thread
        self._round_robin_thread = None
        
        # Worker status tracking
        self._worker_status = [False] * workers_count  # False = idle, True = busy
  
    def start(self, worker_class, worker_args=None, ventilator=None):
        """Starts worker threads.

        :param worker_class: A class of the worker class. The class will be instantiated in the worker process. The
          class must implement :class:`.WorkerBase` protocol
        :param worker_setup_args: Argument that will be passed to ``args`` property of the instantiated
          :class:`.WorkerBase`
        :return: ``None``
        """
        #print(f"Arushi: ThreadPool.start() called for instance {id(self)}")
        #print(f"Arushi: worker_class={worker_class}, worker_args={worker_args}, ventilator={ventilator}")
        
        # Verify stop_event and raise exception if it's already set!
        if self._stop_event.is_set():
            error_msg = f'ThreadPool({len(self._workers)}) cannot be reused! stop_event set? {self._stop_event.is_set()}'
            #print(f"Arushi: ERROR - {error_msg}")
            raise RuntimeError(error_msg)

        #print(f"Arushi: Setting up ventilator queues for {self.workers_count} workers")
        
        # Set up a channel to send work
        self._ventilator_queues = [queue.Queue() for _ in range(self.workers_count)]
        # Todo: Update the results queue size
        self._results_queues = [queue.Queue(5) for _ in range(self.workers_count)]
        self._shared_results_queue = queue.Queue(25)
        self._workers = []
        
        #print(f"Arushi: Creating {self.workers_count} worker threads")
        
        for worker_id in range(self.workers_count):
            # Create a closure that captures the worker_id for this specific worker
            def make_publish_func(worker_id):
                return lambda data: self._stop_aware_put(data, worker_id)
            
            worker_impl = worker_class(worker_id, make_publish_func(worker_id), worker_args)
            # Add thread_pool reference to worker for status tracking
            worker_impl.thread_pool = self
            
            new_thread = WorkerThread(worker_impl, self._stop_event, self._ventilator_queues[worker_id],
                                      self._results_queues[worker_id], self._profiling_enabled)
            # Make the thread daemonic. Since it only reads it's ok to abort while running - no resource corruption
            # will occur.
            new_thread.daemon = True
            self._workers.append(new_thread)
            
            #print(f"Arushi: Created worker thread {worker_id}")

        #print(f"Arushi: Starting {len(self._workers)} worker threads")
        # breakpoint()
        # Spin up all worker threads
        for i, w in enumerate(self._workers):
            #print(f"Arushi: Starting worker thread {i}")
            w.start()
            #print(f"Arushi: Worker thread {i} started, alive={w.is_alive()}")

        # Start round-robin consumer thread
        #print("Before starting round-robin consumer thread")
        
        self._round_robin_thread = Thread(target=self._round_robin_consumer, daemon=True)
        
        #print("After creating round-robin consumer thread")
        
        # TODO: Remove this once we have a working round-robin consumer thread
        self._round_robin_thread.start()
        
        #print(f"Arushi: Round-robin thread started, alive={self._round_robin_thread.is_alive()}")
        # breakpoint()
        if ventilator:
            #print(f"Arushi: Starting ventilator")
            self._ventilator = ventilator
            self._ventilator.start()
            #print(f"Arushi: Ventilator started")
        
        #print(f"Arushi: ThreadPool.start() completed for instance {id(self)}")

    def _set_worker_busy(self, worker_id):
        """Mark worker as busy (processing work)."""
        self._worker_status[worker_id] = True

    def _set_worker_idle(self, worker_id):
        """Mark worker as idle (waiting for work)."""
        self._worker_status[worker_id] = False

    def _is_worker_idle(self, worker_id):
        """Check if worker is idle."""
        return not self._worker_status[worker_id]

    def ventilate(self, items_to_ventilate):
        """Sends a work item to a worker process. Will result in ``worker.process(...)`` call with arbitrary arguments.
        """
        #print(f"Arushi: ventilate() called with {len(items_to_ventilate)} items, instance ID: {id(self)}, workers_count: {self.workers_count}, instance: {self}")
        # sys.stdout.flush()

        #print(f"Arushi: ventilate() called with items_to_ventilate: {items_to_ventilate}")
        # sys.stdout.flush()

        # breakpoint()
        for i, item in enumerate(items_to_ventilate):
            #print(f"Arushi: ventilate() loop, item: {item}, i: {i}, self._ventilator_queues: {self._ventilator_queues}")
            worker_id = i % self.workers_count
            self._ventilator_queues[worker_id].put(item)
            self._ventilated_items += 1
            #print(f"Arushi: Ventilated item {i} to worker {worker_id}")
        
         
        print(f"Arushi: ventilate() completed. Total ventilated items: {self._ventilated_items}, queue lengths: q1: {len(self._ventilator_queues[0].queue)}, q2: {len(self._ventilator_queues[1].queue)}, q3: {len(self._ventilator_queues[2].queue)}, q4: {len(self._ventilator_queues[3].queue)}")

    def all_workers_done(self):
        for worker_id in range(self.workers_count):
            print(f"Arushi: all_workers_done() loop, worker_id: {worker_id}, self._results_queues[worker_id].empty(): {self._results_queues[worker_id].empty()}, self._is_worker_idle(worker_id): {self._is_worker_idle(worker_id)}, self._ventilator_queues[worker_id].empty(): {self._ventilator_queues[worker_id].empty()}")
            if not self._results_queues[worker_id].empty() or not self._is_worker_idle(worker_id) or not self._ventilator_queues[worker_id].empty():
                return False
        return True
    
    def completed(self):
        print(f"Arushi: completed() loop, all_workers_done: {self.all_workers_done()}, self._shared_results_queue.empty(): {self._shared_results_queue.empty()}, shared_results_queue_size: {self._shared_results_queue.qsize()}, self._ventilator: {self._ventilator}, self._ventilator.completed(): {self._ventilator.completed()}")
        # If all workers are done and shared queue is empty, raise EmptyResultError
        if self.all_workers_done() and self._shared_results_queue.empty():
            if not self._ventilator or self._ventilator.completed():
                return True
            
        return False


    def get_results(self):
        """Returns results from worker pool or re-raise worker's exception if any happen in worker thread.

        :param timeout: If None, will block forever, otherwise will raise :class:`.TimeoutWaitingForResultError`
            exception if no data received within the timeout (in seconds)

        :return: arguments passed to ``publish_func(...)`` by a worker. If no more results are anticipated,
                 :class:`.EmptyResultError`.
        """
        #print(f"Arushi: get_results() called")

        while True:
            #print(f"Arushi: get_results() loop, self._results_queues: {self._results_queues}, self._shared_results_queue: {self._shared_results_queue}, self._ventilator: {self._ventilator}, self._ventilator_queues: {self._ventilator_queues}, self._ventilated_items_processed: {self._ventilated_items_processed}, self._ventilated_items: {self._ventilated_items}")
            # breakpoint()
            # Check termination condition: all workers are truly done
            print(f"Arushi: get_results() loop, self.completed(): {self.completed()}")
            if self.completed():
                raise EmptyResultError()

            try:
                result = self._shared_results_queue.get(timeout=_VERIFY_END_OF_VENTILATION_PERIOD)
                # if isinstance(result, VentilatedItemProcessedMessage):
                #     self._ventilated_items_processed += 1
                #     # if self._ventilator:
                #     #     self._ventilator.processed_item()
                if isinstance(result, Exception):
                    #print(f"Arushi: Worker exception received: {result}")
                    self.stop()
                    self.join()
                    raise result
                else:
                    print(f"Arushi: Returning result from get_results(), result: {result}")
                    return result
            except queue.Empty:
                continue

    def stop(self):
        """Stops all workers (non-blocking)."""
        #print(f"Arushi: stop() called for instance {id(self)}")
        
        if self._ventilator:
            self._ventilator.stop()
        if self._round_robin_thread:
            self._round_robin_thread.stop()
        self._stop_event.set()
        
        #print(f"Arushi: stop() completed - stop_event set")

    def join(self):
        """Block until all workers are terminated."""
        #print(f"Arushi: join() called for instance {id(self)}")
        
        for i, w in enumerate(self._workers):
            if w.is_alive():
                #print(f"Arushi: Joining worker thread {i}")
                w.join()
                #print(f"Arushi: Worker thread {i} joined")

        if self._profiling_enabled:
            # If we have profiling set, collect stats and #print them
            stats = None
            for w in self._workers:
                if stats:
                    stats.add(w.prof)
                else:
                    stats = pstats.Stats(w.prof)
            stats.sort_stats('cumulative').print_stats()
        
        #print(f"Arushi: join() completed")

    def _stop_aware_put(self, data, worker_id):
        """This method is called to write the results to the results queue. We use ``put`` in a non-blocking way so we
        can gracefully terminate the worker thread without being stuck on :func:`Queue.put`.

        The method raises :class:`.WorkerTerminationRequested` exception that should be passed through all the way up to
        :func:`WorkerThread.run` which will gracefully terminate main worker loop."""
        while True:
            try:
                self._results_queues[worker_id].put(data, block=True, timeout=IO_TIMEOUT_INTERVAL_S)
                return
            except queue.Full:
                pass

            if self._stop_event.is_set():
                raise WorkerTerminationRequested()

    def _round_robin_consumer(self):
        """Round-robin consumer that takes items from each worker's queue in strict round-robin order
        and puts them into the shared results queue."""
        #print(f"Arushi: _round_robin_consumer() ENTERED for instance {id(self)}")
        
        current_worker = 0
        
        # Determine if we should use non-blocking behavior
        use_non_blocking = self._shuffle_rows and (self._seed is None or self._seed == 0)
        #print(f"Arushi: Round-robin consumer started. shuffle_rows={self._shuffle_rows}, seed={self._seed}, use_non_blocking={use_non_blocking}")
        
        # Log to file as well
        #print(f"Arushi: Round-robin consumer started with {self.workers_count} workers")
        
        iteration_count = 0
        while not self._stop_event.is_set():
            iteration_count += 1
            # if iteration_count % 100 == 0:  # Log every 100 iterations to avoid spam
            #     #print(f"Arushi: Round-robin iteration {iteration_count}, current_worker={current_worker}, stop_event={self._stop_event.is_set()}")
            
            try:
                if self.all_workers_done():
                    print("Arushi: ROUND-ROBIN CONSUMER - Completed")
                    break
                # Check if current worker should be skipped
                should_skip = (
                    self._results_queues[current_worker].empty() and  # Worker result queue is empty
                    self._is_worker_idle(current_worker) and          # Worker is idle
                    self._ventilator_queues[current_worker].empty()   # Ventilator queue for worker is empty
                )
                print(f"Arushi: ROUND-ROBIN CONSUMER - should_skip: {should_skip}, current_worker: {current_worker}, self._results_queues[current_worker].empty(): {self._results_queues[current_worker].empty()}, self._is_worker_idle(current_worker): {self._is_worker_idle(current_worker)}, self._ventilator_queues[current_worker].empty(): {self._ventilator_queues[current_worker].empty()}")
                
                if should_skip:
                    # Skip this worker and move to next
                    #print(f"Arushi: ROUND-ROBIN VIOLATION - Skipping worker {current_worker} (empty={self._results_queues[current_worker].empty()}, idle={self._is_worker_idle(current_worker)}, ventilator_empty={self._ventilator_queues[current_worker].empty()})")
                    current_worker = (current_worker + 1) % self.workers_count
                    continue
                
                # Try to get an item from the current worker's queue
                if use_non_blocking:
                    # Non-blocking: try to get item without waiting
                    try:
                        item = self._results_queues[current_worker].get(block=False)
                        print(f"Arushi: ROUND-ROBIN FOLLOWED - Got item from worker {current_worker} (non-blocking mode)")
                    except queue.Empty:
                        # No item available, move to next worker immediately
                        #print(f"Arushi: ROUND-ROBIN VIOLATION - Worker {current_worker} queue empty, moving to next (non-blocking mode)")
                        current_worker = (current_worker + 1) % self.workers_count
                        continue
                else:
                    # Blocking: wait for item (strict round-robin)
                    print(f"Arushi: ROUND-ROBIN FOLLOWED - Waiting for item from worker {current_worker} (blocking mode)")
                    # Timeout is in seconds
                    item = self._results_queues[current_worker].get(block=True, timeout=5.0)
                    print(f"Arushi: ROUND-ROBIN FOLLOWED - Got item from worker {current_worker} (blocking mode)")
                
                # Put the item into the shared results queue
                self._shared_results_queue.put(item, block=False)
                print(f"Arushi: ROUND-ROBIN FOLLOWED - Put item from worker {current_worker} into shared queue")
                
                # Move to next worker in round-robin fashion
                current_worker = (current_worker + 1) % self.workers_count
            except queue.Empty:
                # No item available from current worker, move to next
                print(f"Arushi: ROUND-ROBIN VIOLATION - Worker {current_worker} queue empty after timeout, moving to next")
                current_worker = (current_worker + 1) % self.workers_count
                continue
            except queue.Full:
                # Shared queue is full, wait a bit and try again
                #print(f"Arushi: ROUND-ROBIN VIOLATION - Shared queue full, retrying worker {current_worker}")
                continue
            except Exception as e:
                # Any other exception, continue to next worker
                print(f"Arushi: ROUND-ROBIN VIOLATION - Exception for worker {current_worker}: {e}")
                current_worker = (current_worker + 1) % self.workers_count
                continue
        
    def results_qsize(self):
        return self._shared_results_queue.qsize()

    @property
    def diagnostics(self):
        return {'output_queue_size': self.results_qsize()}
