"""
Time-Boxed Multi-Processing Engine Demonstration

This code is meant to demonstrate using the python multiprocessing library
to create a multi-processing engine that will work on X number of processes at once
but that will only run as many total processes as can be completed within an
allotted time box.

So for instance, a pool of 10 processes may be selected to work through 100
different tasks that need to be accomplished. But in the current run the
user only wants to run enough of the tasks to fill 15 minutes of time.

The processing engine will load up the current multiprocessing pool with the
first 10 tasks. Then it will wait for the first one to complete. At that point
it will remove the completed task and replace it with the next un-executed
task.

The processing engine will stop running when one of the following conditions
is met:
1. There are no more tasks to execute because they have all been completed.
2. The max allotted time has elapsed.

In the case of hitting the max alloted time, the engine will stop adding new
tasks to the multiprocessing pool and wait for the tasks in the current
pool to run to completion.
"""

import argparse
import logging
import bunch
import os
import random
import sys
import time
import traceback

from datetime import datetime, timedelta
from multiprocessing import Pool

ACCEPTED_DATE_FORMAT = '%Y-%m-%d'

DEFAULT_FORMAT_STRING = '%(asctime)-20s %(levelname)-8s :: %(name)s :: %(message)s'
DEFAULT_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

CRITICAL = logging.CRITICAL
ERROR = logging.ERROR
WARNING = logging.WARNING
INFO = logging.INFO
DEBUG = logging.DEBUG
POSSIBLE_LOG_LEVEL_VALUES = [CRITICAL, ERROR, WARNING, INFO, DEBUG]
POSSIBLE_LOG_LEVEL_DICTIONARY = {'CRITICAL': CRITICAL, 'ERROR': ERROR, 'WARNING': WARNING, 'INFO': INFO, 'DEBUG': DEBUG}

def get_new_logger(name=None, level='INFO', format_string=DEFAULT_FORMAT_STRING, date_format=DEFAULT_DATE_FORMAT):
    """
    Helper function to set up a new logging instance
    """
    new_logger = logging.getLogger(name)

    if new_logger.handlers:
        return new_logger

    stdout_handler = logging.StreamHandler(sys.stdout)

    stdout_formatter = logging.Formatter(fmt=format_string, datefmt=date_format)
    stdout_handler.setFormatter(stdout_formatter)

    new_logger.addHandler(stdout_handler)

    new_logger.setLevel(level)
    for handler in new_logger.handlers:
        handler.setLevel(level)
    return new_logger


def process_task((args, process_date)):
    """
    This is the primary processing function where each individual tasks
    are processed. For this demo code we will just set a sleep timer with
    a random amount of time between the min_time and max_time arguments.
    """
    log = get_new_logger(process_date, args.logging_level)
    try:
        log.info('Starting to work on data for {process_date}'.format(process_date=process_date))
        start_time = time.time()

        sleep_time = random.randint(args.min_task_time, args.max_task_time)
        log.debug('Total duration of work is set to {sleep_time}.'.format(sleep_time=sleep_time))
        time.sleep(sleep_time)

        end_time = time.time()
        duration = round(end_time - start_time)
        log.info('Completed work for {process_date} in {duration} seconds.'.format(process_date=process_date, duration=duration))
    except:
        log.error(traceback.format_exc())
        raise

    return True


def collect_task_dict(args, log):
    """
    Create the list of tasks that we have to process.
    This list is what we will start processing from but
    the processing will stop once we hit our time-box duration.
    """
    task_dict = {}

    # Determine which dates we need to process for
    date_list = []
    curr_date = args.start_date
    while curr_date <= args.end_date:
        date_list.append(curr_date.strftime(ACCEPTED_DATE_FORMAT))
        curr_date = curr_date + timedelta(days=1)

    for process_date in date_list:
        task_dict[process_date] = (args, process_date)

    log.info('There are {count} process dates lined up to be worked on.'.format(count=len(task_dict)))

    return task_dict



def process_args(args, log):
    """
    This is the starting point of the data processing. Here we determine how many
    individual dates that have data there are and then create individual processes
    for each date.
    """
    log.info('Starting the multi-process now')

    task_dict = collect_task_dict(args, log)

    # Determine when our max end time will be
    start_time = time.time()
    end_time = start_time + args.max_execution_seconds

    # Set up the lists that will be used to track each process
    processing_dict = {}
    done_dict = {}
    not_done_dict = {}

    # Now start processing
    pl = Pool(args.processes)
    while True:

        # If there is room add a task to the pool
        while len(processing_dict) < args.processes and len(task_dict) > 0:
            task_date = sorted(task_dict.keys())[0]
            curr_task = task_dict.pop(task_date)
            processing_dict[task_date] = pl.apply_async(process_task, (curr_task,))
            log.info('Added the {date} task to the PROCESSING list'.format(date=task_date))
            log.debug('not started: {task} - processing: {proc} - done: {done} - incomplete: {inc} - total: {total}'
                      .format(task=len(task_dict), proc=len(processing_dict), done=len(done_dict), inc=len(not_done_dict),
                              total=len(task_dict) + len(processing_dict) + len(done_dict) + len(not_done_dict)))

        # Add a bit of delay here
        time.sleep(1)

        # Check if any task is done yet or not
        for proc_date in processing_dict.keys():
            if processing_dict[proc_date].ready():
                done_dict[proc_date] = processing_dict.pop(proc_date, None)
                log.info('Moving the {date} task from PROCESSING to DONE'.format(date=proc_date))
                log.debug('not started: {task} - processing: {proc} - done: {done} - incomplete: {inc} - total: {total}'
                          .format(task=len(task_dict), proc=len(processing_dict), done=len(done_dict), inc=len(not_done_dict),
                                  total=len(task_dict) + len(processing_dict) + len(done_dict) + len(not_done_dict)))
                # If the processing was not successful get the results which may include a thrown exception
                if not done_dict[proc_date].successful():
                    done_dict[proc_date].get()

       # Check if we have reached the end of our list or the requested end of processing time
        time_to_exit = False
        remaining_tasks = len(task_dict) + len(processing_dict)
        timeout = end_time - time.time()

        if remaining_tasks == 0:
            log.info('There are no more tasks to process. Will now start exiting.')
            time_to_exit = True

        if timeout < 0:
            log.info('Have reached the end of the overall alloted time. Will now start exiting.')
            time_to_exit = True

        # If it is time to exit then clean up the task list and break out
        if time_to_exit:
            for task_date in task_dict.keys():
                not_done_dict[task_date] = task_dict.pop(task_date)
                log.info('Moved the {date} task to the NOT DONE list'.format(date=task_date))
                log.debug('not started: {task} - processing: {proc} - done: {done} - incomplete: {inc} - total: {total}'
                          .format(task=len(task_dict), proc=len(processing_dict), done=len(done_dict), inc=len(not_done_dict),
                                  total=len(task_dict) + len(processing_dict) + len(done_dict) + len(not_done_dict)))
            # Break out of the while as long as there are no more tasks that have not started
            if len(task_dict) == 0:
                break # while True

    # At the end we need to wait for the final processing tasks to complete
    while True:
        for proc_date in processing_dict.keys():
            if processing_dict[proc_date].ready():
                done_dict[proc_date] = processing_dict.pop(proc_date, None)
                log.info('Moving the {date} task from PROCESSING to DONE'.format(date=proc_date))
                log.debug('not started: {task} - processing: {proc} - done: {done} - incomplete: {inc} - total: {total}'
                          .format(task=len(task_dict), proc=len(processing_dict), done=len(done_dict), inc=len(not_done_dict),
                                  total=len(task_dict) + len(processing_dict) + len(done_dict) + len(not_done_dict)))
                # If the processing was not successful get the results which may include a thrown exception
                if not done_dict[proc_date].successful():
                    done_dict[proc_date].get()
        # If there are finally no more processing tasks we can exit
        if len(processing_dict) == 0:
            break # while True

    return True

def main(args):
    """
    This is the main entry point.
    """
    log = get_new_logger('root      ', args.logging_level)

    validate_parameters(args, log)

    process_args(args, log)

    exit(0)


def validate_parameters(args, log):
    """
    Extra validation of the argparse arguments
    """
    log.debug('Validating command line arguments.')

    if args.processes <= 0:
        raise ValueError('--processes must be > 0')

    if args.max_execution_seconds <= 0:
        raise ValueError('--max-execution-seconds must be > 0')

    if args.end_date < args.start_date:
        raise ValueError('--end-date must be >= to --start-date')

    if args.max_task_time < args.min_task_time:
        raise ValueError('--max-task-time must be >= to --min-task-time')


def is_valid_date(s):
    try:
        return datetime.strptime(s, ACCEPTED_DATE_FORMAT)
    except ValueError:
        raise argparse.ArgumentTypeError('Not a valid date matching {form} format.'.format(form=ACCEPTED_DATE_FORMAT))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Demonstrate a time-boxed multi-processing engine")

    parser.add_argument(
        '--processes',
        default=1,
        type=int,
        help='number of processes to add to the pool'
    )
    parser.add_argument(
        '--max-execution-seconds',
        default=100,
        type=int,
        help='number of seconds before the the engine stops queing tasks'
    )
    parser.add_argument(
        '--start-date',
        type=is_valid_date,
        required=True,
        help='earliest date to process'
    )
    parser.add_argument(
        '--end-date',
        type=is_valid_date,
        required=True,
        help='latest date to process'
    )
    parser.add_argument(
        '--min-task-time',
        default=1,
        type=int,
        help='minimum number of seconds each task will run for'
    )
    parser.add_argument(
        '--max-task-time',
        default=10,
        type=int,
        help='maximum number of seconds each task will run for'
    )

    parser.add_argument(
        '--logging-level',
        choices=('CRITICAL','ERROR','WARNING','INFO','DEBUG'),
        default='INFO',
        help='determines the level of output logging you will see'
    )

    args = parser.parse_args()
    main(args)
