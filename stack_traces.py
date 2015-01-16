#!/usr/bin/env python

import argparse
from collections import deque
import fileinput
import re
import sys


parser = argparse.ArgumentParser()
parser.add_argument(
    '-a', '--after', '--after-context',
    dest='after_context',
    type=int,
    help="Number of lines of context to print after each stack trace",
    default=0
)
parser.add_argument(
    '-b', '--before', '--before-context',
    dest='before_context',
    type=int,
    help="Number of lines of context to print before each stack trace",
    default=1
)

parser.add_argument(
    '-C', '--context',
    dest='context',
    type=int,
    help="Number of lines of context to print before and after each stack trace",
    default=0
)

parser.add_argument(
    '-c', '-H', '--hist', '--histogram',
    dest='histogram',
#    type=bool,
    action="store_true",
    help="Whether to output the number of occurrences of each stack trace",
    default=False
)

parser.add_argument(
    '-d', '--descending',
    dest='descending',
    # type=bool,
    action="store_true",
    help="Whether to sort the stack traces in descending order of number of occurrences. Implies '-h'.",
    default=False
)

parser.add_argument(
    '-n', '--num',
    dest='max_num',
    type=int,
    help="Maximum number of stack traces to output",
    default=-1
)

parser.add_argument(
    '-A', '--aggregate-after-lines',
    dest='aggregate_after',
    action="store_true",
    help="Whether to include the [after-context] lines in the uniqueness check when counting stack-trace occurrences. Requires '-h or '-d'.",
    default=False
)

parser.add_argument(
    '-B', '--aggregate-before-lines',
    dest='aggregate_before',
    action="store_true",
    help="Whether to include the [before-context] lines in the uniqueness check when counting stack-trace occurrences. Requires '-h or '-d'.",
    default=False
)

parser.add_argument(
    '-s', '--strip-datetimes', '--sd',
    dest='strip_datetimes',
    action='store_true',
    help="Whether to strip datetimes from after- and before-context lines, to more correctly collapse identical stack-traces/context-lines when counting.",
    default=False
)

parser.add_argument(
    '--strip-numbers', '--sn',
    dest='strip_numbers',
    action='store_true',
    help="Whether to strip all numbers from after- and before-context lines, to more correctly collapse identical stack-traces/context-lines when counting.",
    default=False
)

args, unparsed_args = parser.parse_known_args()
sys.argv = [sys.argv[0]] + unparsed_args

if args.context:
    args.before_context = args.context
    args.after_context = args.context

if args.descending:
    args.histogram = True


def exists(pred, l):
    for e in l:
        if pred(e):
            return True
    return False


stack_trace_start_regexs = [
    r'^\s+at .*?\n',
]

during_stack_trace_line_regexs = [
    r'^Exception.*?\n',
    r'^Caused by:.*?\n',
    r'^\s+.*?\n',
    r'^Driver stacktrace:$',
]


def is_during_stack_trace_line(line):
    return exists(lambda r: re.match(r, line), during_stack_trace_line_regexs)


prev_lines = deque()
num_prev_lines = args.before_context
def push_prev_line(line):
    prev_lines.append(line)
    if len(prev_lines) > num_prev_lines:
        prev_lines.popleft()


class StackTrace:

    def __init__(self, pre_lines):

        self.num_post_lines = args.after_context

        self.include_pre_lines_in_key = args.aggregate_before
        self.include_post_lines_in_key = args.aggregate_after
        self.strip_datetimes_from_key = args.strip_datetimes
        self.strip_numbers_from_key = args.strip_numbers

        transfer_from_idx = len(pre_lines)
        while True:
            transfer_from_idx -= 1

            if transfer_from_idx < 0 or not is_during_stack_trace_line(pre_lines[transfer_from_idx]):
                break

        self.pre_lines = pre_lines[:transfer_from_idx]
        self.lines = pre_lines[transfer_from_idx:]
        self.post_lines = []

        self.is_post_stack = False
        self._stack_str = None


    def add(self, line):
        return self.__add__(line)


    def __add__(self, line):
        self.lines.append(line)
        return self


    def add_post_line(self, line):
        if len(self.post_lines) >= self.num_post_lines:
            return False
        self.post_lines.append(line)
        return True


    def stack_str(self):
        if not self._stack_str:
            self._stack_str = ''.join(
                (self.pre_lines if self.include_pre_lines_in_key else []) +
                self.lines +
                (self.post_lines if self.include_post_lines_in_key else [])
            )

            if self.strip_datetimes_from_key:
                self._stack_str = re.sub(
                    r'[0-9]{2}/[0-9]{2}/[0-9]{2}.[0-9]{2}:[0-9]{2}:[0-9]{2}',
                    'XX/XX/XX XX:XX:XX',
                    self._stack_str
                )

            if self.strip_numbers_from_key:
                self._stack_str = re.sub(r'[0-9]+', 'X', self._stack_str)

            #print '\n***\n%s\n***\n' % self._stack_str

        return self._stack_str


    def __str__(self):
        return ''.join(self.pre_lines + self.lines + self.post_lines)


    def __repr__(self):
        return str(self)


total_num_stacks = 0
def streaming_stack_traces():
    global total_num_stacks

    cur_stack_trace = None
    line_no = 0

    for line in fileinput.input():
        line_no += 1
        if not cur_stack_trace:
            if exists(lambda r: re.match(r, line), stack_trace_start_regexs):
                cur_stack_trace = StackTrace(list(prev_lines)).add(line)
                prev_lines.clear()
            else:
                push_prev_line(line)
        else:
            if is_during_stack_trace_line(line):
                if cur_stack_trace.is_post_stack:
                    total_num_stacks += 1
                    yield cur_stack_trace
                    cur_stack_trace = StackTrace(list(prev_lines)).add(line)
                    prev_lines.clear()
                else:
                    cur_stack_trace += line
            else:
                push_prev_line(line)
                cur_stack_trace.is_post_stack = True
                if not cur_stack_trace.add_post_line(line):
                    total_num_stacks += 1
                    yield cur_stack_trace
                    cur_stack_trace = None

    if cur_stack_trace:
        total_num_stacks += 1
        yield cur_stack_trace



if __name__ == '__main__':
    if args.histogram:
        stack_traces = {}

        for stack_trace in streaming_stack_traces():
            stack_trace_str = stack_trace.stack_str()
            if stack_trace_str not in stack_traces:
                stack_traces[stack_trace_str] = []
            stack_traces[stack_trace_str].append(stack_trace)

        sorted_stacks_and_counts = sorted(stack_traces.iteritems(), key=lambda x: len(x[1]), reverse=args.descending)

        if args.max_num >= 0:
            sorted_stacks_and_counts = sorted_stacks_and_counts[:args.max_num]

        print '%d stacks in total\n' % total_num_stacks
        for stack_trace_str, stack_traces in sorted_stacks_and_counts:
            print '%d occurrences:\n%s' % (len(stack_traces), str(stack_traces[0]))

    else:
        stack_traces = streaming_stack_traces()

        if args.max_num >= 0:
            stack_traces = stack_traces[:args.max_num]

        print '\n'.join(map(str, stack_traces))
