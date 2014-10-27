#!/usr/bin/python

import argparse
from collections import deque
import fileinput
import re
import sys


parser = argparse.ArgumentParser()
parser.add_argument(
    '-a', '-A', '--after', '--after-context',
    dest='after_context',
    type=int,
    help="Number of lines of context to print after each stack trace",
    default=0
)
parser.add_argument(
    '-b', '-B', '--before', '--before-context',
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


stack_trace_line_regexs = [
    r'^Exception.*?\n',
    r'^Caused by:.*?\n',
    r'^\s+at .*?\n',
    r'^\s+\.{3} [0-9]+ more\n',
]


def is_stack_trace_line(line):
    return exists(lambda r: re.match(r, line), stack_trace_line_regexs)


prev_lines = deque()
num_prev_lines = args.before_context
def push_prev_line(line):
    prev_lines.append(line)
    if len(prev_lines) > num_prev_lines:
        prev_lines.popleft()


class StackTrace:

    def __init__(self, pre_lines, num_post_lines=args.after_context):
        self.pre_lines = pre_lines
        self.num_post_lines = num_post_lines

        self.lines = []
        self.post_lines = []

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
            self._stack_str = ''.join(self.lines)
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
            if is_stack_trace_line(line):
                cur_stack_trace = StackTrace(list(prev_lines)).add(line)
                prev_lines.clear()
            else:
                push_prev_line(line)
        else:
            if is_stack_trace_line(line):
                cur_stack_trace += line
            else:
                push_prev_line(line)
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
            if stack_trace not in stack_traces:
                stack_traces[stack_trace] = 0
            stack_traces[stack_trace] += 1

        sorted_stacks_and_counts = sorted(stack_traces.iteritems(), key=lambda x: x[1], reverse=args.descending)

        if args.max_num >= 0:
            sorted_stacks_and_counts = sorted_stacks_and_counts[:args.max_num]

        print '%d stacks in total\n' % total_num_stacks
        for stack_trace, count in sorted_stacks_and_counts:
            print '%d occurrences:\n%s' % (count, stack_trace)

    else:
        stack_traces = streaming_stack_traces()

        if args.max_num >= 0:
            stack_traces = stack_traces[:args.max_num]

        print '\n'.join(map(str, stack_traces))
