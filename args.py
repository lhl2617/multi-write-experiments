from tap import Tap

from consts import (
    DEFAULT_LINE_LEN,
    DEFAULT_NUM_LINES_PER_WRITER,
    DEFAULT_NUM_WRITERS,
    DEFAULT_TRIALS,
)

class Args(Tap):
    num_writers: int = DEFAULT_NUM_WRITERS
    num_lines_per_writer: int = DEFAULT_NUM_LINES_PER_WRITER
    line_len: int = DEFAULT_LINE_LEN
    trials: int = DEFAULT_TRIALS
