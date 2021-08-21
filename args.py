from tap import Tap


class Args(Tap):
    num_writers: int = 4
    num_lines_per_writer: int = 100000
    line_len: int = 1000
    trials: int = 3
    appendmmap_block_size: int = 4096 * 4096  # block size to initially allocate and extend during mremap
