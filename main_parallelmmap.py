from multiprocess import Queue, Process
from typing import Optional, NewType, Tuple
import os
import shutil
import dataclasses
import time
import math
from consts import REPO_ROOT
from args import Args
from mmap import mmap

WriteQueueT = NewType("WriteQueueT", "Queue[Optional[bytes]]")
SentinelWriterQueueT = NewType(
    "SentinelWriterQueueT", "Queue[Optional[Tuple[bytes, int]]]"
)

out_subdir = "out-parallelmmap"


@dataclasses.dataclass(frozen=True)
class SentinelWriter:
    input_queue: SentinelWriterQueueT
    m: mmap
    id: int

    def start(self):
        while True:
            x = self.input_queue.get()
            if x is None:
                return
            b, pos = x
            # print(self.id + 1, pos, pos + len(b), self.m.size())
            self.m[pos : pos + len(b)] = b


@dataclasses.dataclass(frozen=True)
class Sentinel:

    file_path: str
    num_writers: int
    write_queue: WriteQueueT
    mremap_block_size: int

    def start(self):
        # Write mremap_block_size of \0's so that we're not mmap-ing an empty file
        f = open(self.file_path, "wb")
        f.write(b"\0" * self.mremap_block_size)
        # print(f"==== {self.mremap_block_size}")
        f.close()

        with open(self.file_path, "r+b") as f:
            with mmap(f.fileno(), 0) as m:
                # Prepare sentinel writers
                sentinel_writer_queue: SentinelWriterQueueT = Queue()
                sentinel_writer_procs = [
                    Process(
                        target=SentinelWriter(
                            input_queue=sentinel_writer_queue, m=m, id=i
                        ).start
                    )
                    for i in range(self.num_writers)
                ]
                for p in sentinel_writer_procs:
                    p.start()

                cur_pos = 0
                while True:
                    b = self.write_queue.get()
                    if b is None:
                        m.resize(cur_pos)  # clear trailing \0's
                        for _ in range(self.num_writers):
                            # Hack to make all sentinel writers return
                            sentinel_writer_queue.put(None)
                        for p in sentinel_writer_procs:
                            p.join()
                        print("SENTINEL DONE")
                        return
                    end_pos = cur_pos + len(b)
                    if end_pos > m.size():
                        size_required = end_pos - m.size()
                        blocks_required = math.ceil(
                            size_required / self.mremap_block_size
                        )
                        m.resize(m.size() + self.mremap_block_size * blocks_required)
                        # print(
                        #     f"Resized to {m.size() + self.mremap_block_size * blocks_required}, {end_pos}"
                        # )

                    sentinel_writer_queue.put((b, cur_pos))
                    cur_pos = end_pos


@dataclasses.dataclass(frozen=True)
class QueueWriter:
    num_lines: int
    line_len: int
    write_queue: WriteQueueT

    def start(self):
        for _ in range(self.num_lines):
            self.write_queue.put(os.urandom(self.line_len))


def main():
    args = Args().parse_args()
    for trial in range(args.trials):
        out_dir = os.path.join(REPO_ROOT, out_subdir)
        if os.path.exists(out_dir):
            shutil.rmtree(out_dir)
        os.makedirs(out_dir)

        time_start = time.perf_counter()
        write_queue: WriteQueueT = Queue()

        sentinel_proc = Process(
            target=Sentinel(
                file_path=os.path.join(out_dir, "out.txt"),
                num_writers=args.num_writers,
                write_queue=write_queue,
                mremap_block_size=args.mremap_block_size,
            ).start
        )
        sentinel_proc.start()

        queue_procs = [
            Process(
                target=QueueWriter(
                    num_lines=args.num_lines_per_writer,
                    line_len=args.line_len,
                    write_queue=write_queue,
                ).start
            )
            for _ in range(args.num_writers)
        ]
        for queue_proc in queue_procs:
            queue_proc.start()

        for queue_proc in queue_procs:
            queue_proc.join()

        write_queue.put(None)

        sentinel_proc.join()
        time_end = time.perf_counter()
        time_taken = time_end - time_start
        print(f"Trial {trial+1}/{args.trials}: Took {time_taken}s")


if __name__ == "__main__":
    print("TODO:- This is broken")
    exit(1)
