from multiprocess import Queue, Pool, Process  # type: ignore
from typing import Optional, NewType
import os
import shutil
import dataclasses
import time
from consts import REPO_ROOT
from args import Args
from mmap import mmap, ACCESS_WRITE

WriteQueueT = NewType("WriteQueueT", "Queue[Optional[bytes]]")  # type: ignore

out_subdir = "out-append2"


@dataclasses.dataclass(frozen=True)
class FileAppender:
    file_path: str
    write_queue: WriteQueueT
    def start(self):

        f = open(self.file_path, "wb")
        f.write(b'\0')
        f.close()

        with open(self.file_path, "r+b") as f:
            with mmap(f.fileno(), 0) as m:
                while True:
                    b = self.write_queue.get()
                    if b is None:
                        return
                    append_pos = m.size()
                    m.resize(append_pos + len(b))
                    m[append_pos:] = b


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
        write_queue: "Queue[Optional[bytes]]" = Queue()

        appender_proc = Process(
            target=FileAppender(
                file_path=os.path.join(out_dir, "out.txt"), write_queue=write_queue
            ).start
        )
        appender_proc.start()

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

        appender_proc.join()
        time_end = time.perf_counter()
        time_taken = time_end - time_start
        print(f"Trial {trial+1}/{args.trials}: Took {time_taken}s")


if __name__ == "__main__":
    main()
