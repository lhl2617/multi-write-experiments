from multiprocess import Process  # type: ignore
import os
import shutil
import dataclasses
import time
from typing import List
from consts import (
    REPO_ROOT,
)
from args import Args

out_subdir = "out-writejoin"


@dataclasses.dataclass(frozen=True)
class Writer:
    num_lines: int
    line_len: int
    file_path: str

    def start(self):
        with open(self.file_path, "ab") as f:
            for _ in range(self.num_lines):
                f.write(os.urandom(self.line_len))


@dataclasses.dataclass(frozen=True)
class Joiner:
    out_file_path: str
    in_file_paths: List[str]

    def start(self):
        with open(self.out_file_path, "ab") as out_file:
            for in_file_path in self.in_file_paths:
                with open(in_file_path, "rb") as in_file:
                    out_file.write(in_file.read())


def main():
    args = Args().parse_args()
    for trial in range(args.trials):
        out_dir = os.path.join(REPO_ROOT, "out-writejoin")
        if os.path.exists(out_dir):
            shutil.rmtree(out_dir)
        os.makedirs(out_dir)

        time_start = time.perf_counter()

        writer_procs = [
            Process(
                target=Writer(
                    num_lines=args.num_lines_per_writer,
                    line_len=args.line_len,
                    file_path=os.path.join(out_dir, f"{i}.txt"),
                ).start
            )
            for i in range(args.num_writers)
        ]
        for writer_proc in writer_procs:
            writer_proc.start()

        for writer_proc in writer_procs:
            writer_proc.join()

        Joiner(
            out_file_path=os.path.join(out_dir, "out.txt"),
            in_file_paths=[
                os.path.join(out_dir, f"{i}.txt") for i in range(args.num_writers)
            ],
        ).start()
        time_end = time.perf_counter()
        time_taken = time_end - time_start
        print(f"Trial {trial+1}/{args.trials}: Took {time_taken}s")


if __name__ == "__main__":
    main()
