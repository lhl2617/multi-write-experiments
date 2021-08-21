# Multiple Concurrent Writers Experiment

Experiments involving multiple writers writing to a file, where each write done by a writer must be "atomic" (i.e. two writers don't overwrite/interleave each other), but the sequence of the writes can be different.

## Requirements
- Python 3.8.x+
- Requirements: `pip install -r requirements`

## Scripts 
### `main_append.py`

- One process appending to the file

### `main_appendmmap.py`

- Runnable on systems with `mremap` only (e.g. `Linux`)
- One process writing to an `mmap`-ed file, dynamically resizing as required.

### `main_parallelmmap.py`

- *BROKEN*, to be fixed.
- Runnable on systems with `mremap` only (e.g. `Linux`)
- One sentinel resizing the `mmap`-ed file to write and signalling to worker processes to write in parallel to distinct regions.

### `main_writejoin.py`

- Write to multiple separate files and join in the end.


## Some benchmarks

Run on default args defined in `args.py`.

### System

- GitHub CodeSpace instance
- Ubuntu 20.04.2 LTS
- 4x cores: Intel(R) Xeon(R) Platinum 8272CL CPU @ 2.60GHz
- 8GB memory

### `main_append.py`
```
Trial 1/3: Took 10.774164649999875s
Trial 2/3: Took 10.680391997999322s
Trial 3/3: Took 10.834956190999947s
```
### `main_appendmmap.py`
```
Trial 1/3: Took 12.914175322999654s
Trial 2/3: Took 11.393153299000915s
Trial 3/3: Took 11.439413550000609s
```

### `main_parallelmmap.py`

- N/A, broken

### `main_writejoin.py`
```
Trial 1/3: Took 4.586620705000314s -- write took 3.9665812170005665, join took 0.6200394879997475
Trial 2/3: Took 4.639693348999572s -- write took 3.966349528000137, join took 0.6733438209994347
Trial 3/3: Took 4.664901778000058s -- write took 4.005203402000916, join took 0.6596983759991417
```