# Benchmark Profiler

![Image title](https://dummyimage.com/600x400/eee/aaa){ align=left }

```bash
▼ 📂 18.993GB (100.00 %) <ROOT>
└── ▼ 📂 18.993GB (100.00 %) <module>  benchmark.py:35
    └── ▼ 📂 18.993GB (100.00 %) with_pandas  benchmark.py:11
        ├── 📄 18.993GB (100.00 %) read_csv  pandas.io.parsers.readers:1026
        ├── 📄 2.969KB (0.00 %) read_csv  pandas.io.parsers.readers:1009
        └── 📄 706.000B (0.00 %) read_csv  pandas.io.parsers.readers:1013

┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
│ Location                                                                         │ <Total Memory> │ Total Memory % │ Own Memory │ Own Memory % │ Alloc.Count │
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ with_pandas at ./benchmark.py                                                    │       18.993GB │        100.00% │    1.572KB │        0.00% │        8590 │
│ <module> at ./benchmark.py                                                       │       18.993GB │        100.00% │     0.000B │        0.00% │        8590 │
│ read_csv at ./pandas/io/parsers/readers.py                                       │       18.993GB │        100.00% │    4.334KB │        0.00% │        8588 │
│ _read at ./pandas/io/parsers/readers.py                                          │       18.993GB │        100.00% │    3.430KB │        0.00% │        8584 │
│ read at ./pandas/io/parsers/readers.py                                           │       18.993GB │        100.00% │    5.740KB │        0.00% │        8545 │
│ read at ./pandas/io/parsers/c_parser_wrapper.py                                  │       11.560GB │         60.87% │    8.257GB │       43.47% │        8490 │
│ __init__ at ./pandas/core/frame.py                                               │        7.433GB │         39.13% │   988.000B │        0.00% │          52 │
│ dict_to_mgr at ./pandas/core/internals/construction.py                           │        7.433GB │         39.13% │    4.543KB │        0.00% │          51 │
│ arrays_to_mgr at ./pandas/core/internals/construction.py                         │        7.433GB │         39.13% │   762.000B │        0.00% │          16 │
│ create_block_manager_from_column_arrays at ./pandas/core/internals/managers.py   │        7.433GB │         39.13% │   768.000B │        0.00% │          15 │
│ _consolidate at ./pandas/core/internals/managers.py                              │        4.129GB │         21.74% │   544.000B │        0.00% │           7 │
│ _consolidate_inplace at ./pandas/core/internals/managers.py                      │        4.129GB │         21.74% │     0.000B │        0.00% │           7 │
│ _merge_blocks at ./pandas/core/internals/managers.py                             │        4.129GB │         21.74% │    2.065GB │       10.87% │           6 │
│ _concatenate_chunks at ./pandas/io/parsers/c_parser_wrapper.py                   │        3.304GB │         17.39% │    1.133KB │        0.00% │          10 │
│ _form_blocks at ./pandas/core/internals/managers.py                              │        3.304GB │         17.39% │     0.000B │        0.00% │           7 │
│ concat_compat at ./pandas/core/dtypes/concat.py                                  │        3.304GB │         17.39% │    3.304GB │       17.39% │           9 │
│ _stack_arrays at ./pandas/core/internals/managers.py                             │        3.304GB │         17.39% │    3.304GB │       17.39% │           5 │
│ vstack at ./numpy/_core/shape_base.py                                            │        2.065GB │         10.87% │    2.065GB │       10.87% │           3 │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

```

```bash
▼ 📂 3.137GB (100.00 %) <ROOT>
└── ▼ 📂 526.513MB (16.39 %) <module>  benchmark.py:37
    ├── ▼ 📂 525.916MB (16.37 %) with_pandas_lazy  benchmark.py:27
    │   └── 📄 525.916MB (16.37 %) collect  pandas_lazy/frame.py:41
    ├── ▶ 📂 595.623KB (0.02 %) with_pandas_lazy  benchmark.py:22
    ├── ▶ 📂 8.219KB (0.00 %) with_pandas_lazy  benchmark.py:26
    ├── ▶ 📂 3.727KB (0.00 %) with_pandas_lazy  benchmark.py:24
    ├── ▶ 📂 3.133KB (0.00 %) with_pandas_lazy  benchmark.py:25
    └── ▶ 📂 392.000B (0.00 %) with_pandas_lazy  benchmark.py:23

┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
│ Location                                     │ <Total Memory> │ Total Memory % │ Own Memory │ Own Memory % │ Alloc.Count │
│━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ ??? at ???                                   │        2.622GB │         83.61% │    2.622GB │       83.61% │       50427 │
│ with_pandas_lazy at ./benchmark.py           │      526.513MB │         16.39% │   520.000B │        0.00% │        7156 │
│ <module> at ./benchmark.py                   │      526.513MB │         16.39% │     0.000B │        0.00% │        7156 │
│ collect at ./pandas_lazy/frame.py            │      525.916MB │         16.37% │  525.916MB │       16.37% │        6872 │
│ read_csv at ./pandas_lazy/reader.py          │      595.623KB │          0.02% │  595.623KB │        0.02% │         191 │
│ drop_duplicates at ./pandas_lazy/frame.py    │        8.219KB │          0.00% │    6.701KB │        0.00% │          33 │
│ __setitem__ at ./pandas_lazy/frame.py        │        6.227KB │          0.00% │    6.227KB │        0.00% │          53 │
│ uuid1 at ./uuid.py                           │        1.518KB │          0.00% │    1.518KB │        0.00% │           1 │
│ __getitem__ at ./pandas_lazy/frame.py        │       392.000B │          0.00% │   392.000B │        0.00% │           5 │
│ astype at ./pandas_lazy/column.py            │       128.000B │          0.00% │   128.000B │        0.00% │           1 │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

```


