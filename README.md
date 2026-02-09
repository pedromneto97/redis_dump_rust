# Redis Dump Rust

## CLI Documentation (English)

Redis Dump Rust is a CLI tool written in Rust for efficiently dumping Redis databases asynchronously, inspired by [redis-dump-go](https://github.com/yannh/redis-dump-go).

### Usage

```bash
redis_dump_rust [OPTIONS]
```

### Options

| Option              | Description                      | Default           |
|---------------------|----------------------------------|-------------------|
| `-h, --host`        | Redis server address              | `127.0.0.1`       |
| `-p, --port`        | Redis server port                 | `6379`            |
| `-a, --auth`        | Redis password                    | -                 |
| `-d, --db`          | Database number                   | All (with data)   |
| `-f, --filter`      | Key pattern filter                | `*`               |
| `-o, --output`      | Output file                       | `redis_dump.resp` |
| `-b, --batch-size`  | Batch size for operations         | `1000`            |
| `-w, --workers`     | Number of parallel workers        | `10`              |
| `-s, --silent`      | Silent mode                       | `false`           |
| `--scan-size`       | Redis scan size                   | `1000`            |

### Example

For larger databases (>1M keys)

```bash
redis_dump_rust --scan-size 20000 -w 50 -b 5000
```

For more help:

```bash
redis_dump_rust --help
```

### Multi-database output

When `--db` is omitted and multiple databases contain keys, the tool dumps each database into a
separate file. The file name is derived from `--output` by inserting `_db{N}` before the file
extension, for example:

- `redis_dump.resp` -> `redis_dump_db0.resp`, `redis_dump_db3.resp`

If only one database has keys, the original output filename is used without a suffix.

