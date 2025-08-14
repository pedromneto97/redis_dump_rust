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
| `-d, --db`          | Database number                   | All               |
| `-f, --filter`      | Key pattern filter                | `*`               |
| `-o, --output`      | Output file                       | `redis_dump.txt`  |
| `-b, --batch-size`  | Batch size for operations         | `1000`            |
| `-w, --workers`     | Number of parallel workers        | `10`              |
| `-s, --silent`      | Silent mode                       | `false`           |

### Example

Dump all keys from the default Redis server:

```bash
redis_dump_rust
```

Dump keys from a specific database with a filter:

```bash
redis_dump_rust --db 1 --filter "user:*" --output users.txt
```

For more help:

```bash
redis_dump_rust --help
```

