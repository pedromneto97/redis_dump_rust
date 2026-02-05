#!/bin/bash

../target/release/redis_dump_rust -h localhost -p 6379 -o dump.resp --scan-size 20000 -w 50 -b 5000