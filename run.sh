#!/bin/sh

input="./sample.d/input.parquet"

geninput() {
	echo generating a sample input file...

	mkdir -p ./sample.d

	printf '%s,%s,%s,%s,%s,%s,%s\n' \
		timestamp severity status method uri size body \
		2025-09-10T00:10:01.012345+09:00 INFO 200 GET /index.html 42 it-works \
		2025-09-10T00:10:01.012345+09:00 INFO 200 GET /index.html 43 it-works |
		ENV_OUT_PQ_NAME="${input}" python3 \
			-c 'import pandas as pd; import sys; import os; import functools; functools.reduce(
				lambda state, f: f(state),
				[
					pd.read_csv(sys.stdin).to_parquet,
				],
				os.getenv("ENV_OUT_PQ_NAME"),
			)'
}

test -f "${input}" || geninput

ENV_PARQUET_FILE_NAME="${input}" ./rs-parquet-schema-show |
	dasel --read=json --write=yaml |
	bat --language=yaml
