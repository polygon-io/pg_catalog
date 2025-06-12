download_postgres_binary:
	./download_postgresql.sh

run_postgresql:
	./run-postgres.sh

create_schema_yaml_files:
        python schema.py generate pg_catalog_data/pg_schema

create_schema_zip:
        zip -r pg_schema.zip pg_catalog_data/pg_schema


dev_server:
        RUST_LOG=info RUST_MIN_STACK=33554432 cargo run ./pg_schema.zip --default-catalog pgtry --default-schema pg_catalog --port 5444
