# Library Preparation Notes

All schema YAML files are packaged into a single archive `pg_schema.zip`.
The archive is created using the `zip` tool and contains the contents of the
`pg_catalog_data/pg_schema` directory.

Approximate size of the archive: 1.6 MB.

Use `make create_schema_zip` to generate the archive.
