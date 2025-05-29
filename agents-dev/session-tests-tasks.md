# Task 101

We run this server with 

```
cargo run ./pg_catalog_data/pg_schema --default-catalog pgtry --default-schema pg_catalog --port 5444
```

What we need is to have another parameter `--capture filename`  
- Save all queries, parameters for those queries and the output of these queries to given filename
- It will be a list of queries with "query", "parameters", "result" fields, result will be a list of dictionaries.
- The format of the file will be a yaml file.
- If the query has errors we need to capture that also. Put a "success: false" field for failed queries, for successful queries it will be "success:true". Add an  "error_details" field to errored queries.

### Approach

Implemented the `--capture` CLI option. When provided, every query is written to
the given YAML file with its parameters, results and success flag. Errors are
recorded with details. Tests cover capturing of successful queries, parameterized
queries and failing queries.

