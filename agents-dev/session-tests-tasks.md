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

# Task 102

You'll see yaml files on captures/*.yaml

What I need you to do is
- write a python test name it "tests/test_captures.py"
- it should first spawn a new server - check the other test file.
- then open the first yaml file, go through each query. 
- if the query is marked with success: false skip that query. 
- if the query is marked with success, send the query to the server, and check the response. 
- Check if the query response matches the data. if the data doesnt match for the query, you can fail the test. 

Remember when matching with the data, null in yaml and and None in python are same. So please be careful about how you match the data !
### Done

Added `tests/test_captures.py` which spawns a server and replays the queries from the first capture YAML file. For successful queries the test sends the SQL with parameters (converting `$n` placeholders) and compares the returned rows with the YAML results, converting raw pgwire values when needed.

The server returns slightly different data than in `captures/dbeaver.yaml` (for example `session_user` and database ACLs), causing the test to fail. After several attempts to normalise the values the mismatch persisted and the task could not be completed successfully.


# Task 103:

When I run this query I see that datacl is "{{=Tc/dbuser,dbuser=CTc/dbuser}}" in first row. 

But when i do capture, in the yaml file i always see it as "null". Can you fix it ?

pgtry=>     SELECT db.oid,db.* FROM pg_catalog.pg_database db WHERE 1 = 1 AND datallowconn AND NOT datistemplate OR db.datname ='pgtry'
    ORDER BY db.datname
;
  oid  |               datacl               | datallowconn | datcollate  | datcollversion | datconnlimit |  datctype   | datdba | datfrozenxid | dathasloginevt | daticurules | datistemplate | datlocale | datlocprovider | datminmxid | datname  | dattablespace | encoding |  oid
-------+------------------------------------+--------------+-------------+----------------+--------------+-------------+--------+--------------+----------------+-------------+---------------+-----------+----------------+------------+----------+---------------+----------+-------
 27734 | "{{=Tc/dbuser,dbuser=CTc/dbuser}}" | t            | C           |                |           -1 | C           |  27735 | 726          |                |             | f             |           |                | 1          | pgtry    |          1663 |        6 | 27734
     5 |                                    | t            | nl_NL.UTF-8 |                |           -1 | nl_NL.UTF-8 |     10 | 730          | f              |             | f             |           | c              | 1          | postgres |          1663 |        6 |     5
(2 rows)