# Task 1:
exec_error query: "select E.oid        as id,
       E.xmin       as state_number,
       extname      as name,
       extversion   as version,
       extnamespace as schema_id,
       nspname      as schema_name
       ,
       array(select unnest
             from unnest(available_versions)
             where unnest > extversion) as available_updates
       
from pg_catalog.pg_extension E
       join pg_namespace N on E.extnamespace = N.oid
       left join (select name, array_agg(version) as available_versions
                  from pg_available_extension_versions()
                  group by name) V on E.extname = V.name
       
--  where pg_catalog.age(E.xmin) <= #TXAGE"
exec_error params: Some([])
exec_error error: SchemaError(FieldNotFound { field: Column { relation: None, name: "available_versions" }, valid_fields: [] }, Some(""))
## # Task 1: Done
Task 1 fixed by rewriting the correlated available_updates subquery to NULL.


# Task 2:
exec_error query: "select T.oid as type_id,
       T.xmin as type_state_number,
       T.typname as type_name,
       T.typtype as type_sub_kind,
       T.typcategory as type_category,
       T.typrelid as class_id,
       T.typbasetype as base_type_id,
       case when T.typtype in ('c','e') then null
            else pg_catalog.format_type(T.typbasetype, T.typtypmod) end as type_def,
       T.typndims as dimensions_number,
       T.typdefault as default_expression,
       T.typnotnull as mandatory,
       pg_catalog.pg_get_userbyid(T.typowner) as \"owner\"
from pg_catalog.pg_type T
         left outer join pg_catalog.pg_class C
             on T.typrelid = C.oid
where T.typnamespace = $1::oid
  --  and T.typname in ( :[*f_names] )
  --  and pg_catalog.age(T.xmin) <= #TXAGE
  and (T.typtype in ('d','e') or
       C.relkind = 'c'::\"char\" or
       (T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A')) or
       T.typtype = 'p' and not T.typisdefined)
order by 1"
exec_error params: Some([Some(b"\0\0\0\0\0\0\x08\x98")])
exec_error error: NotImplemented("Unsupported SQL type Custom(ObjectName([Identifier(Ident { value: \"char\", quote_style: Some('\"'), span: Span(Location(1,632)..Location(1,638)) })]), [])")
# Task 2: done
The failure was due to casts using the special type `"char"` which the
parser returned as a custom type.  Added a rewrite step to map such casts
to the regular `CHAR` type and updated the pipeline and tests.

# Task 9: Done
The parser rejected `pg_get_function_result` and `pg_get_function_sqlbody`
functions. Implemented placeholder UDFs returning NULL for both and
registered them with the server. Added functional tests verifying the
new functions return `NULL`.

