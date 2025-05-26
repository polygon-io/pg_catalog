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

# Task 6:
exec_error query: "select O.oid as id,
       O.amopstrategy as strategy,
       O.amopopr as op_id,
       O.amopopr::regoperator::varchar as op_sig,
       O.amopsortfamily /* null */ as sort_family_id,
       SF.opfname /* null */ as sort_family,
       O.amopfamily as family_id,
       C.oid as class_id
from pg_catalog.pg_amop O
    left join pg_opfamily F on O.amopfamily = F.oid
    left join pg_opfamily SF on O.amopsortfamily = SF.oid
    left join pg_depend D on D.classid = 'pg_amop'::regclass and O.oid = D.objid and D.objsubid = 0
    left join pg_opclass C on D.refclassid = 'pg_opclass'::regclass and C.oid = D.refobjid and D.refobjsubid = 0
where C.opcnamespace = $1::oid or C.opcnamespace is null and F.opfnamespace = $2::oid
  --  and pg_catalog.age(O.xmin) <= #TXAGE
order by C.oid, F.oid"
exec_error params: Some([Some(b"\0\0\0\0\0\0\x08\x98"), Some(b"\0\0\0\0\0\0\x08\x98")])
exec_error error: NotImplemented("Unsupported SQL type Custom(ObjectName([Identifier(Ident { value: \"regoperator\", quote_style: None, span: Span(Location(1,80)..Location(1,91)) })]), [])")
## # Task 6: Done
Added `rewrite_regoperator_cast` which maps `regoperator` casts to `TEXT`.
The filter rewrite pipeline now applies this transformation and tests verify it.

# Task 7:
exec_error query: "select P.oid as id,
       P.amprocnum as num,
       P.amproc::oid as proc_id,
       P.amproc::regprocedure::varchar as proc_sig,
       P.amproclefttype::regtype::varchar as left_type,
       P.amprocrighttype::regtype::varchar as right_type,
       P.amprocfamily as family_id,
       C.oid as class_id
from pg_catalog.pg_amproc P
    left join pg_opfamily F on P.amprocfamily = F.oid
    left join pg_depend D on D.classid = 'pg_amproc'::regclass and P.oid = D.objid and D.objsubid = 0
    left join pg_opclass C on D.refclassid = 'pg_opclass'::regclass and C.oid = D.refobjid and D.refobjsubid = 0
where C.opcnamespace = $1::oid or C.opcnamespace is null and F.opfnamespace = $2::oid
  --  and pg_catalog.age(P.xmin) <= #TXAGE
order by C.oid, F.oid"
exec_error params: Some([Some(b"\0\0\0\0\0\0\x08\x98"), Some(b"\0\0\0\0\0\0\x08\x98")])
exec_error error: NotImplemented("Unsupported SQL type Custom(ObjectName([Identifier(Ident { value: \"regprocedure\", quote_style: None, span: Span(Location(1,77)..Location(1,89)) })]), [])")
## # Task 7: Done
Implemented `rewrite_regprocedure_cast` to replace `regprocedure` casts with `TEXT`.
Pipeline updated and dedicated tests ensure the rewrite works.


# Task 10:
exec_error query: "select E.oid        as id,\n       E.xmin       as state_number,\n       extname      as name,\n       extversion   as version,\n       extnamespace as schema_id,\n       nspname      as schema_name\n       ,\n       array(select unnest\n             from unnest(available_versions)\n             where unnest > extversion) as available_updates\n       \nfrom pg_catalog.pg_extension E\n       join pg_namespace N on E.extnamespace = N.oid\n       left join (select name, array_agg(version) as available_versions\n                  from pg_available_extension_versions()\n                  group by name) V on E.extname = V.name\n       \n--  where pg_catalog.age(E.xmin) <= #TXAGE"
exec_error params: Some([])
exec_error error: Plan("table function 'pg_available_extension_versions' not found")
## # Task 10: Done
Added a stub implementation for the `pg_available_extension_versions()`
table function and registered it during server startup so IntelliJ queries can
resolve successfully.
