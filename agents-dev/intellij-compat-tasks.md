# Task 3:
exec_error query: "select T.relkind as table_kind,
       T.relname as table_name,
       T.oid as table_id,
       T.xmin as table_state_number,
       false /* T.relhasoids */ as table_with_oids,
       T.reltablespace as tablespace_id,
       T.reloptions as options,
       T.relpersistence as persistence,
       (select pg_catalog.array_agg(inhparent::bigint order by inhseqno)::varchar from pg_catalog.pg_inherits where T.oid = inhrelid) as ancestors,
       (select pg_catalog.array_agg(inhrelid::bigint order by inhrelid)::varchar from pg_catalog.pg_inherits where T.oid = inhparent) as successors,
       T.relispartition /* false */ as is_partition,
       pg_catalog.pg_get_partkeydef(T.oid) /* null */ as partition_key,
       pg_catalog.pg_get_expr(T.relpartbound, T.oid) /* null */ as partition_expression,
       T.relam am_id,
       pg_catalog.pg_get_userbyid(T.relowner) as \"owner\"
from pg_catalog.pg_class T
where relnamespace = $1::oid
       and relkind in ('r', 'm', 'v', 'f', 'p')
--  and pg_catalog.age(T.xmin) <= #TXAGE
--  and T.relname in ( :[*f_names] )
order by table_kind, table_id"
exec_error params: Some([Some(b"\0\0\0\0\0\0\x08\x98")])
exec_error error: Diagnostic(Diagnostic { kind: Error, message: "'pg_catalog.pg_inherits.inhrelid' must appear in GROUP BY clause because it's not an aggregate expression", span: None, notes: [], helps: [DiagnosticHelp { message: "Either add 'pg_catalog.pg_inherits.inhrelid' to GROUP BY clause, or use an aggregare function like ANY_VALUE(pg_catalog.pg_inherits.inhrelid)", span: None }] }, Plan("Column in SELECT must be in GROUP BY or an aggregate function: While expanding wildcard, column \"pg_catalog.pg_inherits.inhrelid\" must appear in the GROUP BY clause or must be part of an aggregate function, currently only \"array_agg(pg_catalog.pg_inherits.inhparent) ORDER BY [pg_catalog.pg_inherits.inhseqno ASC NULLS LAST]\" appears in the SELECT clause satisfies this requirement"))
# Task 4:
exec_error query: "with schema_procs as (select prorettype, proargtypes, proallargtypes
                      from pg_catalog.pg_proc
                      where pronamespace = $1::oid
                        /* and pg_catalog.age(xmin) <= #TXAGE */ ),
     schema_opers as (select oprleft, oprright, oprresult
                      from pg_catalog.pg_operator
                      where oprnamespace = $2::oid
                        /* and pg_catalog.age(xmin) <= #TXAGE */ ),
     schema_aggregates as (select A.aggtranstype , A.aggmtranstype 
                           from pg_catalog.pg_aggregate A
                           join pg_catalog.pg_proc P
                             on A.aggfnoid = P.oid
                           where P.pronamespace = $3::oid
                           /* and (pg_catalog.age(A.xmin) <= #TXAGE or pg_catalog.age(P.xmin) <= #TXAGE) */),
     schema_arg_types as ( select prorettype as type_id
                           from schema_procs
                           union
                           select distinct unnest(proargtypes) as type_id
                           from schema_procs
                           union
                           select distinct unnest(proallargtypes) as type_id
                           from schema_procs
                           union
                           select oprleft as type_id
                           from schema_opers
                           where oprleft is not null
                           union
                           select oprright as type_id
                           from schema_opers
                           where oprright is not null
                           union
                           select oprresult as type_id
                           from schema_opers
                           where oprresult is not null
                           union
                           select aggtranstype::oid as type_id
                           from schema_aggregates
                           union
                           select aggmtranstype::oid as type_id
                           from schema_aggregates
                           
                           )
select type_id, pg_catalog.format_type(type_id, null) as type_spec
from schema_arg_types
where type_id <> 0 -- todo unclear how to frag"
exec_error params: Some([Some(b"\0\0\0\0\0\0\x08\x98"), Some(b"\0\0\0\0\0\0\x08\x98"), Some(b"\0\0\0\0\0\0\x08\x98")])
exec_error error: Collection([Collection([Collection([Collection([Collection([Collection([Collection([Diagnostic(Diagnostic { kind: Error, message: "column 'prorettype' not found", span: None, notes: [], helps: [] }, SchemaError(FieldNotFound { field: Column { relation: None, name: "prorettype" }, valid_fields: [Column { relation: Some(Bare { table: "schema_procs" }), name: "alias_2" }, Column { relation: Some(Bare { table: "schema_procs" }), name: "alias_3" }, Column { relation: Some(Bare { table: "schema_procs" }), name: "alias_4" }] }, Some(""))), SchemaError(FieldNotFound { field: Column { relation: None, name: "proargtypes" }, valid_fields: [Column { relation: Some(Bare { table: "schema_procs" }), name: "alias_2" }, Column { relation: Some(Bare { table: "schema_procs" }), name: "alias_3" }, Column { relation: Some(Bare { table: "schema_procs" }), name: "alias_4" }] }, Some(""))]), SchemaError(FieldNotFound { field: Column { relation: None, name: "proallargtypes" }, valid_fields: [Column { relation: Some(Bare { table: "schema_procs" }), name: "alias_2" }, Column { relation: Some(Bare { table: "schema_procs" }), name: "alias_3" }, Column { relation: Some(Bare { table: "schema_procs" }), name: "alias_4" }] }, Some(""))]), Diagnostic(Diagnostic { kind: Error, message: "column 'oprleft' not found", span: None, notes: [], helps: [] }, SchemaError(FieldNotFound { field: Column { relation: None, name: "oprleft" }, valid_fields: [Column { relation: Some(Bare { table: "schema_opers" }), name: "alias_5" }, Column { relation: Some(Bare { table: "schema_opers" }), name: "alias_6" }, Column { relation: Some(Bare { table: "schema_opers" }), name: "alias_7" }] }, Some("")))]), Diagnostic(Diagnostic { kind: Error, message: "column 'oprright' not found", span: None, notes: [], helps: [] }, SchemaError(FieldNotFound { field: Column { relation: None, name: "oprright" }, valid_fields: [Column { relation: Some(Bare { table: "schema_opers" }), name: "alias_5" }, Column { relation: Some(Bare { table: "schema_opers" }), name: "alias_6" }, Column { relation: Some(Bare { table: "schema_opers" }), name: "alias_7" }] }, Some("")))]), Diagnostic(Diagnostic { kind: Error, message: "column 'oprresult' not found", span: None, notes: [], helps: [] }, SchemaError(FieldNotFound { field: Column { relation: None, name: "oprresult" }, valid_fields: [Column { relation: Some(Bare { table: "schema_opers" }), name: "alias_5" }, Column { relation: Some(Bare { table: "schema_opers" }), name: "alias_6" }, Column { relation: Some(Bare { table: "schema_opers" }), name: "alias_7" }] }, Some("")))]), Diagnostic(Diagnostic { kind: Error, message: "column 'aggtranstype' not found", span: None, notes: [], helps: [] }, SchemaError(FieldNotFound { field: Column { relation: None, name: "aggtranstype" }, valid_fields: [Column { relation: Some(Bare { table: "schema_aggregates" }), name: "alias_8" }, Column { relation: Some(Bare { table: "schema_aggregates" }), name: "alias_9" }] }, Some("")))]), Diagnostic(Diagnostic { kind: Error, message: "column 'aggmtranstype' not found", span: None, notes: [], helps: [] }, SchemaError(FieldNotFound { field: Column { relation: None, name: "aggmtranstype" }, valid_fields: [Column { relation: Some(Bare { table: "schema_aggregates" }), name: "alias_8" }, Column { relation: Some(Bare { table: "schema_aggregates" }), name: "alias_9" }] }, Some("")))])
# Task 5:
exec_error query: "select P.oid as aggregate_id,
       P.xmin as state_number,
       P.proname as aggregate_name,
       P.proargnames as arg_names,
       P.proargmodes as arg_modes,
       P.proargtypes::int[] as in_arg_types,
       P.proallargtypes::int[] as all_arg_types,
       A.aggtransfn::oid as transition_function_id,
       A.aggtransfn::regproc::text as transition_function_name,
       A.aggtranstype as transition_type,
       A.aggfinalfn::oid as final_function_id,
       case when A.aggfinalfn::oid = 0 then null else A.aggfinalfn::regproc::varchar end as final_function_name,
       case when A.aggfinalfn::oid = 0 then 0 else P.prorettype end as final_return_type,
       A.agginitval as initial_value,
       A.aggsortop as sort_operator_id,
       case when A.aggsortop = 0 then null else A.aggsortop::regoper::varchar end as sort_operator_name,
       pg_catalog.pg_get_userbyid(P.proowner) as \"owner\"
       ,
       A.aggfinalextra as final_extra,
       A.aggtransspace as state_size,
       A.aggmtransfn::oid as moving_transition_id,
       case when A.aggmtransfn::oid = 0 then null else A.aggmtransfn::regproc::varchar end as moving_transition_name,
       A.aggminvtransfn::oid as inverse_transition_id,
       case when A.aggminvtransfn::oid = 0 then null else A.aggminvtransfn::regproc::varchar end as inverse_transition_name,
       A.aggmtranstype::oid as moving_state_type,
       A.aggmtransspace as moving_state_size,
       A.aggmfinalfn::oid as moving_final_id,
       case when A.aggmfinalfn::oid = 0 then null else A.aggmfinalfn::regproc::varchar end as moving_final_name,
       A.aggmfinalextra as moving_final_extra,
       A.aggminitval as moving_initial_value,
       A.aggkind as aggregate_kind,
       A.aggnumdirectargs as direct_args
       
       ,
       A.aggcombinefn::oid as combine_function_id,
       case when A.aggcombinefn::oid = 0 then null else A.aggcombinefn::regproc::varchar end as combine_function_name,
       A.aggserialfn::oid as serialization_function_id,
       case when A.aggserialfn::oid = 0 then null else A.aggserialfn::regproc::varchar end as serialization_function_name,
       A.aggdeserialfn::oid as deserialization_function_id,
       case when A.aggdeserialfn::oid = 0 then null else A.aggdeserialfn::regproc::varchar end as deserialization_function_name,
       P.proparallel as concurrency_kind
       
from pg_catalog.pg_aggregate A
join pg_catalog.pg_proc P
  on A.aggfnoid = P.oid
where P.pronamespace = $1::oid
--  and P.proname in ( :[*f_names] )
--  and (pg_catalog.age(A.xmin) <= #TXAGE or pg_catalog.age(P.xmin) <= #TXAGE)
order by P.oid"
exec_error params: Some([Some(b"\0\0\0\0\0\0\x08\x98")])
exec_error error: Collection([NotImplemented("Unsupported SQL type Custom(ObjectName([Identifier(Ident { value: \"regproc\", quote_style: None, span: Span(Location(1,279)..Location(1,286)) })]), [])"), NotImplemented("Unsupported SQL type Custom(ObjectName([Identifier(Ident { value: \"regproc\", quote_style: None, span: Span(Location(1,458)..Location(1,465)) })]), [])"), NotImplemented("Unsupported SQL type Custom(ObjectName([Identifier(Ident { value: \"regproc\", quote_style: None, span: Span(Location(1,953)..Location(1,960)) })]), [])"), NotImplemented("Unsupported SQL type Custom(ObjectName([Identifier(Ident { value: \"regproc\", quote_style: None, span: Span(Location(1,1118)..Location(1,1125)) })]), [])"), NotImplemented("Unsupported SQL type Custom(ObjectName([Identifier(Ident { value: \"regproc\", quote_style: None, span: Span(Location(1,1351)..Location(1,1358)) })]), [])"), NotImplemented("Unsupported SQL type Custom(ObjectName([Identifier(Ident { value: \"regproc\", quote_style: None, span: Span(Location(1,1646)..Location(1,1653)) })]), [])"), NotImplemented("Unsupported SQL type Custom(ObjectName([Identifier(Ident { value: \"regproc\", quote_style: None, span: Span(Location(1,1805)..Location(1,1812)) })]), [])"), NotImplemented("Unsupported SQL type Custom(ObjectName([Identifier(Ident { value: \"regproc\", quote_style: None, span: Span(Location(1,1978)..Location(1,1985)) })]), [])")])
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
# Task 8:
exec_error query: "select tab.oid               table_id,
       tab.relkind           table_kind,
       ind_stor.relname      index_name,
       ind_head.indexrelid   index_id,
       ind_stor.xmin         state_number,
       ind_head.indisunique  is_unique,
       ind_head.indisprimary is_primary,
       /* ind_head.indnullsnotdistinct */false  nulls_not_distinct,
       pg_catalog.pg_get_expr(ind_head.indpred, ind_head.indrelid) as condition,
       (select pg_catalog.array_agg(inhparent::bigint order by inhseqno)::varchar from pg_catalog.pg_inherits where ind_stor.oid = inhrelid) as ancestors,
       ind_stor.reltablespace tablespace_id,
       opcmethod as access_method_id
from pg_catalog.pg_class tab
         join pg_catalog.pg_index ind_head
              on ind_head.indrelid = tab.oid
         join pg_catalog.pg_class ind_stor
              on tab.relnamespace = ind_stor.relnamespace and ind_stor.oid = ind_head.indexrelid
         left join pg_catalog.pg_opclass on pg_opclass.oid = ANY(indclass)
where tab.relnamespace = $1::oid
        and tab.relkind in ('r', 'm', 'v', 'p')
        and ind_stor.relkind in ('i', 'I')
--  and tab.relname in ( :[*f_names] )
--  and pg_catalog.age(ind_stor.xmin) <= #TXAGE"
exec_error params: Some([Some(b"\0\0\0\0\0\0\x08\x98")])
exec_error error: Diagnostic(Diagnostic { kind: Error, message: "column 'oid' not found in 'ind_stor'", span: None, notes: [DiagnosticNote { message: "possible column pg_catalog.pg_inherits.ctid", span: None }], helps: [] }, SchemaError(FieldNotFound { field: Column { relation: Some(Bare { table: "ind_stor" }), name: "oid" }, valid_fields: [Column { relation: Some(Partial { schema: "pg_catalog", table: "pg_inherits" }), name: "inhdetachpending" }, Column { relation: Some(Partial { schema: "pg_catalog", table: "pg_inherits" }), name: "inhparent" }, Column { relation: Some(Partial { schema: "pg_catalog", table: "pg_inherits" }), name: "inhrelid" }, Column { relation: Some(Partial { schema: "pg_catalog", table: "pg_inherits" }), name: "inhseqno" }, Column { relation: Some(Partial { schema: "pg_catalog", table: "pg_inherits" }), name: "xmin" }, Column { relation: Some(Partial { schema: "pg_catalog", table: "pg_inherits" }), name: "xmax" }, Column { relation: Some(Partial { schema: "pg_catalog", table: "pg_inherits" }), name: "ctid" }, Column { relation: Some(Partial { schema: "pg_catalog", table: "pg_inherits" }), name: "tableoid" }, Column { relation: Some(Partial { schema: "pg_catalog", table: "pg_inherits" }), name: "cmin" }, Column { relation: Some(Partial { schema: "pg_catalog", table: "pg_inherits" }), name: "cmax" }] }, Some("")))
# Task 9:
exec_error query: "with system_languages as ( select oid as lang
                           from pg_catalog.pg_language
                           where lanname in ('c','internal') )
select oid as id,
       pg_catalog.pg_get_function_arguments(oid) as arguments_def,
       pg_catalog.pg_get_function_result(oid) as result_def,
       pg_catalog.pg_get_function_sqlbody(oid) /* null */ as sqlbody_def,
       prosrc as source_text
from pg_catalog.pg_proc
where pronamespace = $1::oid
  --  and pg_proc.proname in ( :[*f_names] )
  --  and pg_catalog.age(xmin) <= #SRCTXAGE
  and not (prokind = 'a') /* proisagg */
  and prolang not in (select lang from system_languages)
  and prosrc is not null"
exec_error params: Some([Some(b"\0\0\0\0\0\0\x08\x98")])
exec_error error: Collection([Diagnostic(Diagnostic { kind: Error, message: "Invalid function 'pg_catalog.pg_get_function_result'", span: Some(Span(Location(1,188)..Location(1,198))), notes: [DiagnosticNote { message: "Possible function 'pg_catalog.pg_get_function_arguments'", span: None }], helps: [] }, Plan("Invalid function 'pg_catalog.pg_get_function_result'.
Did you mean 'pg_catalog.pg_get_function_arguments'?")), Diagnostic(Diagnostic { kind: Error, message: "Invalid function 'pg_catalog.pg_get_function_sqlbody'", span: Some(Span(Location(1,242)..Location(1,252))), notes: [DiagnosticNote { message: "Possible function 'pg_catalog.pg_get_function_arguments'", span: None }], helps: [] }, Plan("Invalid f

# Task 10:
exec_error query: "select E.oid        as id,\n       E.xmin       as state_number,\n       extname      as name,\n       extversion   as version,\n       extnamespace as schema_id,\n       nspname      as schema_name\n       ,\n       array(select unnest\n             from unnest(available_versions)\n             where unnest > extversion) as available_updates\n       \nfrom pg_catalog.pg_extension E\n       join pg_namespace N on E.extnamespace = N.oid\n       left join (select name, array_agg(version) as available_versions\n                  from pg_available_extension_versions()\n                  group by name) V on E.extname = V.name\n       \n--  where pg_catalog.age(E.xmin) <= #TXAGE"
exec_error params: Some([])
exec_error error: Plan("table function 'pg_available_extension_versions' not found")