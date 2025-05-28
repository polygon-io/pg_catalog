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

# Task 22:
exec_error query: "with schema_procs as (select prorettype, proargtypes, proallargtypes\n                      from pg_catalog.pg_proc\n                      where pronamespace = $1::oid\n                        /* and pg_catalog.age(xmin) <= #TXAGE */ ), ..."
exec_error params: Some([Some(b"\0\0\0\0\0\0\x08\x98"), Some(b"\0\0\0\0\0\0\x08\x98"), Some(b"\0\0\0\0\0\0\x08\x98")])
exec_error error: Collection([Diagnostic(Diagnostic { kind: Error, message: "column 'prorettype' not found" ... )])
## # Task 22: Done
Recursive aliasing renamed columns inside CTEs, causing subquery references to fail.
`alias_all_columns` now only aliases the outer query so CTE columns keep their names.
Unit tests updated accordingly and all tests pass.

# Task 21: Done
Fixed GROUP BY error when rewriting subqueries using `array_agg`.
`array_agg` is now treated as an aggregate function so the join key is grouped.
Added unit test `injects_group_by_for_array_agg` verifying the rewrite.



# Task 31: Done
The server did not support `SHOW search_path`. Added `search_path` to the
session options so DataFusion exposes it via `information_schema.df_settings`.
Implemented tests verifying the new behaviour.
# Task 32:
exec_error query: "select string_agg(word, ',') from pg_catalog.pg_get_keywords() where word <> ALL ('{a,abs,absolute,action,ada,add,admin,after,all,allocate,alter,always,and,any,are,array,as,asc,asensitive,assertion,assignment,asymmetric,at,atomic,attribute,attributes,authorization,avg,before,begin,bernoulli,between,bigint,binary,blob,boolean,both,breadth,by,c,call,called,cardinality,cascade,cascaded,case,cast,catalog,catalog_name,ceil,ceiling,chain,char,char_length,character,character_length,character_set_catalog,character_set_name,character_set_schema,characteristics,characters,check,checked,class_origin,clob,close,coalesce,cobol,code_units,collate,collation,collation_catalog,collation_name,collation_schema,collect,column,column_name,command_function,command_function_code,commit,committed,condition,condition_number,connect,connection_name,constraint,constraint_catalog,constraint_name,constraint_schema,constraints,constructors,contains,continue,convert,corr,corresponding,count,covar_pop,covar_samp,create,cross,cube,cume_dist,current,current_collation,current_date,current_default_transform_group,current_path,current_role,current_time,current_timestamp,current_transform_group_for_type,current_user,cursor,cursor_name,cycle,data,date,datetime_interval_code,datetime_interval_precision,day,deallocate,dec,decimal,declare,default,defaults,deferrable,deferred,defined,definer,degree,delete,dense_rank,depth,deref,derived,desc,describe,descriptor,deterministic,diagnostics,disconnect,dispatch,distinct,domain,double,drop,dynamic,dynamic_function,dynamic_function_code,each,element,else,end,end-exec,equals,escape,every,except,exception,exclude,excluding,exec,execute,exists,exp,external,extract,false,fetch,filter,final,first,float,floor,following,for,foreign,fortran,found,free,from,full,function,fusion,g,general,get,global,go,goto,grant,granted,group,grouping,having,hierarchy,hold,hour,identity,immediate,implementation,in,including,increment,indicator,initially,inner,inout,input,insensitive,insert,instance,instantiable,int,integer,intersect,intersection,interval,into,invoker,is,isolation,join,k,key,key_member,key_type,language,large,last,lateral,leading,left,length,level,like,ln,local,localtime,localtimestamp,locator,lower,m,map,match,matched,max,maxvalue,member,merge,message_length,message_octet_length,message_text,method,min,minute,minvalue,mod,modifies,module,month,more,multiset,mumps,name,names,national,natural,nchar,nclob,nesting,new,next,no,none,normalize,normalized,not,\"null\",nullable,nullif,nulls,number,numeric,object,octet_length,octets,of,old,on,only,open,option,options,or,order,ordering,ordinality,others,out,outer,output,over,overlaps,overlay,overriding,pad,parameter,parameter_mode,parameter_name,parameter_ordinal_position,parameter_specific_catalog,parameter_specific_name,parameter_specific_schema,partial,partition,pascal,path,percent_rank,percentile_cont,percentile_disc,placing,pli,position,power,preceding,precision,prepare,preserve,primary,prior,privileges,procedure,public,range,rank,read,reads,real,recursive,ref,references,referencing,regr_avgx,regr_avgy,regr_count,regr_intercept,regr_r2,regr_slope,regr_sxx,regr_sxy,regr_syy,relative,release,repeatable,restart,result,return,returned_cardinality,returned_length,returned_octet_length,returned_sqlstate,returns,revoke,right,role,rollback,rollup,routine,routine_catalog,routine_name,routine_schema,row,row_count,row_number,rows,savepoint,scale,schema,schema_name,scope_catalog,scope_name,scope_schema,scroll,search,second,section,security,select,self,sensitive,sequence,serializable,server_name,session,session_user,set,sets,similar,simple,size,smallint,some,source,space,specific,specific_name,specifictype,sql,sqlexception,sqlstate,sqlwarning,sqrt,start,state,statement,static,stddev_pop,stddev_samp,structure,style,subclass_origin,submultiset,substring,sum,symmetric,system,system_user,table,table_name,tablesample,temporary,then,ties,time,timestamp,timezone_hour,timezone_minute,to,top_level_count,trailing,transaction,transaction_active,transactions_committed,transactions_rolled_back,transform,transforms,translate,translation,treat,trigger,trigger_catalog,trigger_name,trigger_schema,trim,true,type,uescape,unbounded,uncommitted,under,union,unique,unknown,unnamed,unnest,update,upper,usage,user,user_defined_type_catalog,user_defined_type_code,user_defined_type_name,user_defined_type_schema,using,value,values,var_pop,var_samp,varchar,varying,view,when,whenever,where,width_bucket,window,with,within,without,work,write,year,zone}'::text[])"
exec_error params: Some([])
exec_error error: Plan("table function 'pg_catalog' not found")
# Task 32: Done
Added empty table function `pg_get_keywords` exposing columns `word`,
`catcode` and `catdesc`. The server registers this UDTF so IDE keyword
queries succeed. Unit test `pg_get_keywords_empty` verifies the function
returns zero rows.
# Task 33:
exec_error query: "select c.oid,pg_catalog.pg_total_relation_size(c.oid) as total_rel_size,pg_catalog.pg_relation_size(c.oid) as rel_size\nFROM pg_class c\nWHERE c.relnamespace=$1"
exec_error params: Some([Some(b"\0\0\0\0\0\0\x08\x98")])
exec_error error: Collection([Diagnostic(Diagnostic { kind: Error, message: "Invalid function 'pg_catalog.pg_total_relation_size'", span: Some(Span(Location(1,26)..Location(1,36))), notes: [DiagnosticNote { message: "Possible function 'pg_catalog.pg_get_function_result'", span: None }], helps: [] }, Plan("Invalid function 'pg_catalog.pg_total_relation_size'.\nDid you mean 'pg_catalog.pg_get_function_result'?")), Diagnostic(Diagnostic { kind: Error, message: "Invalid function 'pg_catalog.pg_relation_size'", span: Some(Span(Location(1,86)..Location(1,96))), notes: [DiagnosticNote { message: "Possible function 'pg_catalog.pg_get_one'", span: None }], helps: [] }, Plan("Invalid function 'pg_catalog.pg_relation_size'.\nDid you mean 'pg_catalog.pg_get_one'?"))])
# Task 33: Done
Implemented stub functions `pg_relation_size` and `pg_total_relation_size` returning zero and registered them with the server. Added unit tests verifying their behavior.
# Task 34:
exec_error query: "select cls.xmin as sequence_state_number,\n       sq.seqrelid as sequence_id,\n       cls.relname as sequence_name,\n       pg_catalog.format_type(sq.seqtypid, null) as data_type,\n       sq.seqstart as start_value,\n       sq.seqincrement as inc_value,\n       sq.seqmin as min_value,\n       sq.seqmax as max_value,\n       sq.seqcache as cache_size,\n       sq.seqcycle as cycle_option,\n       pg_catalog.pg_get_userbyid(cls.relowner) as \"owner\"\nfrom pg_catalog.pg_sequence sq\n    join pg_class cls on sq.seqrelid = cls.oid\n    where cls.relnamespace = $1::oid\nand pg_catalog.age(cls.xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)\n--  and cls.relname in ( :[*f_names] )"
exec_error params: Some([Some(b"\0\0\0\0\0\0\x08\x98"), Some(b"\0\0\0\0\0\0\0\0")])
exec_error error: NotImplemented("Unsupported SQL type Custom(ObjectName([Identifier(Ident { value: \"xid\", quote_style: None, span: Span(Location(1,572)..Location(1,575)) })]), [])")
exec_error query: "select T.oid as type_id,\n       T.xmin as type_state_number,\n       T.typname as type_name,\n       T.typtype as type_sub_kind,\n       T.typcategory as type_category,\n       T.typrelid as class_id,\n       T.typbasetype as base_type_id,\n       case when T.typtype in ('c','e') then null\n            else pg_catalog.format_type(T.typbasetype, T.typtypmod) end as type_def,\n       T.typndims as dimensions_number,\n       T.typdefault as default_expression,\n       T.typnotnull as mandatory,\n       pg_catalog.pg_get_userbyid(T.typowner) as \"owner\"\nfrom pg_catalog.pg_type T\n         left outer join pg_catalog.pg_class C\n             on T.typrelid = C.oid\nwhere T.typnamespace = $1::oid\n  --  and T.typname in ( :[*f_names] )\n  and pg_catalog.age(T.xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)\n  and (T.typtype in ('d','e') or\n       C.relkind = 'c'::\"char\" or\n       (T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A')) or\n       T.typtype = 'p' and not T.typisdefined)\norder by 1"
exec_error params: Some([Some(b"\0\0\0\0\0\0\x08\x98"), Some(b"\0\0\0\0\0\0\0\0")])
exec_error error: NotImplemented("Unsupported SQL type Custom(ObjectName([Identifier(Ident { value: \"xid\", quote_style: None, span: Span(Location(1,666)..Location(1,669)) })]), [])")
exec_error query: "select T.relkind as table_kind,\n       T.relname as table_name,\n       T.oid as table_id,\n       T.xmin as table_state_number,\n       false /* T.relhasoids */ as table_with_oids,\n       T.reltablespace as tablespace_id,\n       T.reloptions as options,\n       T.relpersistence as persistence,\n       (select pg_catalog.array_agg(inhparent::bigint order by inhseqno)::varchar from pg_catalog.pg_inherits where T.oid = inhrelid) as ancestors,\n       (select pg_catalog.array_agg(inhrelid::bigint order by inhrelid)::varchar from pg_catalog.pg_inherits where T.oid = inhparent) as successors,\n       T.relispartition /* false */ as is_partition,\n       pg_catalog.pg_get_partkeydef(T.oid) /* null */ as partition_key,\n       pg_catalog.pg_get_expr(T.relpartbound, T.oid) /* null */ as partition_expression,\n       T.relam am_id,\n       pg_catalog.pg_get_userbyid(T.relowner) as \"owner\"\nfrom pg_catalog.pg_class T\nwhere relnamespace = $1::oid\n       and relkind in ('r', 'm', 'v', 'f', 'p')\nand pg_catalog.age(T.xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)\n--  and T.relname in ( :[*f_names] )\norder by table_kind, table_id"
exec_error params: Some([Some(b"\0\0\0\0\0\0\x08\x98"), Some(b"\0\0\0\0\0\0\0\0")])
exec_error error: NotImplemented("Unsupported SQL type Custom(ObjectName([Identifier(Ident { value: \"xid\", quote_style: None, span: Span(Location(1,1138)..Location(1,1141)) })]), [])")
# Task 34: done
Implemented `rewrite_xid_cast` to map casts to `xid` into `BIGINT` so queries using
this system type can be planned. Added unit tests verifying the rewrite.

# Task 44:
exec_error query: "select R.ev_class as table_id, R.oid as rule_id, ... and R.rulename != '_RETURN'::name order by R.ev_class::bigint, ev_type"
exec_error error: NotImplemented("Unsupported SQL type Custom(ObjectName([Identifier(Ident { value: \"name\" ... }))]))")
## # Task 44: Done
`::name` casts were unsupported by the parser. Added `rewrite_name_cast` to convert them to TEXT and invoked it during query rewriting. New tests confirm the cast succeeds.

# Task 43: Done
Updated pg_index handling so indclass is parsed as an array. Added relam-based query and functional test.


# Task 41:
exec_error query: "with schema_procs as (select prorettype, proargtypes, proallargtypes\n                      from pg_catalog.pg_proc\n                      where pronamespace = $1::oid\n                        and pg_catalog.age(xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)  ),\n     schema_opers as (select oprleft, oprright, oprresult\n                      from pg_catalog.pg_operator\n                      where oprnamespace = $3::oid\n                        and pg_catalog.age(xmin) <= coalesce(nullif(greatest(pg_catalog.age($4::varchar::xid), -1), -1), 2147483647)  ),\n     schema_aggregates as (select A.aggtranstype , A.aggmtranstype \n                           from pg_catalog.pg_aggregate A\n                           join pg_catalog.pg_proc P\n                             on A.aggfnoid = P.oid\n                           where P.pronamespace = $5::oid\n                           and (pg_catalog.age(A.xmin) <= coalesce(nullif(greatest(pg_catalog.age($6::varchar::xid), -1), -1), 2147483647) or pg_catalog.age(P.xmin) <= coalesce(nullif(greatest(pg_catalog.age($7::varchar::xid), -1), -1), 2147483647)) ),\n     schema_arg_types as ( select prorettype as type_id\n                           from schema_procs\n                           union\n                           select distinct unnest(proargtypes) as type_id\n                           from schema_procs\n                           union\n                           select distinct unnest(proallargtypes) as type_id\n                           from schema_procs\n                           union\n                           select oprleft as type_id\n                           from schema_opers\n                           where oprleft is not null\n                           union\n                           select oprright as type_id\n                           from schema_opers\n                           where oprright is not null\n                           union\n                           select oprresult as type_id\n                           from schema_opers\n                           where oprresult is not null\n                           union\n                           select aggtranstype::oid as type_id\n                           from schema_aggregates\n                           union\n                           select aggmtranstype::oid as type_id\n                           from schema_aggregates\n                           \n                           )\nselect type_id, pg_catalog.format_type(type_id, null) as type_spec\nfrom schema_arg_types\nwhere type_id <> 0 -- todo unclear how to frag"
exec_error params: Some([Some(b"\0\0\0\0\0\0\x08\x98"), Some(b"\0\0\0\0\0\0\0\0"), Some(b"\0\0\0\0\0\0\x08\x98"), Some(b"\0\0\0\0\0\0\0\0"), Some(b"\0\0\0\0\0\0\x08\x98"), Some(b"\0\0\0\0\0\0\0\0"), Some(b"\0\0\0\0\0\0\0\0")])
exec_error error: Plan("unnest() can only be applied to array, struct and null")
## # Task 41: Done
`proargtypes` is stored as text so `unnest()` failed. Added a rewrite that wraps
`unnest` arguments in `oidvector_to_array()` and implemented this UDF to parse
the string into a list of OIDs.
# Task 42:
exec_error query: "with T as ( select distinct\n                  T.oid as table_id, T.relname as table_name\n            from pg_catalog.pg_class T\n                 , pg_catalog.pg_attribute A\n            where T.relnamespace = $1::oid\n              and T.relkind in ('r', 'm', 'v', 'f', 'p')\n              and (pg_catalog.age(A.xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647) or pg_catalog.age(T.xmin) <= coalesce(nullif(greatest(pg_catalog.age($3::varchar::xid), -1), -1), 2147483647))\n              and A.attrelid = T.oid\n             --  and T.relname in ( :[*f_names] )\n            )\nselect T.table_id,\n       C.attnum as column_position,\n       C.attname as column_name,\n       C.xmin as column_state_number,\n       C.atttypmod as type_mod,\n       C.attndims as dimensions_number,\n       pg_catalog.format_type(C.atttypid, C.atttypmod) as type_spec,\n       C.atttypid as type_id,\n       C.attnotnull as mandatory,\n       pg_catalog.pg_get_expr(D.adbin, T.table_id) /* D.adsrc */ as column_default_expression,\n       not C.attislocal as column_is_inherited,\n       C.attfdwoptions as options,\n       C.attisdropped as column_is_dropped,\n       C.attidentity /* null */ as identity_kind,\n       C.attgenerated /* null */ as generated\nfrom T\n  join pg_catalog.pg_attribute C on T.table_id = C.attrelid\n  left join pg_catalog.pg_attrdef D on (C.attrelid, C.attnum) = (D.adrelid, D.adnum)\nwhere attnum > 0\norder by table_id, attnum"
exec_error params: Some([Some(b"\0\0\0\0\0\0\x08\x98"), Some(b"\0\0\0\0\0\0\0\0"), Some(b"\0\0\0\0\0\0\0\0")])
exec_error error: NotImplemented("Only identifiers and literals are supported in tuples")
## # Task 42: Done
Tuple equality comparisons in join conditions were unsupported.
Implemented `rewrite_tuple_equality` to expand `(a,b) = (c,d)` into
`a = c AND b = d` and added tests verifying the behaviour.
# Task 43:
exec_error query: "select tab.oid               table_id,\n       tab.relkind           table_kind,\n       ind_stor.relname      index_name,\n       ind_head.indexrelid   index_id,\n       ind_stor.xmin         state_number,\n       ind_head.indisunique  is_unique,\n       ind_head.indisprimary is_primary,\n       /* ind_head.indnullsnotdistinct */false  nulls_not_distinct,\n       pg_catalog.pg_get_expr(ind_head.indpred, ind_head.indrelid) as condition,\n       (select pg_catalog.array_agg(inhparent::bigint order by inhseqno)::varchar from pg_catalog.pg_inherits where ind_stor.oid = inhrelid) as ancestors,\n       ind_stor.reltablespace tablespace_id,\n       opcmethod as access_method_id\nfrom pg_catalog.pg_class tab\n         join pg_catalog.pg_index ind_head\n              on ind_head.indrelid = tab.oid\n         join pg_catalog.pg_class ind_stor\n              on tab.relnamespace = ind_stor.relnamespace and ind_stor.oid = ind_head.indexrelid\n         left join pg_catalog.pg_opclass on pg_opclass.oid = ANY(indclass)\nwhere tab.relnamespace = $1::oid\n        and tab.relkind in ('r', 'm', 'v', 'p')\n        and ind_stor.relkind in ('i', 'I')\n--  and tab.relname in ( :[*f_names] )\nand pg_catalog.age(ind_stor.xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)"
exec_error params: Some([Some(b"\0\0\0\0\0\0\x08\x98"), Some(b"\0\0\0\0\0\0\0\0")])
exec_error error: Context("type_coercion", Plan("Failed to coerce arguments to satisfy a call to 'array_has' function: coercion from [Utf8, Int32] to the signature ArraySignature(Array { arguments: [Array, Element], array_coercion: Some(FixedSizedListToList) }) failed"))
# Task 44:
exec_error query: "select R.ev_class as table_id,\n       R.oid as rule_id,\n       R.xmin as rule_state_number,\n       R.rulename as rule_name,\n       pg_catalog.translate(ev_type,'1234','SUID') as rule_event_code,\n       R.ev_enabled as rule_fire_mode,\n       R.is_instead as rule_is_instead\nfrom pg_catalog.pg_rewrite R\nwhere R.ev_class in (\n  select oid\n  from pg_catalog.pg_class\n  where relnamespace = $1::oid\n--  and relname in ( :[*f_names] )\n)\n  and pg_catalog.age(R.xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)\n  and R.rulename != '_RETURN'::name\norder by R.ev_class::bigint, ev_type"
exec_error params: Some([Some(b"\0\0\0\0\0\0\x08\x98"), Some(b"\0\0\0\0\0\0\0\0")])
exec_error error: NotImplemented("Unsupported SQL type Custom(ObjectName([Identifier(Ident { value: \"name\", quote_style: None, span: Span(Location(1,499)..Location(1,503)) })]), [])")
## # Task 44: Done
Casting to the custom `name` type caused a parser error. Added `rewrite_name_cast` to
map such casts to TEXT and integrated it into the query rewrite pipeline.
Unit tests verify the rewrite and casting now succeeds.
# Task 45:
exec_error query: "select T.tgrelid as table_id,\n       T.oid as trigger_id,\n       T.xmin as trigger_state_number,\n       T.tgname as trigger_name,\n       T.tgfoid as function_id,\n       pg_catalog.encode(T.tgargs, 'escape') as function_args,\n       T.tgtype as bits,\n       T.tgdeferrable as is_deferrable,\n       T.tginitdeferred as is_init_deferred,\n       T.tgenabled as trigger_fire_mode,\n       T.tgattr as columns,\n       T.tgconstraint != 0 as is_constraint,\n       T.tgoldtable /* null */ as old_table_name,\n       T.tgnewtable /* null */ as new_table_name,\n       pg_catalog.pg_get_triggerdef(T.oid, true) as source_code\nfrom pg_catalog.pg_trigger T\njoin pg_catalog.pg_class TAB on TAB.oid = T.tgrelid and TAB.relnamespace = $1::oid\nwhere true\n  --  and TAB.relname in ( :[*f_names] )\n  and pg_catalog.age(T.xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 21

exec_error params: Some([Some(b'\0\0\0\0\0\0\x08\x98'), Some(b'\0\0\0\0\0\0\0\0')])
exec_error error: Context("type_coercion", Plan("Failed to coerce arguments to satisfy a call to 'array_has' function: coercion from [Utf8, Int32] to the signature ArraySignature(Array { arguments: [Array, Element], array_coercion: Some(FixedSizedListToList) }) failed"))
# Task 52:
exec_error query: "select tab.oid               table_id,\n       tab.relkind           table_kind,\n       ind_stor.relname      index_name,\n       ind_head.indexrelid   index_id,\n       ind_stor.xmin         state_number,\n       ind_head.indisunique  is_unique,\n       ind_head.indisprimary is_primary,\n       /*ind_head.indnullsnotdistinct */false  nulls_not_distinct,\n       pg_catalog.pg_get_expr(ind_head.indpred, ind_head.indrelid) as condition,\n       (select pg_catalog.array_agg(inhparent::bigint order by inhseqno)::varchar from pg_catalog.pg_inherits where ind_stor.oid = inhrelid) as ancestors,\n       ind_stor.reltablespace tablespace_id,\n       opcmethod as access_method_id\nfrom pg_catalog.pg_class tab\n         join pg_catalog.pg_index ind_head\n              on ind_head.indrelid = tab.oid\n         join pg_catalog.pg_class ind_stor\n              on tab.relnamespace = ind_stor.relnamespace and ind_stor.oid = ind_head.indexrelid\n         left join pg_catalog.pg_opclass on pg_opclass.oid = ANY(indclass)\nwhere tab.relnamespace = $1::oid\n        and tab.relkind in ('r', 'm', 'v', 'p')\n        and ind_stor.relkind in ('i', 'I')\n--  and tab.relname in ( :[*f_names] )\nand pg_catalog.age(ind_stor.xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)"
exec_error params: Some([Some(b'\0\0\0\0\0\0\x08\x98'), Some(b'\0\0\0\0\0\0\0\0')])
exec_error error: Context("type_coercion", Plan("Failed to coerce arguments to satisfy a call to 'array_has' function: coercion from [Utf8, Int32] to the signature ArraySignature(Array { arguments: [Array, Element], array_coercion: Some(FixedSizedListToList) }) failed"))
## # Task 52: Done
Implemented `rewrite_oidvector_any` to wrap ANY() predicates on oidvector columns with `oidvector_to_array`. Added unit and functional tests verifying pg_opclass joins succeed.
