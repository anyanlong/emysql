%%%-------------------------------------------------------------------
%%% @author Jack Tang <jack@taodinet.com>
%%% @copyright (C) 2014, Jack Tang
%%% @doc
%%%
%%% @end
%%% Created : 24 Apr 2014 by Jack Tang <jack@taodinet.com>
%%%-------------------------------------------------------------------
-module(emysql_query).

%% API
-export([find/2, find/4]).
-export([find_first/4, find_each/6]).


-include("emysql.hrl").

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% e.g: find(my_pool, ["SELECT name FROM users where age > ? and time > ?", 12, "2013-12-03" ])
%% 
%%
%% @spec
%% @end
%%--------------------------------------------------------------------
find(ConnPool, [RawSql | Values]) ->
    StmtName = type_utils:any_to_atom("stmt_" ++ type_utils:md5_hex(RawSql)),
    emysql:prepare(StmtName, type_utils:any_to_binary(RawSql)),
    emysql:execute(ConnPool, StmtName, Values).

%%--------------------------------------------------------------------
%% @doc
%% e.g: find(my_pool, users, [ {select, [name, age]},
%%                             {where,  ["age > ?", Age]},
%%                             {order,  "id desc"},
%%                             {limit,  100 }], ?AS_REC(user))
%%
%% @spec
%% @end
%%--------------------------------------------------------------------
find(ConnOrPool, Table, SqlOptions, [Rec, RecFields] = _AsRec)  ->
    {Sql, CondVals} = build_sql(Table, SqlOptions),
    Result = case ConnOrPool of
                 #emysql_connection{} = Conn ->
                     emysql_conn:execute(Conn, Sql, CondVals);
                 Pool ->
                     emysql:execute(Pool, Sql, CondVals)
             end,
    emysql_util:as_record(Result, Rec, RecFields).


%%--------------------------------------------------------------------
%% @doc
%% e.g: find(my_pool, users, [ {select, [name, age]},
%%                             {where,  ["age > ?", Age]},
%%                             {order,  "id desc"} ], ?AS_REC(user))
%%
%% @spec
%% @end
%%--------------------------------------------------------------------
find_first(ConnOrPool, Table, SqlOptions, AsRec) ->
    NSqlOptions = proplists:delete(limit, SqlOptions),
    find(ConnOrPool, Table, [{limit, 1} | NSqlOptions], AsRec).


%%--------------------------------------------------------------------
%% @doc
%%
%% e.g:
%%      Fun = fun(#user{} = U) ->
%%                do_something_to(U)
%%            end,
%%      find(my_pool, users, [ {select, ["name", "age"]},
%%                             {where,  ["age > ?", Age]},
%%                             {order,  "id desc"},
%%                             {limit,  100 }            ], 10, ?AS_REC(user), Fun)
%%
%% @spec
%% @end
%%--------------------------------------------------------------------
find_each(ConnOrPool, Table, SqlOptions, BatchSize, AsRec, Fun) ->
    {FindSql, CountSql, CondVals} = build_sql(Table, SqlOptions),
    Result = case ConnOrPool of
                 #emysql_connection{} = Conn ->
                     emysql_conn:execute(Conn, CountSql, CondVals);
                 Pool ->
                     emysql:execute(Pool, CountSql, CondVals)
             end,
    case Result of
        #ok_packet{} ->
            Total = 1000,
            Limit = proplists:get_value(limit, SqlOptions, 0),
            Remain = 10, % (Total > Limit ? Limit : Total),
            do_find_each(ConnOrPool, Table, FindSql, CondVals, BatchSize, AsRec, Fun, Remain);
        #error_packet{} ->
            ok
    end.
    

do_find_each(ConnOrPool, Table, Sql, CondVals, BatchSize, [Rec, RecFields] = AsRec, Fun, Remain) ->
    Result = case ConnOrPool of
                 #emysql_connection{} = Conn ->
                     emysql_conn:execute(Conn, Sql, CondVals);
                 Pool ->
                     emysql:execute(Pool, Sql, CondVals)
             end,
    lists:foreach(
      fun(Item) ->
              Fun(emysql_util:as_record(Result, Rec, RecFields))
      end, Result),
    
    case Remain - BatchSize > 0 of
        true -> 
            do_find_each(ConnOrPool, Table, Sql, CondVals, BatchSize, AsRec, Fun, (Remain - BatchSize));
        false ->
            ok
    end.
        
%%%===================================================================
%%% Internal functions
%%%===================================================================
build_sql(Table, SqlOptions) ->
    SelectFields =
        case proplists:get_value(select, SqlOptions) of
            undefined         -> "*";
            V when is_list(V) -> string:join(V, ",");
            _ -> throw(bad_sql_select)
        end,
        
    {Where, CondVals} = case proplists:get_value(where, SqlOptions) of
                            undefined -> "";
                            [Cond | Values] -> {"WHERE " ++ Cond, Values}
                        end,
    
    Order = case proplists:get_value(order, SqlOptions) of
                undefined -> "";
                OrderBy   -> " ORDER BY " ++ type_utils:any_to_list(OrderBy)
            end,
    Limit = case proplists:get_value(limit, SqlOptions) of
                undefined   -> "";
                [LV1, LV2]  ->
                    " LIMIT " ++ type_utils:any_to_list(LV1) ++ ", " ++ type_utils:any_to_list(LV2);
                LV3         -> " LIMIT " ++ type_utils:any_to_list(LV3)
            end,

    Table2 = type_utils:any_to_list(Table),
    FindSql  = string:join(["SELECT", SelectFields, "FROM", Table2, Where, Order, Limit], " "),
    CountSql = string:join(["SELECT COUNT(1) FROM", Table2, Where], " "),
    {FindSql, CountSql, CondVals}.
                             
            
                    
