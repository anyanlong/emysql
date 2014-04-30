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
-export([find_first/4]).
-export([find_each/5, find_each/6]).


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
find(ConnOrPool, [RawSql | Values]) ->
    case ConnOrPool of
        #emysql_connection{} = Conn ->
            mysql_conn:execute(Conn, RawSql, Values);
        Pool ->
            emysql:execute(Pool, RawSql, Values)
    end.

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
    {FindSql, CondVals} = build_sql(Table, SqlOptions),
    Result = case ConnOrPool of
                 #emysql_connection{} = Conn ->
                     emysql_conn:execute(Conn, FindSql, CondVals);
                 Pool ->
                     emysql:execute(Pool, FindSql, CondVals)
             end,
    emysql_conv:as_record(Result, Rec, RecFields).


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
find_each(ConnOrPool, Table, SqlOptions, AsRec, Fun) ->
    find_each(ConnOrPool, Table, SqlOptions, 1000, AsRec, Fun).

find_each(ConnOrPool, Table, SqlOptions, BatchSize, AsRec, Fun) ->
    BaseId = 0,
    {[FindSql, FindCondVals], [CountSql, CountCondVals]} = build_sql(Table, SqlOptions, BatchSize, BaseId),
    
    Result = case ConnOrPool of
                 #emysql_connection{} = Conn ->
                     emysql_conn:execute(Conn, CountSql, CountCondVals);
                 Pool ->
                     emysql:execute(Pool, CountSql, CountCondVals)
             end,
    case Result of
        #result_packet{rows = [[Total]]} ->
            Limit = proplists:get_value(limit, SqlOptions),
            Remain = case {Limit, Total > Limit} of
                         {undefined, _}  -> Total;
                         {_,      true}  -> Limit;
                         {_,     false}  -> Total
                     end,
            do_find_each(ConnOrPool, Table, FindSql, FindCondVals, BatchSize,
                         AsRec, Fun, Remain, BaseId);
        #error_packet{code = Code, msg = Msg} ->
            throw({Code, Msg});
        _ ->
            ok
    end.
    
        
%%%===================================================================
%%% Internal functions
%%%===================================================================
build_sql(Table, SqlOptions) ->
    {Ret, _} = build_sql(Table, SqlOptions, undefined, undefined),
    Ret.

build_sql(Table, SqlOptions, BatchSize, BaseId) ->
    SelectFields =
        case proplists:get_value(select, SqlOptions) of
            undefined         -> "*";
            V when is_list(V) -> string:join(V, ",");
            _ -> throw(bad_sql_select)
        end,
        
    {Where, CondVals} = case proplists:get_value(where, SqlOptions) of
                            undefined -> {"", [ ]};
                            [ ]       -> {"", [ ]};
                            [ Cond ]     -> {"WHERE " ++ Cond, [ ]};
                            [Cond | Values] -> {"WHERE " ++ Cond, Values}
                        end,
    {Where2, CondVals2} =
        case {BaseId, Where} of
            {undefined, _} -> {Where, CondVals};
            {_,        ""} -> {"WHERE id > ?", [BaseId]};
            _              ->
                {Where ++ "AND id > ?", lists:append(CondVals, [BaseId])}
        end,
    
        
    Order = case proplists:get_value(order, SqlOptions) of
                undefined -> "";
                OrderBy   -> "ORDER BY " ++ type_utils:any_to_list(OrderBy)
            end,


    Limit = 
        case BatchSize of
            undefined -> 
                case proplists:get_value(limit, SqlOptions) of
                    undefined   -> "";
                    [LV1, LV2]  ->
                        "LIMIT " ++ type_utils:any_to_list(LV1) ++ ", " ++ type_utils:any_to_list(LV2);
                    LV3         -> "LIMIT " ++ type_utils:any_to_list(LV3)
                end;
            _ ->
                "LIMIT " ++ type_utils:any_to_list(BatchSize)
        end,
        

    Table2 = type_utils:any_to_list(Table),
    FindSql  = string:join(["SELECT", SelectFields, "FROM", Table2, Where2, Order, Limit], " "),
    CountSql = string:join(["SELECT COUNT(1) FROM", Table2, Where], " "),
    {[FindSql, CondVals2], [CountSql, CondVals]}.



do_find_each(ConnOrPool, Table, Sql, CondVals, BatchSize, [Rec, RecFields] = AsRec, Fun, Remain, BaseId) ->
    NCondVals = lists:append(lists_utils:droplast(CondVals), [BaseId]),
    
    Result = case ConnOrPool of
                 #emysql_connection{} = Conn ->
                     emysql_conn:execute(Conn, Sql, NCondVals);
                 Pool ->
                     emysql:execute(Pool, Sql, NCondVals)
             end,
    case Result of
        #result_packet{rows = Rows} ->
            emysql_conv:as_record(Result, Rec, RecFields, Fun),
            LastRow = lists_utils:last(Rows),
            [NextId | _Tail] = LastRow,
            case Remain - BatchSize > 0 of
                true -> 
                    do_find_each(ConnOrPool, Table, Sql, CondVals, BatchSize, AsRec,
                                 Fun, (Remain - BatchSize), NextId);
                false ->
                    ok
            end;
        _ ->
            failed
    end.
            
                    
