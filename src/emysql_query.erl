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
-export([find/2, find/3, find/4]).
-export([find_first/4]).
-export([find_each/4, find_each/5, find_each/6]).

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
            emysql_conn:execute(Conn, RawSql, Values);
        Pool ->
            emysql:execute(Pool, RawSql, Values)
    end.

%%--------------------------------------------------------------------
%% @doc
%% e.g: find(my_pool, users, [ {select, ["name", "age"]},
%%                             {where,  ["age > ? AND color in (?)", Age, "blue, red"]},
%%                             {order,  "id desc"},
%%                             {limit,  100 }], ?AS_REC(user))
%% or
%%
%% find(my_pool, users, [ {select, ["name", "age"]},
%%                        {where,  [{age, '>', Age}, {color, in, ["blue", "red"]} ]},
%%                        {order,  "id desc"},
%%                        {limit,  100 }], ?AS_REC(user))
%%
%% or
%%
%% find(my_pool, users, [ {select, ["count(unique name)"]},
%%                        {where,  [{age, '>', Age}]},
%%                        {order,  "id desc"},
%%                        {limit,  100 }], ?AS_VAL)
%%
%%
%% @spec
%% @end
%%--------------------------------------------------------------------
find(ConnOrPool, Table, SqlOptions) ->
    {FindSql, CondVals} = build_sql(Table, SqlOptions),
    case ConnOrPool of
        #emysql_connection{} = Conn ->
            emysql_conn:execute(Conn, FindSql, CondVals);
        Pool ->
            emysql:execute(Pool, FindSql, CondVals)
    end.
find(ConnOrPool, Table, SqlOptions, ?AS_VAL) ->
    case find(ConnOrPool, Table, SqlOptions) of
        #result_packet{rows = [[R] ]} -> R;
        #result_packet{rows = Rs }    ->
            lists:foldl(fun([R], AccIn) ->
                                lists:append(AccIn, [R]) 
                        end, [], Rs);
        #error_packet{code = Code, msg = Msg} ->
            throw({Code, Msg})
    end;
    
find(ConnOrPool, Table, SqlOptions, [Rec, RecFields] = _AsRec)  ->
    Result = find(ConnOrPool, Table, SqlOptions),
    emysql_conv:as_record(Result, Rec, RecFields).


%%--------------------------------------------------------------------
%% @doc
%% e.g: find(my_pool, users, [ {select, ["name", "age"]},
%%                             {where,  ["age > ?", Age]},
%%                             {order,  "id desc"} ], ?AS_REC(user))
%%
%% @spec
%% @end
%%--------------------------------------------------------------------
find_first(ConnOrPool, Table, SqlOptions, ?AS_VAL) ->
    NSqlOptions = proplists:delete(limit, SqlOptions),
    find(ConnOrPool, Table, [{limit, 1} | NSqlOptions], ?AS_VAL);

find_first(ConnOrPool, Table, SqlOptions, [_Rec, _RecFields] = AsRec) ->
    NSqlOptions = proplists:delete(limit, SqlOptions),
    case find(ConnOrPool, Table, [{limit, 1} | NSqlOptions], AsRec) of
        [ ]   -> undefined;
        [Val] -> Val
    end.


%%--------------------------------------------------------------------
%% @doc
%%
%% e.g:
%%      Fun = fun(#user{} = U) ->
%%                do_something_to(U)
%%            end,
%%      find_each(my_pool, users, ?AS_REC(user), Fun).
%%
%%      find_each(my_pool, users, [ {select, ["name", "age"]},
%%                                  {where,  ["age > ?", Age]},
%%                                  % {order,  "id desc"},
%%                                  {order, [id, "DESC"] },
%%                                  {limit,  100 }            ], 10, ?AS_REC(user), Fun).
%%
%% @spec
%% @end
%%--------------------------------------------------------------------
find_each(ConnOrPool, Table, AsRec, Fun) ->
    find_each(ConnOrPool, Table, [], 1000, AsRec, Fun, undefined).

find_each(ConnOrPool, Table, AsRec, Fun, AccIn) ->
    find_each(ConnOrPool, Table, [], 1000, AsRec, Fun, AccIn).

find_each(ConnOrPool, Table, SqlOptions, AsRec, Fun, AccIn) ->
    find_each(ConnOrPool, Table, SqlOptions, 1000, AsRec, Fun, AccIn).

find_each(ConnOrPool, Table, SqlOptions, BatchSize, AsRec, Fun, AccIn) ->
    BaseId = undefined,
    
    {[FindSql, FindCondVals],
     [CountSql, CountCondVals],
     [OField, OSort]} = build_sql(Table, SqlOptions, BatchSize, BaseId),
    
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
            %BaseId = case OSort of
            %             "ASC"  -> 0;
            %             "DESC" -> infinite
            %         end,
            %do_find_each(ConnOrPool, Table, FindSql, FindCondVals, BatchSize,
            %             AsRec, Fun, AccIn, Remain, BaseId);
            do_find_each(ConnOrPool, Table, SqlOptions, BatchSize, AsRec, Fun, AccIn, Remain, BaseId);
        #error_packet{code = Code, msg = Msg} ->
            throw({Code, Msg});
        _ ->
            ok
    end.
    
        
%%%===================================================================
%%% Internal functions
%%%===================================================================
build_sql(Table, SqlOptions) ->
    {[FindSql, FindCondVals], _, _} = build_sql(Table, SqlOptions, undefined, undefined),
    {FindSql, FindCondVals}.

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
                            [ Head | _T] = L when is_tuple(Head) ->
                                {Stmt, Vals} =
                                    lists:foldl(
                                      fun(Item, {StmtAcc, ValAcc}) ->
                                              case Item of
                                                  {K, between, [V1, V2]} ->
                                                      S = type_utils:any_to_list(K) ++ " BETWEEN ? AND ? ",
                                                      {[S | StmtAcc], [V1, V2 | ValAcc]};
                                                  {K, in, V3} when is_list(V3) ->
                                                      S = type_utils:any_to_list(K) ++ " IN (?) ",
                                                      NV3 = string:join(V3, ", "), 
                                                      {[S | StmtAcc], [NV3 | ValAcc]};
                                                  {K, OP, V4} ->
                                                      S = string:join([type_utils:any_to_list(K),
                                                                       type_utils:any_to_list(OP),
                                                                       "?"], " "),
                                                      {[S | StmtAcc], [V4 | ValAcc] };
                                                  {K, V4} ->
                                                      S = type_utils:any_to_list(K) ++ " = ?",
                                                      {[S | StmtAcc], [V4 | ValAcc]};
                                                  _ ->
                                                      {StmtAcc, ValAcc}
                                              end
                                      end, {[], []}, L),
                                {"WHERE " ++ string:join(Stmt, " AND "), Vals};
                            [ Cond ]        -> {"WHERE " ++ Cond, [ ]};
                            [Cond | Values] -> {"WHERE " ++ Cond, Values}
                        end,
    
    [Order, OField, OSort] =
        case proplists:get_value(order, SqlOptions) of
            undefined -> ["", id, "ASC"];
            [OrderField, OrderSort]   ->
                OrderStr1 = "ORDER BY " ++ type_utils:any_to_list(OrderField) ++ " " ++ type_utils:any_to_list(OrderSort),
                [OrderStr1, OrderField, OrderSort];
            [[OrderField, OrderSort] | _] = OrderOpt ->
                MultiOrder = lists:foldl(fun([OField0, OSort0], OAcc) ->
                                                 lists:append(OAcc, [type_utils:any_to_list(OField0) ++ " " ++ type_utils:any_to_list(OSort0)])
                                         end, [], OrderOpt),
                OrderStr2 = "ORDER BY " ++ string:join(MultiOrder, ", "),
                [OrderStr2, OrderField, OrderSort]; %% TODO: should return multi-order
            OrderBy ->
                OrderStr3 = "ORDER BY " ++ type_utils:any_to_list(OrderBy),
                [OrderStr3, undefined, undefined]
        end,
    
    {Where2, CondVals2} =
        case {BaseId, OSort, Where} of
            {infinate,  _,  _} -> {Where, CondVals};
            {undefined, _,  _} -> {Where, CondVals};
            {_,  "ASC",  ""} -> {"WHERE " ++ type_utils:any_to_list(OField) ++ " > ?", [BaseId]};
            {_,  "DESC", ""} -> {"WHERE " ++ type_utils:any_to_list(OField) ++ " < ?", [BaseId]};
            {_,  "ASC",   _} ->
                 {Where ++ " AND " ++ type_utils:any_to_list(OField) ++ " > ?",
                  lists:append(CondVals, [BaseId])};
            {_,  "DESC", _ } ->
                {Where ++ " AND " ++ type_utils:any_to_list(OField) ++ " < ?",
                 lists:append(CondVals, [BaseId])}
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
    {[FindSql, CondVals2], [CountSql, CondVals], [OField, OSort]}.


do_find_each(ConnOrPool, Table, SqlOptions, BatchSize, [Rec, RecFields] = AsRec,
             Fun, AccIn, Remain, BaseId) ->
    {[Sql, CondVals],
     [_CountSql, _CountCondVals],
     [OrderField, _OSort]} = build_sql(Table, SqlOptions, BatchSize, BaseId),
    case OrderField of
        undefined ->
            throw(order_field_is_undefined);
        _ ->
            Result = case ConnOrPool of
                         #emysql_connection{} = Conn ->
                             emysql_conn:execute(Conn, Sql, CondVals);
                         Pool ->
                             emysql:execute(Pool, Sql, CondVals)
                     end,
            case Result of
                #result_packet{rows = Rows} ->
                    case emysql_conv:as_record(Result, Rec, RecFields, Fun, AccIn) of
                        {[], NAccIn}  -> NAccIn;
                        {Rs, NAccIn}  ->
                            LastRow = lists_utils:last(Rs),
                            NextId = tuple_utils:value(OrderField, LastRow, RecFields),
                            case Remain - BatchSize > 0 of
                                true ->
                                    do_find_each(ConnOrPool, Table, SqlOptions, BatchSize, AsRec,
                                                 Fun, NAccIn, (Remain - BatchSize), NextId);
                                false ->
                                    NAccIn
                            end;
                        Rs when is_list(Rs)  ->
                            LastRow = lists_utils:last(Rs),
                            NextId = case erlang:is_record(LastRow, Rec) of
                                         true ->
                                             tuple_utils:value(OrderField, LastRow, RecFields);
                                         _ ->
                                             undefined
                                     end,
                            case Remain - BatchSize > 0 of
                                true ->
                                    do_find_each(ConnOrPool, Table, SqlOptions, BatchSize, AsRec,
                                                 Fun, undefined, (Remain - BatchSize), NextId);
                                false ->
                            ok
                            end
                    end;
                _ ->
                    AccIn
            end
    
    end.
            
                    
