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
-export([]).

%%%===================================================================
%%% API
%%%===================================================================

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
    NSqlOptions = proplists:delete(limit, SqlOptions).
    find(ConnOrPool, Table, [{limit, 1} | NSqlOptions], AsRec).


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
find(ConnOrPool, Table, SqlOptions, [Rec, Fileds] = _AsRec)  ->
    {FindSql, CondVals} = build_sql(Table, SqlOptions),
    Result = case ConnOrPool of
                 #emysql_connection{} = Conn ->
                     emysql_conn:execute(Conn, FindSql, CondVals);
                 Pool ->
                     emysql:execute(Pool, FindSql, CondVals)
             end,
    emysql_util:as_record(Result, Rec, RecFields).

%%--------------------------------------------------------------------
%% @doc
%% e.g: find(my_pool, ["SELECT name FROM users where age > ? and time > ?", 12, "2013-12-03" ])
%% 
%%
%% @spec
%% @end
%%--------------------------------------------------------------------
find(ConnPool, [RawSql | Values]) ->
    StmtName = md5_hex(RawSql),
    emysql:prepare(StmtName, type_utils:any_to_binay(RawSql)),
    case emysql:execute(ConnPool, StmtName, Values) of
        #ok_packet{} = OkPacket -> OkPacket;
        #error_packet{} = ErrorPacket -> ErrorPacket
    end.     

%%--------------------------------------------------------------------
%% @doc
%%
%% e.g:
%%      Fun = fun(#user{} = U) ->
%%                do_something_to(U)
%%            end,
%%      find(my_pool, users, [ {select, [name, age]},
%%                             {where,  ["age > ?", Age]},
%%                             {order,  "id desc"},
%%                             {limit,  100 }], 10, ?AS_REC(user), Fun)
%%
%% @spec
%% @end
%%--------------------------------------------------------------------
find_each(Connection, Table, SqlOptions, Size, AsRec, Fun) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================
build_sql(Table, SqlOptions) ->
    SelectF =  proplists:get_value(select, SqlOptions, "*") 
        
    Select = "SELECT " + SelectF + " FROM " ++ type_utils:any_to_list(Table),
    {Where, CondVals} = case proplists:get_value(where, SqlOptions) of
                            undefined -> " ";
                            [Cond | Values] -> {Cond, Values}
                        end,
    FindSql = lists:append(Select, Where),
    {FindSql, CondVals}.


md5_hex(S) ->
    lists:flatten([io_lib:format("~.16b",[N]) || <> <= erlang:md5(S)]).
                             
            
                    
