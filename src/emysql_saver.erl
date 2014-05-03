%%%-------------------------------------------------------------------
%%% @author Jack Tang <jack@taodinet.com>
%%% @copyright (C) 2014, Jack Tang
%%% @doc
%%%
%%% @end
%%% Created : 28 Apr 2014 by Jack Tang <jack@taodinet.com>
%%%-------------------------------------------------------------------
-module(emysql_saver).

%% API
-export([save/3, save/4]).
-export([find_or_create_by/4]).

-include("emysql.hrl").
%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Options = [{insert_ignore, true},
%%            {batch_size,    1000},
%%            {update_attr,   [data] }]
%% emysql_saver:save(pool, test, ?INPUT(#test{data = "hello"}, Options)
%% 
%% @spec
%% @end
%%--------------------------------------------------------------------
save(_ConnOrPool, _Table, undefined) ->
    ok.
save(ConnOrPool, Table, RecordInput) ->
    save(ConnOrPool, Table, RecordInput, []).

save(ConnOrPool, Table, [Records, Fields] = _RecordInput, Options) ->
    {InsertOrUpdate, Sql, Values} = build_sql(Table, Records, Fields, Options),
    Result = case ConnOrPool of
                 #emysql_connection{} = Conn ->
                     emysql_conn:execute(Conn, Sql, Values);
                 Pool ->
                     emysql:execute(Pool, Sql, Values)
             end,
    case {InsertOrUpdate, Result} of
        {insert, #ok_packet{affected_rows = Affected, insert_id = InsertId}} ->
            case {Affected, length(Records), lists:nth(1, Fields)} of
                {1, 1, id} ->
                    [Record] = Records,
                    setelement(2, Record, InsertId),
                    Record;
                _ -> ok
            end;
        _ -> Records
    end,
                
    ok.

%%--------------------------------------------------------------------
%% @doc
%% 
%% emysql_saver:find_or_create_by(pool, test, )
%% 
%% @spec
%% @end
%%--------------------------------------------------------------------
find_or_create_by(ConnOrPool, Table, FindCond, CreateFun) ->
    [ConnOrPool, Table, FindCond, CreateFun],
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================
%% insert into table(f1, f2, f3) values(v1, v2, v3);
%% insert into table(f1, f2, f3) values(v11, v12, v13), (v21, v22, v23), ...;
%% update table set f1 = ?
build_sql(Table, [Record | Tail] = _Records, Fields, Options) ->

    UpdateFields = case proplists:get_value(update_fields, Options) of
                      undefined -> [];
                      V -> V
                  end,
    
    {_, UpdateFields, UpdateFIndex, UpdateVals} = 
        lists:foldl(
          fun(Field, [Index, EffectedFields, EffectedIndex, Vals]) ->
                  case lists:member(Field, UpdateFields) of
                      true -> 
                          NEffectedFields = [Field | EffectedFields],
                          NEffectedIndex  = [Index | EffectedIndex],
                          Val = element(Index + 1, Record),
                          NVals = [Val | Vals],
                          {Index + 1, NEffectedFields, NEffectedIndex, NVals};
                      false ->
                          {Index + 1, EffectedFields, EffectedIndex, Vals}
                  end
          end, {1, [], [], []}, Fields),

    case element(2, Record) of
        undefined -> % insert
            SqlHead  = "INSERT INTO " ++ type_utils:any_to_list(Table),
            SqlFields = string:join(UpdateFields, ","),
            SqlValues = string:join(lists:duplicate(length(UpdateFields), "?"), ","),
            Sql = string:join([SqlHead, "(", SqlFields, ") VALUES (", SqlValues, ")"], " "),
            {insert, Sql, UpdateVals};
        _ -> % update
            PK = type_utils:any_to_list(lists:nth(1, Fields)),
            SqlHead = "UPDATE " ++ type_utils:any_to_list(Table),
            SqlTail = "WHERE " ++ PK ++ " = ?",
            SqlSet  = string:join([ F ++ " = ?" || F <- UpdateFields], ","),
            Sql = string:join([SqlHead, "SET ", SqlSet, SqlTail], " "),
            {update, Sql, lists:append(UpdateVals, [element(2, Record)])}
    end.

