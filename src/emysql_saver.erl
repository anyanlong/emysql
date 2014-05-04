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
%% Options = [{insert_ignore,  true},
%%            {batch_size,     1000},
%%            {update_attrs,   [data] }]
%% emysql_saver:save(pool, test, ?INPUT(#test{data = "hello"}, Options)
%% 
%% @spec
%% @end
%%--------------------------------------------------------------------
save(_ConnOrPool, _Table, undefined) ->
    ok;
save(_ConnOrPool, _Table, [[], _]) ->
    ok;
save(ConnOrPool, Table, RecordInput) ->
    DefOpt = [{auto_id,    true},
              {batch_size, 1000}],
    save(ConnOrPool, Table, RecordInput, DefOpt).

save(ConnOrPool, Table, [Records, Fields] = _RecordInput, Options) ->
    [FieldPK | _ ] = Fields,
    case build_sql(Table, Records, Fields, Options) of
        {insert, Sqls} ->
            X = lists:foldl(
                  fun({Sql, Values}, AccIn) ->
                          Result = case ConnOrPool of
                                       #emysql_connection{} = Conn ->
                                           emysql_conn:execute(Conn, Sql, Values);
                                       Pool ->
                                           emysql:execute(Pool, Sql, Values)
                                   end,
                          case Result of
                              #ok_packet{affected_rows = Affected, insert_id = InsertId} ->
                                  case {Affected, length(Records), FieldPK} of
                                      {1, 1, id} ->
                                          [Record] = Records,
                                          [setelement(2, Record, InsertId) | AccIn];
                                      _         -> AccIn
                                  end;
                              #error_packet{code = Code, msg = Msg} ->
                                  throw({Code, Msg})
                          end
                  end, [], Sqls),
            case X of
                [] -> ok;
                [V] -> V
            end;
        {update, {Sql, Values} } ->
             case ConnOrPool of
                 #emysql_connection{} = Conn ->
                     emysql_conn:execute(Conn, Sql, Values);
                 Pool ->
                     emysql:execute(Pool, Sql, Values)
             end
    end.

%%--------------------------------------------------------------------
%% @doc
%% 
%% emysql_saver:find_or_create_by(pool, test, ["select * from test where data = ?", "hello"],
%%                                            fun() ->
%%                                                ?INPUT(#test{data = "find me"})
%%                                            end)
%% 
%% @spec
%% @end
%%--------------------------------------------------------------------
find_or_create_by(ConnOrPool, Table, FindSql, CreateFun) ->
    Result = emysql_query:find(ConnOrPool, FindSql),
    [[Record], Fields] = CreateFun(),
    case Result of
        #result_packet{rows = Rows} when length(Rows) =:= 0 ->
            save(ConnOrPool, Table, [[Record], Fields]);
        #result_packet{} ->
            emysql_conv:as_record(Result, element(1, Record), Fields);
        Other ->
            Other
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
%% insert into table(f1, f2, f3) values(v1, v2, v3);
%% insert into table(f1, f2, f3) values(v11, v12, v13), (v21, v22, v23), ...;
%% update table set f1 = ?
build_sql(_Table, [] = _Records, _Fields, _Options) ->
    {error, no_input};
build_sql(Table, [Record | _Tail] = Records, Fields, Options) ->
    BatchSize = proplists:get_value(batch_size, Options, 1000),
                
    {UpdateFields, UpdateFIndex, UpdateVals} = select_update_fields_index(Record, Fields, Options),
    
    case element(2, Record) of
        undefined -> % insert
            SqlAndValues = generate_insert_sql(Table, UpdateFields, UpdateFIndex, Records, BatchSize),
            {insert, SqlAndValues};
        _ -> % update
            [PK | _] = Fields,
            PKPair = {type_utils:any_to_list(PK), element(2, Record)},
            SqlAndValues = generate_update_sql(Table, UpdateFields, UpdateVals, PKPair),
            {update, SqlAndValues}
    end.

select_update_fields_index(Record, Fields, Options) ->
    {UAOptIsSet, UpdateAttrs} =
        case proplists:get_value(update_attrs, Options) of
            undefined -> {no, []};
            V         -> {yes, V}
        end,

    AutoId = case proplists:get_value(auto_id, Options) of
                 true -> true;
                 _    -> false
             end,
    
    {_, UpdateFields, UpdateFIndex, UpdateVals} =
        lists:foldl(
          fun(Field, {Index, EffectedFields, EffectedIndex, Vals}) ->
                  case {Field, AutoId, UAOptIsSet, lists:member(Field, UpdateAttrs)} of
                      {id, true, _, _} ->
                          {Index + 1, EffectedFields, EffectedIndex, Vals};
                      {_,  _,   no, _} ->
                          NEffectedFields = [type_utils:any_to_list(Field) | EffectedFields],
                          NEffectedIndex  = [(Index + 1) | EffectedIndex],
                          Val = element(Index + 1, Record),
                          {Index + 1, NEffectedFields, NEffectedIndex, [Val | Vals]};
                      {_,  _, yes, true} -> 
                          NEffectedFields = [type_utils:any_to_list(Field) | EffectedFields],
                          NEffectedIndex  = [(Index + 1) | EffectedIndex],
                          Val = element(Index + 1, Record),
                          {Index + 1, NEffectedFields, NEffectedIndex, [Val | Vals]};
                      {_, _, yes, false} ->
                          {Index + 1, EffectedFields, EffectedIndex, Vals}
                  end
          end, {1, [], [], []}, Fields),
    {UpdateFields, UpdateFIndex, UpdateVals}.

generate_insert_sql(Table, UpdateFields, UpdateFIndex, Records, BatchSize) ->
    SqlHead  = "INSERT INTO " ++ type_utils:any_to_list(Table),
    SqlFields = string:join(UpdateFields, ","),
    ValuesInSql = "(" ++ string:join(lists:duplicate(length(UpdateFields), "?"), ",") ++ ")",
            
    BatchCount = length(Records) div BatchSize,
    BatchRemainSize = length(Records) rem BatchSize,

    Batch1Sql = case BatchCount of
                    0    -> undefined;
                    _    ->
                        SqlValues1 = string:join(lists:duplicate(BatchSize, ValuesInSql), ","),
                        string:join([SqlHead, "(", SqlFields, ") VALUES ", SqlValues1], " ")
                end,
    Batch2Sql = case BatchRemainSize of
                    0    -> undefined;
                    _    ->
                        SqlValues2 = string:join(lists:duplicate(BatchRemainSize, ValuesInSql), ","),
                        string:join([SqlHead, "(", SqlFields, ") VALUES ", SqlValues2], " ")
                end,   
                                
    RecBatchValues =
        lists_utils:split(BatchSize, Records,
                          fun(NRecords) ->
                                  lists:foldl(
                                    fun(RecItem, AccIn) ->
                                            lists:foldl(
                                              fun(Idx, AccIn2) ->
                                                      Val = element(Idx, RecItem),
                                                      [Val | AccIn2]
                                              end, AccIn, UpdateFIndex)
                                    end, [], NRecords)    
                          end),
    case BatchRemainSize of
        0 -> [{Batch1Sql, lists:merge(RecBatchValues)}];
        _ ->
            {Batch1Values, Batch2Values} = lists:split(BatchCount, RecBatchValues),
            case Batch1Sql of
                undefined -> [{Batch2Sql, lists:merge(Batch2Values)}];
                _         -> [{Batch1Sql, lists:merge(Batch1Values)},
                              {Batch2Sql, lists:merge(Batch2Values)}]
            end
    end.

generate_update_sql(Table, UpdateFields, UpdateVals, {FieldPK, PKVal}) ->
    SqlHead = "UPDATE " ++ type_utils:any_to_list(Table),
    SqlTail = "WHERE " ++ FieldPK ++ " = ?",
    SqlSet  = string:join([ F ++ " = ?" || F <- UpdateFields], ","),
    Sql = string:join([SqlHead, "SET ", SqlSet, SqlTail], " "),
    
    {Sql, lists:append(UpdateVals, [PKVal])}.
