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
-export([update_or_create_by/4,
         update_or_create_by/5]).

-include("emysql.hrl").
%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Options = [{insert_ignore,  true},
%%            {batch_size,     1000},
%%            {update_attrs,   [data] }]
%% emysql_saver:save(pool, test, ?INPUT(#test{data = "hello"}, Options).
%%
%% or
%%
%% emysql_saver:save(pool, test, ?INPUT(#test{data = "hello"})).
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

save(ConnOrPool, Table, [Record0, Fields] = _RecordInput, Options) ->
    [FieldPK | _ ] = Fields,
    Records = case Record0 of
                  V1 when is_list(V1) -> V1;
                  V2                  -> [V2]
              end,
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
             Result = case ConnOrPool of
                          #emysql_connection{} = Conn ->
                              emysql_conn:execute(Conn, Sql, Values);
                          Pool ->
                              emysql:execute(Pool, Sql, Values)
                      end,
            case Result of
                #ok_packet{affected_rows = Affected} ->
                    case {Affected, length(Records)} of
                        {1, 1} ->
                            [Record] = Records,
                            Record;
                         _ -> Records
                    end;
                #error_packet{code = Code, msg = Msg} ->
                    throw({Code, Msg})
            end     
    end.

%%--------------------------------------------------------------------
%% @doc
%% 
%% emysql_saver:find_or_create_by(pool, test,
%%                                ["select * from test where data = ?", "hello"],
%%                                fun() ->
%%                                                ?INPUT(test, #test{data = "find me"})
%%                                end)
%% 
%% @spec
%% @end
%%--------------------------------------------------------------------
find_or_create_by(ConnOrPool, Table, FindSql, CreateFun) ->
    Result = emysql_query:find(ConnOrPool, FindSql),
    case CreateFun() of
        [Record, Fields] ->
            case Result of
                #result_packet{rows = Rows} when length(Rows) =:= 0 ->
                    save(ConnOrPool, Table, [Record, Fields]);
                #result_packet{} ->
                    [R | _] = emysql_conv:as_record(Result, element(1, Record), Fields),
                    R;
                Other ->
                    Other
            end;
        _ ->
            io:format("Forget to wrap return value using ?INPUT?", [])
    end.

%%--------------------------------------------------------------------
%% @doc
%% Options = [{ignore_null, true}]
%% emysql_saver:update_or_create_by(pool, test,
%%                                  ["select * from test where data = ?", "hello"],
%%                                  fun() ->
%%                                                ?INPUT(test, #test{data = "find me"})
%%                                  end, Options)
%% 
%% @spec
%% @end
%%--------------------------------------------------------------------
update_or_create_by(ConnOrPool, Table, FindSql, Fun) ->
    update_or_create_by(ConnOrPool, Table, FindSql, Fun, []).
        
update_or_create_by(ConnOrPool, Table, FindSql, Fun, Options) ->
    Result = emysql_query:find(ConnOrPool, FindSql),
    case Fun() of
        [Record, Fields] ->
            case Result of
                #result_packet{rows = Rows} when length(Rows) =:= 0 ->
                    save(ConnOrPool, Table, [Record, Fields]);
                #result_packet{} ->
                    case emysql_conv:as_record(Result, element(1, Record), Fields) of
                        [RecInDb] ->
                            case merge_rec(RecInDb, Record, Fields, Options) of
                                {changed, MergedRec} ->
                                    save(ConnOrPool, Table, [MergedRec, Fields]);
                                _ ->
                                    RecInDb
                            end;
                        L ->
                            io:format("Only one record expectedh here, but now there are ~p",
                                      [length(L)]),
                            throw({error, many_records})
                    end;
                Other ->
                    Other
            end;
        _ ->
            io:format("Forget to wrap return value using ?INPUT?", [])
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
    case element(2, Record) of
        undefined -> % insert
            {UpdateFields, UpdateFIndex, _UVals} = select_update_fields_index(insert, Record,
                                                                              Fields, Options),
    
            SqlAndValues = generate_insert_sql(Table, UpdateFields, UpdateFIndex, Records, Options),
            {insert, SqlAndValues};
        _        -> % update
            {UpdateFields, _FIndex, UpdateVals} = select_update_fields_index(update, Record,
                                                                             Fields, Options),
            [PK | _] = Fields,
            PKPair = {type_utils:any_to_list(PK), element(2, Record)},
            SqlAndValues = generate_update_sql(Table, UpdateFields, UpdateVals, PKPair),
            {update, SqlAndValues}
    end.

select_update_fields_index(InsertOrUpdate, Record, Fields, Options) ->
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
                  case {Field, AutoId, UAOptIsSet, InsertOrUpdate, lists:member(Field, UpdateAttrs)} of
                      {id, true, _, _, _} ->
                          {Index + 1, EffectedFields, EffectedIndex, Vals};
                      {created_at, _, _, insert, _} ->
                          NEffectedFields = [type_utils:any_to_list(Field) | EffectedFields],
                          NEffectedIndex  = [ timestamp | EffectedIndex],
                          Val = datetime_utils:localtime_as_string(),
                          {Index + 1, NEffectedFields, NEffectedIndex, [Val | Vals]};
                      {created_at, _, _, update, _} ->
                          {Index + 1, EffectedFields, EffectedIndex, Vals};
                      {updated_at, _, _, _, _} ->
                          NEffectedFields = [type_utils:any_to_list(Field) | EffectedFields],
                          NEffectedIndex  = [ timestamp | EffectedIndex],
                          Val = datetime_utils:localtime_as_string(),
                          {Index + 1, NEffectedFields, NEffectedIndex, [Val | Vals]};
                      {_,  _,   no, _, _} ->
                          NEffectedFields = [type_utils:any_to_list(Field) | EffectedFields],
                          NEffectedIndex  = [(Index + 1) | EffectedIndex],
                          Val = element(Index + 1, Record),
                          {Index + 1, NEffectedFields, NEffectedIndex, [Val | Vals]};
                      {_,  _, yes, _, true} -> 
                          NEffectedFields = [type_utils:any_to_list(Field) | EffectedFields],
                          NEffectedIndex  = [(Index + 1) | EffectedIndex],
                          Val = element(Index + 1, Record),
                          {Index + 1, NEffectedFields, NEffectedIndex, [Val | Vals]};
                      {_, _, yes, _, false} ->
                          {Index + 1, EffectedFields, EffectedIndex, Vals}
                  end
          end, {1, [], [], []}, Fields),
    {UpdateFields, UpdateFIndex, UpdateVals}.

generate_insert_sql(Table, UpdateFields, UpdateFIndex, Records, Options) ->
    BatchSize = proplists:get_value(batch_size, Options, 1000),
    SqlHead = case proplists:get_value(insert_ignore, Options, false) of
                  true ->
                      "INSERT IGNORE INTO " ++ type_utils:any_to_list(Table);
                  _ ->
                      "INSERT INTO " ++ type_utils:any_to_list(Table)
              end,
    SqlFields = string:join(lists:reverse(UpdateFields), ","),
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
                                                      Val = case Idx of
                                                                timestamp ->
                                                                    datetime_utils:localtime_as_string();
                                                                _ -> element(Idx, RecItem)
                                                            end,
                                                      [Val | AccIn2] % That's why reverse SqlFields
                                              end, AccIn, UpdateFIndex)
                                    end, [], NRecords)    
                          end),
    case BatchRemainSize of
        0 ->
            [{Batch1Sql, FRecBatchValues} || FRecBatchValues <- lists:delete([], RecBatchValues)];
        _ ->
            {Batch1Values, Batch2Values} = lists:split(BatchCount, RecBatchValues),
            case Batch1Sql of
                undefined -> [{Batch2Sql, lists:merge(Batch2Values)}];
                _         ->
                    lists:merge([{Batch1Sql, FBatch1Values} || FBatch1Values <- Batch1Values],
                                [{Batch2Sql, lists:merge(Batch2Values)}])
            end
    end.

generate_update_sql(Table, UpdateFields, UpdateVals, {FieldPK, PKVal}) ->
    SqlHead = "UPDATE " ++ type_utils:any_to_list(Table),
    SqlTail = "WHERE " ++ FieldPK ++ " = ?",
    SqlSet  = string:join([ F ++ " = ?" || F <- UpdateFields], ","),
    Sql = string:join([SqlHead, "SET ", SqlSet, SqlTail], " "),
    
    {Sql, lists:append(UpdateVals, [PKVal])}.


merge_rec(Rec1, Rec2, Fields, Options) ->
    IgnoreNil = proplists:get_value(ignore_nil, Options, false),
    [_, RecChanged, MergedRec] = 
        lists:foldl(fun(Field, [Index, Changed, RecAcc]) ->
                            Val1 = element(Index + 1, Rec1),
                            Val2 = element(Index + 1, Rec2),
                            case {Field, Changed, IgnoreNil, Val1 =:= Val2, Val2} of
                                {id, _, _, _, _} ->
                                    [Index + 1, Changed, RecAcc];
                                {_, false, true, false, undefined} ->
                                    [Index + 1, true, RecAcc];
                                {_, false, true, false, _} ->
                                    [Index + 1, true, setelement(Index + 1, RecAcc, Val2)];
                                {_, false, _, true,  _} ->
                                    [Index + 1, false, RecAcc];
                                {_, true, true, true, _} ->
                                    [Index + 1, true, RecAcc];
                                {_, _, false, false, _} ->
                                    [Index + 1, true, setelement(Index + 1, RecAcc, Val2)];
                                {_, true, false, true, _} ->
                                    [Index + 1, true, RecAcc]
                            end
                    end, [1, false, Rec1], Fields),
    case RecChanged of
        true -> {changed,   MergedRec};
        _    -> {unchanged, MergedRec}
    end.
