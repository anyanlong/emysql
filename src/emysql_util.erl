%% vim: ts=4 sw=4 et
%% Copyright (c) 2009
%% Bill Warnecke <bill@rupture.com>
%% Jacob Vorreuter <jacob.vorreuter@gmail.com>
%% Mike Oxford <moxford@gmail.com>
%%
%% Permission is hereby granted, free of charge, to any person
%% obtaining a copy of this software and associated documentation
%% files (the "Software"), to deal in the Software without
%% restriction, including without limitation the rights to use,
%% copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the
%% Software is furnished to do so, subject to the following
%% conditions:
%%
%% The above copyright notice and this permission notice shall be
%% included in all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
%% OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
%% NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
%% HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%% WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
%% OTHER DEALINGS IN THE SOFTWARE.
-module(emysql_util).

%% Query data
-export([
	affected_rows/1,
	result_type/1,
         field_names/1,
         insert_id/1
]).
        
%% Conversion routines
-export([
         as_dict/1,
         as_json/1,
         as_proplist/1,
         as_record/3,
         as_record/4
]).

-include("emysql.hrl").

%% @doc Return the field names of a result packet
%% @end
-spec field_names(Result) -> [Name]
  when
    Result :: #result_packet{},
    Name :: binary().
field_names(#result_packet{field_list=FieldList}) ->
    [Field#field.name || Field <- FieldList].

%% @doc package row data as a dict
%%
%% -module(fetch_example).
%%
%% fetch_foo() ->
%%  Res = emysql:execute(pool1, "select * from foo"),
%%  Res:as_dict(Res).
-spec as_dict(Result) -> Dict
  when
    Result :: #result_packet{},
    Dict :: dict().
as_dict(Res = #result_packet{}) ->
    dict:from_list(lists:flatten(as_proplist(Res))).

%% @spec as_proplist(Result) -> proplist
%%      Result = #result_packet{}
%%
%% @doc package row data as a proplist
%%
%% -module(fetch_example).
%%
%% fetch_foo() ->
%%  Res = emysql:execute(pool1, "select * from foo"),
%%  Res:as_proplist(Res).
-spec as_proplist(Result) -> [PropRow]
   when
     Result :: #result_packet{},
     PropRow :: proplists:proplist().
as_proplist(#result_packet{field_list=_Cols,rows=_Rows}) when _Cols =:= undefined,
                                                              _Rows =:= undefined ->
    [];
as_proplist(#result_packet{field_list=_Cols,rows=_Rows}) when is_list(_Cols),
                                                              _Rows =:= undefined ->
    [];
as_proplist(#result_packet{field_list=_Cols,rows=_Rows}) when is_list(_Cols),
                                                              _Rows =:= [] ->
    [];
as_proplist(Res = #result_packet{field_list=Cols,rows=Rows}) when is_list(Cols),
                                                                  is_list(Rows) ->
    Fields = field_names(Res),
    [begin
        [{K, V} || {K, V} <- lists:zip(Fields, R)]
    end || R <- Rows].

%% @spec as_record(Result, RecordName, Fields, Fun) -> Result
%%      Result = #result_packet{}
%%      RecordName = atom()
%%      Fields = [atom()]
%%      Fun = fun()
%%      Result = [Row]
%%      Row = [record()]
%%
%% @doc package row data as records
%%
%% RecordName is the name of the record to generate.
%% Fields are the field names to generate for each record.
%%
%% -module(fetch_example).
%%
%% fetch_foo() ->
%%  Res = emysql:execute(pool1, "select * from foo"),
%%  Res:as_record(foo, record_info(fields, foo)).
as_record(Result = #result_packet{}, RecordName, Fields, Fun) when is_atom(RecordName), is_list(Fields), is_function(Fun) ->
    Columns = Result#result_packet.field_list,

    S = lists:seq(1, length(Columns)),
    P = lists:zip([ binary_to_atom(C1#field.name, utf8) || C1 <- Columns ], S),
    F = fun(FieldName) ->
        case proplists:lookup(FieldName, P) of
            none ->
                    fun(_) -> undefined end;
            {FieldName, Pos} ->
                    fun(Row) -> lists:nth(Pos, Row) end
        end
    end,
    Fs = [ F(FieldName) || FieldName <- Fields ],
    F1 = fun(Row) ->
        RecordData = [ Fx(Row) || Fx <- Fs ],
        Fun(list_to_tuple([RecordName|RecordData]))
    end,
    [ F1(Row) || Row <- Result#result_packet.rows ].

as_record(Result = #result_packet{}, RecordName, Fields) when is_atom(RecordName), is_list(Fields) ->
    as_record(Result, RecordName, Fields, fun(A) -> A end).

%% @spec as_json(Result) -> Result
%% @doc package row data as erlang json (jsx/jiffy compatible)
as_json(#result_packet { rows = Rows } = Result) ->
    Fields = field_names(Result),
    [begin
        [{K, json_val(V)} || {K, V} <- lists:zip(Fields, Row)]
    end || Row <- Rows].

json_val(undefined) ->
    null;
json_val({date,{Year,Month,Day}}) ->
    iolist_to_binary( io_lib:format("~4.4.0w-~2.2.0w-~2.2.0w", [Year, Month, Day]));
json_val({datetime,{ {Year,Month,Day}, {Hour,Min,Sec} }}) ->
    iolist_to_binary( io_lib:format("~4.4.0w-~2.2.0w-~2.2.0wT~2.2.0w:~2.2.0w:~2.2.0wZ", [Year, Month, Day, Hour, Min, Sec]));
json_val(Value) ->
    Value.

%% @doc insert_id/1 extracts the Insert ID from an OK Packet
%% @end
-spec insert_id(#ok_packet{}) -> integer() | binary().
insert_id(#ok_packet{insert_id=ID}) ->
    ID.

%% @doc affected_rows/1 extracts the number of affected rows from an OK Packet
%% @end
-spec affected_rows(#ok_packet{}) -> integer().
affected_rows(#ok_packet{affected_rows=Rows}) ->
    Rows.

%% @doc result_type/1 decodes a packet into its type
%% @end
result_type(#ok_packet{})     -> ok;
result_type(#result_packet{}) -> result;
result_type(#error_packet{})  -> error;
result_type(#eof_packet{})    -> eof.
