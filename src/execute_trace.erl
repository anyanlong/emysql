%%%-------------------------------------------------------------------
%%% @author Jack Tang <jack@taodinet.com>
%%% @copyright (C) 2014, Jack Tang
%%% @doc
%%%
%%% @end
%%% Created : 29 Apr 2014 by Jack Tang <jack@taodinet.com>
%%%-------------------------------------------------------------------
-module(execute_trace).

-behaviour(gen_server).

%% API
-export([prepare/2,
         unprepare/1,
         execute/1,
         execute/2,
         begin_transaction/0,
         commit_transaction/0,
         rollback_transaction/0]).

-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE). 

-record(state, { stmts }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%%
%% @spec
%% @end
%%--------------------------------------------------------------------
prepare(Name, Statement) ->
    gen_server:cast(?SERVER, {prepare, type_utils:any_to_binary(Name), Statement}).

%%--------------------------------------------------------------------
%% @doc
%%
%% @spec
%% @end
%%--------------------------------------------------------------------
unprepare(Name) ->
    gen_server:cast(?SERVER, {unprepare, type_utils:any_to_binary(Name)}).

%%--------------------------------------------------------------------
%% @doc
%%
%% @spec
%% @end
%%--------------------------------------------------------------------
execute(StmtName) ->
    execute(StmtName, []).
execute(raw_query, Query) ->
    gen_server:cast(?SERVER, {raw_query, Query});  
execute(StmtName, Params) ->
    gen_server:cast(?SERVER, {execute, type_utils:any_to_binary(StmtName), Params}).

%%--------------------------------------------------------------------
%% @doc
%%
%% @spec
%% @end
%%--------------------------------------------------------------------
begin_transaction() ->
    gen_server:cast(?SERVER, begin_transaction).

%%--------------------------------------------------------------------
%% @doc
%%
%% @spec
%% @end
%%--------------------------------------------------------------------
commit_transaction() ->
    gen_server:cast(?SERVER, commit).


%%--------------------------------------------------------------------
%% @doc
%%
%% @spec
%% @end
%%--------------------------------------------------------------------
rollback_transaction() ->
    gen_server:cast(?SERVER, rollback).



%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initiates the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    {ok, #state{ stmts = orddict:new() }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({prepare, Name, Statement}, #state{stmts = Stmts}=State) ->
    NStmts = case orddict:find(Name, Stmts) of
                 error ->
                     orddict:store(Name, [Statement, 1], Stmts);
                 {ok, [Statement, Ref]} ->
                     orddict:store(Name, [Statement, Ref + 1], Stmts);
                 {ok, [TheStatement, _]} ->
                     lager:error("Prepare statement (~p) conflicted: ~p <-> ~p",
                                 [Name, TheStatement, Statement]),
                     Stmts
             end,
    {noreply, State#state{stmts = NStmts}};

handle_cast({unprepare, Name}, #state{stmts = Stmts} = State) ->
    NStmts = case orddict:find(Name, Stmts) of
                 error -> Stmts;
                 {ok, [Statement, 1]} ->
                     orddict:erase(Name, Stmts);
                 {ok, [Statement, Ref]} ->
                     orddict:store(Name, [Statement, Ref - 1], Stmts)
             end,
    {noreply, State#state{stmts = NStmts}};

handle_cast({execute, Name, Params}, #state{stmts = Stmts}=State) ->
    case orddict:find(Name, Stmts) of
        error ->
            lager:warning("No statement (~p) found", [Name]);
        {ok, Statement} ->
            lager:debug("Execute ~p: ~p~nParams: ~p",
                        [Name, Statement, Params])
    end,
    {noreply, State};

handle_cast({raw_query, Query}, #state{}=State) ->
    lager:debug("Query: ~p", [Query]),
    {noreply, State};


handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
