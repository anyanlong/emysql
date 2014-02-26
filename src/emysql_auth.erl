%% Copyright (c) 2009
%% Bill Warnecke <bill@rupture.com>
%% Jacob Vorreuter <jacob.vorreuter@gmail.com>
%% Henning Diedrich <hd2010@eonblast.com>
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
-module(emysql_auth).
-export([do_handshake/3]).

-include("emysql.hrl").
-include("crypto_compat.hrl").

do_handshake(Sock, User, Password) ->
    #greeting { seq_num = SeqNum} = Greeting = recv_greeting(Sock),
    case auth(Sock, User, Password, Greeting#greeting{ seq_num = SeqNum + 1}) of
        OK when is_record(OK, ok_packet) ->
            ok;
        #error_packet{} = Err ->
            exit({failed_to_authenticate, Err});
        Other ->
            exit({unexpected_packet, Other})
    end,
    Greeting.

recv_greeting(Sock) ->
    {GreetingPacket,Unparsed} = emysql_tcp:recv_packet(Sock, emysql_app:default_timeout(), <<>>),
    case GreetingPacket#packet.data of
        <<255, _/binary>> ->
            {#error_packet{
                code = Code,
                msg = Msg
            },_, _Rest} = emysql_tcp:response(Sock, emysql_app:default_timeout(), GreetingPacket, Unparsed),
            exit({Code, Msg});
        <<ProtocolVersion:8/integer, Rest1/binary>> ->
            {ServerVersion, Rest2} = asciiz(Rest1),
            <<ThreadID:32/little, Rest3/binary>> = Rest2,
            {Salt, Rest4} = asciiz(Rest3),
            <<ServerCaps:16/little, Rest5/binary>> = Rest4,
            <<ServerLanguage:8/little,
                ServerStatus:16/little,
                ServerCapsHigh:16/little,
                ScrambleLength:8/little,
                _:10/binary-unit:8,
                Rest6/binary>> = Rest5,
            Salt2Length = case ScrambleLength of 0 -> 13; _-> ScrambleLength - 8 end,
            <<Salt2Bin:Salt2Length/binary-unit:8, Plugin/binary>> = Rest6,
            {Salt2, <<>>} = asciiz(Salt2Bin),
            #greeting{
                protocol_version = ProtocolVersion,
                server_version = ServerVersion,
                thread_id = ThreadID,
                salt1 = Salt,
                salt2 = Salt2,
                caps = ServerCaps,
                caps_high = ServerCapsHigh,
                language = ServerLanguage,
                status = ServerStatus,
                seq_num = GreetingPacket#packet.seq_num,
                plugin = Plugin
            };
        What ->
            exit({greeting_failed, What})
    end.

%% password_type/2 discriminates the kind of password we want
password_type(Password, ?MYSQL_OLD_PASSWORD) when is_list(Password); is_binary(Password) -> old;
password_type(Password, _) when is_list(Password); is_binary(Password) -> new;
password_type(_, _) -> empty.

%% capabilities/0 formats a list of capabilities for the wire
capabilities(Cs) ->
    lists:foldl(fun erlang:'bor'/2, 0, Cs).

auth(Sock, User, Password,
     #greeting { seq_num = SeqNum,
                 salt1 = Salt1,
                 salt2 = Salt2,
                 plugin = Plugin }) ->
    ScrambleBuff = case password_type(Password, Plugin) of
    	old -> password_old(Password, <<Salt1/binary, Salt2/binary>>);
    	new -> password_new(Password, <<Salt1/binary, Salt2/binary>>);
    	empty -> <<>>
    end,

    DatabaseB = <<>>,
    Caps = capabilities([
    	?LONG_PASSWORD , ?CLIENT_LOCAL_FILE, ?LONG_FLAG, ?TRANSACTIONS,
	?CLIENT_MULTI_STATEMENTS, ?CLIENT_MULTI_RESULTS,
	?PROTOCOL_41, ?SECURE_CONNECTION
    ]),
    Maxsize = ?MAXPACKETBYTES,
    UserB = unicode:characters_to_binary(User),
    PasswordL = size(ScrambleBuff),
    Packet = <<Caps:32/little, Maxsize:32/little, 8:8, 0:23/integer-unit:8, UserB/binary, 0:8, PasswordL:8, ScrambleBuff/binary, DatabaseB/binary>>,
    case emysql_tcp:send_and_recv_packet(Sock, Packet, SeqNum) of
        #eof_packet{seq_num = SeqNum1} ->
            AuthOld = password_old(Password, Salt1),
            emysql_tcp:send_and_recv_packet(Sock, <<AuthOld/binary, 0:8>>, SeqNum1+1);
        Result ->
            Result
    end.

password_new([], _Salt) -> <<>>;
password_new(Password, Salt) ->
    Stage1 = ?HASH_SHA(Password),
    Stage2 = ?HASH_SHA(Stage1),
    Res = ?HASH_FINAL(
        ?HASH_UPDATE(
            ?HASH_UPDATE(?HASH_INIT(), Salt),
            Stage2
        )
    ),
    bxor_binary(Res, Stage1).

password_old(Password, Salt) ->
    {P1, P2} = hash(Password),
    {S1, S2} = hash(Salt),
    Seed1 = P1 bxor S1,
    Seed2 = P2 bxor S2,
    List = rnd(9, Seed1, Seed2),
    {L, [Extra]} = lists:split(8, List),
    list_to_binary(lists:map(fun (E) -> E bxor (Extra - 64) end, L)).

bxor_binary(B1, B2) ->
    list_to_binary([E1 bxor E2 || {E1, E2} <- lists:zip(binary_to_list(B1), binary_to_list(B2))]).

rnd(N, Seed1, Seed2) ->
    Mod = (1 bsl 30) - 1,
    rnd(N, [], Seed1 rem Mod, Seed2 rem Mod).
rnd(0, List, _, _) ->
    lists:reverse(List);
rnd(N, List, Seed1, Seed2) ->
    Mod = (1 bsl 30) - 1,
    NSeed1 = (Seed1 * 3 + Seed2) rem Mod,
    NSeed2 = (NSeed1 + Seed2 + 33) rem Mod,
    Float = (float(NSeed1) / float(Mod))*31,
    Val = trunc(Float)+64,
    rnd(N - 1, [Val | List], NSeed1, NSeed2).

hash(S) -> hash(S, 1345345333, 305419889, 7).
hash([C | S], N1, N2, Add) ->
    N1_1 = N1 bxor (((N1 band 63) + Add) * C + N1 * 256),
    N2_1 = N2 + ((N2 * 256) bxor N1_1),
    Add_1 = Add + C,
    hash(S, N1_1, N2_1, Add_1);
hash([], N1, N2, _Add) ->
    Mask = (1 bsl 31) - 1,
    {N1 band Mask , N2 band Mask}.

asciiz(Data) when is_binary(Data) ->
    [S, R] = binary:split(Data, <<0>>),
    {S, R}.
