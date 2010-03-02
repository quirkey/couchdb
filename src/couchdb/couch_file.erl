% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_file).
-behaviour(gen_server).

-include("couch_db.hrl").

-define(SIZE_BLOCK, 4096).

-record(file, {
    fd_read,
    write_loop,
    sync_loop
    }).

-export([open/1, open/2, close/1, bytes/1, sync/1, append_binary/2]).
-export([append_term/2, pread_term/2, pread_iolist/2, write_header/2]).
-export([pread_binary/2, read_header/1,truncate/2]).
-export([append_term_md5/2,append_binary_md5/2]).
-export([init/1, terminate/2, handle_call/3, handle_cast/2, code_change/3, handle_info/2]).

%%----------------------------------------------------------------------
%% Args:   Valid Options are [create] and [create,overwrite].
%%  Files are opened in read/write mode.
%% Returns: On success, {ok, Fd}
%%  or {error, Reason} if the file could not be opened.
%%----------------------------------------------------------------------

open(Filepath) ->
    open(Filepath, []).

open(Filepath, Options) ->
    case gen_server:start_link(couch_file,
            {Filepath, Options, self(), Ref = make_ref()}, []) of
    {ok, Fd} ->
        {ok, Fd};
    ignore ->
        % get the error
        receive
        {Ref, Pid, Error} ->
            case process_info(self(), trap_exit) of
            {trap_exit, true} -> receive {'EXIT', Pid, _} -> ok end;
            {trap_exit, false} -> ok
            end,
            Error
        end;
    Error ->
        Error
    end.


%%----------------------------------------------------------------------
%% Purpose: To append an Erlang term to the end of the file.
%% Args:    Erlang term to serialize and append to the file.
%% Returns: {ok, Pos} where Pos is the file offset to the beginning the
%%  serialized  term. Use pread_term to read the term back.
%%  or {error, Reason}.
%%----------------------------------------------------------------------

append_term(Fd, Term) ->
    append_binary(Fd, term_to_binary(Term)).
    
append_term_md5(Fd, Term) ->
    append_binary_md5(Fd, term_to_binary(Term)).


%%----------------------------------------------------------------------
%% Purpose: To append an Erlang binary to the end of the file.
%% Args:    Erlang term to serialize and append to the file.
%% Returns: {ok, Pos} where Pos is the file offset to the beginning the
%%  serialized  term. Use pread_term to read the term back.
%%  or {error, Reason}.
%%----------------------------------------------------------------------

append_binary(Fd, Bin) ->
    Size = iolist_size(Bin),
    #file{write_loop=WL} = get_file_rec(Fd),
    couch_util:simple_call(WL, {append_iolist,
            [<<0:1/integer,Size:31/integer>>, Bin]}).
    
append_binary_md5(Fd, Bin) ->
    Size = iolist_size(Bin),
    #file{write_loop=WL} = get_file_rec(Fd),
    couch_util:simple_call(WL, {append_iolist, 
            [<<1:1/integer,Size:31/integer>>, erlang:md5(Bin), Bin]}).


%%----------------------------------------------------------------------
%% Purpose: Reads a term from a file that was written with append_term
%% Args:    Pos, the offset into the file where the term is serialized.
%% Returns: {ok, Term}
%%  or {error, Reason}.
%%----------------------------------------------------------------------


pread_term(Fd, Pos) ->
    {ok, Bin} = pread_binary(Fd, Pos),
    {ok, binary_to_term(Bin)}.


%%----------------------------------------------------------------------
%% Purpose: Reads a binrary from a file that was written with append_binary
%% Args:    Pos, the offset into the file where the term is serialized.
%% Returns: {ok, Term}
%%  or {error, Reason}.
%%----------------------------------------------------------------------

pread_binary(Fd, Pos) ->
    {ok, L} = pread_iolist(Fd, Pos),
    {ok, iolist_to_binary(L)}.


pread_iolist(Fd, Pos) ->
    #file{fd_read=FdReader} = get_file_rec(Fd),
    {ok, LenIolist, NextPos} = read_raw_iolist(FdReader, Pos, 4),
    case iolist_to_binary(LenIolist) of
    <<1:1/integer,Len:31/integer>> ->
        {ok, Md5List, ValPos} = read_raw_iolist(FdReader, NextPos, 16),
        Md5 = iolist_to_binary(Md5List),
        {ok, IoList, _} = read_raw_iolist(FdReader,ValPos,Len),
        case erlang:md5(IoList) of
        Md5 -> ok;
        _ ->  throw(file_corruption)
        end, 
        {ok, IoList};
    <<0:1/integer,Len:31/integer>> ->
        {ok, Iolist, _} = read_raw_iolist(FdReader, NextPos, Len),
        {ok, Iolist} 
    end.
       

read_raw_iolist(Fd, Pos, Len) ->
    BlockOffset = Pos rem ?SIZE_BLOCK,
    TotalBytes = calculate_total_len(BlockOffset, Len),
    {ok, <<RawBin:TotalBytes/binary>>} = file:pread(Fd, Pos, TotalBytes),
    {ok, remove_block_prefixes(BlockOffset, RawBin), Pos + TotalBytes}.

%%----------------------------------------------------------------------
%% Purpose: The length of a file, in bytes.
%% Returns: {ok, Bytes}
%%  or {error, Reason}.
%%----------------------------------------------------------------------

% length in bytes
bytes(Fd) ->
    #file{fd_read=FdRead} = get_file_rec(Fd),
    file:position(FdRead, eof).

%%----------------------------------------------------------------------
%% Purpose: Ensure all bytes written to the file are flushed to disk.
%% Returns: ok
%%  or {error, Reason}.
%%----------------------------------------------------------------------

sync(Fd) ->
    #file{sync_loop=SL} = get_file_rec(Fd),
    couch_util:simple_call(SL, sync).

%%----------------------------------------------------------------------
%% Purpose: Truncate the file at Pos.
%% Returns: ok
%%  or {error, Reason}.
%%----------------------------------------------------------------------

truncate(Fd, Pos) ->
    #file{write_loop=WL} = get_file_rec(Fd),
    couch_util:simple_call(WL, {truncate, Pos}).

%%----------------------------------------------------------------------
%% Purpose: Close the file. Is performed asynchronously.
%% Returns: ok
%%----------------------------------------------------------------------
close(Fd) ->
    couch_util:shutdown_sync(Fd).


read_header(Fd) ->
    #file{fd_read=FdRead} = get_file_rec(Fd),
    {ok, Pos} = file:position(FdRead, eof),
    find_header(FdRead, Pos div ?SIZE_BLOCK).

write_header(Fd, Data) ->
    Bin = term_to_binary(Data),
    Md5 = erlang:md5(Bin),
    % now we assemble the final header binary and write to disk
    FinalBin = <<Md5/binary, Bin/binary>>,
    #file{write_loop=WL} = get_file_rec(Fd),
    couch_util:simple_call(WL, {write_header, FinalBin}).


get_file_rec(Fd) ->
    case get(Fd) of
    undefined ->
        F = gen_server:call(Fd, get_file_rec),
        put(Fd, F),
        F;
    F ->
        F
    end.

init_status_error(ReturnPid, Ref, Error) ->
    ReturnPid ! {Ref, self(), Error},
    ignore.

% server functions

init({Filepath, Options, ReturnPid, Ref}) ->
    put(start_filepath, Filepath),
    case lists:member(create, Options) of
    true ->
        filelib:ensure_dir(Filepath),
        case file:open(Filepath, [write, read, raw, binary]) of
        {ok, Fd} ->
            try
                {ok, Length} = file:position(Fd, eof),
                case Length > 0 of
                true ->
                    % this means the file already exists and has data.
                    % FYI: We don't differentiate between empty files and 
                    % non-existant files here.
                    case lists:member(overwrite, Options) of
                    true ->
                        {ok, 0} = file:position(Fd, 0),
                        ok = file:truncate(Fd),
                        ok = file:sync(Fd),
                        init_sub_processes(ReturnPid, Ref, Filepath);
                    false ->
                        init_status_error(ReturnPid, Ref, file_exists)
                    end;
                false ->
                    init_sub_processes(ReturnPid, Ref, Filepath)
                end
            after    
                file:close(Fd)
            end;
        Error ->
            init_status_error(ReturnPid, Ref, Error)
        end;
    false ->    
        init_sub_processes(ReturnPid, Ref, Filepath)
    end.

init_sub_processes(ReturnPid, Ref, Filepath) ->    
    % open in read mode first, so we don't create the file if it doesn't
    % exist.
    case file:open(Filepath, [read, binary]) of
    {ok, FdRead} ->
        WL = spawn_link(fun()-> start_write_loop(Filepath) end),
        SL = spawn_link(fun()-> start_sync_loop(Filepath) end),
        couch_stats_collector:track_process_count({couchdb, open_os_files}),
        process_flag(trap_exit, true),
        {ok, #file{write_loop=WL,fd_read=FdRead,sync_loop=SL}};
    Error ->
        init_status_error(ReturnPid, Ref, Error)
    end.


terminate(_Reason, #file{fd_read=Fd,write_loop=WL,sync_loop=SL}) ->
    catch file:close(Fd),
    catch couch_util:shutdown_sync(WL),
    catch couch_util:shutdown_sync(SL),
    ok.


handle_call(get_file_rec, _From, File) ->
    {reply, File, File}.


handle_cast(D, _Fd) ->
    exit({unhandled_cast, D}).


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
    
handle_info({'EXIT', _Pid, normal}, Fd) ->
    {noreply, Fd};
handle_info({'EXIT', _, Reason}, Fd) ->
    {stop, Reason, Fd}.


find_header(_Fd, -1) ->
    no_valid_header;
find_header(Fd, Block) ->
    case (catch load_header(Fd, Block)) of
    {ok, HeaderTerm} ->
        {ok, HeaderTerm};
    _Error ->
        find_header(Fd, Block -1)
    end.

load_header(Fd, Block) ->
    {ok, <<1>>} = file:pread(Fd, Block*?SIZE_BLOCK, 1),
    {ok, <<HeaderLen:32/integer>>} = file:pread(Fd, (Block*?SIZE_BLOCK) + 1, 4),
    TotalBytes = calculate_total_len(1, HeaderLen),
    {ok, <<RawBin:TotalBytes/binary>>} =
            file:pread(Fd, (Block*?SIZE_BLOCK) + 5, TotalBytes),
    <<Md5Sig:16/binary, HeaderBin/binary>> =
        iolist_to_binary(remove_block_prefixes(1, RawBin)),
    Md5Sig = erlang:md5(HeaderBin),
    {ok, binary_to_term(HeaderBin)}.

calculate_total_len(0, FinalLen) ->
    calculate_total_len(1, FinalLen) + 1;
calculate_total_len(BlockOffset, FinalLen) ->
    case ?SIZE_BLOCK - BlockOffset of
    BlockLeft when BlockLeft >= FinalLen ->
        FinalLen;
    BlockLeft ->
        FinalLen + ((FinalLen - BlockLeft) div (?SIZE_BLOCK -1)) +
            if ((FinalLen - BlockLeft) rem (?SIZE_BLOCK -1)) =:= 0 -> 0;
                true -> 1 end
    end.

remove_block_prefixes(_BlockOffset, <<>>) ->
    [];
remove_block_prefixes(0, <<_BlockPrefix,Rest/binary>>) ->
    remove_block_prefixes(1, Rest);
remove_block_prefixes(BlockOffset, Bin) ->
    BlockBytesAvailable = ?SIZE_BLOCK - BlockOffset,
    case size(Bin) of
    Size when Size > BlockBytesAvailable ->
        <<DataBlock:BlockBytesAvailable/binary,Rest/binary>> = Bin,
        [DataBlock | remove_block_prefixes(0, Rest)];
    _Size ->
        [Bin]
    end.

make_blocks(_BlockOffset, []) ->
    [];
make_blocks(0, IoList) ->
    [<<0>> | make_blocks(1, IoList)];
make_blocks(BlockOffset, IoList) ->
    case split_iolist(IoList, (?SIZE_BLOCK - BlockOffset), []) of
    {Begin, End} ->
        [Begin | make_blocks(0, End)];
    _SplitRemaining ->
        IoList
    end.

split_iolist(List, 0, BeginAcc) ->
    {lists:reverse(BeginAcc), List};
split_iolist([], SplitAt, _BeginAcc) ->
    SplitAt;
split_iolist([<<Bin/binary>> | Rest], SplitAt, BeginAcc) when SplitAt >= size(Bin) ->
    split_iolist(Rest, SplitAt - size(Bin), [Bin | BeginAcc]);
split_iolist([<<Bin/binary>> | Rest], SplitAt, BeginAcc) ->
    <<Begin:SplitAt/binary,End/binary>> = Bin,
    split_iolist([End | Rest], 0, [Begin | BeginAcc]);
split_iolist([Sublist| Rest], SplitAt, BeginAcc) when is_list(Sublist) ->
    case split_iolist(Sublist, SplitAt, BeginAcc) of
    {Begin, End} ->
        {Begin, [End | Rest]};
    SplitRemaining ->
        split_iolist(Rest, SplitAt - (SplitAt - SplitRemaining), [Sublist | BeginAcc])
    end;
split_iolist([Byte | Rest], SplitAt, BeginAcc) when is_integer(Byte) ->
    split_iolist(Rest, SplitAt - 1, [Byte | BeginAcc]).


get_sync_reqs(Srcs) ->
    receive {Src, sync} ->
        get_sync_reqs([Src | Srcs])
    after 0 ->
        Srcs
    end.

start_sync_loop(Filename)->    
    {ok, Fd} = file:open(Filename, [append, raw]),
    process_flag(trap_exit, true),
    sync_loop(Fd).

sync_loop(Fd) ->
    receive {Src, sync} ->
        Srcs = get_sync_reqs([Src]),
        ok = file:sync(Fd),
        [Src0 ! {self(), ok} || Src0 <- Srcs],
        sync_loop(Fd);
    {'EXIT', _Src, _Reason} ->
        file:close(Fd)
    end.
    
start_write_loop(Filename) ->
    {ok, Fd} = file:open(Filename, [append, raw]),
    {ok, Pos} = file:position(Fd, eof),
    process_flag(trap_exit, true),
    write_loop(Fd, Pos).
    

write_loop(Fd, Pos) ->
    receive
    {Src, {append_iolist, IoList}} ->
        Offset = Pos rem ?SIZE_BLOCK,
        Len = calculate_total_len(Offset, iolist_size(IoList)),
        Blocks = make_blocks(Offset, IoList),
        case file:write(Fd, Blocks) of
        ok ->
            Src ! {self(), {ok, Pos}};
        Error ->
            exit(Error)
        end,
        write_loop(Fd, Pos + Len);
    {Src, {write_header, Bin}} ->
        BinSize = size(Bin),
        case Pos rem ?SIZE_BLOCK of
        0 ->
            Padding = <<>>;
        BlockOffset ->
            Padding = <<0:(8*(?SIZE_BLOCK-BlockOffset))>>
        end,
        Len = size(Padding) + 5 + calculate_total_len(1, size(Bin)),
        Blocks = [Padding, <<1, BinSize:32/integer>> | make_blocks(1, [Bin])],
        case file:write(Fd, Blocks) of
        ok ->
            Src ! {self(), ok};
        Error ->
            exit(Error)
        end,
        write_loop(Fd, Pos + Len);
    {Src, {truncate, NewLen}} ->
        {ok, NewLen2} = file:position(Fd, NewLen),
        ok = file:truncate(Fd),
        Src ! {self(), ok},
        write_loop(Fd, NewLen2);
    {'EXIT', _Src, _Reason} ->
        file:close(Fd)
    end.

