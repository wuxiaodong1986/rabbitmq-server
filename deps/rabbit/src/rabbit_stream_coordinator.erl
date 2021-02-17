%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% Copyright (c) 2012-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_stream_coordinator).

-behaviour(ra_machine).

-export([start/0]).
-export([format_ra_event/2]).

-export([init/1,
         apply/3,
         state_enter/2,
         init_aux/1,
         handle_aux/6,
         tick/2]).

-export([recover/0,
         start_cluster/1,
         add_replica/2,
         delete_replica/2,
         register_listener/1]).

-export([new_stream/2,
         delete_stream/2]).

-export([policy_changed/1]).

-export([local_pid/1]).
-export([query_local_pid/3]).


-export([log_overview/1]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("amqqueue.hrl").

-define(STREAM_COORDINATOR_STARTUP, {stream_coordinator_startup, self()}).
-define(TICK_TIMEOUT, 60000).
-define(RESTART_TIMEOUT, 1000).
-define(PHASE_RETRY_TIMEOUT, 10000).
-define(CMD_TIMEOUT, 30000).

-type stream_id() :: string().
-type stream() :: #{conf := osiris:config(),
                    atom() => term()}.
-type stream_role() :: leader | follower | listener.
-type queue_ref() :: rabbit_types:r(queue).
-type tail() :: {osiris:offset(), osiris:epoch()} | empty.

-record(member,
        {state = {down, 0} :: {down, osiris:epoch()}
         | {stopped, osiris:epoch(), tail()}
         | {ready, osiris:epoch()}
         %% when a replica disconnects
         | {running | disconnected, osiris:epoch(), pid()}
         | deleted,
         role :: {writer | replica, osiris:epoch()},
         node :: node(),
         %% the currently running action, if any
         current :: undefined
         | stopping
         | starting
         | deleting,
         current_ts :: integer(),
         target = running :: running | stopped | deleted}).

%% member lifecycle
%% down -> stopped(tail) -> running | disconnected -> deleted
%%
%% split the handling of incoming events (down, success | fail of operations)
%% and the actioning of current state (i.e. member A is down but the cluster target
%% is `up` - start a current action to turn member A -> running

-type from() :: {pid(), reference()}.

-record(stream, {id :: stream_id(),
                 epoch = 0 :: osiris:epoch(),
                 queue_ref :: queue_ref(),
                 conf :: osiris:config(),
                 nodes :: [node()],
                 members = #{} :: #{node() := #member{}},
                 listeners = #{} :: #{pid() := LeaderPid :: pid()},
                 reply_to :: undefined | from(),
                 mnesia = {updated, 0} :: {updated | updating, osiris:epoch()},
                 target = running :: running | deleted
                }).

-record(?MODULE, {streams = #{} :: #{stream_id() => #stream{}},
                  monitors = #{} :: #{pid() => {stream_id(), stream_role()}},
                  listeners = #{} :: #{stream_id() =>
                                       #{pid() := queue_ref()}},
                  %% future extensibility
                  reserved_1,
                  reserved_2}).

-type state() :: #?MODULE{}.
-type command() :: {policy_changed, #{stream_id := stream_id()}} |
                   {start_cluster, #{queue := amqqueue:amqqueue()}} |
                   {start_cluster_reply, amqqueue:amqqueue()} |
                   {start_replica, #{stream_id := stream_id(),
                                     node := node(),
                                     retries := non_neg_integer()}} |
                   {start_replica_failed, #{stream_id := stream_id(),
                                            node := node(),
                                            retries := non_neg_integer()},
                    Reply :: term()} |
                   {start_replica_reply, stream_id(), pid()} |

                   {delete_replica, #{stream_id := stream_id(),
                                      node := node()}} |
                   {start_leader_election, stream_id(), osiris:epoch(),
                    Offsets :: term()} |
                   {leader_elected, stream_id(), NewLeaderPid :: pid()} |
                   {replicas_stopped, stream_id()} |
                   {phase_finished, stream_id(), Reply :: term()} |
                   {stream_updated, stream()} |
                   {register_listener, #{pid := pid(),
                                         stream_id := stream_id(),
                                         queue_ref := queue_ref()}} |
                   ra_machine:effect().


-export_type([command/0]).

start() ->
    Nodes = rabbit_mnesia:cluster_nodes(all),
    ServerId = {?MODULE, node()},
    case ra:restart_server(ServerId) of
        {error, Reason} when Reason == not_started orelse
                             Reason == name_not_registered -> 
            case ra:start_server(make_ra_conf(node(), Nodes)) of
                ok ->
                    global:set_lock(?STREAM_COORDINATOR_STARTUP),
                    case find_members(Nodes) of
                        [] ->
                            %% We're the first (and maybe only) one
                            ra:trigger_election(ServerId);
                        Members ->
                            %% What to do if we get a timeout?
                            {ok, _, _} = ra:add_member(Members, ServerId, 30000)
                    end,
                    global:del_lock(?STREAM_COORDINATOR_STARTUP),
                    _ = ra:members(ServerId),
                    ok;
                Error ->
                    exit(Error)
            end;
        ok ->
            ok;
        Error ->
            exit(Error)
    end.

find_members([]) ->
    [];
find_members([Node | Nodes]) ->
    case ra:members({?MODULE, Node}) of
        {_, Members, _} ->
            Members;
        {error, noproc} ->
            find_members(Nodes);
        {timeout, _} ->
            %% not sure what to do here
            find_members(Nodes)
    end.

%% new api

new_stream(Q, LeaderNode)
  when ?is_amqqueue(Q) andalso is_atom(LeaderNode) ->
    #{name := StreamId,
      nodes := Nodes} = amqqueue:get_type_state(Q),
    %% assertion leader is in nodes configuration
    true = lists:member(LeaderNode, Nodes),
    process_command({new_stream, StreamId,
                     #{leader_node => LeaderNode,
                       queue => Q}}).

delete_stream(Q, ActingUser)
  when ?is_amqqueue(Q) ->
    #{name := StreamId} = amqqueue:get_type_state(Q),
    case process_command({delete_stream, StreamId, #{acting_user => ActingUser}}) of
        {ok, ok, _} ->
            QName = amqqueue:get_name(Q),
              _ = rabbit_amqqueue:internal_delete(QName, ActingUser),
            {ok, {ok, 0}};
        Err ->
            Err
    end.

%% end new api

recover() ->
    ra:restart_server({?MODULE, node()}).

start_cluster(Q) ->
    process_command({start_cluster, #{queue => Q}}).

add_replica(StreamId, Node) ->
    process_command({add_replica, StreamId, #{node => Node}}).

delete_replica(StreamId, Node) ->
    process_command({delete_replica, StreamId, #{node => Node}}).

policy_changed(Q) when ?is_amqqueue(Q) ->
    StreamId = maps:get(name, amqqueue:get_type_state(Q)),
    process_command({policy_changed, #{stream_id => StreamId,
                                       queue => Q,
                                       retries => 1}}).

local_pid(StreamId) when is_list(StreamId) ->
    MFA = {?MODULE, query_local_pid, [StreamId, node()]},
    case ra:local_query({?MODULE, node()}, MFA) of
        {ok, {_, Result}, _} ->
            Result;
        {error, _} = Err ->
            Err;
        {timeout, _} ->
            {error, timeout}
    end.

query_local_pid(StreamId, Node, #?MODULE{streams = Streams}) ->
    case Streams of
        #{StreamId := #stream{members =
                              #{Node := #member{state =
                                                {running, _, Pid}}}}} ->
            {ok, Pid};
        _ ->
            {error, not_found}
    end.

-spec register_listener(amqqueue:amqqueue()) ->
    {error, term()} | {ok, ok, atom() | {atom(), atom()}}.
register_listener(Q) when ?is_amqqueue(Q)->
    #{name := StreamId} = amqqueue:get_type_state(Q),
    process_command({register_listener,
                     #{pid => self(),
                       stream_id => StreamId}}).

process_command(Cmd) ->
    global:set_lock(?STREAM_COORDINATOR_STARTUP),
    Servers = ensure_coordinator_started(),
    global:del_lock(?STREAM_COORDINATOR_STARTUP),
    process_command(Servers, Cmd).

process_command([], _Cmd) ->
    {error, coordinator_unavailable};
process_command([Server | Servers], Cmd) ->
    case ra:process_command(Server, Cmd, ?CMD_TIMEOUT) of
        {timeout, _} ->
            rabbit_log:warning("Coordinator timeout on server ~p when processing command ~p",
                               [Server, element(1, Cmd)]),
            process_command(Servers, Cmd);
        {error, noproc} ->
            process_command(Servers, Cmd);
        Reply ->
            Reply
    end.

ensure_coordinator_started() ->
    Local = {?MODULE, node()},
    AllNodes = all_coord_members(),
    case whereis(?MODULE) of
        undefined ->
            case ra:restart_server(Local) of
                {error, Reason} when Reason == not_started orelse
                                     Reason == name_not_registered ->
                    OtherNodes = all_coord_members() -- [Local],
                    %% We can't use find_members/0 here as a process that timeouts means the cluster is up
                    case lists:filter(fun(N) -> global:whereis_name(N) =/= undefined end, OtherNodes) of
                        [] ->
                            start_coordinator_cluster();
                        _ ->
                            OtherNodes
                    end;
                ok ->
                    AllNodes;
                {error, {already_started, _}} ->
                    AllNodes;
                _ ->
                    AllNodes
            end;
        _ ->
            AllNodes
    end.

start_coordinator_cluster() ->
    Nodes = rabbit_mnesia:cluster_nodes(running),
    rabbit_log:debug("Starting stream coordinator on nodes: ~w", [Nodes]),
    case ra:start_cluster([make_ra_conf(Node, Nodes) || Node <-  Nodes]) of
        {ok, Started, _} ->
            rabbit_log:debug("Started stream coordinator on ~w", [Started]),
            Started;
        {error, cluster_not_formed} ->
            rabbit_log:warning("Stream coordinator could not be started on nodes ~w",
                               [Nodes]),
            []
    end.

all_coord_members() ->
    Nodes = rabbit_mnesia:cluster_nodes(running) -- [node()],
    [{?MODULE, Node} || Node <- [node() | Nodes]].

init(_Conf) ->
    #?MODULE{}.

-spec apply(map(), command(), state()) ->
    {state(), term(), ra_machine:effects()}.
apply(Meta, {_CmdTag, StreamId, #{}} = Cmd,
      #?MODULE{streams = Streams0,
               monitors = Monitors0} = State0) ->
    Stream0 = maps:get(StreamId, Streams0, undefined),
    Stream1 = update_stream(Meta, Cmd, Stream0),
    Reply = case Stream1 of
                #stream{reply_to = undefined} ->
                    ok;
                _ ->
                    %% reply_to is set so we'll reply later
                    '$ra_no_reply'
            end,
    case Stream1 of
        undefined ->
            {State0#?MODULE{streams = maps:remove(StreamId, Streams0)},
             Reply, []};
        _ ->
            {Stream2, Effects0} = evaluate_stream(Stream1, []),
            {Stream, Effects1} = eval_listeners(Stream2, Effects0),
            {Monitors, Effects} = ensure_monitors(Stream, Monitors0, Effects1),
            {State0#?MODULE{streams = Streams0#{StreamId => Stream},
                            monitors = Monitors}, Reply, Effects}
    end;
% apply(Meta, {policy_changed, #{stream_id := StreamId,
%                                queue := Q,
%                                retries := Retries}} = Cmd,
%       #?MODULE{streams = Streams0} = State) ->
%     From = maps:get(from, Meta, undefined),
%     case maps:get(StreamId, Streams0, undefined) of
%         undefined ->
%             {State, ok, []};
%         #{conf := Conf,
%           state := running} = SState0 ->
%             case rabbit_stream_queue:update_stream_conf(Q, Conf) of
%                 Conf ->
%                     %% No changes, ensure we only trigger an election if it's a must
%                     {State, ok, []};
%                 #{retention := Retention,
%                   leader_pid := Pid,
%                   replica_pids := Pids} = Conf0 ->
%                     case maps:remove(retention, Conf) == maps:remove(retention, Conf0) of
%                         true ->
%                             %% Only retention policy has changed, it doesn't need a full restart
%                             Phase = phase_update_retention,
%                             %% TODO do it over replicas too
%                             PhaseArgs = [[Pid | Pids], Retention, Conf0, Retries],
%                             SState = update_stream_state(From, update_retention, Phase, PhaseArgs, SState0),
%                             rabbit_log:debug("rabbit_stream_coordinator: ~w entering ~p on node ~p",
%                                              [StreamId, Phase, node()]),
%                             {State#?MODULE{streams = Streams0#{StreamId => SState}}, '$ra_no_reply',
%                              [{aux, {phase, StreamId, Phase, PhaseArgs}}]};
%                         false ->
%                             {State, ok, [{mod_call, osiris_writer, stop, [Conf]}]}
%                     end
%             end;
%         SState0 ->
%             Streams = maps:put(StreamId, add_pending_cmd(From, Cmd, SState0), Streams0),
%             {State#?MODULE{streams = Streams}, '$ra_no_reply', []}

%     end;
% apply(#{from := From}, {start_cluster, #{queue := Q}}, #?MODULE{streams = Streams} = State) ->
%     #{name := StreamId} = Conf0 = amqqueue:get_type_state(Q),
%     Conf = apply_leader_locator_strategy(Conf0, Streams),
%     case maps:is_key(StreamId, Streams) of
%         true ->
%             {State, '$ra_no_reply', wrap_reply(From, {error, already_started})};
%         false ->
%             Phase = phase_start_cluster,
%             PhaseArgs = [amqqueue:set_type_state(Q, Conf)],
%             SState = #{state => start_cluster,
%                        phase => Phase,
%                        phase_args => PhaseArgs,
%                        conf => Conf,
%                        reply_to => From,
%                        pending_cmds => [],
%                        pending_replicas => []},
%             rabbit_log:debug("rabbit_stream_coordinator: ~s entering phase_start_cluster", [StreamId]),
%             {State#?MODULE{streams = maps:put(StreamId, SState, Streams)},
%              '$ra_no_reply',
%              [{aux, {phase, StreamId, Phase, PhaseArgs}}]}
%     end;
% apply(_Meta, {start_cluster_reply, Q}, #?MODULE{streams = Streams,
%                                                 monitors = Monitors0} = State) ->
%     #{name := StreamId,
%       leader_pid := LeaderPid,
%       replica_pids := ReplicaPids} = Conf = amqqueue:get_type_state(Q),
%     %% TODO: this doesn't guarantee that all replicas were started successfully
%     %% we need to do something to check if any were not started and start a
%     %% retry phase to get them running
%     SState0 = maps:get(StreamId, Streams),
%     Phase = phase_repair_mnesia,
%     PhaseArgs = [new, Q],
%     SState = SState0#{conf => Conf,
%                       phase => Phase,
%                       phase_args => PhaseArgs},
%     Monitors = lists:foldl(fun(Pid, M) ->
%                                    maps:put(Pid, {StreamId, follower}, M)
%                            end, maps:put(LeaderPid, {StreamId, leader}, Monitors0), ReplicaPids),
%     MonitorActions = [{monitor, process, Pid} || Pid <- ReplicaPids ++ [LeaderPid]],
%     rabbit_log:debug("rabbit_stream_coordinator: ~s entering ~w "
%                      "after start_cluster_reply", [StreamId, Phase]),
%     {State#?MODULE{streams = maps:put(StreamId, SState, Streams),
%                    monitors = Monitors}, ok,
%      MonitorActions ++ [{aux, {phase, StreamId, Phase, PhaseArgs}}]};
% apply(_Meta, {start_replica_failed, #{stream_id := StreamId,
%                                       node := Node,
%                                       retries := Retries}, Reply},
%       #?MODULE{streams = Streams0} = State) ->
%     case maps:get(StreamId, Streams0, undefined) of
%         undefined ->
%             {State, {error, not_found}, []};
%         #{pending_replicas := Pending,
%           reply_to := From} = SState ->
%             rabbit_log:debug("rabbit_stream_coordinator: ~s start replica on node ~w failed",
%                              [StreamId, Node]),
%             rabbit_log:debug("PENDING ~p", [Pending]),
%             case lists:member(Node, Pending) of
%                 true ->
%                     Streams = Streams0#{StreamId => clear_stream_state(SState#{pending_replicas =>
%                                                                                add_unique(Node, Pending)})},
%                     reply_and_run_pending(
%                       From, StreamId, ok, Reply,
%                       [{timer, {pipeline,
%                                 [{start_replica, #{stream_id => StreamId,
%                                                    node => Node,
%                                                    retries => Retries + 1}}]},
%                         ?RESTART_TIMEOUT * Retries}],
%                       State#?MODULE{streams = Streams});
%                 false ->
%                     {State, ok, []}
%             end
%     end;
% apply(_Meta, {update_retention_failed, StreamId, Q, Retries, Reply},
%       #?MODULE{streams = Streams0} = State) ->
%     rabbit_log:debug("rabbit_stream_coordinator: ~w update retention failed", [StreamId]),
%     case maps:get(StreamId, Streams0, undefined) of
%         undefined ->
%             {State, {error, not_found}, []};
%         #{reply_to := From} = SState  ->
%             Streams = Streams0#{StreamId => clear_stream_state(SState)},
%             reply_and_run_pending(
%               From, StreamId, ok, Reply,
%               [{timer, {pipeline,
%                         [{policy_changed, #{stream_id => StreamId,
%                                             queue => Q,
%                                             retries => Retries + 1}}]},
%                 ?RESTART_TIMEOUT * Retries}],
%               State#?MODULE{streams = Streams})
%     end;
apply(Meta, {down, Pid, _Reason} = Cmd,
      #?MODULE{streams = Streams0,
               listeners = Listeners0,
               monitors = Monitors0} = State) ->
    case maps:take(Pid, Monitors0) of
        {{StreamId, listener}, Monitors} ->
            Listeners = case maps:take(StreamId, Listeners0) of
                            error ->
                                Listeners0;
                            {Pids0, Listeners1} ->
                                case maps:remove(Pid, Pids0) of
                                    Pids when map_size(Pids) == 0 ->
                                        Listeners1;
                                    Pids ->
                                        Listeners1#{StreamId => Pids}
                                end
                        end,
            {State#?MODULE{listeners = Listeners,
                           monitors = Monitors}, ok, []};
        {{StreamId, member}, Monitors} ->
            Stream0 = maps:get(StreamId, Streams0, undefined),
            Stream1 = update_stream(Meta, Cmd, Stream0),
            {Stream, Effects} = evaluate_stream(Stream1, []),
            Streams = Streams0#{StreamId => Stream},
            {State#?MODULE{streams = Streams,
                           monitors = Monitors}, ok, Effects};
        error ->
            {State, ok, []}
    end;
apply(_, {timeout, {aux, Cmd}}, State) ->
    {State, ok, [{aux, Cmd}]};
apply(_Meta, {register_listener, #{pid := Pid,
                                   stream_id := StreamId}},
      #?MODULE{streams = Streams,
               monitors = Monitors0} = State0) ->
    case Streams of
        #{StreamId := #stream{listeners = Listeners0} = Stream0} ->
            Stream1 = Stream0#stream{listeners = maps:put(Pid, undefined, Listeners0)},
            {Stream, Effects} = eval_listeners(Stream1, []),
            Monitors = maps:put(Pid, {StreamId, listener}, Monitors0),
            {State0#?MODULE{streams = maps:put(StreamId, Stream, Streams),
                            monitors = Monitors}, ok,
             [{monitor, process, Pid} | Effects]};
        _ ->
            {State0, stream_not_found, []}
    end;
apply(_Meta, UnkCmd, State) ->
    rabbit_log:debug("rabbit_stream_coordinator: unknown command ~W",
                     [UnkCmd, 10]),
    {State, {error, unknown_command}, []}.

% state_enter(leader, #?MODULE{streams = Streams, monitors = Monitors}) ->
%     maps:fold(fun(_, #{conf := #{name := StreamId},
%                        pending_replicas := Pending,
%                        state := State,
%                        phase := Phase,
%                        phase_args := PhaseArgs}, Acc) ->
%                       restart_aux_phase(State, Phase, PhaseArgs, StreamId) ++
%                           pipeline_restart_replica_cmds(StreamId, Pending) ++
%                           Acc
%               end, [{monitor, process, P} || P <- maps:keys(Monitors)], Streams);
% state_enter(follower, #?MODULE{monitors = Monitors}) ->
%     [{monitor, process, P} || P <- maps:keys(Monitors)];
% state_enter(recover, _) ->
%     put('$rabbit_vm_category', ?MODULE),
%     [];
state_enter(leader, #?MODULE{streams = _Streams, monitors = Monitors}) ->
    rabbit_log:debug("coordinator state enter leader mons: ~p~n~p",
                     [Monitors, _Streams]),
    [{aux, fail_active_actions} |
     [{monitor, process, P} || P <- maps:keys(Monitors)]];
state_enter(S, _) ->
    rabbit_log:debug("coordinator state enter: ~s", [S]),
    %% TODO: need to reset all running tasks, this will need to be a special
    %% reset command
    [].

tick(_Ts, _State) ->
    [{aux, maybe_resize_coordinator_cluster}].

maybe_resize_coordinator_cluster() ->
    spawn(fun() ->
                  case ra:members({?MODULE, node()}) of
                      {_, Members, _} ->
                          MemberNodes = [Node || {_, Node} <- Members],
                          Running = rabbit_mnesia:cluster_nodes(running),
                          All = rabbit_mnesia:cluster_nodes(all),
                          case Running -- MemberNodes of
                              [] ->
                                  ok;
                              New ->
                                  rabbit_log:warning("New rabbit node(s) detected, "
                                                     "adding stream coordinator in: ~p", [New]),
                                  add_members(Members, New)
                          end,
                          case MemberNodes -- All of
                              [] ->
                                  ok;
                              Old ->
                                  rabbit_log:warning("Rabbit node(s) removed from the cluster, "
                                                     "deleting stream coordinator in: ~p", [Old]),
                                  remove_members(Members, Old)
                          end;
                      _ ->
                          ok
                  end
          end).

add_members(_, []) ->
    ok;
add_members(Members, [Node | Nodes]) ->
    Conf = make_ra_conf(Node, [N || {_, N} <- Members]),
    case ra:start_server(Conf) of
        ok ->
            case ra:add_member(Members, {?MODULE, Node}) of
                {ok, NewMembers, _} ->
                    add_members(NewMembers, Nodes);
                _ ->
                    add_members(Members, Nodes)
            end;
        Error ->
            rabbit_log:warning("Stream coordinator failed to start on node ~p : ~p",
                               [Node, Error]),
            add_members(Members, Nodes)
    end.

remove_members(_, []) ->
    ok;
remove_members(Members, [Node | Nodes]) ->
    case ra:remove_member(Members, {?MODULE, Node}) of
        {ok, NewMembers, _} ->
            remove_members(NewMembers, Nodes);
        _ ->
            remove_members(Members, Nodes)
    end.

-record(aux, {actions = #{} ::
              #{pid() := {stream_id(), node(), osiris:epoch()}}}).

init_aux(_Name) ->
    #aux{}.
    % {#{}, undefined}.

%% TODO ensure the dead writer is restarted as a replica at some point in time, increasing timeout?
handle_aux(leader, _, maybe_resize_coordinator_cluster,
           {Monitors, undefined}, LogState, _) ->
    Pid = maybe_resize_coordinator_cluster(),
    {no_reply, {Monitors, Pid}, LogState, [{monitor, process, aux, Pid}]};
handle_aux(leader, _, maybe_resize_coordinator_cluster,
           AuxState, LogState, _) ->
    %% Coordinator resizing is still happening, let's ignore this tick event
    {no_reply, AuxState, LogState};
handle_aux(leader, _, {down, Pid, _},
           {Monitors, Pid}, LogState, _) ->
    %% Coordinator resizing has finished
    {no_reply, {Monitors, undefined}, LogState};
handle_aux(leader, _, {phase, _, Fun, Args} = Cmd,
           {Monitors, Coordinator}, LogState, _) ->
    Pid = erlang:apply(?MODULE, Fun, Args),
    Actions = [{monitor, process, aux, Pid}],
    {no_reply, {maps:put(Pid, Cmd, Monitors), Coordinator}, LogState, Actions};
handle_aux(leader, _, {start_writer, StreamId, #{leader_node := Node} = Conf},
           Aux, LogState, _) ->
    rabbit_log:debug("rabbit_stream_coordinator: running action: 'start_writer'"
                     " for ~s on node ~w", [StreamId, Node]),
    ActionFun = fun () -> phase_start_writer(Conf) end,
    run_action(starting, Node, Conf, ActionFun, Aux, LogState);
handle_aux(leader, _, {start_replica, StreamId, Node, #{epoch := Epoch} = Conf},
           Aux, LogState, _) ->
    rabbit_log:debug("rabbit_stream_coordinator: running action: 'start_replica'"
                     " for ~s on node ~w in epoch ~b", [StreamId, Node, Epoch]),
    ActionFun = fun () -> phase_start_replica(Node, Conf) end,
    run_action(starting, Node, Conf, ActionFun, Aux, LogState);
handle_aux(leader, _, {stop, StreamId, Node, Epoch, Conf},
           Aux, LogState, _) ->
    rabbit_log:debug("rabbit_stream_coordinator: running action: 'stop'"
                     " for ~s on node ~w in epoch ~b", [StreamId, Node, Epoch]),
    ActionFun = fun () -> phase_stop_member(Node, Epoch, Conf) end,
    run_action(stopping, Node, Conf, ActionFun, Aux, LogState);
handle_aux(leader, _, {update_mnesia, StreamId, Conf},
           #aux{actions = _Monitors} = Aux, LogState,
           #?MODULE{streams = _Streams}) ->
    rabbit_log:debug("rabbit_stream_coordinator: running action: 'update_mnesia'"
                     " for ~s", [StreamId]),
    ActionFun = fun () -> phase_update_mnesia(Conf) end,
    run_action(updating_mnesia, node(), Conf, ActionFun, Aux, LogState);
handle_aux(leader, _, {delete_member, StreamId, Node, Conf},
           #aux{actions = _Monitors} = Aux, LogState,
           #?MODULE{streams = _Streams}) ->
    rabbit_log:debug("rabbit_stream_coordinator: running action: 'delete_member'"
                     " for ~s ~s", [StreamId, Node]),
    ActionFun = fun () -> phase_delete_member(StreamId, Node, Conf) end,
    run_action(delete_member, node(), Conf, ActionFun, Aux, LogState);
handle_aux(leader, _, fail_active_actions,
           #aux{actions = Monitors} = Aux, LogState,
           #?MODULE{streams = Streams}) ->
    Exclude = maps:from_list([{S, ok} || {S, _, _} <- maps:values(Monitors)]),
    fail_active_actions(Streams, Exclude),
    {no_reply, Aux, LogState, []};
handle_aux(leader, _, {down, Pid, normal},
           #aux{actions = Monitors} = Aux, LogState, _) ->
    %% action process finished normally, just remove from actions map
    {no_reply, Aux#aux{actions = maps:remove(Pid, Monitors)}, LogState, []};
handle_aux(leader, _, {down, Pid, Reason},
           #aux{actions = Monitors0} = Aux, LogState, _) ->
    %% An action has failed - report back to the state machine
    case maps:get(Pid, Monitors0, undefined) of
        {StreamId, Action, Node, Epoch} ->
            rabbit_log:warning("Error while executing action for stream queue ~s, "
                               " node ~s, epoch ~b Err: ~w", [StreamId, Node, Epoch, Reason]),
            Monitors = maps:remove(Pid, Monitors0),
            Cmd = {action_failed, StreamId,
                   #{action => Action,
                     node => Node,
                     epoch => Epoch}},
            send_self_command(Cmd),
            {no_reply, Aux#aux{actions = maps:remove(Pid, Monitors)},
             LogState, []};
        undefined ->
            %% should this ever happen?
            {no_reply, Aux, LogState, []}
    end;
handle_aux(_, _, _, AuxState, LogState, _) ->
    {no_reply, AuxState, LogState}.

run_action(Action, Node, #{epoch := E, name := StreamId},
           ActionFun,
           #aux{actions = Actions0} = Aux, Log) ->
    Pid = spawn(ActionFun),
    % Effects = [{monitor, process, aux, Pid}],
    Effects = [],
    Actions = Actions0#{Pid => {StreamId, Action, Node, E}},
    {no_reply, Aux#aux{actions = Actions}, Log, Effects}.

% reply_and_run_pending(From, StreamId, Reply, WrapReply, Actions0, #?MODULE{streams = Streams} = State) ->
%     #{pending_cmds := Pending} = SState0 = maps:get(StreamId, Streams),
%     AuxActions = [{mod_call, ra, pipeline_command, [{?MODULE, node()}, Cmd]}
%                   || Cmd <- Pending],
%     SState = maps:put(pending_cmds, [], SState0),
%     Actions = case From of
%                   undefined ->
%                       AuxActions ++ Actions0;
%                   _ ->
%                       wrap_reply(From, WrapReply) ++ AuxActions ++ Actions0
%               end,
%     {State#?MODULE{streams = Streams#{StreamId => SState}}, Reply, Actions}.

wrap_reply(From, Reply) ->
    [{reply, From, {wrap_reply, Reply}}].

phase_start_replica(Node, #{epoch := Epoch,
                            name := StreamId} = Conf0) ->
    spawn(
      fun() ->
              try
                  case osiris_replica:start(Node, Conf0) of
                      {ok, Pid} ->
                          rabbit_log:debug("~s: ~s: replica started on ~s",
                                           [?MODULE, StreamId, Node]),
                          send_self_command({member_started, StreamId,
                                             #{epoch => Epoch,
                                               pid => Pid}});
                      {error, already_present} ->
                          %% need to remove child record if this is the case
                          %% can it ever happen?
                          _ = osiris_replica:stop(Node, Conf0),
                          send_action_failed(StreamId, starting, Node, Epoch);
                      {error, {already_started, Pid}} ->
                          %% TODO: we need to check that the current epoch is the same
                          %% before we can be 100% sure it is started in the correct
                          %% epoch, can this happen? who knows...
                          send_self_command({member_started, StreamId,
                                             #{epoch => Epoch,
                                               pid => Pid}});
                      {error, Reason} ->
                          rabbit_log:warning("Error while starting replica for ~p : ~p",
                                             [maps:get(name, Conf0), Reason]),
                      send_action_failed(StreamId, starting, Node, Epoch)
                  end
              catch _:E->
                      rabbit_log:warning("Error while starting replica for ~s : ~p",
                                         [maps:get(name, Conf0), E]),
                      send_action_failed(StreamId, starting, Node, Epoch)
              end
      end).

send_action_failed(StreamId, Action, Node, Epoch) ->
  send_self_command({action_failed, StreamId,
                     #{action => Action,
                       node => Node,
                       epoch => Epoch}}).

send_self_command(Cmd) ->
    ra:pipeline_command({?MODULE, node()}, Cmd),
    ok.


phase_delete_member(StreamId, Node, Conf) ->
    spawn(
      fun() ->
              case osiris_server_sup:delete_child(Node, Conf) of
                  ok ->
                      Arg = #{node => Node},
                      send_self_command({member_deleted, StreamId, Arg});
                  _ ->
                      send_self_command({action_failed, StreamId,
                                         #{action => deleting,
                                           node => Node,
                                           epoch => undefined}})
              end
      end).

phase_stop_member(Node, Epoch, #{name := StreamId} = Conf) ->
    spawn(
      fun() ->
              case osiris_server_sup:stop_child(Node, StreamId) of
                  ok ->
                      rabbit_log:debug("~s: ~s: member stopped on ~s in ~b",
                                       [?MODULE, StreamId, Node, Epoch]),
                      %% get tail
                      case get_replica_tail(Node, Conf) of
                          {ok, Tail} ->
                              Arg = #{node => Node,
                                      epoch => Epoch,
                                      tail => Tail},
                              send_self_command({member_stopped, StreamId, Arg});
                          Err ->
                              rabbit_log:warning("Stream coordinator failed to get tail
                                                  of member ~s ~w Error: ~w",
                                                 [StreamId, Node, Err]),
                              send_self_command({action_failed, StreamId,
                                                 #{action => stopping,
                                                   node => Node,
                                                   epoch => Epoch}})
                      end;
                  Err ->
                      rabbit_log:warning("Stream coordinator failed to stop
                                          member ~s ~w Error: ~w",
                                         [StreamId, Node, Err]),
                      send_self_command({action_failed, StreamId,
                                         #{action => stopping,
                                           node => Node,
                                           epoch => Epoch}})
              end
      end).

phase_start_writer(#{name := StreamId,
                     epoch := Epoch,
                     leader_node := Node} = Conf) ->
    spawn(
      fun() ->
              case catch osiris_writer:start(Conf) of
                  {ok, Pid} ->
                      Args = #{epoch => Epoch,
                               pid => Pid},
                      send_self_command({member_started, StreamId, Args});
                  Err ->
                      rabbit_log:warning("Stream coordinator failed to start
                                          writer ~s ~w Error: ~w",
                                         [StreamId, Node, Err]),
                      send_self_command({action_failed, StreamId,
                                         #{action => starting,
                                           node => Node,
                                           epoch => Epoch}})
              end
      end).

phase_update_retention(Pids0, Retention, #{name := StreamId}, Retries) ->
    spawn(
      fun() ->
              case update_retention(Pids0, Retention) of
                  ok ->
                      ra:pipeline_command({?MODULE, node()}, {phase_finished, StreamId, ok});
                  {error, Reason} ->
                      ra:pipeline_command({?MODULE, node()},
                                          {update_retention_failed, StreamId,
                                           Retention, Retries,
                                           {error, Reason}})
              end
      end).

update_retention([], _) ->
    ok;
update_retention([Pid | Pids], Retention) ->
    case osiris:update_retention(Pid, Retention) of
        ok ->
            update_retention(Pids, Retention);
        {error, Reason} ->
            {error, Reason}
    end.

get_replica_tail(Node, Conf) ->
    case rpc:call(Node, ?MODULE, log_overview, [Conf]) of
        {badrpc, nodedown} ->
            {error, nodedown};
        {_Range, Offsets} ->
            {ok, select_highest_offset(Offsets)}
    end.

select_highest_offset([]) ->
    empty;
select_highest_offset(Offsets) ->
    lists:last(Offsets).

log_overview(Config) ->
    Dir = osiris_log:directory(Config),
    osiris_log:overview(Dir).

is_quorum(1, 1) ->
    true;
is_quorum(NumReplicas, NumAlive) ->
    NumAlive >= ((NumReplicas div 2) + 1).

phase_update_mnesia(#{reference := QName,
                      leader_pid := LeaderPid,
                      name := StreamId} = Conf) ->

    rabbit_log:debug("sc: running mnesia update for ~s: ~w", [StreamId, Conf]),
    Fun = fun (Q) ->
                  amqqueue:set_type_state(amqqueue:set_pid(Q, LeaderPid), Conf)
          end,
    spawn(fun() ->
                  case rabbit_misc:execute_mnesia_transaction(
                         fun() ->
                                 rabbit_amqqueue:update(QName, Fun)
                         end) of
                      not_found ->
                          %% This can happen during recovery
                          [Q] = mnesia:dirty_read(rabbit_durable_queue, QName),
                          rabbit_amqqueue:ensure_rabbit_queue_record_is_initialized(Fun(Q));
                      _ ->
                          ok
                  end,
                  send_self_command({mnesia_updated, StreamId, Conf})
          end).

format_ra_event(ServerId, Evt) ->
    {stream_coordinator_event, ServerId, Evt}.

make_ra_conf(Node, Nodes) ->
    UId = ra:new_uid(ra_lib:to_binary(?MODULE)),
    Formatter = {?MODULE, format_ra_event, []},
    Members = [{?MODULE, N} || N <- Nodes],
    TickTimeout = application:get_env(rabbit, stream_tick_interval,
                                      ?TICK_TIMEOUT),
    #{cluster_name => ?MODULE,
      id => {?MODULE, Node},
      uid => UId,
      friendly_name => atom_to_list(?MODULE),
      metrics_key => ?MODULE,
      initial_members => Members,
      log_init_args => #{uid => UId},
      tick_timeout => TickTimeout,
      machine => {module, ?MODULE, #{}},
      ra_event_formatter => Formatter}.

apply_leader_locator_strategy(#{leader_locator_strategy := <<"client-local">>} = Conf, _) ->
    Conf;
apply_leader_locator_strategy(#{leader_node := Leader,
                                replica_nodes := Replicas0,
                                leader_locator_strategy := <<"random">>,
                                name := StreamId} = Conf, _) ->
    Replicas = [Leader | Replicas0],
    ClusterSize = length(Replicas),
    Hash = erlang:phash2(StreamId),
    Pos = (Hash rem ClusterSize) + 1,
    NewLeader = lists:nth(Pos, Replicas),
    NewReplicas = lists:delete(NewLeader, Replicas),
    Conf#{leader_node => NewLeader,
          replica_nodes => NewReplicas};
apply_leader_locator_strategy(#{leader_node := Leader,
                                replica_nodes := Replicas0,
                                leader_locator_strategy := <<"least-leaders">>} = Conf,
                                Streams) ->
    Replicas = [Leader | Replicas0],
    Counters0 = maps:from_list([{R, 0} || R <- Replicas]),
    Counters = maps:to_list(maps:fold(fun(_Key, #{conf := #{leader_node := L}}, Acc) ->
                                              maps:update_with(L, fun(V) -> V + 1 end, 0, Acc)
                                      end, Counters0, Streams)),
    Ordered = lists:sort(fun({_, V1}, {_, V2}) ->
                                 V1 =< V2
                         end, Counters),
    %% We could have potentially introduced nodes that are not in the list of replicas if
    %% initial cluster size is smaller than the cluster size. Let's select the first one
    %% that is on the list of replicas
    NewLeader = select_first_matching_node(Ordered, Replicas),
    NewReplicas = lists:delete(NewLeader, Replicas),
    Conf#{leader_node => NewLeader,
          replica_nodes => NewReplicas}.

select_first_matching_node([{N, _} | Rest], Replicas) ->
    case lists:member(N, Replicas) of
        true -> N;
        false -> select_first_matching_node(Rest, Replicas)
    end.

update_stream(#{system_time := Ts} = Meta,
              {new_stream, StreamId, #{leader_node := LeaderNode,
                                       queue := Q}}, undefined) ->
    #{nodes := Nodes} = Conf = amqqueue:get_type_state(Q),
    %% this jumps straight to the state where all members
    %% have been stopped and a new writer has been chosen
    E = 1,
    QueueRef = amqqueue:get_name(Q),
    Members = maps:from_list(
                [{N, #member{role = case LeaderNode of
                                        N -> {writer, E};
                                        _ -> {replica, E}
                                    end,
                             node = N,
                             state = {ready, E},
                             %% no members are running actions
                             current = undefined,
                             current_ts = Ts}
                 } || N <- Nodes]),
    #stream{id = StreamId,
            epoch = E,
            nodes = Nodes,
            queue_ref = QueueRef,
            conf = Conf,
            members = Members,
            reply_to = maps:get(from, Meta, undefined)};
update_stream(#{system_time := _Ts} = _Meta,
              {delete_stream, _StreamId, #{}},
              #stream{members = Members0,
                      target = _} = Stream0) ->
    Members = maps:map(
                fun (_, M) ->
                        M#member{target = deleted}
                end, Members0),
    Stream0#stream{members = Members,
                   % reply_to = maps:get(from, Meta, undefined),
                   target = deleted};
update_stream(#{system_time := _Ts} = _Meta,
              {add_replica, _StreamId, #{node := Node}},
              #stream{members = Members0,
                      epoch = Epoch,
                      nodes = Nodes,
                      target = _} = Stream0) ->
    case maps:is_key(Node, Members0) of
        true ->
            Stream0;
        false ->
            Members = maps:map(
                        fun (_, M) ->
                                M#member{target = stopped}
                        end, Members0#{Node => #member{role = {replica, Epoch},
                                                       node = Node,
                                                       target = stopped}}),
            Stream0#stream{members = Members,
                           nodes = lists:sort([Node | Nodes])}
    end;
update_stream(#{system_time := _Ts} = _Meta,
              {delete_replica, _StreamId, #{node := Node}},
              #stream{members = Members0,
                      epoch = _Epoch,
                      nodes = Nodes,
                      target = _} = Stream0) ->
    case maps:is_key(Node, Members0) of
        true ->
            %% TODO: check of duplicate
            Members = maps:map(
                        fun (K, M) when K == Node ->
                                M#member{target = deleted};
                            (_, M) ->
                                M#member{target = stopped}
                        end, Members0),
            Stream0#stream{members = Members,
                           nodes = lists:delete(Node, Nodes)};
        false ->
            Stream0
    end;
update_stream(#{system_time := _Ts},
              {member_started, _StreamId,
               #{epoch := E,
                 pid := Pid}}, #stream{epoch = E,
                                       members = Members} = Stream0) ->
    Node = node(Pid),
    case maps:get(Node, Members) of
        #member{role = {_, E},
                state = {ready, E}} = Member0 ->
            %% this is what we expect, leader epoch should match overall
            %% epoch
            Member = Member0#member{state = {running, E, Pid},
                                    current = undefined},
            %% TODO: we need to tell the machine to monitor the leader
            Stream0#stream{members =
                           Members#{Node => Member}};
        _ ->
            %% do we just ignore any writer_started events from unexpected
            %% epochs?
            Stream0
    end;
update_stream(#{system_time := _Ts},
              {member_deleted, _StreamId, #{node := Node}},
              #stream{nodes = Nodes,
                      members = Members0} = Stream0) ->
    case maps:take(Node, Members0) of
        {_, Members} when map_size(Members) == 0 ->
            undefined;
        {#member{state = _}, Members} ->
            %% this is what we expect, leader epoch should match overall
            %% epoch
            Stream0#stream{nodes = lists:delete(Node, Nodes),
                           members = Members};
        _ ->
            %% do we just ignore any writer_started events from unexpected
            %% epochs?
            Stream0
    end;
update_stream(#{system_time := _Ts},
              {member_stopped, _StreamId,
               #{node := Node,
                 epoch := StoppedEpoch,
                 tail := Tail}}, #stream{epoch = Epoch,
                                         target = Target,
                                         nodes = Nodes,
                                         members = Members0} = Stream0) ->
    IsLeaderInCurrent = case find_leader(Members0) of
                            {#member{role = {writer, Epoch},
                                     target = running,
                                     state = {ready, Epoch}}, _} ->
                                true;
                            {#member{role = {writer, Epoch},
                                     target = running,
                                     state = {running, Epoch, _}}, _} ->
                                true;
                            _ ->
                                false
                        end,

    case maps:get(Node, Members0) of
        #member{role = {replica, Epoch},
                state = _} = Member0
          when IsLeaderInCurrent ->
            %% A leader has already been selected so skip straight to ready state
            Member = Member0#member{state = {ready, Epoch},
                                    target = Target,
                                    current = undefined},
            Members1 = Members0#{Node => Member},
            Stream0#stream{members = Members1};
        #member{role = {_, Epoch},
                state = _} = Member0 ->
            %% this is what we expect, member epoch should match overall
            %% epoch
            Member = case StoppedEpoch of
                         Epoch ->
                             Member0#member{state = {stopped, StoppedEpoch, Tail},
                                            target = Target,
                                            current = undefined};
                         _ ->
                             %% if stopped epoch is from another epoch
                             %% leave target as is to retry stop in current term
                             Member0#member{state = {stopped, StoppedEpoch, Tail},
                                            current = undefined}
                     end,

            Members1 = Members0#{Node => Member},

            Offsets = [{N, T}
                       || #member{state = {stopped, E, T},
                                  target = running,
                                  node = N} <- maps:values(Members1),
                          E == Epoch],
            case is_quorum(length(Nodes), length(Offsets)) of
                true ->
                    %% select leader
                    NewWriterNode = select_leader(Offsets),
                    NextEpoch = Epoch + 1,
                    Members = maps:map(
                                fun (N, #member{state = {stopped, E, _}} = M)
                                      when E == Epoch ->
                                        case NewWriterNode of
                                            N ->
                                                %% new leader
                                                M#member{role = {writer, NextEpoch},
                                                         state = {ready, NextEpoch}};
                                            _ ->
                                                M#member{role = {replica, NextEpoch},
                                                         state = {ready, NextEpoch}}
                                        end;
                                    (_N, #member{target = deleted} = M) ->
                                        M;
                                    (_N, M) ->
                                        M#member{role = {replica, NextEpoch}}
                                end, Members1),
                    Stream0#stream{epoch = NextEpoch,
                                   members = Members};
                false ->
                    Stream0#stream{members = Members1}
            end
    end;
update_stream(#{system_time := _Ts},
              {mnesia_updated, _StreamId, #{epoch := E}},
              Stream0) ->
    %% reset mnesia state
    case Stream0 of
        undefined ->
            undefined;
        _ ->
            Stream0#stream{mnesia = {updated, E}}
    end;
update_stream(#{system_time := _Ts},
              {action_failed, _StreamId, #{action := updating_mnesia}},
              #stream{members = _Members0,
                      mnesia = {_, E}} = Stream0) ->
    %% reset mnesia state
    Stream0#stream{mnesia = {updated, E}};
update_stream(#{system_time := _Ts},
              {action_failed, _StreamId,
               #{node := Node,
                 epoch := _Epoch}}, #stream{members = Members0} = Stream0) ->
    Members1 = maps:update_with(Node, fun (M) ->
                                              M#member{current = undefined}
                                      end, Members0),
    case Members0 of
        #{Node := #member{role = {writer, E},
                          state = {ready, E},
                          current = starting}} ->
            %% the leader failed to start = we need a new election
            %% stop all members
            Members = maps:map(fun (_K, M) ->
                                       M#member{target = stopped}
                               end, Members1),
            Stream0#stream{members = Members};
        _ ->
            Stream0#stream{members = Members1}
    end;
update_stream(#{system_time := _Ts},
              {down, Pid, _Reason},
              #stream{epoch = E,
                      members = Members0} = Stream0) ->

    DownNode = node(Pid),
    case Members0 of
        #{DownNode := #member{role = {writer, E},
                              state = {running, E, Pid}} = Member} ->
            Members1 = Members0#{DownNode => Member#member{state = {down, E}}},
            %% leader is down, set all members to stop
            Members = maps:map(fun (_, M) -> M#member{target = stopped} end,
                               Members1),
            Stream0#stream{members = Members};
        #{DownNode := #member{state = {running, _, Pid}} = Member} ->
            %% the down process is currently running with the correct Pid
            %% set state to down
            Members = Members0#{DownNode => Member#member{state = {down, E}}},
            Stream0#stream{members = Members};
        _ ->
            Stream0
    end.

eval_listeners(#stream{listeners = Listeners0,
                       queue_ref = QRef,
                       members = Members} = Stream, Effects0) ->
    case find_leader(Members) of
        {#member{state = {running, _, LeaderPid}}, _} ->
            %% a leader is running, check all listeners to see if any of them
            %% has not been notified of the current leader pid
            {Listeners, Effects} =
                maps:fold(
                  fun(_, P, Acc) when P == LeaderPid ->
                          Acc;
                     (LPid, _, {L, Acc}) ->
                          {L#{LPid => LeaderPid},
                           [{send_msg, LPid,
                             {queue_event, QRef,
                              {stream_leader_change, LeaderPid}},
                             cast} | Acc]}
                  end, {Listeners0, Effects0}, Listeners0),
            {Stream#stream{listeners = Listeners}, Effects};
        _ ->
            {Stream, Effects0}
    end.


%% this function should be idempotent,
%% it should modify the state such that it won't issue duplicate
%% actions when called again
evaluate_stream(#stream{id = StreamId,
                        reply_to = From,
                        epoch = Epoch,
                        mnesia = {MnesiaTag, MnesiaEpoch},
                        members = Members0} = Stream0, Effs0) ->
     case find_leader(Members0) of
         {#member{state = LState,
                  node = LeaderNode,
                  target = deleted,
                  current = undefined} = Writer0, Replicas}
           when LState =/= deleted ->
             Action = {aux, {delete_member, StreamId, LeaderNode,
                             make_writer_conf(Writer0, Stream0)}},
             Writer = Writer0#member{current = deleting},
             Effs = case From of
                        undefined ->
                            [Action | Effs0];
                        _ ->
                            wrap_reply(From, {ok, 0}) ++ [Action | Effs0]
                    end,
             Stream = Stream0#stream{reply_to = undefined},
             eval_replicas(Writer, Replicas, Stream, Effs);
         {#member{state = {down, Epoch},
                  node = LeaderNode,
                  current = undefined} = Writer0, Replicas} ->
             %% leader is down - all replicas need to be stopped
             %% and tail infos retrieved
             %% some replicas may already be in stopping or ready state
             Action = {aux, {stop, StreamId, LeaderNode, Epoch,
                             make_writer_conf(Writer0, Stream0)}},
             Writer = Writer0#member{current = stopping},
             eval_replicas(Writer, Replicas, Stream0, [Action | Effs0]);
         {#member{state = {ready, Epoch}, %% writer ready in current epoch
                  target = running,
                  node = LeaderNode,
                  current = undefined} = Writer0, _Replicas} ->
             %% ready check has been completed and a new leader has been chosen
             %% time to start writer,
             %% if leader start fails, revert back to down state for all and re-run
             Members = Members0#{LeaderNode =>
                                 Writer0#member{current = starting}},
             Actions = [{aux, {start_writer, StreamId,
                               make_writer_conf(Writer0, Stream0)}} | Effs0],
             {Stream0#stream{members = Members}, Actions};
         {#member{state = {running, Epoch, LeaderPid},
                  target = running} = Writer, Replicas}
           when From =/= undefined ->
             %% we need a reply effect here
             Effs = wrap_reply(From, {ok, LeaderPid}) ++ Effs0,
             Stream = Stream0#stream{reply_to = undefined},
             eval_replicas(Writer, Replicas, Stream, Effs);
         {#member{state = {running, Epoch, LeaderPid}} = Writer, Replicas}
           when MnesiaTag == updated andalso MnesiaEpoch < Epoch ->
             Effs = [{aux, {update_mnesia, StreamId,
                            make_replica_conf(LeaderPid, Stream0)}} |  Effs0],
             Stream = Stream0#stream{mnesia = {updating, MnesiaEpoch}},
             eval_replicas(Writer, Replicas, Stream, Effs);
         {#member{state = S,
                  target = stopped,
                  node = LeaderNode,
                  current = undefined} = Writer0, Replicas}
           when element(1, S) =/= stopped ->
             %% leader should be stopped
             Action = {aux, {stop, StreamId, LeaderNode, Epoch,
                             make_writer_conf(Writer0, Stream0)}},
             Writer = Writer0#member{current = stopping},
             eval_replicas(Writer, Replicas, Stream0, [Action | Effs0]);
         {Writer, Replicas} ->
             eval_replicas(Writer, Replicas, Stream0, Effs0)
     end.

eval_replicas(undefined, Replicas, Stream, Actions0) ->
    {Members, Actions} = lists:foldl(
                           fun (R, Acc) ->
                                   eval_replica(R, deleted, Stream, Acc)
                           end, {#{}, Actions0},
                           Replicas),
    {Stream#stream{members = Members}, Actions};
eval_replicas(#member{state = LeaderState,
                      node = WriterNode} = Writer, Replicas,
              Stream, Actions0) ->
    {Members, Actions} = lists:foldl(
                           fun (R, Acc) ->
                                   eval_replica(R, LeaderState,
                                                Stream, Acc)
                           end, {#{WriterNode => Writer}, Actions0},
                           Replicas),
    {Stream#stream{members = Members}, Actions}.

eval_replica(#member{state = _State,
                     target = stopped,
                     node = Node,
                     current = undefined} = Replica,
             _LeaderState,
             #stream{id = StreamId,
                     epoch = Epoch,
                     conf = Conf},
             {Replicas, Actions}) ->
    %% if we're not running anything and we aren't stopped and not caught
    %% by previous clauses we probably should stop
    {Replicas#{Node => Replica#member{current = stopping}},
     [{aux, {stop, StreamId, Node, Epoch, Conf}} | Actions]};
eval_replica(#member{state = _,
                     node = Node,
                     current = Current,
                     target = deleted} = Replica,
             _LeaderState, #stream{id = StreamId,
                                   conf = Conf}, {Replicas, Actions0}) ->

    case Current of
        undefined ->
            Actions = [{aux, {delete_member, StreamId, Node, Conf}} |
                       Actions0],
            {Replicas#{Node => Replica#member{current = deleting}},
             Actions};
        _ ->
            {Replicas#{Node => Replica}, Actions0}
    end;
eval_replica(#member{state = {State, Epoch},
                     node = Node,
                     target = running,
                     current = undefined} = Replica,
             {running, Epoch, Pid},
             #stream{id = StreamId,
                     epoch = Epoch} = Stream,
             {Replicas, Actions})
  when State == ready; State == down ->
    %% replica is down or ready and the leader is running
    %% time to start it
    Conf = make_replica_conf(Pid, Stream),
    {Replicas#{Node => Replica#member{current = starting}},
     [{aux, {start_replica, StreamId, Node, Conf}} | Actions]};
eval_replica(#member{state = {running, Epoch, _},
                     target = running,
                     node = Node} = Replica,
             {running, Epoch, _}, _Stream, {Replicas, Actions}) ->
    {Replicas#{Node => Replica}, Actions};
eval_replica(#member{state = {stopped, _E, _},
                     node = Node,
                     current = undefined} = Replica,
             _LeaderState, _Stream,
             {Replicas, Actions}) ->
    %%  if stopped we should just wait for a quorum to reach stopped and
    %%  update_stream will move to ready state
    {Replicas#{Node => Replica}, Actions};
eval_replica(#member{state = {ready, E},
                     target = running,
                     node = Node,
                     current = undefined} = Replica,
             {ready, E}, _Stream,
             {Replicas, Actions}) ->
    %% if we're ready and so is the leader we just wait a swell
    {Replicas#{Node => Replica}, Actions};
% eval_replica(#member{state = _State,
%                      node = Node,
%                      current = undefined} = Replica,
%              _LeaderState,
%              #stream{id = StreamId,
%                      epoch = Epoch,
%                      conf = Conf},
%              {Replicas, Actions}) ->
%     %% if we're not running anything and we aren't stopped and not caught
%     %% by previous clauses we probably should stop
%     {Replicas#{Node => Replica#member{current = stopping}},
%      [{aux, {stop, StreamId, Node, Epoch, Conf}} | Actions]};
% eval_replica(#member{state = {down, _},
%                      node = Node,
%                      current = undefined} = Replica,
%              {running, Epoch, _},
%              #stream{id = StreamId,
%                      epoch = Epoch,
%                      conf = Conf},
%              {Replicas, Actions}) ->
%     %% replica is down and leader is running in current epoch
%     %% need to stop
%     {Replicas#{Node => Replica#member{current = stopping}},
%      [{aux, {stop, StreamId, Node, Epoch, Conf}} | Actions]};
eval_replica(#member{node = Node} = Replica, _LeaderState, _Stream,
             {Replicas, Actions}) ->
    {Replicas#{Node => Replica}, Actions}.

fail_active_actions(Streams, Exclude) ->
    maps:map(
      fun (_,  #stream{id = Id, members = Members})
            when not is_map_key(Id, Exclude)  ->
              _ = maps:map(fun(_, M) ->
                                   fail_action(Id, M)
                           end, Members)
      end, Streams),

    ok.

fail_action(_StreamId, #member{current = undefined}) ->
    ok;
fail_action(StreamId, #member{role = {_, E},
                              current = Action,
                              node = Node}) ->
    %% if we have an action send failure message
    send_self_command({action_failed, StreamId,
                       #{action => Action,
                         node => Node,
                         epoch => E}}).

ensure_monitors(#stream{id = StreamId,
                        members = Members}, Monitors, Effects) ->
    maps:fold(
      fun (_, #member{state = {running, _, Pid}}, {M, E})
            when not is_map_key(Pid, M) ->
              {M#{Pid => {StreamId, member}},
               [{monitor, process, Pid} | E]};
          (_, _, Acc) ->
              Acc
      end, {Monitors, Effects}, Members).

make_replica_conf(LeaderPid,
                  #stream{epoch = Epoch,
                          nodes = Nodes,
                          conf = Conf}) ->
    LeaderNode = node(LeaderPid),
    Conf#{leader_node => LeaderNode,
          nodes => Nodes,
          leader_pid => LeaderPid,
          replica_nodes => lists:delete(LeaderNode, Nodes),
          epoch => Epoch}.

make_writer_conf(#member{node = Node}, #stream{epoch = Epoch,
                                               nodes = Nodes,
                                               conf = Conf}) ->
    Conf#{leader_node => Node,
          nodes => Nodes,
          replica_nodes => lists:delete(Node, Nodes),
          epoch => Epoch}.


find_leader(Members) ->
    case lists:partition(
           fun (#member{target = deleted}) ->
                   false;
               (#member{role = {Role, _}}) ->
                   Role == writer
           end, maps:values(Members)) of
        {[Writer], Replicas} ->
            {Writer, Replicas};
        {[], Replicas} ->
            {undefined, Replicas}
    end.

select_leader(Offsets) ->
    [{Node, _} | _] = lists:sort(fun({_, {Ao, E}}, {_, {Bo, E}}) ->
                                         Ao >= Bo;
                                    ({_, {_, Ae}}, {_, {_, Be}}) ->
                                         Ae >= Be;
                                    ({_, empty}, _) ->
                                         false;
                                    (_, {_, empty}) ->
                                         true
                                 end, Offsets),
    Node.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

new_stream_test() ->
    [N1, N2, N3] = Nodes = [r@n1, r@n2, r@n3],
    StreamId = atom_to_list(?FUNCTION_NAME),
    Name = list_to_binary(StreamId),
    TypeState = #{name => StreamId,
                  nodes => Nodes},
    Q = new_q(Name, TypeState),
    From = {self(), make_ref()},
    Meta = #{system_time => ?LINE,
             from => From},
    S0 = update_stream(Meta, {new_stream, StreamId,
                              #{leader_node => N1,
                                queue => Q}}, undefined),
    E = 1,
    %% ready means a new leader has been chosen
    %% and the epoch incremented
    ?assertMatch(#stream{nodes = Nodes,
                         members = #{N1 := #member{role = {writer, E},
                                                   current = undefined,
                                                   state = {ready, E}},
                                     N2 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {ready, E}},
                                     N3 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {ready, E}}}},
                 S0),

    %% we expect the next action to be starting the writer
    {S1, Actions} = evaluate_stream(S0, []),
    ?assertMatch([{aux, {start_writer, StreamId, #{epoch := E,
                                                   leader_node := N1,
                                                   replica_nodes := [N2, N3]}}}],
                 Actions),
    ?assertMatch(#stream{nodes = Nodes,
                         members = #{N1 := #member{role = {writer, E},
                                                   current = starting,
                                                   state = {ready, E}}}},

                 S1),

    E1LeaderPid = fake_pid(N1),
    S2 = update_stream(Meta, {member_started, StreamId,
                              #{epoch => E, pid => E1LeaderPid}}, S1),
    ?assertMatch(#stream{nodes = Nodes,
                         epoch = E,
                         members = #{N1 :=
                                     #member{role = {writer, E},
                                             current = undefined,
                                             state = {running, E, E1LeaderPid}}}},
                         S2),
    {S3, Actions2} = evaluate_stream(S2, []),
    ?assertMatch([{aux, {start_replica, StreamId, N2,
                         #{epoch := E,
                           leader_pid := E1LeaderPid,
                           leader_node := N1}}},
                  {aux, {start_replica, StreamId, N3,
                         #{epoch := E,
                           leader_pid := E1LeaderPid,
                           leader_node := N1}}},
                  %% we reply to the caller once the leader has started
                  {reply, From, {wrap_reply, {ok, E1LeaderPid}}}
                 ], lists:sort(Actions2)),

    ?assertMatch(#stream{nodes = Nodes,
                         members = #{N1 := #member{role = {writer, E},
                                                   current = undefined,
                                                   state = {running, E, E1LeaderPid}},
                                     N2 := #member{role = {replica, E},
                                                   current = starting,
                                                   state = {ready, E}},
                                     N3 := #member{role = {replica, E},
                                                   current = starting,
                                                   state = {ready, E}}}},
                 S3),
    R1Pid = fake_pid(N2),
    S4 = update_stream(Meta, {member_started, StreamId,
                              #{epoch => E, pid => R1Pid}}, S3),
    {S5, [{aux, {update_mnesia, _, _}}]} = evaluate_stream(S4, []),
    R2Pid = fake_pid(N3),
    S6 = update_stream(Meta, {member_started, StreamId,
                              #{epoch => E, pid => R2Pid}}, S5),
    {S7, []} = evaluate_stream(S6, []),
    %% actions should have start_replica requests
    ?assertMatch(#stream{nodes = Nodes,
                         members = #{N1 := #member{role = {writer, E},
                                                   current = undefined,
                                                   state = {running, E, E1LeaderPid}},
                                     N2 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {running, E, R1Pid}},
                                     N3 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {running, E, R2Pid}}}},
                 S7),

    ok.

leader_down_test() ->
    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = ReplicaPids = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    N3 = node(Replica2),

    S0 = started_stream(StreamId, LeaderPid, ReplicaPids),
    S1 = update_stream(meta(?LINE), {down, LeaderPid, boom}, S0),
    ?assertMatch(#stream{members = #{N1 := #member{role = {writer, E},
                                                   current = undefined,
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {running, E, Replica1}},
                                     N3 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {running, E, Replica2}}}},
                 S1),
    {S2, Actions} = evaluate_stream(S1, []),
    %% expect all members to be stopping now
    %% replicas will receive downs however as will typically exit if leader does
    %% this is ok
    ?assertMatch([{aux, {stop, StreamId, N1, E,  _}},
                  {aux, {stop, StreamId, N2, E, _}},
                  {aux, {stop, StreamId, N3, E, _}}], lists:sort(Actions)),
    ?assertMatch(#stream{members = #{N1 := #member{role = {writer, E},
                                                   current = stopping,
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E},
                                                   current = stopping,
                                                   state = {running, E, Replica1}},
                                     N3 := #member{role = {replica, E},
                                                   current = stopping,
                                                   state = {running, E, Replica2}}}},
                 S2),

    %% idempotency check
    {S2, []} = evaluate_stream(S2, []),
    N2Tail = {E, 101},
    S3 = update_stream(meta(?LINE), {member_stopped, StreamId, #{node => N2,
                                                                 epoch => E,
                                                                 tail => N2Tail}}, S2),
    ?assertMatch(#stream{members = #{N2 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {stopped, E, N2Tail}}}},
                 S3),
    {S3, []} = evaluate_stream(S3, []),
    N3Tail = {E, 102},
    S4 = update_stream(meta(?LINE), {member_stopped, StreamId, #{node => N3,
                                                                 epoch => E,
                                                                 tail => N3Tail}}, S3),
    E2 = E + 1,
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = stopping,
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E2},
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     %% N3 has the higher offset so should
                                     %% be selected as writer of E2
                                     N3 := #member{role = {writer, E2},
                                                   current = undefined,
                                                   state = {ready, E2}}}},
                 S4),
    {S5, Actions4} = evaluate_stream(S4, []),
    %% new leader has been selected so should be started
    ?assertMatch([{aux, {start_writer, StreamId, #{leader_node := N3}}}],
                  lists:sort(Actions4)),
    ?assertMatch(#stream{epoch = E2}, S5),

    E2LeaderPid = fake_pid(n3),
    S6 = update_stream(meta(?LINE), {member_started, StreamId,
                                     #{epoch => E2, pid => E2LeaderPid}}, S5),
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = stopping,
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E2},
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     %% N3 has the higher offset so should
                                     %% be selected as writer of E2
                                     N3 := #member{role = {writer, E2},
                                                   current = undefined,
                                                   state = {running, E2, E2LeaderPid}}}},
                 S6),
    {S7, Actions6} = evaluate_stream(S6, []),
    ?assertMatch([{aux, {update_mnesia, _, _}},
                  {aux, {start_replica, StreamId, N2,
                         #{leader_pid := E2LeaderPid}}}],
                 lists:sort(Actions6)),
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = stopping,
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E2},
                                                   current = starting,
                                                   state = {ready, E2}},
                                     N3 := #member{role = {writer, E2},
                                                   current = undefined,
                                                   state = {running, E2, E2LeaderPid}}}},
                 S7),
    E2RepllicaN2Pid = fake_pid(n2),
    S8 = update_stream(meta(?LINE), {member_started, StreamId,
                                     #{epoch => E2,
                                       pid => E2RepllicaN2Pid}}, S7),
    ?assertMatch(#stream{members = #{N2 := #member{role = {replica, E2},
                                                   current = undefined,
                                                   state = {running, E2, E2RepllicaN2Pid}}}},
                 S8),
    %% nothing to do
    {S8, []} = evaluate_stream(S8, []),

    S9 = update_stream(meta(?LINE), {action_failed, StreamId,
                                     #{action => stopping,
                                       node => N1,
                                       epoch => E}}, S8),
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = undefined,
                                                   state = {down, E}}}},
                 S9),

    {S10, Actions9} = evaluate_stream(S9, []),
    %% retries action
    ?assertMatch([{aux, {stop, StreamId, N1, E2, _}}], lists:sort(Actions9)),
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = stopping,
                                                   state = {down, E}}}},
                 S10),

    %% now finally succeed in stopping the old writer
    N1Tail = {1, 107},
    S11 = update_stream(meta(?LINE),
                        {member_stopped, StreamId, #{node => N1,
                                                     epoch => E2,
                                                     tail => N1Tail}}, S10),
    %% skip straight to ready as cluster is already operative
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = undefined,
                                                   state = {ready, E2}}}},
                 S11),

    {S12, Actions11} = evaluate_stream(S11, []),
    ?assertMatch([{aux, {start_replica, StreamId, N1,
                         #{leader_pid := E2LeaderPid}}}],
                 lists:sort(Actions11)),
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = starting,
                                                   state = {ready, E2}}}},
                 S12),

    ok.

replica_down_test() ->
    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = ReplicaPids = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    N3 = node(Replica2),

    S0 = started_stream(StreamId, LeaderPid, ReplicaPids),
    S1 = update_stream(meta(?LINE), {down, Replica1, boom}, S0),
    ?assertMatch(#stream{members = #{N1 := #member{role = {writer, E},
                                                   current = undefined,
                                                   state = {running, E, LeaderPid}},
                                     N2 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {down, E}},
                                     N3 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {running, E, Replica2}}}},
                 S1),
    {S2, Actions} = evaluate_stream(S1, []),
    ?assertMatch([
                  {aux, {start_replica, StreamId, N2,
                         #{leader_pid := LeaderPid}}}
                 ],
                 lists:sort(Actions)),
    ?assertMatch(#stream{members = #{N2 := #member{role = {replica, E},
                                                   current = starting,
                                                   state = {down, E}}
                                     }},
                 S2),
    ok.

leader_start_failed_test() ->

    %% after a leader is selected we need to handle the case where the leader
    %% start fails
    %% this can happen if a node hosting the leader disconnects then connects
    %% then disconnects again (rabbit seems to do this sometimes).
    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = ReplicaPids = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    N3 = node(Replica2),

    S0 = started_stream(StreamId, LeaderPid, ReplicaPids),
    S1 = update_stream(meta(?LINE), {down, LeaderPid, boom}, S0),
    {S2, _Actions} = evaluate_stream(S1, []),
    %% leader was down but a temporary reconnection allowed the stop to complete
    S3 = update_stream(meta(?LINE),
                       {member_stopped, StreamId, #{node => N1,
                                                    epoch => E,
                                                    tail => {1, 2}}}, S2),

    {S3, []} = evaluate_stream(S3, []),
    S4 = update_stream(meta(?LINE),
                       {member_stopped, StreamId, #{node => N2,
                                                    epoch => E,
                                                    tail => {1, 1}}}, S3),
    E2 = E+1,
    {S5, Actions4} = evaluate_stream(S4, []),
    ?assertMatch([{aux, {start_writer, StreamId, #{epoch := E2,
                                                   leader_node := N1}}}],
                  lists:sort(Actions4)),
    S6 = update_stream(meta(?LINE),
                       {action_failed, StreamId, #{node => N1,
                                                   epoch => E2}}, S5),
    ?assertMatch(#stream{members = #{N1 := #member{role = {writer, E2},
                                                   current = undefined,
                                                   target = stopped,
                                                   state = {ready, E2}},
                                     N2 := #member{role = {replica, E2},
                                                   target = stopped,
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     N3 := #member{role = {replica, E2},
                                                   target = stopped,
                                                   current = stopping,
                                                   state = {running, E, _}}}},
                 S6),
    E3 = E2+1,
    {S7, Actions6} = evaluate_stream(S6, []),
    ?assertMatch([{aux, {stop, StreamId, N1, E2, _}},
                  {aux, {stop, StreamId, N2, E2, _}}
                 ], lists:sort(Actions6)),
    %% late stop from prior epoch - need to run stop again to make sure
    S8 = update_stream(meta(?LINE),
                       {member_stopped, StreamId, #{node => N3,
                                                    epoch => E,
                                                    tail => {1, 1}}}, S7),
    ?assertMatch(#stream{members = #{N1 := #member{role = {writer, E2},
                                                   current = stopping,
                                                   target = stopped,
                                                   state = {ready, E2}},
                                     N2 := #member{role = {replica, E2},
                                                   target = stopped,
                                                   current = stopping,
                                                   state = {ready, E2}},
                                     N3 := #member{role = {replica, E2},
                                                   target = stopped,
                                                   current = undefined,
                                                   state = {stopped, E, _}}}},
                 S8),
    {_S9, Actions8} = evaluate_stream(S8, []),
    ?assertMatch([{aux, {stop, StreamId, N3, E2, _}}
                 ], lists:sort(Actions8)),


    ok.

leader_down_scenario_1_test() ->
    %% leader ended up in a stopped state in epoch 2 but on ereplica was
    %% in ready, 2 and the other down 1

    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = ReplicaPids = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    N3 = node(Replica2),

    S0 = started_stream(StreamId, LeaderPid, ReplicaPids),
    S1 = update_stream(meta(?LINE), {down, LeaderPid, boom}, S0),
    ?assertMatch(#stream{members = #{N1 := #member{role = {writer, E},
                                                   current = undefined,
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {running, E, Replica1}},
                                     N3 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {running, E, Replica2}}}},
                 S1),
    {S2, Actions} = evaluate_stream(S1, []),
    %% expect all members to be stopping now
    %% replicas will receive downs however as will typically exit if leader does
    %% this is ok
    ?assertMatch([{aux, {stop, StreamId, N1, E2, _}},
                  {aux, {stop, StreamId, N2, E2, _}},
                  {aux, {stop, StreamId, N3, E2, _}}], lists:sort(Actions)),
    ?assertMatch(#stream{members = #{N1 := #member{role = {writer, E},
                                                   current = stopping,
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E},
                                                   current = stopping,
                                                   state = {running, E, Replica1}},
                                     N3 := #member{role = {replica, E},
                                                   current = stopping,
                                                   state = {running, E, Replica2}}}},
                 S2),

    %% idempotency check
    {S2, []} = evaluate_stream(S2, []),
    N2Tail = {E, 101},
    S3 = update_stream(meta(?LINE), {member_stopped, StreamId, #{node => N2,
                                                                 epoch => E,
                                                                 tail => N2Tail}}, S2),
    ?assertMatch(#stream{members = #{N2 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {stopped, E, N2Tail}}}},
                 S3),
    {S3, []} = evaluate_stream(S3, []),
    N3Tail = {E, 102},
    S4 = update_stream(meta(?LINE), {member_stopped, StreamId, #{node => N3,
                                                                 epoch => E,
                                                                 tail => N3Tail}}, S3),
    E2 = E + 1,
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = stopping,
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E2},
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     %% N3 has the higher offset so should
                                     %% be selected as writer of E2
                                     N3 := #member{role = {writer, E2},
                                                   current = undefined,
                                                   state = {ready, E2}}}},
                 S4),
    {S5, Actions4} = evaluate_stream(S4, []),
    %% new leader has been selected so should be started
    ?assertMatch([{aux, {start_writer, StreamId, #{leader_node := N3}}}],
                  lists:sort(Actions4)),
    ?assertMatch(#stream{epoch = E2}, S5),

    E2LeaderPid = fake_pid(n3),
    S6 = update_stream(meta(?LINE), {member_started, StreamId,
                                     #{epoch => E2, pid => E2LeaderPid}}, S5),
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = stopping,
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E2},
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     %% N3 has the higher offset so should
                                     %% be selected as writer of E2
                                     N3 := #member{role = {writer, E2},
                                                   current = undefined,
                                                   state = {running, E2, E2LeaderPid}}}},
                 S6),
    {S6b, Actions6} = evaluate_stream(S6, []),
    ?assertMatch([
                  {aux, {update_mnesia, _, _}},
                  {aux, {start_replica, StreamId, N2, _}}
                 ],
                 lists:sort(Actions6)),

    S7 = update_stream(meta(?LINE), {down, E2LeaderPid, boom}, S6b),
    {S8, Actions7} = evaluate_stream(S7, []),
    ?assertMatch([{aux, {stop, StreamId, N3, E2, _}}], lists:sort(Actions7)),
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = stopping,
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E2},
                                                   current = starting,
                                                   state = {ready, E2}},
                                     N3 := #member{role = {writer, E2},
                                                   current = stopping,
                                                   state = {down, E2}}}},
                 S8),
    %% writer is stopped before the ready replica has been started
    S9 = update_stream(meta(?LINE), {member_stopped, StreamId, #{node => N3,
                                                                 epoch => E2,
                                                                 tail => N3Tail}},
                       S8),
    ?assertMatch(#stream{members = #{N3 := #member{role = {writer, E2},
                                                   current = undefined,
                                                   state = {stopped, E2, N3Tail}}}},
                 S9),
    {S10, []} = evaluate_stream(S9, []),
    S11 = update_stream(meta(?LINE), {action_failed, StreamId,
                                      #{action => starting,
                                        node => N2,
                                        epoch => E2}},
                        S10),
    ?assertMatch(#stream{members = #{N2 := #member{role = {replica, E2},
                                                   current = undefined,
                                                   state = {ready, E2}}}},
                 S11),
    {S12, Actions11} = evaluate_stream(S11, []),
    ?assertMatch([{aux, {stop, StreamId, N2, E2, _}}], lists:sort(Actions11)),
    ?assertMatch(#stream{members = #{N2 := #member{role = {replica, E2},
                                                   current = stopping,
                                                   state = {ready, E2}}}},
                 S12),
    S13 = update_stream(meta(?LINE), {member_stopped, StreamId, #{node => N2,
                                                                  epoch => E2,
                                                                  tail => N2Tail}},
                        S12),
    E3 = E2 + 1,
    ?assertMatch(#stream{members = #{N3 := #member{role = {writer, E3},
                                                   current = undefined,
                                                   state = {ready, E3}},
                                     N2 := #member{role = {replica, E3},
                                                   current = undefined,
                                                   state = {ready, E3}},
                                     N1 := #member{role = {replica, E3},
                                                   current = stopping,
                                                   state = {down, E}}
                                    }},
                 S13),
    ok.

delete_stream_test() ->
    %% leader ended up in a stopped state in epoch 2 but one replica was
    %% in ready, 2 and the other down 1

    % E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = ReplicaPids = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    N3 = node(Replica2),

    S0 = started_stream(StreamId, LeaderPid, ReplicaPids),
    From = {self(), make_ref()},
    Meta1 = (meta(?LINE))#{from => From},
    S1 = update_stream(Meta1, {delete_stream, StreamId, #{}}, S0),
    ?assertMatch(#stream{target = deleted,
                         members = #{N3 := #member{target = deleted,
                                                   current = undefined,
                                                   state = _},
                                     N2 := #member{target = deleted,
                                                   current = undefined,
                                                   state = _},
                                     N1 := #member{target = deleted,
                                                   current = undefined,
                                                   state = _}
                                    }},
                 S1),
    {S2, Actions1} = evaluate_stream(S1, []),
    %% expect all members to be stopping now
    %% replicas will receive downs however as will typically exit if leader does
    %% this is ok
    ?assertMatch([{aux, {delete_member, StreamId, N1, _}},
                  {aux, {delete_member, StreamId, N2, _}},
                  {aux, {delete_member, StreamId, N3, _}}
                  % {reply, From, {wrap_reply, {ok, 0}}}
                 ],
                 lists:sort(Actions1)),
    ?assertMatch(#stream{target = deleted,
                         members = #{N3 := #member{target = deleted,
                                                   current = deleting,
                                                   state = _},
                                     N2 := #member{target = deleted,
                                                   current = deleting,
                                                   state = _},
                                     N1 := #member{target = deleted,
                                                   current = deleting,
                                                   state = _}
                                    }},
                 S2),
    S3 = update_stream(meta(?LINE), {member_deleted, StreamId, #{node => N1}},
                       S2),
    ?assertMatch(#stream{target = deleted,
                         members = #{N2 := _, N3 := _} = M3}
                   when not is_map_key(N1, M3), S3),
    {S4, []} = evaluate_stream(S3, []),
    ?assertMatch(#stream{target = deleted,
                         members = #{N2 := _, N3 := _} = M3}
                   when not is_map_key(N1, M3), S4),
    S5 = update_stream(meta(?LINE), {member_deleted, StreamId, #{node => N2}},
                       S4),
    ?assertMatch(#stream{target = deleted,
                         members = #{N3 := _} = M5}
                   when not is_map_key(N2, M5), S5),
    {S6, []} = evaluate_stream(S5, []),
    S7 = update_stream(meta(?LINE), {member_deleted, StreamId, #{node => N3}},
                       S6),
    ?assertEqual(undefined, S7),
    ok.

add_replica_test() ->
    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    %% this is to be added
    N3 = node(Replica2),

    S0 = started_stream(StreamId, LeaderPid, [Replica1]),
    From = {self(), make_ref()},
    Meta1 = (meta(?LINE))#{from => From},
    S1 = update_stream(Meta1, {add_replica, StreamId, #{node => N3}}, S0),
    ?assertMatch(#stream{target = running,
                         nodes = [N1, N2, N3],
                         members = #{N1 := #member{target = stopped,
                                                   current = undefined,
                                                   state = {running, _, _}},
                                     N2 := #member{target = stopped,
                                                   current = undefined,
                                                   state = {running, _, _}},
                                     N3 := #member{target = stopped,
                                                   current = undefined,
                                                   state = {down, 0}}
                                    }},
                 S1),
    {S2, Actions1} = evaluate_stream(S1, []),
    ?assertMatch([{aux, {stop, StreamId, N1, E, _}},
                  {aux, {stop, StreamId, N2, E, _}},
                  {aux, {stop, StreamId, N3, E, _}}], lists:sort(Actions1)),
    ?assertMatch(#stream{target = running,
                         nodes = [N1, N2, N3],
                         members = #{N1 := #member{target = stopped,
                                                   current = stopping,
                                                   state = {running, _, _}},
                                     N2 := #member{target = stopped,
                                                   current = stopping,
                                                   state = {running, _, _}},
                                     N3 := #member{target = stopped,
                                                   current = stopping,
                                                   state = {down, 0}}
                                    }},
                 S2),
    N1Tail = {E, 101},
    S3 = update_stream(meta(?LINE), {member_stopped, StreamId, #{node => N1,
                                                                 epoch => E,
                                                                 tail => N1Tail}},
                        S2),
    ?assertMatch(#stream{target = running,
                         nodes = [N1, N2, N3],
                         members = #{N1 := #member{target = running,
                                                   current = undefined,
                                                   state = {stopped, _, _}},
                                     N2 := #member{target = stopped,
                                                   current = stopping,
                                                   state = {running, _, _}},
                                     N3 := #member{target = stopped,
                                                   current = stopping,
                                                   state = {down, 0}}
                                    }}, S3),
    {S3, []} = evaluate_stream(S3, []),
    N2Tail = {E, 100},
    S4 = update_stream(meta(?LINE), {member_stopped, StreamId, #{node => N2,
                                                                 epoch => E,
                                                                 tail => N2Tail}},
                        S3),
    E2 = E + 1,
    ?assertMatch(#stream{target = running,
                         nodes = [N1, N2, N3],
                         members = #{N1 := #member{target = running,
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     N2 := #member{target = running,
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     N3 := #member{target = stopped,
                                                   current = stopping,
                                                   state = {down, 0}}
                                    }}, S4),
    {S3, []} = evaluate_stream(S3, []),
    {S5, Actions4} = evaluate_stream(S4, []),
    ?assertMatch([{aux, {start_writer, StreamId, #{leader_node := N1}}}],
                  lists:sort(Actions4)),
    ?assertMatch(#stream{epoch = E2}, S5),
    S6 = update_stream(meta(?LINE), {member_stopped, StreamId, #{node => N3,
                                                                 epoch => E,
                                                                 tail => empty}},
                        S5),
    ?assertMatch(#stream{target = running,
                         nodes = [N1, N2, N3],
                         members = #{N1 := #member{target = running,
                                                   current = starting,
                                                   role = {writer, _},
                                                   state = {ready, E2}},
                                     N2 := #member{target = running,
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     N3 := #member{target = running,
                                                   current = undefined,
                                                   state = {ready, E2}}
                                    }}, S6),
    ok.

delete_replica_test() ->
    %% TOOD: replica and leader needs to be tested
    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    %% this is to be added
    N3 = node(Replica2),

    S0 = started_stream(StreamId, LeaderPid, [Replica1, Replica2]),
    From = {self(), make_ref()},
    Meta1 = (meta(?LINE))#{from => From},
    S1 = update_stream(Meta1, {delete_replica, StreamId, #{node => N3}}, S0),
    ?assertMatch(#stream{target = running,
                         nodes = [N1, N2],
                         members = #{N1 := #member{target = stopped,
                                                   current = undefined,
                                                   state = {running, _, _}},
                                     N2 := #member{target = stopped,
                                                   current = undefined,
                                                   state = {running, _, _}},
                                     N3 := #member{target = deleted,
                                                   current = undefined,
                                                   state = {running, _, _}}
                                    }},
                 S1),
    {S2, Actions1} = evaluate_stream(S1, []),
    ?assertMatch([{aux, {delete_member, StreamId, N3, _}},
                  {aux, {stop, StreamId, N1, E, _}},
                  {aux, {stop, StreamId, N2, E, _}}], lists:sort(Actions1)),
    S3 = update_stream(meta(?LINE), {member_deleted, StreamId, #{node => N3}},
                       S2),
    ?assertMatch(#stream{target = running,
                         nodes = [N1, N2],
                         members = #{N1 := #member{target = stopped,
                                                   current = stopping,
                                                   state = {running, _, _}},
                                     N2 := #member{target = stopped,
                                                   current = stopping,
                                                   state = {running, _, _}}
                                    } = Members}
                   when not is_map_key(N3, Members), S3),
    {S3, []} = evaluate_stream(S3, []),
    S4 = update_stream(meta(?LINE),
                       {member_stopped, StreamId, #{node => N1,
                                                    epoch => E,
                                                    tail => {E, 100}}},
                       S3),
    {S4, []} = evaluate_stream(S4, []),
    S5 = update_stream(meta(?LINE),
                       {member_stopped, StreamId, #{node => N2,
                                                    epoch => E,
                                                    tail => {E, 101}}},
                       S4),
    {S6, Actions5} = evaluate_stream(S5, []),
    E2 = E + 1,
    ?assertMatch(#stream{target = running,
                         nodes = [N1, N2],
                         members = #{N1 := #member{target = running,
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     N2 := #member{target = running,
                                                   role = {writer, E2},
                                                   current = starting,
                                                   state = {ready, E2}}
                                    }}, S6),
    ?assertMatch([{aux, {start_writer, StreamId, #{nodes := [N1, N2]}}}
                  ], lists:sort(Actions5)),
    {S4, []} = evaluate_stream(S4, []),
    ok.

delete_replica_leader_test() ->
    %% TOOD: replica and leader needs to be tested
    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, _Replica2] = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    %% this is to be added
    % N3 = node(Replica2),

    S0 = started_stream(StreamId, LeaderPid, [Replica1]),
    From = {self(), make_ref()},
    Meta1 = (meta(?LINE))#{from => From},
    S1 = update_stream(Meta1, {delete_replica, StreamId, #{node => N1}}, S0),
    ?assertMatch(#stream{target = running,
                         nodes = [N2],
                         members = #{N1 := #member{target = deleted,
                                                   current = undefined,
                                                   state = {running, _, _}},
                                     N2 := #member{target = stopped,
                                                   current = undefined,
                                                   state = {running, _, _}}
                                    }},
                 S1),
    {S2, Actions1} = evaluate_stream(S1, []),
    ?assertMatch([{aux, {delete_member, StreamId, N1, _}},
                  {aux, {stop, StreamId, N2, E, _}}], lists:sort(Actions1)),
    S3 = S2,
    S4 = update_stream(meta(?LINE),
                       {member_stopped, StreamId, #{node => N2,
                                                    epoch => E,
                                                    tail => {E, 100}}},
                       S3),
    E2 = E+1,
    ?assertMatch(#stream{target = running,
                         nodes = [N2],
                         members = #{N1 := #member{target = deleted,
                                                   current = deleting,
                                                   state = {running, _, _}},
                                     N2 := #member{target = running,
                                                   role = {writer, E2},
                                                   current = undefined,
                                                   state = {ready, E2}}
                                    }},
                 S4),
    ok.

meta(Ts) ->
    #{system_time => Ts}.

started_stream(StreamId, LeaderPid, ReplicaPids) ->
    E = 1,
    Ts = 0,
    Nodes = [node(LeaderPid) | [node(P) || P <- ReplicaPids]],
    Conf = #{name => StreamId,
             nodes => Nodes},

    VHost = <<"/">>,
    QName = #resource{kind = queue,
                      name = list_to_binary(StreamId),
                      virtual_host = VHost},
    Members0 = #{node(LeaderPid) => #member{role = {writer, E},
                                            node = node(LeaderPid),
                                            state = {running, E, LeaderPid},
                                            current = undefined,
                                            current_ts = Ts}},
    Members = lists:foldl(fun (R, Acc) ->
                                  N = node(R),
                                  Acc#{N => #member{role = {replica, E},
                                                    node = N,
                                                    state = {running, E, R},
                                                    current = undefined,
                                                    current_ts = Ts}}
                          end, Members0, ReplicaPids),


    #stream{id = StreamId,
            epoch = 1,
            nodes = Nodes,
            queue_ref = QName,
            conf = Conf,
            mnesia = {updated, 1},
            members = Members}.

new_q(Name, TypeState) ->
    VHost = <<"/">>,
    QName = #resource{kind = queue,
                      name = Name,
                      virtual_host = VHost},
    amqqueue:set_type_state(
      amqqueue:new_with_version(amqqueue_v2,
                                QName,
                                none,
                                true,
                                false,
                                none,
                                [],
                                VHost,
                                #{},
                                rabbit_stream_queue), TypeState).

fake_pid(Node) ->
    NodeBin = atom_to_binary(Node),
    ThisNodeSize = size(term_to_binary(node())) + 1,
    Pid = spawn(fun () -> ok end),
    %% drop the local node data from a local pid
    <<Pre:ThisNodeSize/binary, LocalPidData/binary>> = term_to_binary(Pid),
    S = size(NodeBin),
    %% get the encoding type of the pid
    <<_:8, Type:8/unsigned, _/binary>> = Pre,
    %% replace it with the incoming node binary
    Final = <<131, Type, 100, S:16/unsigned, NodeBin/binary, LocalPidData/binary>>,
    binary_to_term(Final).
-endif.
