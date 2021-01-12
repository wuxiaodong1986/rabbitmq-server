%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2019-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @author The RabbitMQ team
%% @copyright 2019-2021 VMware, Inc. or its affiliates.
%%
%% @doc
%% This module manages the configuration of the Erlang Logger facility. In
%% other words, it translates the RabbitMQ logging configuration (in the
%% Cuttlefish format or classic Erlang-term-based configuration) into Erlang
%% Logger handler setups.
%%
%% Configuring the Erlang Logger is done in two steps:
%% <ol>
%% <li>Logger handler configurations are created based on the configuration
%% and the context (see {@link //rabbit_common/rabbit_env}).</li>
%% <li>Created handlers are installed (i.e. they become active). Any handlers
%% previously installed by this module are removed.</li>
%% </ol>
%%
%% It also takes care of setting the `$ERL_CRASH_DUMP' variable to enable
%% Erlang core dumps.
%%
%% Note that before this module handles the Erlang Logger, {@link
%% //rabbitmq_prelaunch/rabbit_prelaunch_early_logging} configures basic
%% logging to have messages logged as soon as possible during RabbitMQ
%% startup.
%%
%% == How to configure RabbitMQ logging ==
%%
%% RabbitMQ supports a main/default logging output and per-category outputs.
%% An output is a combination of a destination (a text file or stdout for
%% example) and a message formatted (e.g. plain text or JSON).
%%
%% Here is the Erlang-term-based configuration expected and supported by this
%% module:
%%
%% ```
%% {rabbit, [
%%   {log_root, string()},
%%   {log, [
%%     {categories, [
%%       {default, [
%%         {level, Level}
%%       ]},
%%       {CategoryName, [
%%         {level, Level},
%%         {file, Filename}
%%       ]}
%%     ]},
%%
%%     {console, [
%%       {level, Level},
%%       {enabled, boolean()}
%%     ]},
%%
%%     {exchange, [
%%       {level, Level},
%%       {enabled, boolean()}
%%     ]},
%%
%%     {file, [
%%       {level, Level},
%%       {file, Filename | false},
%%       {date, RotationDateSpec},
%%       {size, RotationSize},
%%       {count, RotationCount},
%%     ]},
%%
%%     {syslog, [
%%       {level, Level},
%%       {enabled, boolean()}
%%     ]}
%%   ]}
%% ]}.
%%
%% Level = logger:level().
%% Filename = file:filename().
%% RotationDateSpec = string(). % Pattern format used by newsyslog.conf(5).
%% RotationSize = non_neg_integer() | infinity.
%% RotationCount = non_neg_integer().
%% '''
%%
%% See `priv/schema/rabbit.schema' for the definition of the Cuttlefish
%% configuration schema.

-module(rabbit_prelaunch_logging).

-include_lib("kernel/include/logger.hrl").
-include_lib("rabbit_common/include/logging.hrl").

-export([setup/1,
         set_log_level/1,
         log_locations/0]).

-ifdef(TEST).
-export([clear_config_run_number/0, get_less_severe_level/2]).
-endif.

-export_type([log_location/0]).

-type log_location() :: file:filename() | string().
%% A short description of an output.
%%
%% If the output is the console, the location is either `"<stdout>"' or
%% `"<stderr>"'.
%%
%% If the output is an exchange, the location is the string `"exchange:"' with
%% the exchange name appended.
%%
%% If the output is a file, the location is the absolute filename.
%%
%% If the output is syslog, the location is the string `"syslog:"' with the
%% syslog server hostname appended.

-type category_name() :: atom().

-type console_props() :: [{level, logger:level()} |
                          {enabled, boolean()}].
-type exchange_props() :: console_props().
-type file_props() :: [{level, logger:level()} |
                       {file, file:filename() | false} |
                       {date, string()} |
                       {size, non_neg_integer()} |
                       {count, non_neg_integer()}].
-type syslog_props() :: console_props().
-type main_log_env() :: [{console, console_props()} |
                         {exchange, exchange_props()} |
                         {file, file_props()} |
                         {syslog, syslog_props()}].
-type per_cat_env() :: [{level, logger:level()} |
                        {file, file:filename()}].
-type default_cat_env() :: [{level, logger:level()}].
-type log_app_env() :: [main_log_env() |
                        {categories, [{default, default_cat_env()} |
                                      {category_name(), per_cat_env()}]}].

-type per_cat_log_config() :: #{level => logger:level() | all | none,
                                outputs := [logger:handler_config()]}.
-type global_log_config() :: per_cat_log_config().
-type log_config() :: #{global := global_log_config(),
                        per_category := #{
                          category_name() => per_cat_log_config()}}.
-type handler_key() :: atom().

-type id_assignment_state() :: #{config_run_number := pos_integer(),
                                 next_file := pos_integer()}.

-spec setup(rabbit_env:context()) -> ok.
%% @doc
%% Configures or reconfigures logging.
%%
%% The logging framework is the builtin Erlang Logger API. The configuration
%% is based on the configuration file and the environment.
%%
%% In addition to logging, it sets the `$ERL_CRASH_DUMP' environment variable
%% to enable Erlang crash dumps.
%%
%% @param Context the RabbitMQ context (see {@link
%% //rabbitmq_prelaunch/rabbit_prelaunch:get_context/0}).

setup(Context) ->
    ?LOG_DEBUG("\n== Logging ==",
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ok = compute_config_run_number(),
    ok = set_ERL_CRASH_DUMP_envvar(Context),
    ok = configure_logger(Context).

-spec set_log_level(logger:level()) -> ok | {error, term()}.
%% @doc
%% Changes the global log level.
%%
%% Any message with a less severe log level will be discarded. However, more
%% severe messages may still be discarded as well if an output is configured
%% with a less verbose log level: this function does not change per-output log
%% level.
%%
%% @param Level the log level to set the primary config to.

set_log_level(Level) ->
    logger:set_primary_config(level, Level).

-spec log_locations() -> [file:filename() | string()].
%% @doc
%% Returns the list of output locations.
%%
%% @returns the list of output locations.
%%
%% @see log_location()

log_locations() ->
    Handlers = logger:get_handler_config(),
    log_locations(Handlers, []).

log_locations([#{module := Mod,
                 config := #{type := file,
                             file := Filename}} | Rest],
              Locations)
  when ?IS_STD_H_COMPAT(Mod) ->
    Locations1 = add_once(Locations, Filename),
    log_locations(Rest, Locations1);
log_locations([#{module := Mod,
                 config := #{type := standard_io}} | Rest],
              Locations)
  when ?IS_STD_H_COMPAT(Mod) ->
    Locations1 = add_once(Locations, "<stdout>"),
    log_locations(Rest, Locations1);
log_locations([#{module := Mod,
                 config := #{type := standard_error}} | Rest],
              Locations)
  when ?IS_STD_H_COMPAT(Mod) ->
    Locations1 = add_once(Locations, "<stderr>"),
    log_locations(Rest, Locations1);
log_locations([#{module := syslog_logger_h} | Rest],
              Locations) ->
    Host = application:get_env(syslog, dest_host, ""),
    Locations1 = add_once(
                   Locations,
                   rabbit_misc:format("syslog:~s", [Host])),
    log_locations(Rest, Locations1);
log_locations([#{module := rabbit_logger_exchange_h,
                 config := #{exchange := Exchange}} | Rest],
              Locations) ->
    Locations1 = add_once(
                   Locations,
                   rabbit_misc:format("exchange:~p", [Exchange])),
    log_locations(Rest, Locations1);
log_locations([_ | Rest], Locations) ->
    log_locations(Rest, Locations);
log_locations([], Locations) ->
    lists:sort(Locations).

add_once(Locations, Location) ->
    case lists:member(Location, Locations) of
        false -> [Location | Locations];
        true  -> Locations
    end.

%% -------------------------------------------------------------------
%% ERL_CRASH_DUMP setting.
%% -------------------------------------------------------------------

-spec set_ERL_CRASH_DUMP_envvar(rabbit_env:context()) -> ok.

set_ERL_CRASH_DUMP_envvar(Context) ->
    case os:getenv("ERL_CRASH_DUMP") of
        false ->
            LogBaseDir = get_log_base_dir(Context),
            ErlCrashDump = filename:join(LogBaseDir, "erl_crash.dump"),
            ?LOG_DEBUG(
              "Setting $ERL_CRASH_DUMP environment variable to \"~ts\"",
              [ErlCrashDump],
              #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            os:putenv("ERL_CRASH_DUMP", ErlCrashDump),
            ok;
        ErlCrashDump ->
            ?LOG_DEBUG(
              "$ERL_CRASH_DUMP environment variable already set to \"~ts\"",
              [ErlCrashDump],
              #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            ok
    end.

-spec get_log_base_dir(rabbit_env:context()) -> file:filename().

get_log_base_dir(#{log_base_dir := LogBaseDirFromEnv} = Context) ->
    case rabbit_env:has_var_been_overridden(Context, log_base_dir) of
        false -> application:get_env(rabbit, log_root, LogBaseDirFromEnv);
        true  -> LogBaseDirFromEnv
    end.

%% -------------------------------------------------------------------
%% Logger's handlers configuration.
%% -------------------------------------------------------------------

-define(CONFIG_RUN_NUMBER_KEY, {?MODULE, config_run_number}).

-spec compute_config_run_number() -> ok.

compute_config_run_number() ->
    RunNum = persistent_term:get(?CONFIG_RUN_NUMBER_KEY, 0),
    ok = persistent_term:put(?CONFIG_RUN_NUMBER_KEY, RunNum + 1).

-spec get_config_run_number() -> pos_integer().

get_config_run_number() ->
    persistent_term:get(?CONFIG_RUN_NUMBER_KEY).

-ifdef(TEST).
-spec clear_config_run_number() -> ok.

clear_config_run_number() ->
    _ = persistent_term:erase(?CONFIG_RUN_NUMBER_KEY),
    ok.
-endif.

-spec configure_logger(rabbit_env:context()) -> ok.

configure_logger(Context) ->
    %% Configure main handlers.
    %% We distinguish them by their type and possibly other
    %% parameters (file name, syslog settings, etc.).
    LogConfig0 = get_log_configuration_from_app_env(),
    LogConfig1 = handle_default_and_overridden_outputs(LogConfig0, Context),
    LogConfig2 = apply_log_levels_from_env(LogConfig1, Context),
    LogConfig3 = make_filenames_absolute(LogConfig2, Context),
    LogConfig4 = configure_formatters(LogConfig3, Context),
    Handlers = create_logger_handlers_conf(LogConfig4),
    ?LOG_DEBUG("Logger handlers:~n  ~p", [Handlers],
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ?LOG_NOTICE("Logging: switching to configured handler(s); following "
                "messages may not be visible in this log output",
                #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ok = install_handlers(Handlers),
    ?LOG_NOTICE("Logging: configured log handlers are now ACTIVE",
                #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ok = maybe_log_test_messages(LogConfig3).

-spec get_log_configuration_from_app_env() -> log_config().

get_log_configuration_from_app_env() ->
    %% The log configuration in the Cuttlefish configuration file or the
    %% application environment is not structured logically. This functions is
    %% responsible for extracting the configuration and organize it. If one day
    %% we decide to fix the configuration structure, we just have to modify
    %% this function and normalize_*().
    Env = get_log_app_env(),
    DefaultAndCatProps = proplists:get_value(categories, Env, []),
    DefaultProps = proplists:get_value(default, DefaultAndCatProps, []),
    CatProps = proplists:delete(default, DefaultAndCatProps),
    PerCatConfig = maps:from_list(
                     [{Cat, normalize_per_cat_log_config(Props)}
                      || {Cat, Props} <- CatProps]),
    EnvWithoutCats = proplists:delete(categories, Env),
    GlobalConfig = normalize_main_log_config(EnvWithoutCats, DefaultProps),
    #{global => GlobalConfig,
      per_category => PerCatConfig}.

-spec get_log_app_env() -> log_app_env().

get_log_app_env() ->
    application:get_env(rabbit, log, []).

-spec normalize_main_log_config(main_log_env(), default_cat_env()) ->
    global_log_config().

normalize_main_log_config(Props, DefaultProps) ->
    Outputs = case proplists:get_value(level, DefaultProps) of
                  undefined -> #{outputs => []};
                  Level     -> #{outputs => [],
                                 level => Level}
              end,
    normalize_main_log_config1(Props, Outputs).

normalize_main_log_config1([{Type, Props} | Rest],
                           #{outputs := Outputs} = LogConfig) ->
    Outputs1 = normalize_main_output(Type, Props, Outputs),
    LogConfig1 = LogConfig#{outputs => Outputs1},
    normalize_main_log_config1(Rest, LogConfig1);
normalize_main_log_config1([], LogConfig) ->
    LogConfig.

-spec normalize_main_output
(console, console_props(), [logger:handler_config()]) ->
    [logger:handler_config()];
(exchange, exchange_props(), [logger:handler_config()]) ->
    [logger:handler_config()];
(file, file_props(), [logger:handler_config()]) ->
    [logger:handler_config()];
(syslog, syslog_props(), [logger:handler_config()]) ->
    [logger:handler_config()].

normalize_main_output(file, Props, Outputs) ->
    normalize_main_file_output(
      Props,
      #{module => rabbit_logger_std_h,
        config => #{type => file}},
      Outputs);
normalize_main_output(console, Props, Outputs) ->
    normalize_main_console_output(
      Props,
      #{module => rabbit_logger_std_h,
        config => #{type => standard_io}},
      Outputs);
normalize_main_output(syslog, Props, Outputs) ->
    normalize_main_console_output(
      Props,
      #{module => syslog_logger_h,
        config => #{}},
      Outputs);
normalize_main_output(exchange, Props, Outputs) ->
    normalize_main_console_output(
      Props,
      #{module => rabbit_logger_exchange_h,
        config => #{}},
      Outputs).

-spec normalize_main_file_output(file_props(), logger:handler_config(),
                                 [logger:handler_config()]) ->
    [logger:handler_config()].

normalize_main_file_output([{file, false} | _], _, Outputs) ->
    lists:filter(
      fun
          (#{module := rabbit_logger_std_h,
             config := #{type := file}}) -> false;
          (_)                            -> true
      end, Outputs);
normalize_main_file_output([{file, Filename} | Rest],
                           #{config := Config} = Output, Outputs) ->
    Output1 = Output#{config => Config#{file => Filename}},
    normalize_main_file_output(Rest, Output1, Outputs);
normalize_main_file_output([{level, Level} | Rest],
                           Output, Outputs) ->
    Output1 = Output#{level => Level},
    normalize_main_file_output(Rest, Output1, Outputs);
normalize_main_file_output([{date, DateSpec} | Rest],
                           #{config := Config} = Output, Outputs) ->
    Output1 = Output#{config => Config#{rotate_on_date => DateSpec}},
    normalize_main_file_output(Rest, Output1, Outputs);
normalize_main_file_output([{size, Size} | Rest],
                           #{config := Config} = Output, Outputs) ->
    Output1 = Output#{config => Config#{max_no_bytes => Size}},
    normalize_main_file_output(Rest, Output1, Outputs);
normalize_main_file_output([{count, Count} | Rest],
                           #{config := Config} = Output, Outputs) ->
    Output1 = Output#{config => Config#{max_no_files => Count}},
    normalize_main_file_output(Rest, Output1, Outputs);
normalize_main_file_output([], Output, Outputs) ->
    [Output | Outputs].

-spec normalize_main_console_output(console_props(), logger:handler_config(),
                                    [logger:handler_config()]) ->
    [logger:handler_config()].

normalize_main_console_output(
  [{enabled, false} | _],
  #{module := Mod1, config := #{type := Stddev}},
  Outputs)
  when ?IS_STD_H_COMPAT(Mod1) andalso
       ?IS_STDDEV(Stddev) ->
    lists:filter(
      fun
          (#{module := Mod2,
             config := #{type := standard_io}})
            when ?IS_STD_H_COMPAT(Mod2) ->
              false;
          (#{module := Mod2,
             config := #{type := standard_error}})
            when ?IS_STD_H_COMPAT(Mod2) ->
              false;
          (_) ->
              true
      end, Outputs);
normalize_main_console_output(
  [{enabled, false} | _],
  #{module := Mod},
  Outputs)
  when Mod =:= syslog_logger_h orelse
       Mod =:= rabbit_logger_exchange_h ->
    lists:filter(fun(#{module := M}) -> M =/= Mod end, Outputs);
normalize_main_console_output([{enabled, true} | Rest], Output, Outputs) ->
    normalize_main_console_output(Rest, Output, Outputs);
normalize_main_console_output([{level, Level} | Rest],
                              Output, Outputs) ->
    Output1 = Output#{level => Level},
    normalize_main_console_output(Rest, Output1, Outputs);
normalize_main_console_output([], Output, Outputs) ->
    [Output | Outputs].

-spec normalize_per_cat_log_config(per_cat_env()) -> per_cat_log_config().

normalize_per_cat_log_config(Props) ->
    normalize_per_cat_log_config(Props, #{outputs => []}).

normalize_per_cat_log_config([{level, Level} | Rest], LogConfig) ->
    LogConfig1 = LogConfig#{level => Level},
    normalize_per_cat_log_config(Rest, LogConfig1);
normalize_per_cat_log_config([{file, Filename} | Rest],
                             #{outputs := Outputs} = LogConfig) ->
    Output = #{module => rabbit_logger_std_h,
               config => #{type => file,
                           file => Filename}},
    LogConfig1 = LogConfig#{outputs => [Output | Outputs]},
    normalize_per_cat_log_config(Rest, LogConfig1);
normalize_per_cat_log_config([], LogConfig) ->
    LogConfig.

-spec handle_default_and_overridden_outputs(log_config(),
                                            rabbit_env:context()) ->
    log_config().

handle_default_and_overridden_outputs(LogConfig, Context) ->
    LogConfig1 = handle_default_main_output(LogConfig, Context),
    LogConfig2 = handle_default_upgrade_cat_output(LogConfig1, Context),
    LogConfig2.

-spec handle_default_main_output(log_config(), rabbit_env:context()) ->
    log_config().

handle_default_main_output(
  #{global := #{outputs := Outputs} = GlobalConfig} = LogConfig,
  #{main_log_file := MainLogFile} = Context) ->
    NoOutputsConfigured = Outputs =:= [],
    Overridden = rabbit_env:has_var_been_overridden(Context, main_log_file),
    Outputs1 = case MainLogFile of
                   "-" when NoOutputsConfigured orelse Overridden ->
                       [#{module => rabbit_logger_std_h,
                          config => #{type => standard_io}}];
                   "-stderr" when NoOutputsConfigured orelse Overridden ->
                       [#{module => rabbit_logger_std_h,
                          config => #{type => standard_error}}];
                   Filename when NoOutputsConfigured orelse Overridden ->
                       [#{module => rabbit_logger_std_h,
                          config => #{type => file,
                                      file => Filename}}];
                   Filename ->
                       [case Output of
                            #{module := Mod,
                              config := #{type := file, file := _}}
                              when ?IS_STD_H_COMPAT(Mod) ->
                                Output;
                            #{module := Mod,
                              config := #{type := file} = Config}
                              when ?IS_STD_H_COMPAT(Mod) ->
                                Output#{config => Config#{file => Filename}};
                            _ ->
                                Output
                        end || Output <- Outputs]
               end,
    case Outputs1 of
        Outputs -> LogConfig;
        _       -> LogConfig#{
                     global => GlobalConfig#{
                                 outputs => Outputs1}}
    end.

-spec handle_default_upgrade_cat_output(log_config(), rabbit_env:context()) ->
    log_config().

handle_default_upgrade_cat_output(
  #{per_category := PerCatConfig} = LogConfig,
  #{upgrade_log_file := UpgLogFile} = Context) ->
    UpgCatConfig = case PerCatConfig of
                       #{upgrade := CatConfig} -> CatConfig;
                       _                       -> #{outputs => []}
                   end,
    #{outputs := Outputs} = UpgCatConfig,
    NoOutputsConfigured = Outputs =:= [],
    Overridden = rabbit_env:has_var_been_overridden(Context, upgrade_log_file),
    Outputs1 = case UpgLogFile of
                   "-" when NoOutputsConfigured orelse Overridden ->
                       %% We re-use the default logger handler.
                       [];
                   "-stderr" when NoOutputsConfigured orelse Overridden ->
                       [#{module => rabbit_logger_std_h,
                          config => #{type => standard_error}}];
                   Filename when NoOutputsConfigured orelse Overridden ->
                       [#{module => rabbit_logger_std_h,
                          config => #{type => file,
                                      file => Filename}}];
                   _ ->
                       Outputs
               end,
    case Outputs1 of
        Outputs -> LogConfig;
        _       -> LogConfig#{
                     per_category => PerCatConfig#{
                                       upgrade => UpgCatConfig#{
                                                    outputs => Outputs1}}}
    end.

-spec apply_log_levels_from_env(log_config(), rabbit_env:context()) ->
    log_config().

apply_log_levels_from_env(LogConfig, #{log_levels := LogLevels})
  when is_map(LogLevels) ->
    maps:fold(
      fun
          (_, Value, LC) when is_boolean(Value) ->
              %% Ignore the 'color' and 'json' flags.
              LC;
          (global, Level, #{global := GlobalConfig} = LC) ->
              GlobalConfig1 = GlobalConfig#{level => Level},
              LC#{global => GlobalConfig1};
          (CatString, Level, #{per_category := PerCatConfig} = LC) ->
              CatAtom = list_to_atom(CatString),
              CatConfig0 = maps:get(CatAtom, PerCatConfig, #{outputs => []}),
              CatConfig1 = CatConfig0#{level => Level},
              PerCatConfig1 = PerCatConfig#{CatAtom => CatConfig1},
              LC#{per_category => PerCatConfig1}
      end, LogConfig, LogLevels);
apply_log_levels_from_env(LogConfig, _) ->
    LogConfig.

-spec make_filenames_absolute(log_config(), rabbit_env:context()) ->
    log_config().

make_filenames_absolute(
  #{global := GlobalConfig, per_category := PerCatConfig} = LogConfig,
  Context) ->
    LogBaseDir = get_log_base_dir(Context),
    GlobalConfig1 = make_filenames_absolute1(GlobalConfig, LogBaseDir),
    PerCatConfig1 = maps:map(
                      fun(_, CatConfig) ->
                              make_filenames_absolute1(CatConfig, LogBaseDir)
                      end, PerCatConfig),
    LogConfig#{global => GlobalConfig1, per_category => PerCatConfig1}.

make_filenames_absolute1(#{outputs := Outputs} = Config, LogBaseDir) ->
    Outputs1 = lists:map(
                 fun
                     (#{module := Mod,
                        config := #{type := file,
                                    file := Filename} = Cfg} = Output)
                       when ?IS_STD_H_COMPAT(Mod) ->
                         Cfg1 = Cfg#{file => filename:absname(
                                               Filename, LogBaseDir)},
                         Output#{config => Cfg1};
                     (Output) ->
                         Output
                 end, Outputs),
    Config#{outputs => Outputs1}.

-spec configure_formatters(log_config(), rabbit_env:context()) ->
    log_config().

configure_formatters(
  #{global := GlobalConfig, per_category := PerCatConfig} = LogConfig,
  Context) ->
    GlobalConfig1 = configure_formatters1(GlobalConfig, Context),
    PerCatConfig1 = maps:map(
                      fun(_, CatConfig) ->
                              configure_formatters1(CatConfig, Context)
                      end, PerCatConfig),
    LogConfig#{global => GlobalConfig1, per_category => PerCatConfig1}.

configure_formatters1(#{outputs := Outputs} = Config, Context) ->
    ConsFormatter =
    rabbit_prelaunch_early_logging:default_console_formatter(Context),
    FileFormatter =
    rabbit_prelaunch_early_logging:default_file_formatter(Context),
    SyslogFormatter =
    rabbit_prelaunch_early_logging:default_syslog_formatter(Context),
    Outputs1 = lists:map(
                 fun
                     (#{module := Mod,
                        config := #{type := Stddev}} = Output)
                       when ?IS_STD_H_COMPAT(Mod) andalso
                            ?IS_STDDEV(Stddev) ->
                         case maps:is_key(formatter, Output) of
                             true  -> Output;
                             false -> Output#{formatter => ConsFormatter}
                         end;
                     (#{module := syslog_logger_h} = Output) ->
                         case maps:is_key(formatter, Output) of
                             true  -> Output;
                             false -> Output#{formatter => SyslogFormatter}
                         end;
                     (Output) ->
                         case maps:is_key(formatter, Output) of
                             true  -> Output;
                             false -> Output#{formatter => FileFormatter}
                         end
                 end, Outputs),
    Config#{outputs => Outputs1}.

-spec create_logger_handlers_conf(log_config()) ->
    [logger:handler_config()].

create_logger_handlers_conf(
  #{global := GlobalConfig, per_category := PerCatConfig}) ->
    Handlers0 = create_global_handlers_conf(GlobalConfig),
    Handlers1 = create_per_cat_handlers_conf(PerCatConfig, Handlers0),
    Handlers2 = adjust_log_levels(Handlers1),
    Handlers3 = assign_handler_ids(Handlers2),
    Handlers3.

-spec create_global_handlers_conf(global_log_config()) ->
    #{handler_key() := logger:handler_config()}.

create_global_handlers_conf(#{outputs := Outputs} = GlobalConfig) ->
    create_handlers_conf(Outputs, global, GlobalConfig, #{}).

-spec create_per_cat_handlers_conf(
        #{category_name() => per_cat_log_config()},
        #{handler_key() => logger:handler_config()}) ->
    #{handler_key() => logger:handler_config()}.

create_per_cat_handlers_conf(PerCatConfig, Handlers) ->
    maps:fold(
      fun
          (CatName, #{outputs := []} = CatConfig, Hdls) ->
              filter_cat_in_global_handlers(Hdls, CatName, CatConfig);
          (CatName, #{outputs := Outputs} = CatConfig, Hdls) ->
              Hdls1 = create_handlers_conf(Outputs, CatName, CatConfig, Hdls),
              filter_out_cat_in_other_handlers(Hdls1, CatName)
      end, Handlers, PerCatConfig).

-spec create_handlers_conf(
        [logger:handler_config()], global | category_name(),
        global_log_config() | per_cat_log_config(),
        #{handler_key() => logger:handler_config()}) ->
    #{handler_key() => logger:handler_config()}.

create_handlers_conf([Output | Rest], CatName, Config, Handlers) ->
    Key = create_handler_key(Output),
    Handler = case maps:is_key(Key, Handlers) of
                  false -> create_handler_conf(Output, CatName, Config);
                  true  -> update_handler_conf(maps:get(Key, Handlers),
                                               CatName, Output)
              end,
    Handlers1 = Handlers#{Key => Handler},
    create_handlers_conf(Rest, CatName, Config, Handlers1);
create_handlers_conf([], _, _, Handlers) ->
    Handlers.

-spec create_handler_key(logger:handler_config()) -> handler_key().

create_handler_key(
  #{module := Mod, config := #{type := file, file := Filename}})
  when ?IS_STD_H_COMPAT(Mod) ->
    {file, Filename};
create_handler_key(
  #{module := Mod, config := #{type := standard_io}})
  when ?IS_STD_H_COMPAT(Mod) ->
    {console, standard_io};
create_handler_key(
  #{module := Mod, config := #{type := standard_error}})
  when ?IS_STD_H_COMPAT(Mod) ->
    {console, standard_error};
create_handler_key(
  #{module := syslog_logger_h}) ->
    syslog;
create_handler_key(
  #{module := rabbit_logger_exchange_h}) ->
    exchange.

-spec create_handler_conf(
        logger:handler_config(), global | category_name(),
        global_log_config() | per_cat_log_config()) ->
    logger:handler_config().

create_handler_conf(Output, global, Config) ->
    Level = compute_level_from_config_and_output(Config, Output),
    Output#{level => Level,
            filter_default => log,
            filters => [{?FILTER_NAME,
                         {fun filter_log_event/2, #{global => Level}}}]};
create_handler_conf(Output, CatName, Config) ->
    Level = compute_level_from_config_and_output(Config, Output),
    Output#{level => Level,
            filter_default => stop,
            filters => [{?FILTER_NAME,
                         {fun filter_log_event/2, #{CatName => Level}}}]}.

-spec update_handler_conf(
        logger:handler_config(), global | category_name(),
        logger:handler_config()) ->
    logger:handler_config().

update_handler_conf(
  #{level := ConfiguredLevel} = Handler, global, Output) ->
    case Output of
        #{level := NewLevel} ->
            Handler#{level =>
                     get_less_severe_level(NewLevel, ConfiguredLevel)};
        _ ->
            Handler
    end;
update_handler_conf(Handler, CatName, Output) ->
    add_cat_filter(Handler, CatName, Output).

-spec compute_level_from_config_and_output(
        global_log_config() | per_cat_log_config(),
        logger:handler_config()) ->
    logger:level().

compute_level_from_config_and_output(Config, Output) ->
    case Output of
        #{level := Level} ->
            Level;
        _ ->
            case Config of
                #{level := Level} -> Level;
                _                 -> ?DEFAULT_LOG_LEVEL
            end
    end.

-spec filter_cat_in_global_handlers(
        #{handler_key() => logger:handler_config()}, category_name(),
        per_cat_log_config()) ->
    #{handler_key() => logger:handler_config()}.

filter_cat_in_global_handlers(Handlers, CatName, CatConfig) ->
    maps:map(
      fun(_, Handler) ->
              add_cat_filter(Handler, CatName, CatConfig)
      end, Handlers).

-spec filter_out_cat_in_other_handlers(
        #{handler_key() => logger:handler_config()}, category_name()) ->
    #{handler_key() => logger:handler_config()}.

filter_out_cat_in_other_handlers(Handlers, CatName) ->
    maps:map(
      fun(_, #{filters := Filters} = Handler) ->
              {_, FilterConfig} = proplists:get_value(?FILTER_NAME, Filters),
              case maps:is_key(CatName, FilterConfig) of
                  true  -> Handler;
                  false -> add_cat_filter(Handler, CatName, #{level => none,
                                                              outputs => []})
              end
      end, Handlers).

-spec add_cat_filter(
        logger:handler_config(), category_name(),
        per_cat_log_config() | logger:handler_config()) ->
    logger:handler_config().

add_cat_filter(Handler, CatName, CatConfigOrOutput) ->
    Level = case CatConfigOrOutput of
                #{level := L} -> L;
                _             -> maps:get(level, Handler)
            end,
    do_add_cat_filter(Handler, CatName, Level).

do_add_cat_filter(#{filters := Filters} = Handler, CatName, Level) ->
    {Fun, FilterConfig} = proplists:get_value(?FILTER_NAME, Filters),
    FilterConfig1 = FilterConfig#{CatName => Level},
    Filters1 = lists:keystore(?FILTER_NAME, 1, Filters,
                              {?FILTER_NAME, {Fun, FilterConfig1}}),
    Handler#{filters => Filters1}.

-spec filter_log_event(logger:log_even(), term()) -> logger:filter_return().

filter_log_event(LogEvent, FilterConfig) ->
    rabbit_prelaunch_early_logging:filter_log_event(LogEvent, FilterConfig).

-spec adjust_log_levels(#{handler_key() => logger:handler_config()}) ->
    #{handler_key() => logger:handler_config()}.

adjust_log_levels(Handlers) ->
    maps:map(
      fun(_, #{level := GeneralLevel, filters := Filters} = Handler) ->
              {_, FilterConfig} = proplists:get_value(?FILTER_NAME, Filters),
              Level = maps:fold(
                        fun(_, LvlA, LvlB) ->
                                get_less_severe_level(LvlA, LvlB)
                        end, GeneralLevel, FilterConfig),
              Handler#{level => Level}
      end, Handlers).

-spec assign_handler_ids(#{handler_key() => logger:handler_config()}) ->
    [logger:handler_config()].

assign_handler_ids(Handlers) ->
    Handlers1 = [maps:get(Key, Handlers)
                 || Key <- lists:sort(maps:keys(Handlers))],
    assign_handler_ids(Handlers1,
                       #{config_run_number => get_config_run_number(),
                         next_file => 1},
                       []).

-spec assign_handler_ids(
        [logger:handler_config()], id_assignment_state(),
        [logger:handler_config()]) ->
    [logger:handler_config()].

assign_handler_ids(
  [#{module := Mod, config := #{type := file}} = Handler | Rest],
  #{next_file := NextFile} = State,
  Result)
  when ?IS_STD_H_COMPAT(Mod) ->
    Id = format_id("file_~b", [NextFile], State),
    Handler1 = Handler#{id => Id},
    assign_handler_ids(
      Rest, State#{next_file => NextFile + 1}, [Handler1 | Result]);
assign_handler_ids(
  [#{module := Mod, config := #{type := standard_io}} = Handler | Rest],
  State,
  Result)
  when ?IS_STD_H_COMPAT(Mod) ->
    Id = format_id("stdout", [], State),
    Handler1 = Handler#{id => Id},
    assign_handler_ids(Rest, State, [Handler1 | Result]);
assign_handler_ids(
  [#{module := Mod, config := #{type := standard_error}} = Handler | Rest],
  State,
  Result)
  when ?IS_STD_H_COMPAT(Mod) ->
    Id = format_id("stderr", [], State),
    Handler1 = Handler#{id => Id},
    assign_handler_ids(Rest, State, [Handler1 | Result]);
assign_handler_ids(
  [#{module := syslog_logger_h} = Handler
   | Rest],
  State,
  Result) ->
    Id = format_id("syslog", [], State),
    Handler1 = Handler#{id => Id},
    assign_handler_ids(Rest, State, [Handler1 | Result]);
assign_handler_ids(
  [#{module := rabbit_logger_exchange_h} = Handler
   | Rest],
  State,
  Result) ->
    Id = format_id("exchange", [], State),
    Handler1 = Handler#{id => Id},
    assign_handler_ids(Rest, State, [Handler1 | Result]);
assign_handler_ids([], _, Result) ->
    lists:reverse(Result).

-spec format_id(io:format(), [term()], id_assignment_state()) ->
    logger:handler_id().

format_id(Format, Args, #{config_run_number := RunNum}) ->
    list_to_atom(rabbit_misc:format("rmq_~b_" ++ Format, [RunNum | Args])).

-spec install_handlers([logger:handler_config()]) -> ok | no_return().

install_handlers([]) ->
    throw(no_logger_handler_configured);
install_handlers(Handlers) ->
    ok = do_install_handlers(Handlers),
    ok = remove_old_handlers(),
    ok = define_primary_level(Handlers),
    ok.

-spec do_install_handlers([logger:handler_config()]) -> ok | no_return().

do_install_handlers([#{id := Id, module := Module} = Handler | Rest]) ->
    case logger:add_handler(Id, Module, Handler) of
        ok ->
            do_install_handlers(Rest);
        {error, {handler_not_added, {open_failed, Filename, Reason}}} ->
            throw({error, {cannot_log_to_file, Filename, Reason}});
        {error, {handler_not_added, Reason}} ->
            throw({error, {cannot_log_to_file, unknown, Reason}})
    end;
do_install_handlers([]) ->
    ok.

-spec remove_old_handlers() -> ok.

remove_old_handlers() ->
    _ = logger:remove_handler(default),
    RunNum = get_config_run_number(),
    lists:foreach(
      fun(Id) ->
              Ret = re:run(atom_to_list(Id), "^rmq_([0-9]+)_",
                           [{capture, all_but_first, list}]),
              case Ret of
                  {match, [NumStr]} ->
                      Num = erlang:list_to_integer(NumStr),
                      if
                          Num < RunNum ->
                              ?LOG_DEBUG(
                                "Removing old logger handler ~s",
                                [Id],
                                #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
                              ok = logger:remove_handler(Id);
                          true ->
                              ok
                      end;
                  _ ->
                      ok
              end
      end, lists:sort(logger:get_handler_ids())),
    ok.

-spec define_primary_level([logger:handler_config()]) ->
    ok | {error, term()}.

define_primary_level(Handlers) ->
    define_primary_level(Handlers, emergency).

-spec define_primary_level([logger:handler_config()], logger:level()) ->
    ok | {error, term()}.

define_primary_level([#{level := Level} | Rest], PrimaryLevel) ->
    NewLevel = get_less_severe_level(Level, PrimaryLevel),
    define_primary_level(Rest, NewLevel);
define_primary_level([], PrimaryLevel) ->
    logger:set_primary_config(level, PrimaryLevel).

-spec get_less_severe_level(logger:level(), logger:level()) -> logger:level().
%% @doc
%% Compares two log levels and returns the less severe one.
%%
%% @param LevelA the log level to compare to LevelB.
%% @param LevelB the log level to compare to LevelA.
%%
%% @returns the less severe log level.

get_less_severe_level(LevelA, LevelB) ->
    case logger:compare_levels(LevelA, LevelB) of
        lt -> LevelA;
        _  -> LevelB
    end.

-spec maybe_log_test_messages(log_config()) -> ok.

maybe_log_test_messages(
  #{per_category := #{prelaunch := #{level := debug}}}) ->
    log_test_messages();
maybe_log_test_messages(
  #{global := #{level := debug}}) ->
    log_test_messages();
maybe_log_test_messages(_) ->
    ok.

-spec log_test_messages() -> ok.

log_test_messages() ->
    ?LOG_DEBUG("Testing debug log level",
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ?LOG_INFO("Testing info log level",
              #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ?LOG_NOTICE("Testing notice log level",
                #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ?LOG_WARNING("Testing warning log level",
                 #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ?LOG_ERROR("Testing error log level",
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ?LOG_CRITICAL("Testing critical log level",
                  #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ?LOG_ALERT("Testing alert log level",
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ?LOG_EMERGENCY("Testing emergency log level",
                   #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}).
