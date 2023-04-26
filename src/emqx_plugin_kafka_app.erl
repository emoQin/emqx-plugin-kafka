%%--------------------------------------------------------------------
%% ctl 扩展功能
%%--------------------------------------------------------------------

-module(emqx_plugin_kafka_app).

-behaviour(application).

-emqx_plugin(?MODULE).

%% 应用程序回调
-export([ start/2, stop/1]).

%% 启动监听
start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_plugin_kafka_sup:start_link(),
    emqx_plugin_kafka:load(application:get_all_env()),
    io:format("emq_plugin_kafka start.~n", []),
    {ok, Sup}.

%% 停止监听
stop(_State) ->
    emqx_plugin_kafka:unload(),
  io:format("emq_plugin_kafka stop.~n", []).