%%--------------------------------------------------------------------
%% 插件功能核心文件
%%--------------------------------------------------------------------

% 指定当前源文件定义的模块名。每个Erlang源文件都应该有一个唯一的模块名，以便可以在其他源文件中引用它.
-module(emqx_plugin_kafka).

-include("emqx.hrl").                             %% EMQ X 的依赖,生命周期监听
-include_lib("brod/include/brod.hrl").            %% kafka 依赖库
-include_lib("kernel/include/logger.hrl").        %% 日志库


-export([load/1, unload/0]).

%% 客户端 生命周期 钩子
-export([on_client_connect/3
  , on_client_connack/4
  , on_client_connected/3
  , on_client_disconnected/4
  , on_client_authenticate/3
  , on_client_check_acl/5
  , on_client_subscribe/4
  , on_client_unsubscribe/4
]).

%% 会话 生命周期 钩子
-export([on_session_created/3
  , on_session_subscribed/4
  , on_session_unsubscribed/4
  , on_session_resumed/3
  , on_session_discarded/3
  , on_session_takeovered/3
  , on_session_terminated/4
]).

%% 消息订阅 钩子
-export([on_message_publish/2
  , on_message_delivered/3
  , on_message_acked/3
  , on_message_dropped/4
]).

%% 所有可用钩子
load(Env) ->
  kafka_init([Env]),
  %% 客户端生命周期钩子
  emqx:hook('client.connect', {?MODULE, on_client_connect, [Env]}),                  % client.connect = 处理连接报文 -> 服务端收到客户端的连接报文时
  emqx:hook('client.connack', {?MODULE, on_client_connack, [Env]}),                  % client.connack = 下发连接应答 -> 服务端准备下发连接应答报文时
  emqx:hook('client.connected', {?MODULE, on_client_connected, [Env]}),                % client.connected = 客户端成功接入 -> 客户端认证完成并成功接入系统后
  emqx:hook('client.disconnected', {?MODULE, on_client_disconnected, [Env]}),             % client.disconnected = 客户端连接断开 -> 客户端连接层在准备关闭时
  emqx:hook('client.authenticate', {?MODULE, on_client_authenticate, [Env]}),             % client.authenticate = 连接认证 -> 执行完 client.connect 后
  emqx:hook('client.check_acl', {?MODULE, on_client_check_acl, [Env]}),                % client.check_acl = ACL 校验 -> 执行 发布/订阅 操作前
  emqx:hook('client.subscribe', {?MODULE, on_client_subscribe, [Env]}),                % client.subscribe = 客户端订阅主题 -> 收到订阅报文后，执行 client.check_acl 鉴权前
  emqx:hook('client.unsubscribe', {?MODULE, on_client_unsubscribe, [Env]}),              % client.unsubscribe = 客户端取消订阅主题 -> 收到取消订阅报文后

  %% 会话 生命周期钩子
  emqx:hook('session.created', {?MODULE, on_session_created, [Env]}),                 % session.created = 会话创建 -> client.connected 执行完成，且创建新的会话后
  emqx:hook('session.subscribed', {?MODULE, on_session_subscribed, [Env]}),              % session.subscribed = 会话订阅主题 -> 完成订阅操作后
  emqx:hook('session.unsubscribed', {?MODULE, on_session_unsubscribed, [Env]}),            % session.unsubscribed = 会话取消订阅主题 -> 完成取消订阅操作后
  emqx:hook('session.resumed', {?MODULE, on_session_resumed, [Env]}),                 % session.resumed = 会话恢复 -> client.connected 执行完成，且成功恢复旧的会话信息后
  emqx:hook('session.discarded', {?MODULE, on_session_discarded, [Env]}),               % session.discarded = 会话被移除 -> 会话由于被移除而终止后
  emqx:hook('session.takeovered', {?MODULE, on_session_takeovered, [Env]}),              % session.takeovered = 会话被接管	-> 会话由于被接管而终止后
  emqx:hook('session.terminated', {?MODULE, on_session_terminated, [Env]}),              % session.terminated = 会话终止 -> 会话由于其他原因被终止后

  %% 会话 生命周期钩子
  emqx:hook('message.publish', {?MODULE, on_message_publish, [Env]}),                 % message.publish = MQTT 消息发布 -> 服务端在发布（路由）消息前
  emqx:hook('message.delivered', {?MODULE, on_message_delivered, [Env]}),               % message.delivered = 消息投递 -> 消息准备投递到客户端前
  emqx:hook('message.acked', {?MODULE, on_message_acked, [Env]}),                   % message.acked = 消息回执 -> 服务端在收到客户端发回的消息 ACK 后
  emqx:hook('message.dropped', {?MODULE, on_message_dropped, [Env]}).                 % message.dropped = MQTT消息丢弃 -> 发布出的消息被丢弃后

%% --------------------------------------------------------------------
%% 客户端生命周期钩子
%% --------------------------------------------------------------------
% 处理连接报文 -> 服务端收到客户端的连接报文时
on_client_connect(ConnInfo = #{clientid := ClientId}, Props, _Env) ->
  io:format("Client(~s) connect, ConnInfo: ~p, Props: ~p~n", [ClientId, ConnInfo, Props]),
  {ok, Props}.

% 下发连接应答 -> 服务端准备下发连接应答报文时
on_client_connack(ConnInfo = #{clientid := ClientId}, Rc, Props, _Env) ->
  io:format("Client(~s) connack, ConnInfo: ~p, Rc: ~p, Props: ~p~n", [ClientId, ConnInfo, Rc, Props]),
  {ok, Props}.

% 客户端成功接入 -> 客户端认证完成并成功接入系统后
on_client_connected(ClientInfo = #{clientid := ClientId}, ConnInfo, _Env) ->
  io:format("Client(~s) connected, ClientInfo:~n~p~n, ConnInfo:~n~p~n", [ClientId, ClientInfo, ConnInfo]).

% 客户端连接断开 -> 客户端连接层在准备关闭时
on_client_disconnected(ClientInfo = #{clientid := ClientId}, ReasonCode, ConnInfo, _Env) ->
  io:format("Client(~s) disconnected due to ~p, ClientInfo:~n~p~n, ConnInfo:~n~p~n", [ClientId, ReasonCode, ClientInfo, ConnInfo]).

% 连接认证 -> 执行完 client.connect 后
on_client_authenticate(_ClientInfo = #{clientid := ClientId}, Result, _Env) ->
  io:format("Client(~s) authenticate, Result:~n~p~n", [ClientId, Result]),
  {ok, Result}.

% ACL 校验 -> 执行 发布/订阅 操作前
on_client_check_acl(_ClientInfo = #{clientid := ClientId}, Topic, PubSub, Result, _Env) ->
  io:format("Client(~s) check_acl, PubSub:~p, Topic:~p, Result:~p~n", [ClientId, PubSub, Topic, Result]),
  {ok, Result}.

% 客户端订阅主题 -> 收到订阅报文后，执行 client.check_acl 鉴权前
on_client_subscribe(#{clientid := ClientId}, _Properties, TopicFilters, _Env) ->
  io:format("Client(~s) will subscribe: ~p~n", [ClientId, TopicFilters]),
  {ok, TopicFilters}.

% 客户端取消订阅主题 -> 收到取消订阅报文后
on_client_unsubscribe(#{clientid := ClientId}, _Properties, TopicFilters, _Env) ->
  io:format("Client(~s) will unsubscribe ~p~n", [ClientId, TopicFilters]),
  {ok, TopicFilters}.

%% --------------------------------------------------------------------
%% 会话 生命周期钩子
%% --------------------------------------------------------------------
% 会话创建 -> client.connected 执行完成，且创建新的会话后
on_session_created(#{clientid := ClientId}, SessInfo, _Env) ->
  io:format("Session(~s) created, Session Info:~n~p~n", [ClientId, SessInfo]).

% 会话订阅主题 -> 完成订阅操作后
on_session_subscribed(#{clientid := ClientId}, Topic, SubOpts, _Env) ->
  io:format("Session(~s) subscribed ~s with subopts: ~p~n", [ClientId, Topic, SubOpts]).

% 会话取消订阅主题 -> 完成取消订阅操作后
on_session_unsubscribed(#{clientid := ClientId}, Topic, Opts, _Env) ->
  io:format("Session(~s) unsubscribed ~s with opts: ~p~n", [ClientId, Topic, Opts]).

% 会话恢复 -> client.connected 执行完成，且成功恢复旧的会话信息后
on_session_resumed(#{clientid := ClientId}, SessInfo, _Env) ->
  io:format("Session(~s) resumed, Session Info:~n~p~n", [ClientId, SessInfo]).

% 会话被移除 -> 会话由于被移除而终止后
on_session_discarded(_ClientInfo = #{clientid := ClientId}, SessInfo, _Env) ->
  io:format("Session(~s) is discarded. Session Info: ~p~n", [ClientId, SessInfo]).

% 会话被接管	-> 会话由于被接管而终止后
on_session_takeovered(_ClientInfo = #{clientid := ClientId}, SessInfo, _Env) ->
  io:format("Session(~s) is takeovered. Session Info: ~p~n", [ClientId, SessInfo]).

% 会话终止 -> 会话由于其他原因被终止后
on_session_terminated(_ClientInfo = #{clientid := ClientId}, Reason, SessInfo, _Env) ->
  io:format("Session(~s) is terminated due to ~p~nSession Info: ~p~n", [ClientId, Reason, SessInfo]).

%% --------------------------------------------------------------------
%% 消息订阅钩子
%% --------------------------------------------------------------------
%% MQTT 消息发布 -> 服务端在发布（路由）消息前
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
  {ok, Message};

on_message_publish(Message, _Env) ->
  ClientId = Message#message.from,                      % mqtt 客户端ID
  Payload = Message#message.payload,                    % 有效载荷
  Topic = list_to_binary([get_config_publish_topic()]), % kafka topic
  produce_kafka_message(Topic, Payload, ClientId),
  {ok, Message}
.

% 消息投递 -> 消息准备投递到客户端前
on_message_delivered(_ClientInfo = #{clientid := ClientId}, Message, _Env) ->
  io:format("Message delivered to client(~s): ~s~n", [ClientId, emqx_message:format(Message)]),
  {ok, Message}.

% 消息回执 -> 服务端在收到客户端发回的消息 ACK 后
on_message_acked(_ClientInfo = #{clientid := ClientId}, Message, _Env) ->
  io:format("Message acked by client(~s): ~s~n", [ClientId, emqx_message:format(Message)]).

% MQTT消息丢弃 -> 发布出的消息被丢弃后
on_message_dropped(#message{topic = <<"$SYS/", _/binary>>}, _By, _Reason, _Env) ->
  ok;
on_message_dropped(Message, _By = #{node := Node}, Reason, _Env) ->
  io:format("Message dropped by node ~s due to ~s: ~s~n", [Node, Reason, emqx_message:format(Message)]).


%% 当插件应用程序停止时调用
unload() ->
  emqx:unhook('client.connect', {?MODULE, on_client_connect}),
  emqx:unhook('client.connack', {?MODULE, on_client_connack}),
  emqx:unhook('client.connected', {?MODULE, on_client_connected}),
  emqx:unhook('client.disconnected', {?MODULE, on_client_disconnected}),
  emqx:unhook('client.authenticate', {?MODULE, on_client_authenticate}),
  emqx:unhook('client.check_acl', {?MODULE, on_client_check_acl}),
  emqx:unhook('client.subscribe', {?MODULE, on_client_subscribe}),
  emqx:unhook('client.unsubscribe', {?MODULE, on_client_unsubscribe}),
  emqx:unhook('session.created', {?MODULE, on_session_created}),
  emqx:unhook('session.subscribed', {?MODULE, on_session_subscribed}),
  emqx:unhook('session.unsubscribed', {?MODULE, on_session_unsubscribed}),
  emqx:unhook('session.resumed', {?MODULE, on_session_resumed}),
  emqx:unhook('session.discarded', {?MODULE, on_session_discarded}),
  emqx:unhook('session.takeovered', {?MODULE, on_session_takeovered}),
  emqx:unhook('session.terminated', {?MODULE, on_session_terminated}),
  emqx:unhook('message.publish', {?MODULE, on_message_publish}),
  emqx:unhook('message.delivered', {?MODULE, on_message_delivered}),
  emqx:unhook('message.acked', {?MODULE, on_message_acked}),
  emqx:unhook('message.dropped', {?MODULE, on_message_dropped}).



%% --------------------------------------------------------------------
%% 获取配置文件方法 -> 获取配置文件方法 >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
%% --------------------------------------------------------------------

% 获取集群节点 node_point_list
get_config_node_point_list() ->
  {ok, AppConfig} = application:get_env(?MODULE, config),
  NodePointList = proplists:get_value(nodePointList, AppConfig),
  Fun = fun(S) ->
            case string:tokens(S, ":") of
              [Domain] -> {Domain, 9092};
              [Domain, Port] -> {Domain, list_to_integer(Port)}
            end
        end,
  KafkaBootstrapEndpoints = [Fun(E) || E <- string:tokens(NodePointList, ",")],         % 集群节点
  KafkaBootstrapEndpoints.

% 获取producer_write_async -> kafka 生产者 写数据方式,默认true (true = async , false = sync)
% @return boolean 类型
get_config_producer_write_async() ->
  {ok, AppConfig} = application:get_env(?MODULE, config),
  ProducerWriteAsync = proplists:get_value(producerWriteAsync, AppConfig),         % 写数据是否异步
  ProducerWriteAsync.

% 获取 reconnect_cool_down_seconds 重连间隔时间,秒数
% @return integer 类型
get_config_reconnect_cool_down_seconds() ->
  {ok, AppConfig} = application:get_env(?MODULE, config),
  ReconnectCoolDownSeconds = proplists:get_value(reconnectCoolDownSeconds, AppConfig),  % 重连间隔时间,秒数
  ReconnectCoolDownSeconds.

% 获取 MQTT 消息推送 的 kafka主题 publish_topic
% @return string 类型
get_config_publish_topic() ->
  {ok, AppConfig} = application:get_env(?MODULE, config),
  PublishTopic = proplists:get_value(publishTopic, AppConfig),  % kafka topic 前缀匹配
  PublishTopic.

%% --------------------------------------------------------------------
%% 获取配置文件方法 <- 获取配置文件方法 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
%% --------------------------------------------------------------------

% 客户端连接
kafka_init(_Env) ->
  io:format("Start to init emqx plugin kafka..... ~n"),
  % 1.加载插件
  {ok, _} = application:ensure_all_started(gproc),
  {ok, _} = application:ensure_all_started(brod),

  % 2.获取环境变量
  KafkaBootstrapEndpoints = get_config_node_point_list(),   % 集群节点
  ClientConfig = [{auto_start_producers, true},
  {default_producer_config, []},
  {reconnect_cool_down_seconds, get_config_reconnect_cool_down_seconds()}],

  % 3.启动客户端
  ok = brod:start_client(KafkaBootstrapEndpoints, brod_client_1, ClientConfig),
  io:format("load brod with ~p~n", [KafkaBootstrapEndpoints]).



% 往kafka 里面发送消息
% @param Topic binary 类型
% @param Msg binary 类型
% @param ClientId binary 类型
produce_kafka_message(Topic, Msg, ClientId) ->
  % 1.加载数据
  Partition = get_random_partition(Topic),                                % 获取随机分区消息即将发送的分区
  ProducerWriteAsync = get_config_producer_write_async(),                 % 写数据是否异步

  % 2.发送消息
  if
    ProducerWriteAsync ->
      % brod:produce/3 - 此方法将生产者发出的每条消息视为重要，并希望Kafka服务器确认它们已经接收到。如果发送失败，则该方法会抛出异常。
      % brod:produce_no_ack/3 - 此方法相对较快，但是不会等待 Kafka 服务器的确认或处理错误。这些消息可能会丢失而永远不会传递给 Kafka 服务器。
      brod:produce_no_ack(brod_client_1, Topic, Partition, ClientId, Msg);
    true ->
      % brod:produce_sync/3 提供的同步消息生产者方法，它将等待 Kafka 服务器对每条消息进行确认，并返回一个包含成功发送消息数量的整数。
      % 如果发生错误，则该方法将返回 {error, Reason} 元组。
      % 此方法会阻塞调用线程，直到 Kafka 服务器对所有消息进行确认。由于此方法具有阻塞性质，不适用于在高并发环境中使用
      brod:produce_sync(brod_client_1, Topic, Partition, ClientId, Msg)
  end,
  ok.

% 根据 kafka topic 将要发送的消息随机指定一个分区
% @param KafkaTopic binary 类型
% @return integer 类型
get_random_partition(KafkaTopic) ->
  {ok, PartitionsCount} = brod:get_partitions_count(brod_client_1, KafkaTopic),
  if PartitionsCount > 1 ->
    % 在Kafka中，分区编号是从0开始计数的.
    % 如果主题有5个分区，则它们的编号应该是0、1、2、3和4。因此，brod:get_partitions_count/2函数将返回值5
    % 所以这里是 PartitionsCount - 1
    crypto:rand_uniform(0, PartitionsCount - 1);
    true -> 0
  end.
