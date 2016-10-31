// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNet.SignalR;
using Microsoft.AspNet.SignalR.Messaging;
using Microsoft.AspNet.SignalR.Redis;
using Microsoft.AspNet.SignalR.Tracing;

namespace SignalR.Redis
{
    /// <summary>
    /// Uses Redis pub-sub to scale-out SignalR applications in web farms.
    /// </summary>
    public class RedisMessageBus : ScaleoutMessageBus
    {
        private readonly int _db;
        private readonly string _key;
        private readonly TraceSource _trace;

        private readonly IRedisConnection _connection;
        private readonly string _connectionString;
        private int _state;
        private readonly object _callbackLock = new object();

        public RedisMessageBus(IDependencyResolver resolver, RedisScaleoutConfiguration configuration, IRedisConnection connection)
            : this(resolver, configuration, connection, true)
        {
        }
        
        internal RedisMessageBus(IDependencyResolver resolver, RedisScaleoutConfiguration configuration, IRedisConnection connection, bool connectAutomatically)
            : base(resolver, configuration)
        {
            if (configuration == null)
            {
                throw new ArgumentNullException(nameof(configuration));
            }

            _connection = connection;

            _connectionString = configuration.ConnectionString;
            _db = configuration.Database;
            _key = configuration.EventKey;

            var traceManager = resolver.Resolve<ITraceManager>();

            _trace = traceManager["SignalR." + typeof(RedisMessageBus).Name];

            ReconnectDelay = TimeSpan.FromSeconds(2);

            if (connectAutomatically)
            {
                ThreadPool.QueueUserWorkItem(_ =>
                {
                    var ignore = ConnectWithRetry();
                });
            }
        }

        public TimeSpan ReconnectDelay { get; set; }
        
        public virtual void OpenStream(int streamIndex)
        {
            Open(streamIndex);
        }

        protected override Task Send(int streamIndex, IList<Message> messages)
        {
            return _connection.ScriptEvaluateAsync(
                _db,
                @"local newId = redis.call('INCR', KEYS[1])
                  local payload = newId .. ' ' .. ARGV[1]
                  redis.call('PUBLISH', KEYS[1], payload)
                  return {newId, ARGV[1], payload}",
                _key,
                RedisMessage.ToBytes(messages));
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                var oldState = Interlocked.Exchange(ref _state, State.Disposing);

                switch (oldState)
                {
                    case State.Connected:
                        Shutdown();
                        break;
                    case State.Closed:
                    case State.Disposing:
                        // No-op
                        break;
                    case State.Disposed:
                        Interlocked.Exchange(ref _state, State.Disposed);
                        break;
                    default:
                        break;
                }
            }

            base.Dispose(disposing);
        }

        private void Shutdown()
        {
            _trace.TraceInformation("Shutdown()");

            if (_connection != null)
            {
                _connection.Close(_key, allowCommandsToComplete: false);
            }

            Interlocked.Exchange(ref _state, State.Disposed);
        }

        private void OnConnectionFailed(Exception ex)
        {
            string errorMessage = (ex != null) ? ex.Message : Resources.Error_RedisConnectionClosed;

            _trace.TraceInformation("OnConnectionFailed - " + errorMessage);

            Interlocked.Exchange(ref _state, State.Closed);
        }

        private void OnConnectionError(Exception ex)
        {
            OnError(0, ex);
            _trace.TraceError("OnConnectionError - " + ex.Message);
        }

        private async void OnConnectionRestored(Exception ex)
        {
            await _connection.RestoreLatestValueForKey(_db, _key);

            _trace.TraceInformation("Connection restored");

            Interlocked.Exchange(ref _state, State.Connected);

            OpenStream(0);
        }

        internal async Task ConnectWithRetry()
        {
            while (true)
            {
                try
                {
                    await ConnectToRedisAsync();

                    var oldState = Interlocked.CompareExchange(ref _state,
                                               State.Connected,
                                               State.Closed);

                    if (oldState == State.Closed)
                    {
                        OpenStream(0);
                    }
                    else
                    {
                        Debug.Assert(oldState == State.Disposing, "unexpected state");

                        Shutdown();
                    }

                    break;
                }

                catch (Exception ex)
                {
                    _trace.TraceError("Error connecting to Redis - " + ex.GetBaseException());
                }

                if (_state == State.Disposing)
                {
                    Shutdown();
                    break;
                }

                await Task.Delay(ReconnectDelay);
            }
        }

        private async Task ConnectToRedisAsync()
        {
            if (_connection != null)
            {
                _connection.ConnectionFailed -= OnConnectionFailed;
                _connection.ErrorMessage -= OnConnectionError;
                _connection.ConnectionRestored -= OnConnectionRestored;
            }

            _trace.TraceInformation("Connecting...");

            await _connection.ConnectAsync(_connectionString, _trace);

            _trace.TraceInformation("Connection opened");

            _connection.ConnectionFailed += OnConnectionFailed;
            _connection.ErrorMessage += OnConnectionError;
            _connection.ConnectionRestored += OnConnectionRestored;

            await _connection.SubscribeAsync(_key, OnMessage);

            _trace.TraceVerbose("Subscribed to event " + _key);
        }

        private void OnMessage(int streamIndex, RedisMessage message)
        {
            // locked to avoid overlapping calls (even though we have set the mode 
            // to preserve order on the subscription)
            lock (_callbackLock)
            {
                OnReceived(streamIndex, message.Id, message.ScaleoutMessage);
            }
        }
        
        internal static class State
        {
            public const int Closed = 0;
            public const int Connected = 1;
            public const int Disposing = 2;
            public const int Disposed = 3;
        }
    }
}
