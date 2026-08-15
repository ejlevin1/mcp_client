import 'dart:async';

import 'package:fake_async/fake_async.dart';
import 'package:mcp_client/mcp_client.dart';
import 'package:test/test.dart';

import 'mock_transport.dart';

/// Dual-clock (`idleTimeout` + `maxTimeout`) coverage for [Client.callTool].
///
/// These tests run inside [fakeAsync] rather than against real short durations
/// (the style of `timeout_cancellation_test.dart`) for two reasons the
/// single-clock tests did not have:
///  * "fires EXACTLY at the max deadline, no matter how much progress arrived"
///    is a statement about wall-clock arithmetic that a real-time test can only
///    approximate, and flakily;
///  * "no timer leaked on this exit path" is only checkable via
///    [FakeAsync.pendingTimers].

/// Connect [client] to [transport] with a canned `initialize` result so the
/// client is ready to issue requests, without leaving the fake zone.
Client _connectedClient(MockTransport transport, FakeAsync async) {
  final client = Client(name: 'Test', version: '1.0.0');
  transport.queueResponse({
    'jsonrpc': McpProtocol.jsonRpcVersion,
    'id': 1,
    'result': {
      'protocolVersion': McpProtocol.defaultVersion,
      'serverInfo': {'name': 'Mock Server', 'version': '1.0.0'},
      'capabilities': {
        'tools': {'listChanged': true},
      },
    },
  });
  unawaited(client.connect(transport));
  async.flushMicrotasks();
  transport.sentMessages.clear();
  return client;
}

/// Deliver a spec `notifications/progress` for [progressToken] and let the
/// client's message pump drain.
void _sendProgress(
  MockTransport transport,
  FakeAsync async,
  Object progressToken,
  num progress,
) {
  transport.sendMockNotification({
    'jsonrpc': McpProtocol.jsonRpcVersion,
    'method': McpProtocol.methodProgress,
    'params': {
      'progressToken': progressToken,
      'progress': progress,
    },
  });
  async.flushMicrotasks();
}

Map<String, dynamic> _sentToolCall(MockTransport transport) =>
    transport.sentMessages.firstWhere((m) => m['method'] == 'tools/call');

Object? _sentProgressToken(MockTransport transport) {
  final params = _sentToolCall(transport)['params'] as Map;
  return (params['_meta'] as Map)['progressToken'];
}

List<Map<String, dynamic>> _cancelNotifications(MockTransport transport) =>
    transport.sentMessages
        .where((m) => m['method'] == 'notifications/cancelled')
        .toList();

/// A successful `tools/call` result.
Map<String, dynamic> _toolResult() => {
      'jsonrpc': McpProtocol.jsonRpcVersion,
      'result': {
        'content': [
          {'type': 'text', 'text': 'ok'},
        ],
      },
    };

void main() {
  group('idle clock', () {
    test('a matching progressToken re-arms it', () {
      fakeAsync((async) {
        final transport = MockTransport();
        final client = _connectedClient(transport, async);

        Object? error;
        final call = client.callTool(
          'slow',
          const {},
          idleTimeout: const Duration(milliseconds: 100),
          maxTimeout: const Duration(hours: 1),
          progressToken: 'tok',
        );
        unawaited(call.then((_) {}, onError: (Object e) => error = e));
        async.flushMicrotasks();

        // Five 80ms quiet periods, each interrupted by progress. 400ms have
        // passed — four times the idle budget — yet the call is alive.
        for (var i = 0; i < 5; i++) {
          async.elapse(const Duration(milliseconds: 80));
          _sendProgress(transport, async, 'tok', i + 1);
          expect(error, isNull);
        }

        // Go quiet: the idle clock expires 100ms after the LAST progress.
        async.elapse(const Duration(milliseconds: 99));
        expect(error, isNull);
        async.elapse(const Duration(milliseconds: 1));

        expect(error, isA<McpError>());
        expect(error.toString(), contains('timed out (idle)'));

        client.disconnect();
        async.flushMicrotasks();
      });
    });

    test('an unrelated progressToken does not re-arm it', () {
      fakeAsync((async) {
        final transport = MockTransport();
        final client = _connectedClient(transport, async);

        Object? error;
        final call = client.callTool(
          'slow',
          const {},
          idleTimeout: const Duration(milliseconds: 100),
          maxTimeout: const Duration(hours: 1),
          progressToken: 'mine',
        );
        unawaited(call.then((_) {}, onError: (Object e) => error = e));
        async.flushMicrotasks();

        async.elapse(const Duration(milliseconds: 80));
        // Progress for a different in-flight call (or a stale one) must not
        // buy this request another 100ms.
        _sendProgress(transport, async, 'someone-elses-token', 1);

        async.elapse(const Duration(milliseconds: 19));
        expect(error, isNull);
        async.elapse(const Duration(milliseconds: 1));

        expect(error, isA<McpError>());
        expect(error.toString(), contains('timed out (idle)'));

        client.disconnect();
        async.flushMicrotasks();
      });
    });

    test('a stale token still reaches the global listener, not the per-call one',
        () {
      fakeAsync((async) {
        final transport = MockTransport();
        final client = _connectedClient(transport, async);

        final global = <McpProgress>[];
        final perCall = <McpProgress>[];
        client.onProgress(global.add);

        Object? error;
        final call = client.callTool(
          'slow',
          const {},
          idleTimeout: const Duration(seconds: 10),
          maxTimeout: const Duration(hours: 1),
          progressToken: 'mine',
          onProgress: perCall.add,
        );
        unawaited(call.then((_) {}, onError: (Object e) => error = e));
        async.flushMicrotasks();

        _sendProgress(transport, async, 'stale', 1);
        expect(perCall, isEmpty);
        expect(global, hasLength(1));

        _sendProgress(transport, async, 'mine', 2);
        expect(perCall, hasLength(1));
        expect(perCall.single.progress, 2);
        expect(global, hasLength(2));

        expect(error, isNull);
        client.disconnect();
        async.flushMicrotasks();
      });
    });
  });

  group('max clock', () {
    test('progress extends the idle clock but never the max clock', () {
      fakeAsync((async) {
        final transport = MockTransport();
        final client = _connectedClient(transport, async);

        Object? error;
        final call = client.callTool(
          'slow',
          const {},
          idleTimeout: const Duration(milliseconds: 100),
          maxTimeout: const Duration(milliseconds: 500),
          progressToken: 'tok',
        );
        unawaited(call.then((_) {}, onError: (Object e) => error = e));
        async.flushMicrotasks();

        final sentAt = async.elapsed;
        // Progress every 80ms would keep the 100ms idle clock alive forever.
        while (error == null &&
            async.elapsed - sentAt < const Duration(seconds: 2)) {
          async.elapse(const Duration(milliseconds: 20));
          if ((async.elapsed - sentAt).inMilliseconds % 80 == 0) {
            _sendProgress(transport, async, 'tok', 1);
          }
        }

        expect(error, isA<McpError>());
        expect(error.toString(), contains('timed out (max)'));
        // Measured from SEND time, not from the last progress.
        expect(async.elapsed - sentAt, const Duration(milliseconds: 500));

        client.disconnect();
        async.flushMicrotasks();
      });
    });

    test('maxTimeout wins over timeout when both are supplied', () {
      fakeAsync((async) {
        final transport = MockTransport();
        final client = _connectedClient(transport, async);

        Object? error;
        final call = client.callTool(
          'slow',
          const {},
          timeout: const Duration(hours: 1),
          maxTimeout: const Duration(milliseconds: 200),
        );
        unawaited(call.then((_) {}, onError: (Object e) => error = e));
        async.flushMicrotasks();

        async.elapse(const Duration(milliseconds: 199));
        expect(error, isNull);
        async.elapse(const Duration(milliseconds: 1));
        expect(error.toString(), contains('timed out (max)'));

        client.disconnect();
        async.flushMicrotasks();
      });
    });
  });

  group('notifications/cancelled on expiry', () {
    test('fires exactly once with the original int request id', () {
      fakeAsync((async) {
        final transport = MockTransport();
        final client = _connectedClient(transport, async);

        Object? error;
        final call = client.callTool(
          'slow',
          const {},
          idleTimeout: const Duration(milliseconds: 100),
          maxTimeout: const Duration(milliseconds: 120),
          progressToken: 'tok',
        );
        unawaited(call.then((_) {}, onError: (Object e) => error = e));
        async.flushMicrotasks();

        final requestId = _sentToolCall(transport)['id'];

        // Both clocks expire within 20ms of each other; the loser must find the
        // request already removed and stay silent.
        async.elapse(const Duration(seconds: 5));

        expect(error, isA<McpError>());
        final cancels = _cancelNotifications(transport);
        expect(cancels, hasLength(1));
        final params = cancels.single['params'] as Map;
        expect(params['requestId'], isA<int>());
        expect(params['requestId'], equals(requestId));
        expect(params['reason'], contains('idle'));

        client.disconnect();
        async.flushMicrotasks();
      });
    });

    test('never fires for a request that already completed', () {
      fakeAsync((async) {
        final transport = MockTransport();
        final client = _connectedClient(transport, async);
        transport.queueResponse(_toolResult());

        CallToolResult? result;
        final call = client.callTool(
          'fast',
          const {},
          idleTimeout: const Duration(milliseconds: 50),
          maxTimeout: const Duration(milliseconds: 100),
        );
        unawaited(call.then((r) => result = r, onError: (Object _) {}));
        async.flushMicrotasks();

        expect(result, isNotNull);
        // Elapsing past BOTH deadlines must not resurrect either clock.
        async.elapse(const Duration(seconds: 5));
        expect(_cancelNotifications(transport), isEmpty);

        client.disconnect();
        async.flushMicrotasks();
      });
    });
  });

  group('timer cleanup', () {
    test('no timer is left pending after a successful response', () {
      fakeAsync((async) {
        final transport = MockTransport();
        final client = _connectedClient(transport, async);
        transport.queueResponse(_toolResult());

        CallToolResult? result;
        unawaited(client
            .callTool(
              'fast',
              const {},
              idleTimeout: const Duration(milliseconds: 50),
              maxTimeout: const Duration(minutes: 5),
            )
            .then((r) => result = r, onError: (Object _) {}));
        async.flushMicrotasks();

        expect(result, isNotNull);
        expect(async.pendingTimers, isEmpty);

        client.disconnect();
        async.flushMicrotasks();
      });
    });

    test('no timer is left pending after an error response', () {
      fakeAsync((async) {
        final transport = MockTransport();
        final client = _connectedClient(transport, async);
        transport.queueResponse({
          'jsonrpc': McpProtocol.jsonRpcVersion,
          'error': {'code': -32000, 'message': 'tool exploded'},
        });

        Object? error;
        unawaited(client
            .callTool(
              'boom',
              const {},
              idleTimeout: const Duration(milliseconds: 50),
              maxTimeout: const Duration(minutes: 5),
            )
            .then((_) {}, onError: (Object e) => error = e));
        async.flushMicrotasks();

        expect(error, isA<McpError>());
        expect(error.toString(), contains('tool exploded'));
        expect(async.pendingTimers, isEmpty);

        client.disconnect();
        async.flushMicrotasks();
      });
    });

    test('no timer is left pending when the transport rejects the send', () {
      fakeAsync((async) {
        final transport = MockTransport();
        final client = _connectedClient(transport, async);
        transport.onSend = (message) {
          if (message is Map && message['method'] == 'tools/call') {
            throw StateError('socket gone');
          }
        };

        Object? error;
        unawaited(client
            .callTool(
              'slow',
              const {},
              idleTimeout: const Duration(milliseconds: 50),
              maxTimeout: const Duration(minutes: 5),
            )
            .then((_) {}, onError: (Object e) => error = e));
        async.flushMicrotasks();

        expect(error, isA<McpError>());
        expect(error.toString(), contains('Failed to send request'));
        // The clocks are armed only after a successful send, so this path
        // cannot leak one.
        expect(async.pendingTimers, isEmpty);

        client.disconnect();
        async.flushMicrotasks();
      });
    });

    test('no timer is left pending after a disconnect', () {
      fakeAsync((async) {
        final transport = MockTransport();
        final client = _connectedClient(transport, async);

        Object? error;
        unawaited(client
            .callTool(
              'slow',
              const {},
              idleTimeout: const Duration(minutes: 1),
              maxTimeout: const Duration(hours: 1),
            )
            .then((_) {}, onError: (Object e) => error = e));
        async.flushMicrotasks();
        expect(async.pendingTimers, isNotEmpty);

        client.disconnect();
        async.flushMicrotasks();

        expect(error, isA<McpError>());
        expect(error.toString(), contains('Transport disconnected'));
        expect(async.pendingTimers, isEmpty);
      });
    });

    test('no timer is left pending after a cancellation', () {
      fakeAsync((async) {
        final transport = MockTransport();
        final client = _connectedClient(transport, async);

        final token = McpCancellationToken();
        Object? error;
        unawaited(client
            .callTool(
              'slow',
              const {},
              idleTimeout: const Duration(minutes: 1),
              maxTimeout: const Duration(hours: 1),
              cancellationToken: token,
            )
            .then((_) {}, onError: (Object e) => error = e));
        async.flushMicrotasks();

        token.cancel(reason: 'user aborted');
        async.flushMicrotasks();

        expect(error, isA<McpError>());
        expect(error.toString(), contains('cancelled'));
        expect(async.pendingTimers, isEmpty);

        client.disconnect();
        async.flushMicrotasks();
      });
    });
  });

  group('late response', () {
    test('a response arriving after the idle timeout is ignored', () {
      fakeAsync((async) {
        final transport = MockTransport();
        final client = _connectedClient(transport, async);

        Object? error;
        var settledTwice = false;
        final call = client.callTool(
          'slow',
          const {},
          idleTimeout: const Duration(milliseconds: 100),
          maxTimeout: const Duration(hours: 1),
        );
        unawaited(call.then(
          (_) => settledTwice = true,
          onError: (Object e) {
            if (error != null) settledTwice = true;
            error = e;
          },
        ));
        async.flushMicrotasks();

        final requestId = _sentToolCall(transport)['id'];
        async.elapse(const Duration(milliseconds: 100));
        expect(error.toString(), contains('timed out (idle)'));

        // The server finally answers. The id is no longer pending, so the
        // result is dropped rather than completing an already-failed future.
        final late = _toolResult()..['id'] = requestId;
        transport.simulateMessage(late);
        async.elapse(const Duration(seconds: 1));

        expect(settledTwice, isFalse);
        expect(error.toString(), contains('timed out (idle)'));
        expect(async.pendingTimers, isEmpty);

        client.disconnect();
        async.flushMicrotasks();
      });
    });
  });

  group('progress payload parsing', () {
    test('an integer progress with no total and no message does not throw', () {
      fakeAsync((async) {
        final transport = MockTransport();
        final client = _connectedClient(transport, async);

        final global = <McpProgress>[];
        final perCall = <McpProgress>[];
        client.onProgress(global.add);

        Object? error;
        final call = client.callTool(
          'slow',
          const {},
          idleTimeout: const Duration(seconds: 10),
          maxTimeout: const Duration(hours: 1),
          progressToken: 'tok',
          onProgress: perCall.add,
        );
        unawaited(call.then((_) {}, onError: (Object e) => error = e));
        async.flushMicrotasks();

        transport.sendMockNotification({
          'jsonrpc': McpProtocol.jsonRpcVersion,
          'method': McpProtocol.methodProgress,
          'params': {'progressToken': 'tok', 'progress': 3},
        });
        async.flushMicrotasks();

        expect(error, isNull);
        expect(perCall, hasLength(1));
        expect(perCall.single.progress, 3);
        expect(perCall.single.total, isNull);
        expect(perCall.single.message, isNull);
        expect(perCall.single.fraction, isNull);
        expect(global, hasLength(1));

        client.disconnect();
        async.flushMicrotasks();
      });
    });

    test('a numeric progressToken correlates without stringification', () {
      fakeAsync((async) {
        final transport = MockTransport();
        final client = _connectedClient(transport, async);

        Object? error;
        final call = client.callTool(
          'slow',
          const {},
          idleTimeout: const Duration(milliseconds: 100),
          maxTimeout: const Duration(hours: 1),
          progressToken: 42,
        );
        unawaited(call.then((_) {}, onError: (Object e) => error = e));
        async.flushMicrotasks();

        async.elapse(const Duration(milliseconds: 80));
        _sendProgress(transport, async, 42, 1);
        async.elapse(const Duration(milliseconds: 80));
        expect(error, isNull, reason: 'the numeric token should have matched');

        async.elapse(const Duration(milliseconds: 20));
        expect(error.toString(), contains('timed out (idle)'));

        client.disconnect();
        async.flushMicrotasks();
      });
    });

    test('a malformed payload is dropped instead of throwing', () {
      fakeAsync((async) {
        final transport = MockTransport();
        final client = _connectedClient(transport, async);

        final global = <McpProgress>[];
        client.onProgress(global.add);

        // No `progress` at all, and a non-string message.
        transport.sendMockNotification({
          'jsonrpc': McpProtocol.jsonRpcVersion,
          'method': McpProtocol.methodProgress,
          'params': {'progressToken': 'tok', 'message': 7},
        });
        async.flushMicrotasks();
        expect(global, isEmpty);

        // Wrongly-typed optional fields are ignored, not rejected.
        transport.sendMockNotification({
          'jsonrpc': McpProtocol.jsonRpcVersion,
          'method': McpProtocol.methodProgress,
          'params': {
            'progressToken': 'tok',
            'progress': 1,
            'total': 'lots',
            'message': 7,
          },
        });
        async.flushMicrotasks();
        expect(global, hasLength(1));
        expect(global.single.total, isNull);
        expect(global.single.message, isNull);

        client.disconnect();
        async.flushMicrotasks();
      });
    });
  });

  group('progressToken minting', () {
    test('callTool writes a 32-char hex _meta.progressToken by default', () {
      fakeAsync((async) {
        final transport = MockTransport();
        final client = _connectedClient(transport, async);
        transport.queueResponse(_toolResult());

        unawaited(client.callTool('t', const {}).then(
          (_) {},
          onError: (Object _) {},
        ));
        async.flushMicrotasks();

        final token = _sentProgressToken(transport);
        expect(token, isA<String>());
        expect(token as String, matches(RegExp(r'^[0-9a-f]{32}$')));

        client.disconnect();
        async.flushMicrotasks();
      });
    });

    test('an explicit progressToken is written verbatim', () {
      fakeAsync((async) {
        final transport = MockTransport();
        final client = _connectedClient(transport, async);
        transport.queueResponse(_toolResult());

        unawaited(client
            .callTool('t', const {}, progressToken: 'caller-supplied')
            .then((_) {}, onError: (Object _) {}));
        async.flushMicrotasks();

        expect(_sentProgressToken(transport), 'caller-supplied');

        client.disconnect();
        async.flushMicrotasks();
      });
    });
  });
}
