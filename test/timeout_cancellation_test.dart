import 'dart:async';

import 'package:mcp_client/mcp_client.dart';
import 'package:test/test.dart';

import 'mock_transport.dart';

/// Connect [client] to [transport] with a canned `initialize` result so the
/// client is ready to issue requests.
Future<void> _handshake(Client client, MockTransport transport) async {
  transport.queueResponse({
    'jsonrpc': McpProtocol.jsonRpcVersion,
    'id': 1,
    'result': {
      'protocolVersion': McpProtocol.defaultVersion,
      'serverInfo': {'name': 'Mock Server', 'version': '1.0.0'},
      'capabilities': {
        'tools': {'listChanged': true},
        'resources': {'listChanged': true, 'subscribe': true},
        'prompts': {'listChanged': true},
      },
    },
  });
  await client.connect(transport);
  transport.sentMessages.clear();
}

Map<String, dynamic>? _lastCancelNotification(MockTransport transport) {
  for (final message in transport.sentMessages.reversed) {
    if (message['method'] == 'notifications/cancelled') return message;
  }
  return null;
}

void main() {
  late Client client;
  late MockTransport transport;

  setUp(() {
    transport = MockTransport();
  });

  tearDown(() {
    client.disconnect();
  });

  group('per-call timeout', () {
    test('callTool honors an explicit short timeout with an McpError',
        () async {
      client = Client(name: 'Test', version: '1.0.0');
      await _handshake(client, transport);

      // No queued response -> the request hangs until the deadline.
      Object? caught;
      try {
        await client.callTool(
          'slow',
          const {},
          timeout: const Duration(milliseconds: 50),
        );
      } catch (e) {
        caught = e;
      }

      expect(caught, isA<McpError>());
      expect(caught.toString(), contains('timed out'));
      // Never a TimeoutException — callers rely on the single McpError type.
      expect(caught, isNot(isA<TimeoutException>()));
    });

    test('defaultRequestTimeout applies when no per-call timeout is given',
        () async {
      client = Client(
        name: 'Test',
        version: '1.0.0',
        defaultRequestTimeout: const Duration(milliseconds: 50),
      );
      await _handshake(client, transport);

      await expectLater(
        client.listTools(),
        throwsA(isA<McpError>()),
      );
    });

    test('timeout emits notifications/cancelled with the original int id',
        () async {
      client = Client(
        name: 'Test',
        version: '1.0.0',
        defaultRequestTimeout: const Duration(milliseconds: 50),
      );
      await _handshake(client, transport);

      await expectLater(client.listTools(), throwsA(isA<McpError>()));

      final request = transport.sentMessages
          .firstWhere((m) => m['method'] == 'tools/list');
      final cancel = _lastCancelNotification(transport);
      expect(cancel, isNotNull);
      final requestId = (cancel!['params'] as Map)['requestId'];
      expect(requestId, isA<int>());
      expect(requestId, equals(request['id']));
    });

    test('McpClientConfig.requestTimeout reaches the created client', () {
      client = McpClient.createClient(
        const McpClientConfig(
          name: 'Test',
          version: '1.0.0',
          requestTimeout: Duration(seconds: 61),
        ),
      );
      expect(client.defaultRequestTimeout, const Duration(seconds: 61));
    });
  });

  group('cancellation', () {
    test('cancel() completes the in-flight call with an McpError', () async {
      client = Client(name: 'Test', version: '1.0.0');
      await _handshake(client, transport);

      final token = McpCancellationToken();
      final call = client.callTool('slow', const {}, cancellationToken: token);

      // Let the request reach the wire before cancelling.
      await Future<void>.delayed(Duration.zero);
      token.cancel(reason: 'user aborted');

      await expectLater(
        call,
        throwsA(isA<McpError>().having(
          (e) => e.toString(),
          'message',
          allOf(contains('cancelled'), contains('user aborted')),
        )),
      );
      expect(token.isCancelled, isTrue);
    });

    test('cancel() emits notifications/cancelled with an int requestId',
        () async {
      client = Client(name: 'Test', version: '1.0.0');
      await _handshake(client, transport);

      final token = McpCancellationToken();
      final call = client.callTool('slow', const {}, cancellationToken: token);
      await Future<void>.delayed(Duration.zero);

      final request = transport.sentMessages
          .firstWhere((m) => m['method'] == 'tools/call');
      token.cancel(reason: 'user aborted');
      await expectLater(call, throwsA(isA<McpError>()));

      final cancel = _lastCancelNotification(transport);
      expect(cancel, isNotNull);
      final params = cancel!['params'] as Map;
      // Must be the raw int id — a stringified "7" is silently dropped by a
      // server that matches on the typed request id.
      expect(params['requestId'], isA<int>());
      expect(params['requestId'], equals(request['id']));
      expect(params['reason'], 'user aborted');
    });

    test('a cancel latched before binding prevents the request being sent',
        () async {
      client = Client(name: 'Test', version: '1.0.0');
      await _handshake(client, transport);

      final token = McpCancellationToken()..cancel(reason: 'aborted early');

      await expectLater(
        client.callTool('slow', const {}, cancellationToken: token),
        throwsA(isA<McpError>().having(
          (e) => e.toString(),
          'message',
          contains('cancelled before dispatch'),
        )),
      );

      expect(
        transport.sentMessages.where((m) => m['method'] == 'tools/call'),
        isEmpty,
      );
      // Nothing was ever on the wire, so no cancellation notification either.
      expect(_lastCancelNotification(transport), isNull);
    });

    test('cancel() after the call completed does not emit a stale notification',
        () async {
      client = Client(name: 'Test', version: '1.0.0');
      await _handshake(client, transport);
      transport.queueResponse({
        'jsonrpc': McpProtocol.jsonRpcVersion,
        'result': {'tools': <dynamic>[]},
      });

      final token = McpCancellationToken();
      await client.listTools(cancellationToken: token);

      token.cancel();
      expect(_lastCancelNotification(transport), isNull);
    });

    test('cancel() from a disconnected client never throws', () async {
      client = Client(name: 'Test', version: '1.0.0');
      await _handshake(client, transport);

      final token = McpCancellationToken();
      final call = client.callTool('slow', const {}, cancellationToken: token);
      await Future<void>.delayed(Duration.zero);

      client.disconnect();
      expect(() => token.cancel(), returnsNormally);
      await expectLater(call, throwsA(isA<McpError>()));
    });

    test('notifyCancelled on an uninitialized client does not throw', () {
      client = Client(name: 'Test', version: '1.0.0');
      expect(() => client.notifyCancelled(7, reason: 'teardown'),
          returnsNormally);
    });
  });

  group('disconnect', () {
    test('pending requests fail immediately instead of waiting for the timeout',
        () async {
      client = Client(
        name: 'Test',
        version: '1.0.0',
        defaultRequestTimeout: const Duration(seconds: 30),
      );
      await _handshake(client, transport);

      final call = client.listTools();
      await Future<void>.delayed(Duration.zero);

      final stopwatch = Stopwatch()..start();
      client.disconnect();
      await expectLater(
        call,
        throwsA(isA<McpError>().having(
          (e) => e.toString(),
          'message',
          contains('Transport disconnected'),
        )),
      );
      stopwatch.stop();
      expect(stopwatch.elapsed, lessThan(const Duration(seconds: 5)));
    });
  });
}
