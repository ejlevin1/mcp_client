// A request whose wire fails must fail with it.
//
// A transport reports a failed POST by adding an error to its message stream.
// `Client.connect` listened to that stream without an `onError`, so the error
// went unhandled and the pending `initialize` completer sat until its
// 30-second timeout. From the caller's side an unreachable endpoint and a slow
// one were indistinguishable for half a minute.

import 'dart:async';

import 'package:mcp_client/mcp_client.dart';
import 'package:test/test.dart';

/// A transport that accepts a send and then reports the wire failing.
class _FailingTransport implements ClientTransport {
  final _messages = StreamController<dynamic>.broadcast();
  final _closed = Completer<void>();

  @override
  Stream<dynamic> get onMessage => _messages.stream;

  @override
  Future<void> get onClose => _closed.future;

  @override
  void send(dynamic message) {
    // What an HTTP transport does when the POST is refused.
    Future<void>.microtask(
        () => _messages.addError(Exception('Connection refused')));
  }

  @override
  void close() {
    if (!_closed.isCompleted) _closed.complete();
    _messages.close();
  }
}

void main() {
  test('connect fails when the transport reports an error', () async {
    final client = Client(
      name: 'test',
      version: '1',
      capabilities: const ClientCapabilities(),
    );

    final sw = Stopwatch()..start();
    await expectLater(
      client.connect(_FailingTransport()),
      throwsA(isA<McpError>()),
    );
    // The point is the speed: the request timeout is 30 s, and waiting it out
    // was the old behaviour.
    expect(sw.elapsed, lessThan(const Duration(seconds: 5)));
  });
}
