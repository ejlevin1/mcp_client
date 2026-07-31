/// Headers that are fetched, not fixed.
///
/// `config.headers` is set when the transport is built, so anything that
/// expires cannot live there. A host that must ask for the value at call time
/// — an attestation token, a short-lived session credential — needs the
/// provider, and what matters is that the value actually reaches the wire and
/// that a failing provider never costs the request.
///
/// Asserted against a real socket: the header map is assembled inside the
/// transport and handed to an HTTP client, so a mock of that client would be
/// asserting on our own arrangement rather than on what a server receives.
@TestOn('vm')
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:mcp_client/mcp_client.dart';
import 'package:test/test.dart';

/// Records the headers of every request it is sent.
class Recorder {
  Recorder(this._server) {
    _server.listen((request) async {
      seen.add(request.headers);
      await request.drain<void>();
      request.response
        ..statusCode = 200
        ..headers.contentType = ContentType.json
        ..headers.set('mcp-session-id', 'session-1')
        ..write(jsonEncode({
          'jsonrpc': '2.0',
          'id': 1,
          'result': {
            'protocolVersion': '2025-06-18',
            'capabilities': <String, dynamic>{},
            'serverInfo': {'name': 'recorder', 'version': '1.0.0'},
          },
        }));
      await request.response.close();
    });
  }

  final HttpServer _server;
  final List<HttpHeaders> seen = <HttpHeaders>[];

  String get url => 'http://localhost:${_server.port}/mcp';
  Future<void> close() => _server.close(force: true);

  String? headerOf(int index, String name) => seen[index].value(name);
}

Future<Recorder> recorder() async =>
    Recorder(await HttpServer.bind(InternetAddress.loopbackIPv4, 0));

/// Opens a transport and sends one request through it.
Future<void> send(
  Recorder server, {
  RequestHeadersProvider? provider,
  Map<String, String>? headers,
  Duration? providerTimeout,
}) async {
  final transport = await StreamableHttpClientTransport.create(
    baseUrl: server.url,
    headers: headers,
    headersProvider: provider,
    headersProviderTimeout: providerTimeout,
  );
  transport.send({
    'jsonrpc': '2.0',
    'id': 1,
    'method': 'initialize',
    'params': <String, dynamic>{},
  });
  // Let the request reach the socket.
  await Future<void>.delayed(const Duration(milliseconds: 300));
  transport.close();
}

void main() {
  late Recorder server;

  setUp(() async => server = await recorder());
  tearDown(() => server.close());

  test('a supplied header reaches the wire', () async {
    await send(server,
        provider: (_) => {'X-Firebase-AppCheck': 'token-1'});

    expect(server.seen, isNotEmpty);
    expect(server.headerOf(0, 'x-firebase-appcheck'), 'token-1');
  });

  test('the provider is told which request it is for', () async {
    final asked = <({String url, String method})>[];
    await send(server, provider: (request) {
      asked.add(request);
      return const <String, String>{};
    });

    // One provider can serve several transports, so it has to be able to
    // decide per destination.
    expect(asked.first.url, server.url);
    expect(asked.first.method, 'POST');
    // Closing terminates the session with a DELETE, which needs the same
    // credential as everything else — so it goes through the provider too.
    expect(asked.map((a) => a.method), contains('DELETE'));
  });

  test('it is asked again for the next request', () async {
    // The whole reason this exists: a value that expires must not be captured
    // once. A provider asked a single time is `config.headers` with extra
    // steps.
    var calls = 0;
    final transport = await StreamableHttpClientTransport.create(
      baseUrl: server.url,
      headersProvider: (_) {
        calls++;
        return {'X-Token': 'v$calls'};
      },
    );
    transport.send({'jsonrpc': '2.0', 'id': 1, 'method': 'initialize'});
    await Future<void>.delayed(const Duration(milliseconds: 200));
    transport.send({'jsonrpc': '2.0', 'id': 2, 'method': 'ping'});
    await Future<void>.delayed(const Duration(milliseconds: 200));
    transport.close();

    expect(calls, greaterThanOrEqualTo(2));
    expect(server.headerOf(0, 'x-token'), 'v1');
    expect(server.headerOf(1, 'x-token'), 'v2');
  });

  group('a provider cannot break the exchange', () {
    test('reserved names are dropped, not honoured', () async {
      await send(server, provider: (_) => {
            'Content-Type': 'text/plain',
            'Accept': 'text/plain',
            'MCP-Session-Id': 'forged',
            'MCP-Protocol-Version': '1999-01-01',
            'X-Allowed': 'yes',
          });

      // Honouring these would break the exchange in a way that reads as the
      // server's fault.
      expect(server.headerOf(0, 'content-type'), contains('application/json'));
      expect(server.headerOf(0, 'accept'), contains('text/event-stream'));
      expect(server.headerOf(0, 'mcp-session-id'), isNot('forged'));
      expect(server.headerOf(0, 'mcp-protocol-version'), isNot('1999-01-01'));
      // Everything else still goes.
      expect(server.headerOf(0, 'x-allowed'), 'yes');
    });

    test('a name in any casing is still reserved', () async {
      await send(server, provider: (_) => {'content-TYPE': 'text/plain'});

      expect(server.headerOf(0, 'content-type'), contains('application/json'));
    });
  });

  group('a failing provider never costs the request', () {
    test('a throwing provider sends the request without its headers',
        () async {
      // Failing the request instead would report a hook problem as an
      // unreachable server, and send whoever is looking at the screen to
      // check their network for something that is not there.
      await send(server, provider: (_) => throw StateError('no token'));

      expect(server.seen, isNotEmpty);
      expect(server.headerOf(0, 'content-type'), contains('application/json'));
    });

    test('a hanging provider is bounded', () async {
      // A hang has no error to report and no end, which is worse than a
      // failure.
      final started = DateTime.now();
      await send(
        server,
        provider: (_) => Completer<Map<String, String>>().future,
        providerTimeout: const Duration(milliseconds: 150),
      );

      expect(server.seen, isNotEmpty);
      expect(DateTime.now().difference(started),
          lessThan(const Duration(seconds: 3)));
    });

    test('the failure is recorded rather than swallowed', () async {
      final transport = await StreamableHttpClientTransport.create(
        baseUrl: server.url,
        headersProvider: (_) => throw StateError('no token'),
      );
      transport.send({'jsonrpc': '2.0', 'id': 1, 'method': 'initialize'});
      await Future<void>.delayed(const Duration(milliseconds: 300));

      // "The origin refused us" and "we never attached what it wanted" look
      // identical on the wire and send a debugger to different places.
      expect(transport.lastHeadersProviderError, isA<StateError>());
      transport.close();
    });
  });

  test('fixed headers still win over supplied ones', () async {
    // A host that configured a value explicitly meant it; a provider is the
    // general case and yields to the specific one.
    await send(server,
        headers: {'X-Token': 'fixed'}, provider: (_) => {'X-Token': 'supplied'});

    expect(server.headerOf(0, 'x-token'), 'fixed');
  });

  test('no provider changes nothing', () async {
    await send(server);

    expect(server.seen, isNotEmpty);
    expect(server.headerOf(0, 'content-type'), contains('application/json'));
  });
}
