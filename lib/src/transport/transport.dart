/// Client transports.
///
/// [ClientTransport] is platform-free. The STDIO and legacy SSE
/// implementations need `dart:io`, so they resolve per platform: the native
/// implementations where `dart:io` exists, and stubs that throw a clear error
/// elsewhere. `StreamableHttpClientTransport` carries no such requirement and
/// is exported directly.
library;

export 'client_transport.dart';
export 'transport_stub.dart' if (dart.library.io) 'transport_io.dart';
