/// Legacy SSE transport variants (auth / compressed / heartbeat).
///
/// These are built on `dart:io` HTTP types, so they resolve per platform:
/// the real implementations where `dart:io` exists, stubs that throw a clear
/// error elsewhere. `StreamableHttpClientTransport` is the supported transport
/// on platforms without `dart:io`.
library;

export 'legacy_sse_stub.dart' if (dart.library.io) 'legacy_sse_io.dart';
