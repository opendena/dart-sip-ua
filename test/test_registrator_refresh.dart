import 'dart:async';
import 'dart:io';

import 'package:test/test.dart';

import 'package:sip_ua/src/sip_ua_helper.dart';
import 'package:sip_ua/src/transport_type.dart';

/// Regression tests for the [Registrator]: every successful REGISTER must be
/// visible to the application, not only the first one.
///
/// The consuming application gates its work on the 200 OK of the REGISTER it
/// just triggered, a binding being an ephemeral routing hint. As long as the
/// event was tied to the unregistered -> registered transition, a refresh -
/// explicit or from the expiry timer - was pure silence.
///
/// Everything runs against a local WebSocket registrar answering 200 OK to
/// every REGISTER, the cheapest way to drive the real code path end to end.
List<void Function()> testFunctions = <void Function()>[
  () => test(' Registrator: a cold REGISTER is notified', () async {
        _FakeRegistrar registrar = await _FakeRegistrar.start();
        _RegistrationRecorder recorder = _RegistrationRecorder();
        SIPUAHelper helper = SIPUAHelper();
        helper.addSipUaHelperListener(recorder);

        await helper.start(_settings(registrar.port));
        await recorder.waitFor(RegistrationStateEnum.REGISTERED, 1);

        expect(registrar.registerCount, 1);

        await _shutdown(helper, registrar);
      }),
  () => test(' Registrator: an explicit REGISTER while registered is notified',
          () async {
        _FakeRegistrar registrar = await _FakeRegistrar.start();
        _RegistrationRecorder recorder = _RegistrationRecorder();
        SIPUAHelper helper = SIPUAHelper();
        helper.addSipUaHelperListener(recorder);

        await helper.start(_settings(registrar.port));
        await recorder.waitFor(RegistrationStateEnum.REGISTERED, 1);

        // The registration is up: before the fix, this refresh was answered
        // 200 OK and swallowed, no event of any kind.
        helper.register();
        await recorder.waitFor(RegistrationStateEnum.REGISTERED, 2);

        expect(registrar.registerCount, 2);

        await _shutdown(helper, registrar);
      }),
  () => test(' Registrator: the automatic refresh is notified', () async {
        _FakeRegistrar registrar = await _FakeRegistrar.start();
        _RegistrationRecorder recorder = _RegistrationRecorder();
        SIPUAHelper helper = SIPUAHelper();
        helper.addSipUaHelperListener(recorder);

        // The registrator re-registers 5 s before the expiry, and clamps the
        // expiry to MIN_REGISTER_EXPIRES (10 s): the refresh lands ~5 s later.
        await helper.start(_settings(registrar.port, expires: 10));
        await recorder.waitFor(RegistrationStateEnum.REGISTERED, 1);
        await recorder.waitFor(RegistrationStateEnum.REGISTERED, 2,
            timeout: const Duration(seconds: 20));

        expect(registrar.registerCount, greaterThanOrEqualTo(2));

        await _shutdown(helper, registrar);
      }),
  () => test(' Registrator: un-REGISTER is notified once', () async {
        _FakeRegistrar registrar = await _FakeRegistrar.start();
        _RegistrationRecorder recorder = _RegistrationRecorder();
        SIPUAHelper helper = SIPUAHelper();
        helper.addSipUaHelperListener(recorder);

        await helper.start(_settings(registrar.port));
        await recorder.waitFor(RegistrationStateEnum.REGISTERED, 1);

        expect(await helper.unregister(), true);
        await recorder.waitFor(RegistrationStateEnum.UNREGISTERED, 1);

        // The un-REGISTER transition stays a transition: no extra REGISTERED.
        expect(recorder.countOf(RegistrationStateEnum.REGISTERED), 1);

        await _shutdown(helper, registrar);
      }),
  () => test(' Registrator: a rejected REGISTER still fails', () async {
        _FakeRegistrar registrar = await _FakeRegistrar.start(rejectWith: 500);
        _RegistrationRecorder recorder = _RegistrationRecorder();
        SIPUAHelper helper = SIPUAHelper();
        helper.addSipUaHelperListener(recorder);

        await helper.start(_settings(registrar.port));
        await recorder.waitFor(RegistrationStateEnum.REGISTRATION_FAILED, 1);

        expect(recorder.countOf(RegistrationStateEnum.REGISTERED), 0);

        await _shutdown(helper, registrar);
      }),
];

UaSettings _settings(int port, {int expires = 3600}) {
  UaSettings settings = UaSettings();
  settings.transportType = TransportType.WS;
  settings.webSocketUrl = 'ws://127.0.0.1:$port';
  settings.uri = 'sip:alice@127.0.0.1';
  settings.authorizationUser = 'alice';
  settings.password = 'secret';
  settings.displayName = 'Alice';
  settings.userAgent = 'dart-sip-ua registrator test';
  settings.instanceId = '8f1b5c26-0d3f-4a1e-9a5f-2f0d9b6f0c11';
  settings.register = true;
  settings.register_expires = expires;
  return settings;
}

Future<void> _shutdown(SIPUAHelper helper, _FakeRegistrar registrar) async {
  helper.stop();
  await Future<void>.delayed(const Duration(milliseconds: 200));
  await registrar.stop();
}

/// Records what the application sees, ie the [SipUaHelperListener] callbacks.
class _RegistrationRecorder implements SipUaHelperListener {
  final List<RegistrationState> states = <RegistrationState>[];
  final StreamController<RegistrationState> _events =
      StreamController<RegistrationState>.broadcast();

  int countOf(RegistrationStateEnum state) =>
      states.where((RegistrationState item) => item.state == state).length;

  /// Completes once [state] has been notified [count] times.
  Future<void> waitFor(RegistrationStateEnum state, int count,
      {Duration timeout = const Duration(seconds: 10)}) async {
    if (countOf(state) >= count) return;
    await _events.stream
        .firstWhere((RegistrationState _) => countOf(state) >= count)
        .timeout(timeout,
            onTimeout: () => throw StateError(
                'expected $count $state event(s), got ${countOf(state)}'));
  }

  @override
  void registrationStateChanged(RegistrationState state) {
    print('registrationStateChanged => ${state.state}');
    states.add(state);
    _events.add(state);
  }

  @override
  void transportStateChanged(TransportState state) {}

  @override
  void callStateChanged(Call call, CallState state) {}

  @override
  void onNewMessage(SIPMessageRequest msg) {}

  @override
  void onNewNotify(Notify ntf) {}

  @override
  void onNewReinvite(ReInvite event) {}
}

/// Minimal SIP over WebSocket registrar: answers every REGISTER, echoing back
/// the headers the registrator needs to accept the response (same Via branch
/// and CSeq, and the Contact carrying the granted expiry).
class _FakeRegistrar {
  _FakeRegistrar._(this._server, this._rejectWith);

  final HttpServer _server;
  final int? _rejectWith;
  final List<WebSocket> _clients = <WebSocket>[];
  int registerCount = 0;

  int get port => _server.port;

  static Future<_FakeRegistrar> start({int? rejectWith}) async {
    HttpServer server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    _FakeRegistrar registrar = _FakeRegistrar._(server, rejectWith);
    server.listen((HttpRequest request) async {
      WebSocket socket = await WebSocketTransformer.upgrade(request,
          protocolSelector: (List<String> protocols) => 'sip');
      registrar._clients.add(socket);
      socket.listen((dynamic data) {
        registrar._onMessage(socket, data.toString());
      }, onError: (Object error) {}, cancelOnError: true);
    });
    return registrar;
  }

  Future<void> stop() async {
    for (WebSocket socket in _clients) {
      await socket.close();
    }
    await _server.close(force: true);
  }

  void _onMessage(WebSocket socket, String message) {
    if (!message.startsWith('REGISTER ')) {
      // Keep alive CRLF, or anything else we have no business answering.
      return;
    }
    registerCount++;
    socket.add(_answer(message));
  }

  String _answer(String request) {
    Map<String, String> headers = <String, String>{};
    for (String line in request.split('\r\n').skip(1)) {
      if (line.isEmpty) break;
      int separator = line.indexOf(':');
      if (separator < 0) continue;
      headers[line.substring(0, separator).trim().toLowerCase()] =
          line.substring(separator + 1).trim();
    }

    String to = headers['to'] ?? '';
    if (!to.contains(';tag=')) {
      to += ';tag=registrar${registerCount}tag';
    }

    List<String> answer = <String>[
      'SIP/2.0 ${_rejectWith ?? 200} ${_rejectWith != null ? 'Server Internal Error' : 'OK'}',
      'Via: ${headers['via']}',
      'From: ${headers['from']}',
      'To: $to',
      'Call-ID: ${headers['call-id']}',
      'CSeq: ${headers['cseq']}',
    ];
    if (_rejectWith == null && headers['contact'] != null) {
      // Granting the requested binding as is: the registrator looks the
      // Contact up by user and reads its expires parameter from there.
      answer.add('Contact: ${headers['contact']}');
    }
    answer.add('Content-Length: 0');

    return '${answer.join('\r\n')}\r\n\r\n';
  }
}

void main() {
  for (Function func in testFunctions) {
    func();
  }
}
