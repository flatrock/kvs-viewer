import {
  KinesisVideoClient,
  GetSignalingChannelEndpointCommand,
} from "https://cdn.jsdelivr.net/npm/@aws-sdk/client-kinesis-video/+esm";

import {
  KinesisVideoSignalingClient,
  GetIceServerConfigCommand,
} from "https://cdn.jsdelivr.net/npm/@aws-sdk/client-kinesis-video-signaling/+esm";

const $ = (id) => document.getElementById(id);

const els = {
  region: $("region"),
  accessKeyId: $("accessKeyId"),
  secretAccessKey: $("secretAccessKey"),
  sessionToken: $("sessionToken"),
  channelArn: $("channelArn"),
  clientId: $("clientId"),
  startViewer: $("viewer-button"),
  stopViewer: $("stop-viewer-button"),
  remoteVideo: $("remoteVideo"),
  signalingStatus: $("signalingStatus"),
  iceStatus: $("iceStatus"),
  logs: $("logs"),
  clearLogs: $("clear-logs"),
  recorderWsUrl: $("recorderWsUrl"),
  recorderId: $("recorderId"),
  fps: $("fps"),
  segmentEnabled: $("segmentEnabled"),
  segmentTime: $("segmentTime"),
  segmentTimeField: $("segmentTimeField"),
  recStartBtn: $("recStartBtn"),
  recStopBtn: $("recStopBtn"),
  wsStatus: $("wsStatus"),
  recStatus: $("recStatus"),
};


// ------------------------------------------------------------
// ICE candidate policy for ESP WebRTC compatibility
// ------------------------------------------------------------
// ESP WebRTC currently keeps only a limited number of remote candidates.
// To avoid filling that limit with host/srflx/TURNS-TCP candidates, this
// viewer sends only TURN/UDP relay candidates to the KVS Master.
const MAX_TURN_SERVER_CONFIGS = 1;
const USE_RELAY_ONLY = true;
const SEND_ONLY_TURN_UDP_RELAY_CANDIDATES = true;

function isTurnUdpUrl(url) {
  return typeof url === "string" && url.startsWith("turn:") && url.includes("transport=udp");
}

function isRelayCandidate(candidateText) {
  return typeof candidateText === "string" && candidateText.includes(" typ relay ");
}

function shouldSendIceCandidate(candidate) {
  if (!SEND_ONLY_TURN_UDP_RELAY_CANDIDATES) return true;

  const candidateText = candidate?.candidate ?? "";
  const url = candidate?.url ?? "";
  return isRelayCandidate(candidateText) && isTurnUdpUrl(url);
}

function summarizeIceCandidate(candidate) {
  const candidateText = candidate?.candidate ?? "";
  const type = candidateText.match(/ typ (\S+)/)?.[1] ?? "unknown";
  return {
    sdpMid: candidate?.sdpMid,
    sdpMLineIndex: candidate?.sdpMLineIndex,
    type,
    url: candidate?.url,
    relayProtocol: candidate?.relayProtocol,
  };
}

function log(level, ...args) {
  const line = `[${new Date().toISOString()}] [${level}] ${args.map(formatLogArg).join(" ")}`;
  console[level === "ERROR" ? "error" : "log"](...args);
  els.logs.textContent += `${line}\n`;
  els.logs.scrollTop = els.logs.scrollHeight;
}

function formatLogArg(arg) {
  if (arg instanceof Error) return `${arg.name}: ${arg.message}`;
  if (typeof arg === "string") return arg;
  try { return JSON.stringify(arg); } catch { return String(arg); }
}

function setText(el, text) {
  if (el) el.textContent = text;
}

function readConfig() {
  const region = els.region.value.trim() || "us-west-2";
  const accessKeyId = els.accessKeyId.value.trim();
  const secretAccessKey = els.secretAccessKey.value.trim();
  const sessionToken = els.sessionToken.value.trim();
  const channelArn = els.channelArn.value.trim();
  const clientId = els.clientId.value.trim() || crypto.randomUUID();

  if (!channelArn || !accessKeyId || !secretAccessKey) {
    throw new Error("Channel ARN, Access Key ID, and Secret Access Key are required.");
  }

  return {
    region,
    channelArn,
    clientId,
    credentials: {
      accessKeyId,
      secretAccessKey,
      ...(sessionToken ? { sessionToken } : {}),
    },
  };
}

function applyUrlParams() {
  const params = new URLSearchParams(window.location.search);
  for (const [param, el] of [
    ["region", els.region],
    ["channelArn", els.channelArn],
    ["clientId", els.clientId],
  ]) {
    const value = params.get(param);
    if (value) el.value = value;
  }
}

class KvsViewer {
  constructor() {
    this.pc = null;
    this.signalingClient = null;
    this.remoteStream = null;
    this.frameReader = null;
    this.videoTrack = null;
    this.width = 1280;
    this.height = 720;
  }

  async start(config) {
    log("INFO", "Starting viewer", { region: config.region, channelArn: config.channelArn, clientId: config.clientId });
    setText(els.signalingStatus, "resolving endpoints");

    const endpoints = await this.getSignalingEndpoints(config);
    const iceServers = await this.getIceServers(config, endpoints.httpsEndpoint);

    this.pc = new RTCPeerConnection({
      iceServers,
      iceTransportPolicy: USE_RELAY_ONLY ? "relay" : "all",
    });
    this.installPeerConnectionHandlers();

    this.signalingClient = new KVSWebRTC.SignalingClient({
      role: KVSWebRTC.Role.VIEWER,
      channelARN: config.channelArn,
      channelEndpoint: endpoints.wssEndpoint,
      region: config.region,
      clientId: config.clientId,
      credentials: config.credentials,
    });
    this.installSignalingHandlers();

    setText(els.signalingStatus, "opening");
    this.signalingClient.open();
  }

  async stop() {
    log("INFO", "Stopping viewer");
    recorder.stopRecording();

    if (this.signalingClient) {
      this.signalingClient.close();
      this.signalingClient = null;
    }

    if (this.frameReader) {
      try { await this.frameReader.cancel(); } catch (_) {}
      this.frameReader = null;
    }

    if (this.pc) {
      this.pc.getSenders().forEach((sender) => sender.track?.stop());
      this.pc.getReceivers().forEach((receiver) => receiver.track?.stop());
      this.pc.close();
      this.pc = null;
    }

    this.videoTrack = null;
    this.remoteStream = null;
    els.remoteVideo.srcObject = null;
    setText(els.signalingStatus, "idle");
    setText(els.iceStatus, "idle");
  }

  async getSignalingEndpoints(config) {
    const kv = new KinesisVideoClient({ region: config.region, credentials: config.credentials });
    const response = await kv.send(new GetSignalingChannelEndpointCommand({
      ChannelARN: config.channelArn,
      SingleMasterChannelEndpointConfiguration: {
        Protocols: ["WSS", "HTTPS"],
        Role: "VIEWER",
      },
    }));

    const endpointMap = new Map((response.ResourceEndpointList ?? []).map((e) => [e.Protocol, e.ResourceEndpoint]));
    const wssEndpoint = endpointMap.get("WSS");
    const httpsEndpoint = endpointMap.get("HTTPS");
    if (!wssEndpoint || !httpsEndpoint) {
      throw new Error("Failed to resolve KVS signaling endpoints.");
    }

    log("INFO", "Resolved signaling endpoints", { wssEndpoint, httpsEndpoint });
    return { wssEndpoint, httpsEndpoint };
  }

  async getIceServers(config, httpsEndpoint) {
    const kvs = new KinesisVideoSignalingClient({
      region: config.region,
      credentials: config.credentials,
      endpoint: httpsEndpoint,
    });

    const response = await kvs.send(new GetIceServerConfigCommand({ ChannelARN: config.channelArn }));

    const turnUdpServers = (response.IceServerList ?? [])
      .slice(0, MAX_TURN_SERVER_CONFIGS)
      .map((s) => ({
        urls: (s.Uris ?? []).filter(isTurnUdpUrl),
        username: s.Username,
        credential: s.Password,
      }))
      .filter((s) => s.urls.length > 0);

    if (turnUdpServers.length === 0) {
      throw new Error("No TURN/UDP ICE server was returned by KVS.");
    }

    log("INFO", "Loaded filtered ICE servers", {
      policy: USE_RELAY_ONLY ? "relay-only" : "all",
      maxTurnServerConfigs: MAX_TURN_SERVER_CONFIGS,
      serverCount: turnUdpServers.length,
      urls: turnUdpServers.flatMap((s) => s.urls),
    });

    return turnUdpServers;
  }

  installPeerConnectionHandlers() {
    this.pc.ontrack = (event) => {
      log("INFO", "Received remote track", { kind: event.track.kind });
      if (!this.remoteStream) {
        this.remoteStream = new MediaStream();
        els.remoteVideo.srcObject = this.remoteStream;
      }
      this.remoteStream.addTrack(event.track);

      if (event.track.kind === "video") {
        this.videoTrack = event.track;
        const settings = event.track.getSettings();
        this.width = settings.width ?? this.width;
        this.height = settings.height ?? this.height;
        recorder.setVideoTrack(event.track, this.width, this.height);
      }
    };

    this.pc.onicecandidate = (event) => {
      if (!event.candidate || !this.signalingClient) return;

      const summary = summarizeIceCandidate(event.candidate);
      if (!shouldSendIceCandidate(event.candidate)) {
        log("INFO", "Dropped local ICE candidate", summary);
        return;
      }

      log("INFO", "Sending local ICE candidate", summary);
      this.signalingClient.sendIceCandidate(event.candidate);
    };

    this.pc.oniceconnectionstatechange = () => {
      setText(els.iceStatus, this.pc.iceConnectionState);
      log("INFO", "ICE connection state", this.pc.iceConnectionState);
    };
  }

  installSignalingHandlers() {
    this.signalingClient.on("open", async () => {
      log("INFO", "Signaling connection opened");
      setText(els.signalingStatus, "open");

      this.pc.addTransceiver("audio", {
        direction: "recvonly",
      });

      this.pc.addTransceiver("video", {
        direction: "recvonly",
      });

      const offer = await this.pc.createOffer();
      await this.pc.setLocalDescription(offer);
      this.signalingClient.sendSdpOffer(this.pc.localDescription);

      log("INFO", "Sent audio+video recvonly SDP offer");
    });

    this.signalingClient.on("sdpAnswer", async (answer) => {
      await this.pc.setRemoteDescription(answer);
      log("INFO", "Applied SDP answer");
    });

    this.signalingClient.on("iceCandidate", async (candidate) => {
      try {
        await this.pc.addIceCandidate(candidate);
      } catch (e) {
        log("WARN", "addIceCandidate failed", e);
      }
    });

    this.signalingClient.on("close", () => {
      log("INFO", "Signaling connection closed");
      setText(els.signalingStatus, "closed");
    });

    this.signalingClient.on("error", (e) => {
      log("ERROR", "Signaling error", e);
      setText(els.signalingStatus, "error");
    });
  }
}

class RecorderBridge {
  constructor() {
    this.ws = null;
    this.wsConnected = false;
    this.recording = false;
    this.frameReader = null;
    this.videoTrack = null;
    this.width = 1280;
    this.height = 720;
  }

  setVideoTrack(track, width, height) {
    this.videoTrack = track;
    this.width = width ?? this.width;
    this.height = height ?? this.height;
    this.updateButtons();
  }

  connect() {
    if (this.ws && [WebSocket.OPEN, WebSocket.CONNECTING].includes(this.ws.readyState)) return;

    const url = els.recorderWsUrl.value.trim() || "ws://127.0.0.1:8080";
    setText(els.wsStatus, "connecting");
    this.ws = new WebSocket(url);
    this.ws.binaryType = "arraybuffer";

    this.ws.onopen = () => {
      this.wsConnected = true;
      setText(els.wsStatus, "connected");
      log("INFO", "Recorder WebSocket connected", url);
      this.updateButtons();
    };
    this.ws.onerror = (ev) => {
      this.wsConnected = false;
      setText(els.wsStatus, "error");
      log("ERROR", "Recorder WebSocket error", ev);
      this.updateButtons();
    };
    this.ws.onclose = () => {
      this.wsConnected = false;
      setText(els.wsStatus, "disconnected");
      log("INFO", "Recorder WebSocket closed");
      this.stopRecording(false);
      this.updateButtons();
    };
  }

  startRecording() {
    if (!this.wsConnected) {
      alert("Recorder WebSocket server is not connected.");
      return;
    }
    if (!this.videoTrack) {
      alert("No remote video track is available yet.");
      return;
    }
    if (this.recording) return;

    const recorderId = els.recorderId.value.trim() || "1";
    const fps = Number.parseInt(els.fps.value, 10) || 30;
    const segmentTimeSeconds = this.getSegmentTimeSeconds();

    this.recording = true;
    setText(els.recStatus, "recording");
    this.updateButtons();

    this.ws.send(JSON.stringify({
      type: "recorder",
      op: "create",
      recorder_id: recorderId,
      width: this.width,
      height: this.height,
      fps,
      segment_time_seconds: segmentTimeSeconds,
    }));

    log("INFO", "REC started", { recorderId, width: this.width, height: this.height, fps, segmentTimeSeconds });
    this.readFramesLoop(fps);
  }

  stopRecording(sendClose = true) {
    if (!this.recording) return;

    this.recording = false;
    setText(els.recStatus, "idle");
    this.updateButtons();

    if (sendClose && this.wsConnected) {
      const recorderId = els.recorderId.value.trim() || "1";
      this.ws.send(JSON.stringify({ type: "recorder", op: "close", recorder_id: recorderId }));
      log("INFO", "REC stopped", { recorderId });
    }
  }

  async readFramesLoop(fps) {
    if (typeof MediaStreamTrackProcessor === "undefined") {
      log("ERROR", "MediaStreamTrackProcessor is not supported by this browser.");
      this.stopRecording();
      return;
    }

    const processor = new MediaStreamTrackProcessor({ track: this.videoTrack });
    this.frameReader = processor.readable.getReader();

    try {
      while (this.recording) {
        const { value: frame, done } = await this.frameReader.read();
        if (done || !this.recording) break;

        const timestampUs = frame.timestamp ?? Math.round(performance.now() * 1000);
        const size = frame.allocationSize();
        const raw = new Uint8Array(size);
        await frame.copyTo(raw);

        const packet = new ArrayBuffer(8 + size);
        const view = new DataView(packet);
        view.setBigUint64(0, BigInt(timestampUs), true);
        new Uint8Array(packet, 8).set(raw);

        if (this.wsConnected) this.ws.send(packet);
        frame.close();
        await new Promise((resolve) => setTimeout(resolve, 1000 / fps));
      }
    } catch (e) {
      log("ERROR", "Frame read loop failed", e);
      this.stopRecording();
    } finally {
      try { await this.frameReader?.cancel(); } catch (_) {}
      this.frameReader = null;
    }
  }

  getSegmentTimeSeconds() {
    if (!els.segmentEnabled.checked) return 0;
    const v = Number.parseInt(els.segmentTime.value, 10);
    return Number.isFinite(v) && v > 0 ? v : 0;
  }

  updateButtons() {
    els.recStartBtn.disabled = !this.wsConnected || !this.videoTrack || this.recording;
    els.recStopBtn.disabled = !this.recording;
  }
}

const viewer = new KvsViewer();
const recorder = new RecorderBridge();

function updateSegmentUI() {
  const enabled = els.segmentEnabled.checked;
  els.segmentTimeField.classList.toggle("segment-disabled", !enabled);
  els.segmentTime.disabled = !enabled;
}

applyUrlParams();
updateSegmentUI();
recorder.connect();

els.viewerButton = els.startViewer;
els.startViewer.addEventListener("click", async () => {
  try {
    els.startViewer.disabled = true;
    els.stopViewer.disabled = false;
    await viewer.start(readConfig());
  } catch (e) {
    log("ERROR", "Failed to start viewer", e);
    els.startViewer.disabled = false;
    els.stopViewer.disabled = true;
    setText(els.signalingStatus, "error");
  }
});

els.stopViewer.addEventListener("click", async () => {
  await viewer.stop();
  els.startViewer.disabled = false;
  els.stopViewer.disabled = true;
  recorder.updateButtons();
});

els.recStartBtn.addEventListener("click", () => recorder.startRecording());
els.recStopBtn.addEventListener("click", () => recorder.stopRecording());
els.segmentEnabled.addEventListener("change", updateSegmentUI);
els.clearLogs.addEventListener("click", () => { els.logs.textContent = ""; });

window.addEventListener("beforeunload", () => {
  recorder.stopRecording();
  viewer.stop();
});
