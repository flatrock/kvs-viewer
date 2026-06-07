// v7

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
// ICE policy
// ------------------------------------------------------------
// P2P-first video-only offer variant for the M5Stack/ESP WebRTC Master.
//
// Key points:
// - Default mode is no-relay: allow UDP host/srflx candidates and reject relay.
// - KVS TURN servers are not used unless iceMode=all or iceMode=relay-only.
// - Browser mDNS host candidates can be rewritten with viewerIp, but the
//   current ESP-side mDNS resolver also supports viewerIp-free operation.
// - Candidate sending is deferred until SDP_OFFER is sent.
// - Local candidates are filtered and capped per m-line to avoid overflowing
//   the embedded peer's remote candidate table.
// - Offer m-line order is video only; video becomes BUNDLE mid:0.
//
// URL option:
//   iceMode=lan-only    : send UDP host candidates only; reject relay/srflx
//   iceMode=no-relay    : send UDP host + srflx candidates; reject relay
//   iceMode=all         : allow UDP host + srflx + relay; use KVS TURN
//   iceMode=relay-only  : send relay/UDP only; use KVS TURN
// Legacy defaults are kept for compatibility; runtime behavior is controlled by iceMode.
const USE_RELAY_ONLY = false;
const USE_KVS_TURN_SERVERS = false;
const USE_ONLY_ONE_TURN_SERVER_SET = true;
const ADD_KVS_STUN_SERVER = false;
const DEFER_ICE_CANDIDATES_UNTIL_OFFER_SENT = true;
const SEND_RELAY_UDP_ONLY = false;
const DROP_RELAY_CANDIDATES = true;
const MAX_RELAY_UDP_CANDIDATES_PER_MID = 1;
const ICE_SERVER_URLS_TURN_UDP_ONLY = true;
const VIEWER_HOST_CANDIDATE_IP_PARAM = "viewerIp";
const ICE_MODE_PARAM = "iceMode";
const DEFAULT_ICE_MODE = "no-relay";
const VALID_ICE_MODES = new Set(["lan-only", "no-relay", "all", "relay-only"]);
const MAX_LOCAL_UDP_CANDIDATES_PER_MID = 2;

function readIcePolicyFromUrl() {
  const params = new URLSearchParams(window.location.search);
  const requestedMode = (params.get(ICE_MODE_PARAM) || DEFAULT_ICE_MODE).trim().toLowerCase();
  const mode = VALID_ICE_MODES.has(requestedMode) ? requestedMode : DEFAULT_ICE_MODE;

  if (mode !== requestedMode) {
    log("WARN", "Ignored invalid iceMode URL parameter", { requestedMode, fallbackMode: mode });
  }

  return {
    mode,
    useRelayOnly: mode === "relay-only",
    useKvsTurnServers: mode === "all" || mode === "relay-only",
    dropRelay: mode === "lan-only" || mode === "no-relay",
    allowSrflx: mode === "no-relay" || mode === "all",
    allowRelay: mode === "all" || mode === "relay-only",
  };
}

function getCandidateText(candidate) {
  return candidate?.candidate ?? "";
}

function getCandidateType(candidate) {
  return getCandidateText(candidate).match(/ typ (\S+)/)?.[1] ?? "unknown";
}

function isUdpCandidate(candidate) {
  return getCandidateText(candidate).includes(" udp ");
}

function shouldSendIceCandidateForPolicy(candidate, policy) {
  const type = getCandidateType(candidate);

  if (!isUdpCandidate(candidate)) return false;
  if (type === "host") return true;
  if (type === "srflx") return policy.allowSrflx;
  if (type === "relay") return policy.allowRelay && isRelayUdpCandidate(candidate);
  return false;
}

function isRelayCandidate(candidateText) {
  return typeof candidateText === "string" && candidateText.includes(" typ relay ");
}


function isIceCandidateRelay(candidate) {
  return isRelayCandidate(candidate?.candidate ?? "");
}

function readViewerHostCandidateIpFromUrl() {
  const params = new URLSearchParams(window.location.search);
  const value = params.get(VIEWER_HOST_CANDIDATE_IP_PARAM)?.trim();
  if (!value) return "";

  const ipv4Pattern = /^(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}$/;
  if (!ipv4Pattern.test(value)) {
    log("WARN", "Ignored invalid viewerIp URL parameter", { viewerIp: value });
    return "";
  }
  return value;
}

function isMdnsCandidateAddress(address) {
  return typeof address === "string" && address.endsWith(".local");
}

function rewriteCandidateAddress(candidateText, replacementIp) {
  if (!replacementIp || typeof candidateText !== "string") return candidateText;

  const tokens = candidateText.split(" ");
  if (tokens.length < 8) return candidateText;

  const addressIndex = 4;
  const typeIndex = tokens.indexOf("typ");
  const candidateType = typeIndex >= 0 ? tokens[typeIndex + 1] : "";

  if (candidateType !== "host") return candidateText;
  if (!isMdnsCandidateAddress(tokens[addressIndex])) return candidateText;

  const originalAddress = tokens[addressIndex];
  tokens[addressIndex] = replacementIp;

  const rewritten = tokens.join(" ");
  log("INFO", "Rewrote mDNS host candidate address", { from: originalAddress, to: replacementIp });
  return rewritten;
}

function rewriteIceCandidateForSignaling(candidate, replacementIp) {
  if (!candidate || !replacementIp) return candidate;
  const originalCandidateText = candidate.candidate ?? "";
  const rewrittenCandidateText = rewriteCandidateAddress(originalCandidateText, replacementIp);
  if (rewrittenCandidateText === originalCandidateText) return candidate;
  return new RTCIceCandidate({
    candidate: rewrittenCandidateText,
    sdpMid: candidate.sdpMid,
    sdpMLineIndex: candidate.sdpMLineIndex,
    usernameFragment: candidate.usernameFragment,
  });
}

function rewriteMdnsCandidatesInSdp(sdp, replacementIp) {
  if (!replacementIp || typeof sdp !== "string") return sdp;
  return sdp
    .split(/\r\n|\n/)
    .map((line) => {
      if (!line.startsWith("a=candidate:")) return line;
      const rewrittenCandidate = rewriteCandidateAddress(line.slice("a=".length), replacementIp);
      return `a=${rewrittenCandidate}`;
    })
    .join("\r\n");
}

function rewriteLocalDescriptionForSignaling(desc, replacementIp) {
  if (!desc?.sdp || !replacementIp) return desc;
  const rewrittenSdp = rewriteMdnsCandidatesInSdp(desc.sdp, replacementIp);
  if (rewrittenSdp === desc.sdp) return desc;
  log("INFO", "Rewrote mDNS host candidates in local SDP offer", { viewerIp: replacementIp });
  return new RTCSessionDescription({ type: desc.type, sdp: rewrittenSdp });
}

function removeRelayCandidatesFromSdp(sdp) {
  if (typeof sdp !== "string") return sdp;
  return sdp
    .split(/\r\n|\n/)
    .filter((line) => !(line.startsWith("a=candidate:") && line.includes(" typ relay ")))
    .join("\r\n");
}

function sanitizeRemoteDescription(desc, dropRelayCandidates) {
  if (!dropRelayCandidates || !desc?.sdp) return desc;
  const filteredSdp = removeRelayCandidatesFromSdp(desc.sdp);
  if (filteredSdp === desc.sdp) return desc;
  log("INFO", "Dropped relay candidates from remote SDP");
  return new RTCSessionDescription({ type: desc.type, sdp: filteredSdp });
}
function isTurnUdpUrl(url) {
  return typeof url === "string" && url.startsWith("turn:") && url.includes("transport=udp");
}
function filterTurnUdpUrls(urls) {
  const list = Array.isArray(urls) ? urls : [urls];
  if (!ICE_SERVER_URLS_TURN_UDP_ONLY) return list.filter(Boolean);
  return list.filter(isTurnUdpUrl);
}

function getCandidateMid(candidate) {
  return candidate?.sdpMid ?? String(candidate?.sdpMLineIndex ?? "unknown");
}

function isRelayUdpCandidate(candidate) {
  const candidateText = candidate?.candidate ?? "";
  const url = candidate?.url ?? "";

  if (!candidateText.includes(" typ relay ")) return false;
  if (!candidateText.includes(" udp ")) return false;
  if (url) return isTurnUdpUrl(url);
  return true;
}

function shouldSendIceCandidate(candidate) {
  return !SEND_RELAY_UDP_ONLY || isRelayUdpCandidate(candidate);
}


function preferH264CodecForTransceiver(transceiver) {
  try {
    if (!transceiver?.setCodecPreferences || !RTCRtpSender?.getCapabilities) return;

    const capabilities = RTCRtpSender.getCapabilities("video");
    const codecs = capabilities?.codecs ?? [];
    const h264Codecs = codecs.filter((codec) =>
      codec.mimeType?.toLowerCase() === "video/h264"
    );

    if (!h264Codecs.length) {
      log("WARN", "No H264 codec capability found; keeping browser default codec list");
      return;
    }

    const baselinePacketizationMode1 = h264Codecs.filter((codec) => {
      const sdpFmtpLine = codec.sdpFmtpLine ?? "";
      return sdpFmtpLine.includes("profile-level-id=42001f") &&
        sdpFmtpLine.includes("packetization-mode=1");
    });

    const preferred = baselinePacketizationMode1.length ? baselinePacketizationMode1 : h264Codecs;
    transceiver.setCodecPreferences(preferred);
    log("INFO", "Applied H264-only codec preferences", preferred.map((codec) => ({
      mimeType: codec.mimeType,
      sdpFmtpLine: codec.sdpFmtpLine,
    })));
  } catch (e) {
    log("WARN", "Failed to apply H264-only codec preferences", e);
  }
}

function summarizeIceCandidate(candidate) {
  const candidateText = candidate?.candidate ?? "";
  const type = getCandidateType(candidate);
  return {
    sdpMid: candidate?.sdpMid,
    sdpMLineIndex: candidate?.sdpMLineIndex,
    type,
    url: candidate?.url,
    relayProtocol: candidate?.relayProtocol,
    isRelay: isRelayCandidate(candidateText),
    isTurnUdp: isTurnUdpUrl(candidate?.url ?? ""),
  };
}

function log(level, ...args) {
  const line = `[${new Date().toISOString()}] [${level}] ${args.map(formatLogArg).join(" ")}`;
  console[level === "ERROR" ? "error" : level === "WARN" ? "warn" : "log"](...args);
  if (els.logs) {
    els.logs.textContent += `${line}\n`;
    els.logs.scrollTop = els.logs.scrollHeight;
  }
}

function formatLogArg(arg) {
  if (arg instanceof Error) return `${arg.name}: ${arg.message}`;
  if (typeof arg === "string") return arg;
  try {
    return JSON.stringify(arg);
  } catch {
    return String(arg);
  }
}

function setText(el, text) {
  if (el) el.textContent = text;
}

function readConfig() {
  const region = els.region?.value.trim() || "us-west-2";
  const accessKeyId = els.accessKeyId?.value.trim();
  const secretAccessKey = els.secretAccessKey?.value.trim();
  const sessionToken = els.sessionToken?.value.trim();
  const channelArn = els.channelArn?.value.trim();
  const clientId = els.clientId?.value.trim() || crypto.randomUUID();

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
    if (value && el) el.value = value;
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
    this.sdpOfferSent = false;
    this.queuedLocalIceCandidates = [];
    this.sentRelayUdpCandidateCountsByMid = new Map();
    this.sentLocalCandidateCountsByMid = new Map();
    this.viewerHostCandidateIp = "";
    this.icePolicy = readIcePolicyFromUrl();
  }

  async start(config) {
    this.icePolicy = readIcePolicyFromUrl();
    this.viewerHostCandidateIp = readViewerHostCandidateIpFromUrl();

    log("INFO", "Starting viewer", {
      region: config.region,
      channelArn: config.channelArn,
      clientId: config.clientId,
      viewerHostCandidateIp: this.viewerHostCandidateIp || "(not set)",
      iceMode: this.icePolicy.mode,
    });
    this.resetCandidateGate();
    setText(els.signalingStatus, "resolving endpoints");

    const endpoints = await this.getSignalingEndpoints(config);
    const iceServers = await this.getIceServers(config, endpoints.httpsEndpoint);

    this.pc = new RTCPeerConnection({
      iceServers,
      iceTransportPolicy: this.icePolicy.useRelayOnly ? "relay" : "all",
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
      try {
        await this.frameReader.cancel();
      } catch (_) {}
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
    this.resetCandidateGate();
    if (els.remoteVideo) els.remoteVideo.srcObject = null;
    setText(els.signalingStatus, "idle");
    setText(els.iceStatus, "idle");
  }

  async getSignalingEndpoints(config) {
    const kv = new KinesisVideoClient({
      region: config.region,
      credentials: config.credentials,
    });

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
    if (!this.icePolicy.useKvsTurnServers) {
      const iceServers = [];
      if (ADD_KVS_STUN_SERVER) {
        iceServers.push({ urls: [`stun:stun.kinesisvideo.${config.region}.amazonaws.com:443`] });
      }
      log("INFO", "Loaded ICE servers", {
        policy: this.icePolicy.mode,
        useKvsTurnServers: false,
        addKvsStunServer: ADD_KVS_STUN_SERVER,
        deferIceCandidatesUntilOfferSent: DEFER_ICE_CANDIDATES_UNTIL_OFFER_SENT,
        sendCandidates: this.icePolicy.mode,
        serverCount: iceServers.length,
        urls: iceServers.flatMap((s) => s.urls),
      });
      return iceServers;
    }

    const kvs = new KinesisVideoSignalingClient({
      region: config.region,
      credentials: config.credentials,
      endpoint: httpsEndpoint,
    });

    const response = await kvs.send(new GetIceServerConfigCommand({ ChannelARN: config.channelArn }));
    const iceServers = [];

    if (ADD_KVS_STUN_SERVER) {
      iceServers.push({
        urls: [`stun:stun.kinesisvideo.${config.region}.amazonaws.com:443`],
      });
    }

    const rawTurnServerList = response.IceServerList ?? [];
    const selectedTurnServerList = USE_ONLY_ONE_TURN_SERVER_SET
      ? rawTurnServerList.slice(0, 1)
      : rawTurnServerList;

    const turnServers = selectedTurnServerList
      .map((s) => ({
        urls: filterTurnUdpUrls(s.Uris ?? []),
        username: s.Username,
        credential: s.Password,
      }))
      .filter((s) => s.urls.length > 0);

    iceServers.push(...turnServers);

    if (this.icePolicy.useKvsTurnServers && iceServers.length === 0) {
      throw new Error("No ICE server was returned by KVS.");
    }

    log("INFO", "Loaded ICE servers", {
      policy: this.icePolicy.mode,
      useKvsTurnServers: this.icePolicy.useKvsTurnServers,
      useOnlyOneTurnServerSet: USE_ONLY_ONE_TURN_SERVER_SET,
      addKvsStunServer: ADD_KVS_STUN_SERVER,
      deferIceCandidatesUntilOfferSent: DEFER_ICE_CANDIDATES_UNTIL_OFFER_SENT,
      sendCandidates: this.icePolicy.mode,
      maxRelayUdpCandidatesPerMid: MAX_RELAY_UDP_CANDIDATES_PER_MID,
      serverCount: iceServers.length,
      urls: iceServers.flatMap((s) => s.urls),
    });

    return iceServers;
  }

  resetCandidateGate() {
    this.sdpOfferSent = false;
    this.queuedLocalIceCandidates = [];
    this.sentRelayUdpCandidateCountsByMid = new Map();
    this.sentLocalCandidateCountsByMid = new Map();
  }

  handleLocalIceCandidate(candidate) {
    const candidateForSignaling = rewriteIceCandidateForSignaling(
      candidate,
      this.viewerHostCandidateIp
    );
    const summary = summarizeIceCandidate(candidateForSignaling);

    if (!shouldSendIceCandidateForPolicy(candidateForSignaling, this.icePolicy)) {
      log("INFO", "Dropped local ICE candidate by policy", { iceMode: this.icePolicy.mode, ...summary });
      return;
    }
    if (DEFER_ICE_CANDIDATES_UNTIL_OFFER_SENT && !this.sdpOfferSent) {
      this.queuedLocalIceCandidates.push(candidateForSignaling);
      log("INFO", "Queued local ICE candidate until SDP offer is sent", summary);
      return;
    }
    this.sendLocalIceCandidateIfAllowed(candidateForSignaling, "Sent local ICE candidate");
  }

  sendLocalIceCandidateIfAllowed(candidate, logMessage) {
    const mid = getCandidateMid(candidate);
    const candidateType = getCandidateType(candidate);

    if (!isRelayUdpCandidate(candidate)) {
      const sentCount = this.sentLocalCandidateCountsByMid.get(mid) ?? 0;
      if (sentCount >= MAX_LOCAL_UDP_CANDIDATES_PER_MID) {
        log("INFO", "Dropped extra local UDP ICE candidate for same mid", {
          mid,
          sentCount,
          maxPerMid: MAX_LOCAL_UDP_CANDIDATES_PER_MID,
          ...summarizeIceCandidate(candidate),
        });
        return false;
      }

      this.sentLocalCandidateCountsByMid.set(mid, sentCount + 1);
      this.signalingClient.sendIceCandidate(candidate);
      log("INFO", logMessage, { mid, sentCount: sentCount + 1, candidateType, ...summarizeIceCandidate(candidate) });
      return true;
    }

    const sentCount = this.sentRelayUdpCandidateCountsByMid.get(mid) ?? 0;

    if (sentCount >= MAX_RELAY_UDP_CANDIDATES_PER_MID) {
      log("INFO", "Dropped extra relay/udp local ICE candidate for same mid", {
        mid,
        sentCount,
        maxPerMid: MAX_RELAY_UDP_CANDIDATES_PER_MID,
        ...summarizeIceCandidate(candidate),
      });
      return false;
    }

    this.sentRelayUdpCandidateCountsByMid.set(mid, sentCount + 1);
    this.signalingClient.sendIceCandidate(candidate);
    log("INFO", logMessage, {
      mid,
      sentCount: sentCount + 1,
      candidateType,
      ...summarizeIceCandidate(candidate),
    });
    return true;
  }

  flushQueuedLocalIceCandidates() {
    if (!this.queuedLocalIceCandidates.length) {
      log("INFO", "No queued local ICE candidates to flush after SDP offer");
      return;
    }

    const candidates = this.queuedLocalIceCandidates;
    this.queuedLocalIceCandidates = [];

    for (const candidate of candidates) {
      this.sendLocalIceCandidateIfAllowed(candidate, "Flushed queued local ICE candidate after SDP offer");
    }
  }

  installPeerConnectionHandlers() {
    this.pc.ontrack = (event) => {
      log("INFO", "Received remote track", { kind: event.track.kind });

      if (!this.remoteStream) {
        this.remoteStream = new MediaStream();
        if (els.remoteVideo) els.remoteVideo.srcObject = this.remoteStream;
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
      this.handleLocalIceCandidate(event.candidate);
    };

    this.pc.oniceconnectionstatechange = () => {
      setText(els.iceStatus, this.pc.iceConnectionState);
      log("INFO", "ICE connection state", this.pc.iceConnectionState);
    };

    this.pc.onconnectionstatechange = () => {
      log("INFO", "Peer connection state", this.pc.connectionState);
    };

    this.pc.onicegatheringstatechange = () => {
      log("INFO", "ICE gathering state", this.pc.iceGatheringState);
    };
  }

  installSignalingHandlers() {
    this.signalingClient.on("open", async () => {
      log("INFO", "Signaling connection opened");
      setText(els.signalingStatus, "open");

      // Video-only offer: make video the BUNDLE mid:0 transport.
      // This avoids an inactive audio m-line becoming the bundle-tag.
      const videoTransceiver = this.pc.addTransceiver("video", { direction: "recvonly" });
      preferH264CodecForTransceiver(videoTransceiver);

      const offer = await this.pc.createOffer();
      await this.pc.setLocalDescription(offer);
      const offerForSignaling = rewriteLocalDescriptionForSignaling(
        this.pc.localDescription,
        this.viewerHostCandidateIp
      );
      this.signalingClient.sendSdpOffer(offerForSignaling);
      this.sdpOfferSent = true;

      log("INFO", "Sent video-only H264-only recvonly SDP offer", { mLineOrder: ["video"] });
      this.flushQueuedLocalIceCandidates();
    });

    this.signalingClient.on("sdpAnswer", async (answer) => {
      try {
        await this.pc.setRemoteDescription(sanitizeRemoteDescription(answer, this.icePolicy.dropRelay));
        log("INFO", "Applied SDP answer");
      } catch (e) {
        log("ERROR", "setRemoteDescription failed", e);
        throw e;
      }
    });

    this.signalingClient.on("iceCandidate", async (candidate) => {
      try {
        if (this.icePolicy.dropRelay && isIceCandidateRelay(candidate)) {
          log("INFO", "Dropped remote relay ICE candidate", summarizeIceCandidate(candidate));
          return;
        }
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
    if (!els.recorderWsUrl) return;
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

    const recorderId = els.recorderId?.value.trim() || "1";
    const fps = Number.parseInt(els.fps?.value, 10) || 30;
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
      const recorderId = els.recorderId?.value.trim() || "1";
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
      try {
        await this.frameReader?.cancel();
      } catch (_) {}
      this.frameReader = null;
    }
  }

  getSegmentTimeSeconds() {
    if (!els.segmentEnabled?.checked) return 0;
    const v = Number.parseInt(els.segmentTime?.value, 10);
    return Number.isFinite(v) && v > 0 ? v : 0;
  }

  updateButtons() {
    if (els.recStartBtn) {
      els.recStartBtn.disabled = !this.wsConnected || !this.videoTrack || this.recording;
    }
    if (els.recStopBtn) {
      els.recStopBtn.disabled = !this.recording;
    }
  }
}

const viewer = new KvsViewer();
const recorder = new RecorderBridge();

function updateSegmentUI() {
  if (!els.segmentEnabled || !els.segmentTime || !els.segmentTimeField) return;
  const enabled = els.segmentEnabled.checked;
  els.segmentTimeField.classList.toggle("segment-disabled", !enabled);
  els.segmentTime.disabled = !enabled;
}

applyUrlParams();
updateSegmentUI();
recorder.connect();

if (els.startViewer) {
  els.startViewer.addEventListener("click", async () => {
    try {
      els.startViewer.disabled = true;
      if (els.stopViewer) els.stopViewer.disabled = false;
      await viewer.start(readConfig());
    } catch (e) {
      log("ERROR", "Failed to start viewer", e);
      els.startViewer.disabled = false;
      if (els.stopViewer) els.stopViewer.disabled = true;
      setText(els.signalingStatus, "error");
    }
  });
}

if (els.stopViewer) {
  els.stopViewer.addEventListener("click", async () => {
    await viewer.stop();
    if (els.startViewer) els.startViewer.disabled = false;
    els.stopViewer.disabled = true;
    recorder.updateButtons();
  });
}

if (els.recStartBtn) els.recStartBtn.addEventListener("click", () => recorder.startRecording());
if (els.recStopBtn) els.recStopBtn.addEventListener("click", () => recorder.stopRecording());
if (els.segmentEnabled) els.segmentEnabled.addEventListener("change", updateSegmentUI);
if (els.clearLogs) els.clearLogs.addEventListener("click", () => { els.logs.textContent = ""; });

window.addEventListener("beforeunload", () => {
  recorder.stopRecording();
  viewer.stop();
});
