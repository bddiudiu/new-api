// https://docs.volcengine.com/docs/DoubaoVoice/task-submission-http-1?lang=zh
// https://docs.volcengine.com/docs/DoubaoVoice/Tasksubmission?lang=zh
const ASR_MODELS = ["volc.seedasr.auc", "volc.bigasr.auc"];
const TTS_MODELS = ["seed-tts-2.0"];
const ASR_USAGE = { audio_seconds: { type: "number", unit: "second", description: { en: "Speech recognition unit price", zh: "语音识别单价" } } };
const TTS_USAGE = { characters: { type: "number", unit: "character", description: { en: "Speech synthesis unit price", zh: "语音合成单价" } } };

export const meta = {
  apiVersion: 1, key: "volcengine-speech", name: "Volcengine Speech", icon: "Doubao.Color",
  version: "1.0.0", author: { name: "QuantumNous" },
  description: { en: "Volcengine file speech recognition and non-realtime speech synthesis", zh: "火山文件语音识别与非实时语音合成" },
  baseUrl: "https://openspeech.bytedance.com", fetchMode: "per_task",
  models: [...TTS_MODELS, ...ASR_MODELS],
  protocols: [{ name: "openai_speech", supports: ["sync"], models: TTS_MODELS }],
  routes: [
    { method: "POST", path: "/volcengine-speech/v1/audio/transcriptions", type: "submit", models: ASR_MODELS, decode: "transcribe", render: "taskCreated" },
    { method: "POST", path: "/volcengine-speech/v1/audio/speech", type: "submit", models: TTS_MODELS, decode: "synthesize", render: "taskCreated" },
    { method: "GET", path: "/volcengine-speech/v1/tasks/:task_id", type: "query", render: "taskStatus" },
  ],
  usageSchema: TTS_USAGE,
  usageExamples: [{ label: "10,000 characters", facts: { characters: 10000 } }],
  usageProfiles: [{ models: ASR_MODELS, schema: ASR_USAGE, examples: [{ label: "60 seconds", facts: { audio_seconds: 60 } }] }],
};

function requestObject(body, fields) {
  if (!body || typeof body !== "object" || Array.isArray(body)) throw new Error("JSON object required");
  for (const key of Object.keys(body)) if (!fields.includes(key)) throw new Error("Unsupported speech option: " + key);
  return body;
}

function mediaURL(value) {
  if (typeof value !== "string" || value.length > 65536 || !/^https?:\/\/[^\s/@?#]+(?::\d+)?(?:[/?][^\s#]*)?$/.test(value)) throw new Error("An absolute HTTP(S) audio URL without credentials or fragment is required");
  return value;
}

function asrRequest(value) {
  const body = requestObject(value, ["model", "audio_url", "format", "language"]);
  const url = mediaURL(body.audio_url);
  const extension = /\.(wav|mp3|ogg)(?:\?|$)/i.exec(url);
  const format = body.format === undefined ? (extension ? extension[1].toLowerCase() : "") : body.format;
  if (!["wav", "mp3", "ogg"].includes(format)) throw new Error("format must be wav, mp3 or ogg");
  if (body.language !== undefined && (typeof body.language !== "string" || !/^[a-z]{2,3}(?:-[A-Za-z]{2,4})?$/.test(body.language))) throw new Error("Invalid recognition language");
  return { audio_url: url, format: format, ...(body.language === undefined ? {} : { language: body.language }) };
}

// This plugin prices input Unicode characters, including punctuation and spaces.
// Do not borrow MiniMax/Alibaba's Han-double rule. Query req_text_length replaces
// the estimate. synthesize_text_length is retained for audit, not assumed to be
// the billed amount (the async API does not document it as a billing field).
function characters(text) {
  if (typeof text !== "string" || !text.trim()) throw new Error("input must be a non-empty string");
  let count = 0;
  for (const ch of text) {
    const cp = ch.codePointAt(0);
    if (++count > 20000 || (cp >= 0xd800 && cp <= 0xdfff) || (cp < 32 && ch !== "\t" && ch !== "\n")) throw new Error("input must contain at most 20000 valid Unicode characters");
  }
  return count;
}

function speechRequest(value) {
  const body = requestObject(value, ["model", "input", "voice", "speed", "response_format", "stream", "stream_format", "background"]);
  characters(body.input);
  if (typeof body.voice !== "string" || !body.voice.trim() || body.voice.length > 256) throw new Error("voice must be a Volcengine speaker ID");
  if (body.stream !== undefined && body.stream !== false) throw new Error("Only non-realtime speech synthesis is supported");
  if (body.background !== undefined && body.background !== false) throw new Error("Use the native speech task endpoint for asynchronous synthesis");
  if (body.stream_format !== undefined && body.stream_format !== "audio") throw new Error("stream_format must be audio");
  if (body.response_format !== undefined && body.response_format !== "mp3") throw new Error("response_format must be mp3");
  // V3 uses an integer speech_rate, not OpenAI's speed multiplier. Keep the
  // neutral rate until a complete speed conversion contract is supported.
  if (body.speed !== undefined && body.speed !== 1) throw new Error("Volcengine asynchronous speech currently supports speed 1 only");
  return { input: body.input, voice: body.voice.trim() };
}

function decode(ctx, action, models) {
  if (!ctx.body || ctx.body.kind !== "json") throw new Error("JSON body required; file uploads are not supported");
  const body = ctx.body.value;
  const model = ctx.upstreamModel || ctx.model || body.model;
  if (!models.includes(model)) throw new Error("Unsupported Volcengine speech model");
  return { kind: "submit", model: ctx.model || body.model, action: action, requestBody: action === "speech_to_text" ? asrRequest(body) : speechRequest(body) };
}

export const native = {
  transcribe(ctx) { return decode(ctx, "speech_to_text", ASR_MODELS); },
  synthesize(ctx) { return decode(ctx, "text_to_speech", TTS_MODELS); },
  taskCreated(_ctx, task) { return { task_id: task.task_id, status: task.status, data: task.data }; },
  taskStatus(_ctx, task) { return { task_id: task.task_id, status: task.status, data: task.data }; },
};

export const protocols = {
  openai_speech: {
    decodeRequest(ctx) {
      if (ctx.stream) throw new Error("Streaming speech is not supported");
      return decode(ctx, "text_to_speech", TTS_MODELS);
    },
    render(_ctx, task) {
      if (task.status !== "SUCCESS") throw new Error("Speech is not ready");
      return { artifactKey: "audio" };
    },
  },
};

function requestID(ctx) {
  const hex = utils.hmacSHA256(ctx.publicTaskId, "new-api:volcengine-speech").slice(0, 32);
  return hex.slice(0, 8) + "-" + hex.slice(8, 12) + "-" + hex.slice(12, 16) + "-" + hex.slice(16, 20) + "-" + hex.slice(20);
}

function vendorHeaders(ctx) {
  return { "Content-Type": "application/json", "X-Api-Key": ctx.apiKey, "X-Api-Resource-Id": ctx.upstreamModel || ctx.model };
}

export function buildSubmitRequest(ctx) {
  const model = ctx.upstreamModel || ctx.model;
  const headers = vendorHeaders(ctx);
  headers["X-Api-Request-Id"] = requestID(ctx);
  const base = ctx.baseUrl.replace(/\/+$/, "");
  if (ctx.action === "speech_to_text" && ASR_MODELS.includes(model)) {
    const body = asrRequest(ctx.requestBody);
    utils.audioDuration(body.audio_url);
    headers["X-Api-Sequence"] = "-1";
    return { url: base + "/api/v3/auc/bigmodel/submit", method: "POST", headers: headers,
      body: { audio: { url: body.audio_url, format: body.format, ...(body.language ? { language: body.language } : {}) }, request: { model_name: "bigmodel", enable_itn: true, enable_punc: true, show_utterances: false } } };
  }
  if (ctx.action !== "text_to_speech" || !TTS_MODELS.includes(model)) throw new Error("Unsupported Volcengine speech model or action");
  const body = speechRequest(ctx.requestBody);
  return { url: base + "/api/v3/tts/submit", method: "POST", headers: headers,
    body: { unique_id: requestID(ctx), req_params: { text: body.input, speaker: body.voice, audio_params: { format: "mp3", sample_rate: 24000 } } } };
}

export function extractUsage(ctx) {
  if (ctx.usagePurpose === "billing_ratios") return null;
  if (ctx.action === "speech_to_text") return { audio_seconds: utils.audioDuration(asrRequest(ctx.requestBody).audio_url) };
  return { characters: characters(speechRequest(ctx.requestBody).input) };
}

function asrStatus(body, headers) {
  for (const key of Object.keys(headers || {})) {
    if (key.toLowerCase() === "x-api-status-code") {
      const value = headers[key];
      return String(Array.isArray(value) ? value[0] : value);
    }
  }
  return body && body["X-Api-Status-Code"] !== undefined ? String(body["X-Api-Status-Code"]) : "";
}

export function parseSubmitResponse(ctx, response) {
  const body = response.body;
  let id;
  if (ctx.action === "speech_to_text") {
    if (asrStatus(body, response.headers) !== "20000000") throw new Error("Volcengine speech recognition submission failed");
    id = body && body.task_id ? body.task_id : requestID(ctx);
  } else {
    if (!body || body.code !== 20000000 || !body.data) throw new Error("Volcengine speech synthesis submission failed");
    id = body.data.task_id;
  }
  if (typeof id !== "string" || !/^[A-Za-z0-9_-]{1,256}$/.test(id)) throw new Error("Volcengine returned an invalid speech task ID");
  return { taskId: id, taskData: { task_id: id } };
}

export function buildQueryRequest(ctx) {
  const headers = vendorHeaders(ctx);
  const base = ctx.baseUrl.replace(/\/+$/, "");
  if (ctx.action === "speech_to_text") {
    headers["X-Api-Request-Id"] = ctx.taskId;
    return { url: base + "/api/v3/auc/bigmodel/query", method: "POST", headers: headers, body: {} };
  }
  return { url: base + "/api/v3/tts/query", method: "POST", headers: headers, body: { task_id: ctx.taskId } };
}

export function parseTaskResult(ctx, body, response) {
  if (ctx.action === "speech_to_text") {
    const code = asrStatus(body, response && response.headers);
    if (code === "20000001") return { status: "IN_PROGRESS" };
    if (code === "20000002") return { status: "QUEUED" };
    if (code === "20000003") return { status: "SUCCESS", progress: "100%" }; // Accepted silent audio, no transcript.
    if (code === "20000000") return body && body.result ? { status: "SUCCESS", progress: "100%" } : { status: "UNKNOWN" };
    if (/^45\d{6}$/.test(code)) return { status: "FAILURE", reason: "Volcengine speech recognition failed" };
    return { status: "UNKNOWN" }; // Retry transient 55xxxxxx errors through host polling.
  }
  if (!body || body.code !== 20000000 || !body.data) return { status: "UNKNOWN" };
  if (body.data.task_status === 1) return { status: "IN_PROGRESS" };
  if (body.data.task_status === 3) return { status: "FAILURE", reason: "Volcengine speech synthesis failed" };
  if (body.data.task_status !== 2) return { status: "UNKNOWN" };
  return { status: "SUCCESS", progress: "100%", url: mediaURL(body.data.audio_url),
    taskData: { code: body.code, data: { task_id: body.data.task_id, task_status: 2, audio_url: body.data.audio_url,
      url_expire_time: body.data.url_expire_time, req_text_length: body.data.req_text_length, synthesize_text_length: body.data.synthesize_text_length } } };
}

export function extractUsageOnComplete(ctx, _result, body) {
  if (ctx.action === "speech_to_text") {
    const ms = body && body.audio_info && body.audio_info.duration;
    return typeof ms === "number" && Number.isFinite(ms) && ms >= 0 && ms <= 3600000 ? { audio_seconds: ms / 1000 } : null;
  }
  const count = body && body.data && body.data.req_text_length;
  return typeof count === "number" && Number.isInteger(count) && count >= 0 && count <= 20000 ? { characters: count } : null;
}

export function listArtifacts(task) {
  return task.status === "SUCCESS" && task.action === "text_to_speech" && task.data && task.data.data && task.data.data.audio_url
    ? [{ key: "audio", type: "audio", mimeType: "audio/mpeg" }] : [];
}

export function buildContentRequest(ctx) {
  const data = ctx.data && ctx.data.data;
  if (ctx.artifactKey !== "audio" || !data || !data.audio_url) throw new Error("Speech audio is unavailable");
  if (typeof data.url_expire_time !== "number" || data.url_expire_time <= utils.unixNow() + 30) {
    if (ctx.contentRefreshed) throw new Error("Volcengine returned an expired audio URL");
    return { refreshTask: true };
  }
  return { url: mediaURL(data.audio_url), method: "GET", credentialless: true };
}
