// https://help.aliyun.com/zh/model-studio/fun-asr-recorded-speech-recognition-http-api
// https://help.aliyun.com/zh/model-studio/qwen-audio-tts-http-api
// Prices are administrator-configured task expressions, in USD.
const ASR_MODELS = ["fun-asr", "paraformer-v2"];
const TTS_MODELS = ["qwen-audio-3.0-tts-flash", "qwen-audio-3.0-tts-plus"];
const ASR_USAGE = { audio_seconds: { type: "number", unit: "second", description: { en: "Speech recognition unit price", zh: "语音识别单价" } } };
const TTS_USAGE = { characters: { type: "number", unit: "character", description: { en: "Speech synthesis unit price", zh: "语音合成单价" } } };

export const meta = {
  apiVersion: 1, key: "alibaba-speech", name: "Alibaba Speech", icon: "Alibaba.Color",
  version: "1.0.0", author: { name: "QuantumNous" },
  description: { en: "Alibaba file speech recognition and non-realtime speech synthesis", zh: "阿里文件语音识别与非实时语音合成" },
  baseUrl: "https://dashscope.aliyuncs.com", fetchMode: "per_task",
  models: [...TTS_MODELS, ...ASR_MODELS],
  protocols: [{ name: "openai_speech", supports: ["sync"], models: TTS_MODELS }],
  routes: [
    { method: "POST", path: "/alibaba-speech/v1/audio/transcriptions", type: "submit", models: ASR_MODELS, decode: "transcribe", render: "taskCreated" },
    { method: "POST", path: "/alibaba-speech/v1/audio/speech", type: "submit", models: TTS_MODELS, decode: "synthesize", render: "taskCreated" },
    { method: "GET", path: "/alibaba-speech/v1/tasks/:task_id", type: "query", render: "taskStatus" },
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
  if (typeof value !== "string" || value.length > 65536 || !/^https?:\/\/[^\s/@?#]+(?::\d+)?(?:[/?][^\s#]*)?$/.test(value)) {
    throw new Error("An absolute HTTP(S) audio URL without credentials or fragment is required");
  }
  return value;
}

function asrRequest(value) {
  const body = requestObject(value, ["model", "audio_url", "language"]);
  if (body.language !== undefined && (typeof body.language !== "string" || !/^[a-z]{2,3}(?:-[A-Za-z]{2,4})?$/.test(body.language))) throw new Error("Invalid recognition language");
  return { audio_url: mediaURL(body.audio_url), ...(body.language === undefined ? {} : { language: body.language }) };
}

// Alibaba's character-priced TTS counts Han twice. SSML and text-replacement
// options are deliberately not accepted: the metered text must be the sent text.
function characters(text) {
  if (typeof text !== "string" || !text.trim()) throw new Error("input must be a non-empty string");
  let count = 0;
  let length = 0;
  for (const ch of text) {
    const cp = ch.codePointAt(0);
    if (++length > 20000 || (cp >= 0xd800 && cp <= 0xdfff)) throw new Error("input must contain at most 20000 valid Unicode characters");
    count += /\p{Script=Han}/u.test(ch) ? 2 : 1;
  }
  return count;
}

function speechRequest(value) {
  const body = requestObject(value, ["model", "input", "voice", "speed", "response_format", "stream", "stream_format", "background"]);
  characters(body.input);
  if (typeof body.voice !== "string" || !body.voice.trim() || body.voice.length > 256) throw new Error("voice must be an Alibaba voice ID");
  if (body.stream !== undefined && body.stream !== false) throw new Error("Only non-realtime speech synthesis is supported");
  if (body.background !== undefined && body.background !== false) throw new Error("Use the native speech task endpoint for asynchronous synthesis");
  if (body.stream_format !== undefined && body.stream_format !== "audio") throw new Error("stream_format must be audio");
  if (body.response_format !== undefined && body.response_format !== "mp3") throw new Error("response_format must be mp3");
  const speed = body.speed === undefined ? 1 : body.speed;
  if (typeof speed !== "number" || !Number.isFinite(speed) || speed < 0.5 || speed > 2) throw new Error("speed must be between 0.5 and 2");
  return { input: body.input, voice: body.voice.trim(), speed: speed };
}

function decode(ctx, action, models) {
  if (!ctx.body || ctx.body.kind !== "json") throw new Error("JSON body required; file uploads are not supported");
  const body = ctx.body.value;
  const model = ctx.upstreamModel || ctx.model || body.model;
  if (!models.includes(model)) throw new Error("Unsupported Alibaba speech model");
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

export function buildSubmitRequest(ctx) {
  const model = ctx.upstreamModel || ctx.model;
  const headers = { "Content-Type": "application/json", Authorization: "Bearer " + ctx.apiKey };
  const base = ctx.baseUrl.replace(/\/+$/, "");
  if (ctx.action === "speech_to_text" && ASR_MODELS.includes(model)) {
    const body = asrRequest(ctx.requestBody);
    utils.audioDuration(body.audio_url); // Validate the file before even per-call billing.
    headers["X-DashScope-Async"] = "enable";
    return { url: base + "/api/v1/services/audio/asr/transcription", method: "POST", headers: headers,
      body: { model: model, input: { file_urls: [body.audio_url] }, parameters: { channel_id: [0], ...(body.language ? { language_hints: [body.language] } : {}) } } };
  }
  if (ctx.action !== "text_to_speech" || !TTS_MODELS.includes(model)) throw new Error("Unsupported Alibaba speech model or action");
  const body = speechRequest(ctx.requestBody);
  return { url: base + "/api/v1/services/audio/tts/SpeechSynthesizer", method: "POST", headers: headers,
    body: { model: model, input: { text: body.input, voice: body.voice, format: "mp3", sample_rate: 24000, rate: body.speed } } };
}

export function extractUsage(ctx) {
  if (ctx.usagePurpose === "billing_ratios") return null;
  if (ctx.action === "speech_to_text") return { audio_seconds: Math.ceil(utils.audioDuration(asrRequest(ctx.requestBody).audio_url)) };
  return { characters: characters(speechRequest(ctx.requestBody).input) };
}

export function parseSubmitResponse(ctx, response) {
  const body = response.body;
  if (!body || typeof body !== "object" || Array.isArray(body) || body.code || !body.output) throw new Error("Alibaba speech submission failed or returned an invalid response");
  if (ctx.action === "speech_to_text") {
    const id = body.output.task_id;
    if (typeof id !== "string" || !/^[A-Za-z0-9_-]{1,256}$/.test(id) || !["PENDING", "RUNNING", "SUCCEEDED"].includes(body.output.task_status)) throw new Error("Alibaba returned no valid transcription task");
    return { taskId: id, taskData: { output: { task_id: id, task_status: body.output.task_status } } };
  }
  const audio = body.output.audio;
  if (!audio) throw new Error("Alibaba returned no completed audio");
  const usage = body.usage && body.usage.characters;
  return { taskId: ctx.publicTaskId, taskData: { audio_url: mediaURL(audio.url), audio_format: "mp3", usage_characters: validCharacters(usage) ? usage : null },
    immediate: { status: "SUCCESS", progress: "100%" } };
}

export function buildQueryRequest(ctx) {
  if (ctx.action !== "speech_to_text") throw new Error("Synchronous Alibaba speech has no remote task to poll");
  return { url: ctx.baseUrl.replace(/\/+$/, "") + "/api/v1/tasks/" + encodeURIComponent(ctx.taskId), method: "GET", headers: { Authorization: "Bearer " + ctx.apiKey } };
}

export function parseTaskResult(_ctx, body) {
  const output = body && body.output;
  if (!output) return { status: "UNKNOWN" };
  if (["FAILED", "CANCELED"].includes(output.task_status)) return { status: "FAILURE", reason: "Alibaba speech recognition failed" };
  if (output.task_status === "PENDING") return { status: "QUEUED" };
  if (output.task_status === "RUNNING") return { status: "IN_PROGRESS" };
  if (output.task_status !== "SUCCEEDED") return { status: "UNKNOWN" };
  // The enclosing job can say SUCCEEDED even when its sole file failed.
  const results = output.results;
  if (!Array.isArray(results) || results.length !== 1) return { status: "UNKNOWN" };
  if (results[0].subtask_status === "FAILED") return { status: "FAILURE", reason: "Alibaba audio file transcription failed" };
  if (results[0].subtask_status !== "SUCCEEDED") return { status: "UNKNOWN" };
  return { status: "SUCCESS", progress: "100%", url: mediaURL(results[0].transcription_url) };
}

function validCharacters(value) { return typeof value === "number" && Number.isInteger(value) && value >= 0 && value <= 40000; }

export function extractUsageOnComplete(ctx, _result, data) {
  if (ctx.action === "speech_to_text") {
    const seconds = data && data.usage && data.usage.duration;
    return typeof seconds === "number" && Number.isFinite(seconds) && seconds >= 0 && seconds <= 3600 ? { audio_seconds: seconds } : null;
  }
  return data && validCharacters(data.usage_characters) ? { characters: data.usage_characters } : null;
}

export function listArtifacts(task) {
  if (task.status !== "SUCCESS" || !task.data) return [];
  if (task.action === "speech_to_text") return [{ key: "transcript", type: "file", mimeType: "application/json" }];
  return task.data.audio_url ? [{ key: "audio", type: "audio", mimeType: "audio/mpeg" }] : [];
}

export function buildContentRequest(ctx) {
  if (ctx.artifactKey === "audio" && ctx.data && ctx.data.audio_url) return { url: mediaURL(ctx.data.audio_url), method: "GET", credentialless: true };
  const results = ctx.data && ctx.data.output && ctx.data.output.results;
  if (ctx.artifactKey === "transcript" && Array.isArray(results) && results.length === 1 && results[0].subtask_status === "SUCCEEDED") {
    return { url: mediaURL(results[0].transcription_url), method: "GET", credentialless: true };
  }
  throw new Error("Speech artifact is unavailable");
}
