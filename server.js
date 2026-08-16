const express = require("express");
const http = require("http");
const path = require("path");
const fs = require("fs");
const { google } = require("googleapis");
const WebSocket = require("ws");
const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");

const PORT = process.env.PORT || 3000;
const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const PROTO_PATH = path.join(__dirname, "stream_list.proto");
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: false,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});
const youtubeProto = grpc.loadPackageDefinition(packageDefinition).youtube.api.v3;

const state = {
  active: false,
  broadcastId: "",
  chatId: "",
  keyword: "ارواح!",
  participants: [],
  history: new Set(),
  queue: [],
  streamCall: null,
  competitionStartedAt: 0,
  status: "متوقف"
};

function publicState() {
  return {
    active: state.active,
    broadcastId: state.broadcastId,
    keyword: state.keyword,
    participants: state.participants.map((p) => ({
      youtubeName: p.youtubeName,
      comment: p.comment
    })),
    queued: state.queue.length,
    historyCount: state.history.size,
    status: state.status
  };
}

function broadcast() {
  const data = JSON.stringify({ type: "state", state: publicState() });
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) client.send(data);
  }
}

function cleanText(value, maxLength = 500) {
  return String(value || "").replace(/\s+/g, " ").trim().slice(0, maxLength);
}

function normalized(value) {
  return String(value || "").trim().toLocaleLowerCase("ar");
}

function registrationComment(text) {
  const comment = cleanText(text);
  const keyword = cleanText(state.keyword, 100);
  if (!comment || !keyword) return null;

  const c = normalized(comment);
  const k = normalized(keyword);

  if (!c.startsWith(k)) return null;

  const next = comment.charAt(keyword.length);
  if (next && !/\s/.test(next)) return null;

  return comment;
}

function addParticipant(displayName, userId, comment) {
  const youtubeName = cleanText(displayName, 100);
  const fullComment = cleanText(comment, 500);
  if (!youtubeName || !fullComment) return false;

  const key = userId
    ? `id:${userId}`
    : `name:${normalized(youtubeName)}`;

  if (state.history.has(key)) return false;
  state.history.add(key);

  const participant = { youtubeName, comment: fullComment, key };

  if (state.participants.length < 40) {
    state.participants.push(participant);
  } else {
    state.queue.push(participant);
  }

  broadcast();
  return true;
}

function fillFromQueue() {
  while (state.participants.length < 40 && state.queue.length) {
    state.participants.push(state.queue.shift());
  }
}

function deleteParticipant(index) {
  if (!Number.isInteger(index) || index < 0 || index >= state.participants.length) return;
  state.participants.splice(index, 1);
  fillFromQueue();
  broadcast();
}

function stopStream() {
  if (state.streamCall) {
    try { state.streamCall.cancel(); } catch (_) {}
    state.streamCall = null;
  }
}

function getConfig() {
  const credsPath = process.env.GOOGLE_CLIENT_SECRET || path.join(__dirname, "client_secret.json");
  if (!fs.existsSync(credsPath)) {
    throw new Error("ضع ملف client_secret.json داخل مجلد المشروع أولاً.");
  }
  const creds = JSON.parse(fs.readFileSync(credsPath, "utf8"));
  const config = creds.installed || creds.web;
  if (!config) throw new Error("ملف Google OAuth غير صحيح.");
  return config;
}

function makeOAuthClient() {
  const config = getConfig();
  return new google.auth.OAuth2(
    config.client_id,
    config.client_secret,
    `http://localhost:${PORT}/oauth2callback`
  );
}

async function getYouTubeClient() {
  const oauth2 = makeOAuthClient();
  const tokenPath = path.join(__dirname, "token.json");

  if (!fs.existsSync(tokenPath)) {
    throw new Error("الحساب غير مربوط. اضغط «🔗 ربط حساب YouTube» أولاً.");
  }

  oauth2.setCredentials(JSON.parse(fs.readFileSync(tokenPath, "utf8")));
  return { youtube: google.youtube({ version: "v3", auth: oauth2 }), oauth2 };
}

app.get("/api/auth", (req, res) => {
  try {
    const oauth2 = makeOAuthClient();
    const url = oauth2.generateAuthUrl({
      access_type: "offline",
      scope: ["https://www.googleapis.com/auth/youtube.readonly"],
      prompt: "consent"
    });
    res.redirect(url);
  } catch (e) {
    res.status(500).send("تعذر بدء ربط YouTube: " + e.message);
  }
});

app.get("/api/state", (req, res) => res.json(publicState()));

app.post("/api/config", (req, res) => {
  if (Object.prototype.hasOwnProperty.call(req.body, "broadcastId")) {
    state.broadcastId = String(req.body.broadcastId || "").trim();
  }

  if (Object.prototype.hasOwnProperty.call(req.body, "keyword")) {
    const keyword = cleanText(req.body.keyword, 100);
    if (!keyword) {
      return res.status(400).json({ ok: false, error: "كلمة التسجيل لا يمكن أن تكون فارغة." });
    }
    state.keyword = keyword;
  }

  broadcast();
  res.json({ ok: true, state: publicState() });
});

app.post("/api/start", async (req, res) => {
  try {
    stopStream();

    state.participants = [];
    state.history = new Set();
    state.queue = [];

    if (req.body.broadcastId) {
      state.broadcastId = String(req.body.broadcastId).trim();
    }

    if (req.body.keyword) {
      state.keyword = cleanText(req.body.keyword, 100);
    }

    if (!state.keyword) throw new Error("اكتب كلمة التسجيل أولًا.");

    // This timestamp defines the beginning of THIS competition.
    // Messages received from the initial stream history are ignored if they were posted before it.
    state.competitionStartedAt = Date.now();
    state.active = true;
    state.status = "يستعد لقراءة الشات...";
    broadcast();

    await startStreaming();

    broadcast();
    res.json({ ok: true, state: publicState() });
  } catch (e) {
    state.active = false;
    state.status = "خطأ";
    stopStream();
    broadcast();
    res.status(400).json({ ok: false, error: e.message });
  }
});

app.post("/api/stop", (req, res) => {
  state.active = false;
  state.status = "متوقف";
  stopStream();
  broadcast();
  res.json({ ok: true, state: publicState() });
});

app.post("/api/delete/:index", (req, res) => {
  deleteParticipant(Number(req.params.index));
  res.json({ ok: true, state: publicState() });
});

app.post("/api/reset", (req, res) => {
  state.active = false;
  state.status = "متوقف";
  stopStream();
  state.participants = [];
  state.history = new Set();
  state.queue = [];
  state.competitionStartedAt = 0;
  broadcast();
  res.json({ ok: true, state: publicState() });
});

app.get("/oauth2callback", async (req, res) => {
  try {
    if (req.query.error) {
      return res.status(400).send("تم إلغاء ربط YouTube: " + req.query.error);
    }
    if (!req.query.code) {
      return res.status(400).send("لم يصل رمز المصادقة من Google.");
    }

    const oauth2 = makeOAuthClient();
    const { tokens } = await oauth2.getToken(req.query.code);
    fs.writeFileSync(path.join(__dirname, "token.json"), JSON.stringify(tokens, null, 2));

    res.send(`
      <!doctype html><html lang="ar" dir="rtl"><meta charset="utf-8">
      <title>تم الربط</title>
      <body style="font-family:Arial;text-align:center;padding:50px;background:#101318;color:white">
        <h2>✅ تم ربط حساب YouTube بنجاح</h2>
        <p>يمكنك الآن العودة إلى لوحة التحكم والضغط على «بداية».</p>
        <a href="/control.html" style="color:white;background:#16834a;padding:12px 20px;border-radius:10px;text-decoration:none">العودة إلى لوحة التحكم</a>
      </body></html>
    `);
  } catch (e) {
    res.status(500).send("فشل الربط: " + e.message);
  }
});

async function findLiveChat(youtube) {
  if (state.broadcastId) {
    const b = await youtube.liveBroadcasts.list({
      part: "snippet,status",
      id: [state.broadcastId]
    });
    const item = (b.data.items || [])[0];
    if (!item) throw new Error("لم أجد البث بهذا Broadcast ID.");
    if (!item.snippet?.liveChatId) throw new Error("لم أجد Live Chat لهذا البث.");
    return item.snippet.liveChatId;
  }

  // Avoid the invalid mine + broadcastStatus combination from the old code.
  const r = await youtube.liveBroadcasts.list({
    part: "id,snippet,status",
    mine: true,
    maxResults: 50
  });

  const active = (r.data.items || []).find(
    (item) => item.status?.lifeCycleStatus === "live"
  );

  if (!active) {
    throw new Error("لم أجد بث YouTube مباشرًا نشطًا. ضع Broadcast ID في لوحة التحكم.");
  }

  state.broadcastId = active.id;
  if (!active.snippet?.liveChatId) throw new Error("لم أجد Live Chat لهذا البث.");
  return active.snippet.liveChatId;
}

async function startStreaming() {
  const { youtube, oauth2 } = await getYouTubeClient();
  state.chatId = await findLiveChat(youtube);

  const access = await oauth2.getAccessToken();
  const accessToken = typeof access === "string" ? access : access?.token;
  if (!accessToken) throw new Error("تعذر الحصول على OAuth access token.");

  const client = new youtubeProto.V3DataLiveChatMessageService(
    "youtube.googleapis.com:443",
    grpc.credentials.createSsl()
  );

  const metadata = new grpc.Metadata();
  metadata.set("authorization", `Bearer ${accessToken}`);

  const request = {
    liveChatId: state.chatId,
    part: ["id", "snippet", "authorDetails"]
  };

  state.status = "متصل بالشات ويستقبل الرسائل الجديدة";
  broadcast();

  const call = client.streamList(request, metadata);
  state.streamCall = call;

  call.on("data", (response) => {
    if (!state.active || state.streamCall !== call) return;

    const items = response.items || [];

    for (const m of items) {
      const publishedAt = m.snippet?.publishedAt;
      const publishedMs = publishedAt ? Date.parse(publishedAt) : NaN;

      // Ignore the initial history that predates the current competition.
      if (Number.isFinite(publishedMs) && publishedMs < state.competitionStartedAt) {
        continue;
      }

      const text = m.snippet?.displayMessage || "";
      const comment = registrationComment(text);
      if (!comment) continue;

      addParticipant(
        m.authorDetails?.displayName,
        m.authorDetails?.channelId,
        comment
      );
    }
  });

  call.on("error", (err) => {
    if (!state.active || state.streamCall !== call) return;

    state.streamCall = null;
    state.status = "إعادة الاتصال...";
    broadcast();

    setTimeout(() => {
      if (!state.active) return;
      startStreaming().catch((e) => {
        state.status = "خطأ في الاتصال: " + e.message;
        broadcast();
        setTimeout(() => {
          if (state.active) startStreaming().catch(() => {});
        }, 5000);
      });
    }, 1000);
  });

  call.on("end", () => {
    if (!state.active || state.streamCall !== call) return;

    state.streamCall = null;
    state.status = "إعادة الاتصال...";
    broadcast();

    setTimeout(() => {
      if (state.active) startStreaming().catch(() => {});
    }, 1000);
  });
}

wss.on("connection", (ws) => {
  ws.send(JSON.stringify({ type: "state", state: publicState() }));
});

server.listen(PORT, () => {
  console.log(`لوحة التحكم: http://localhost:${PORT}/control.html`);
  console.log(`شاشة OBS:       http://localhost:${PORT}/display.html`);
});
