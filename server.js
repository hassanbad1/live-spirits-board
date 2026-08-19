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

/* =========================
   الملفات العامة
========================= */

app.use(
  express.static(
    path.join(__dirname, "public")
  )
);

/* =========================
   صفحات الموقع
========================= */

app.get("/", (req, res) => {
  const publicIndex = path.join(
    __dirname,
    "public",
    "index.html"
  );

  const rootIndex = path.join(
    __dirname,
    "index.html"
  );

  if (fs.existsSync(publicIndex)) {
    return res.sendFile(publicIndex);
  }

  if (fs.existsSync(rootIndex)) {
    return res.sendFile(rootIndex);
  }

  res
    .status(404)
    .send("لم يتم العثور على index.html.");
});

app.get(
  "/control.html",
  (req, res, next) => {
    const publicControl = path.join(
      __dirname,
      "public",
      "control.html"
    );

    const rootControl = path.join(
      __dirname,
      "control.html"
    );

    if (fs.existsSync(publicControl)) {
      return res.sendFile(
        publicControl
      );
    }

    if (fs.existsSync(rootControl)) {
      return res.sendFile(
        rootControl
      );
    }

    next();
  }
);

/* =========================
   ملف البروتوكول
========================= */

const PROTO_PATH = path.join(
  __dirname,
  "stream_list.proto"
);

if (!fs.existsSync(PROTO_PATH)) {
  throw new Error(
    `ملف stream_list.proto غير موجود في مجلد المشروع: ${PROTO_PATH}`
  );
}

const packageDefinition =
  protoLoader.loadSync(
    PROTO_PATH,
    {
      keepCase: false,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true
    }
  );

const youtubeProto =
  grpc.loadPackageDefinition(
    packageDefinition
  ).youtube.api.v3;

/* =========================
   حالة التطبيق
========================= */

const state = {
  active: false,

  broadcastId: "",

  // يتم حفظ chatId حتى لا نعيد طلبه
  chatId: "",

  keyword: "ارواح!",

  participants: [],

  history: new Set(),

  queue: [],

  streamCall: null,

  competitionStartedAt: 0,

  status: "متوقف",

  // يمنع تشغيل أكثر من اتصال
  reconnecting: false,

  // عدد محاولات إعادة الاتصال
  reconnectAttempts: 0,

  // مؤقت إعادة الاتصال
  reconnectTimer: null,

  // يمنع إعادة البحث عن البث
  broadcastLookupDone: false
};

/* =========================
   الحالة العامة
========================= */

function publicState() {
  return {
    active: state.active,

    broadcastId:
      state.broadcastId,

    keyword:
      state.keyword,

    participants:
      state.participants.map(
        (p) => ({
          youtubeName:
            p.youtubeName,

          comment:
            p.comment
        })
      ),

    queued:
      state.queue.length,

    historyCount:
      state.history.size,

    status:
      state.status
  };
}

/* =========================
   WebSocket
========================= */

function broadcast() {
  const data =
    JSON.stringify({
      type: "state",
      state: publicState()
    });

  for (
    const client of wss.clients
  ) {
    if (
      client.readyState ===
      WebSocket.OPEN
    ) {
      try {
        client.send(data);
      } catch (err) {
        console.error(
          "❌ WebSocket send error:",
          err
        );
      }
    }
  }
}

/* =========================
   تنظيف النصوص
========================= */

function cleanText(
  value,
  maxLength = 500
) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, maxLength);
}

function normalized(value) {
  return String(value || "")
    .trim()
    .toLocaleLowerCase("ar");
}

/* =========================
   فحص كلمة التسجيل
========================= */

function registrationComment(
  text
) {
  const comment =
    cleanText(text);

  const keyword =
    cleanText(
      state.keyword,
      100
    );

  if (
    !comment ||
    !keyword
  ) {
    return null;
  }

  const c =
    normalized(comment);

  const k =
    normalized(keyword);

  if (!c.startsWith(k)) {
    return null;
  }

  const next =
    comment.charAt(
      keyword.length
    );

  // يجب أن تكون هناك مسافة
  // بعد كلمة التسجيل
  if (
    next &&
    !/\s/.test(next)
  ) {
    return null;
  }

  return comment;
}

/* =========================
   إضافة مشارك
========================= */

function addParticipant(
  displayName,
  userId,
  comment
) {
  const youtubeName =
    cleanText(
      displayName,
      100
    );

  const fullComment =
    cleanText(
      comment,
      500
    );

  if (
    !youtubeName ||
    !fullComment
  ) {
    return false;
  }

  const key = userId
    ? `id:${userId}`
    : `name:${normalized(
        youtubeName
      )}`;

  if (
    state.history.has(key)
  ) {
    return false;
  }

  state.history.add(key);

  const participant = {
    youtubeName,
    comment: fullComment,
    key
  };

  if (
    state.participants
      .length < 40
  ) {
    state.participants.push(
      participant
    );
  } else {
    state.queue.push(
      participant
    );
  }

  broadcast();

  return true;
}

/* =========================
   تعبئة القائمة من الانتظار
========================= */

function fillFromQueue() {
  while (
    state.participants
      .length < 40 &&
    state.queue.length > 0
  ) {
    state.participants.push(
      state.queue.shift()
    );
  }
}

/* =========================
   حذف مشارك
========================= */

function deleteParticipant(
  index
) {
  if (
    !Number.isInteger(index) ||
    index < 0 ||
    index >=
      state.participants.length
  ) {
    return;
  }

  state.participants.splice(
    index,
    1
  );

  fillFromQueue();

  broadcast();
}

/* =========================
   إيقاف الاتصال
========================= */

function stopStream() {
  state.reconnecting =
    false;

  state.reconnectAttempts =
    0;

  if (
    state.reconnectTimer
  ) {
    clearTimeout(
      state.reconnectTimer
    );

    state.reconnectTimer =
      null;
  }

  if (
    state.streamCall
  ) {
    try {
      state.streamCall.cancel();
    } catch (_) {}

    state.streamCall =
      null;
  }

  /*
   * مهم:
   * لا نحذف chatId هنا.
   *
   * إذا كانت المسابقة نفسها
   * تعيد الاتصال، نستعمل نفس chatId
   * بدون طلب جديد إلى YouTube.
   */
}

/* =========================
   إعداد Google OAuth
========================= */

function getConfig() {
  const credsPath =
    process.env.GOOGLE_CLIENT_SECRET ||
    path.join(
      __dirname,
      "client_secret.json"
    );

  if (
    !fs.existsSync(credsPath)
  ) {
    throw new Error(
      "ضع ملف client_secret.json داخل إعدادات Render كـ Secret File أو اضبط GOOGLE_CLIENT_SECRET."
    );
  }

  const creds =
    JSON.parse(
      fs.readFileSync(
        credsPath,
        "utf8"
      )
    );

  const config =
    creds.installed ||
    creds.web;

  if (!config) {
    throw new Error(
      "ملف Google OAuth غير صحيح."
    );
  }

  return config;
}

function makeOAuthClient() {
  const config =
    getConfig();

  /*
   * في Render نستخدم REDIRECT_URI
   * الموجود في Environment Variables.
   *
   * القيمة الصحيحة:
   * https://live-spirits-board.onrender.com/oauth2callback
   */

  const redirectUri =
    process.env.REDIRECT_URI ||
    `http://localhost:${PORT}/oauth2callback`;

  return new google.auth.OAuth2(
    config.client_id,
    config.client_secret,
    redirectUri
  );
}

/* =========================
   YouTube Client
========================= */

async function getYouTubeClient() {
  const oauth2 =
    makeOAuthClient();

  const tokenPath =
    process.env.YOUTUBE_TOKEN_PATH ||
    path.join(
      __dirname,
      "token.json"
    );

  if (
    !fs.existsSync(tokenPath)
  ) {
    throw new Error(
      "الحساب غير مربوط. اضغط «🔗 ربط حساب YouTube» أولاً."
    );
  }

  oauth2.setCredentials(
    JSON.parse(
      fs.readFileSync(
        tokenPath,
        "utf8"
      )
    )
  );

  return {
    youtube:
      google.youtube({
        version: "v3",
        auth: oauth2
      }),

    oauth2
  };
}

/* =========================
   بدء OAuth
========================= */

app.get(
  "/api/auth",
  (req, res) => {
    try {
      const oauth2 =
        makeOAuthClient();

      const url =
        oauth2.generateAuthUrl({
          access_type:
            "offline",

          scope: [
            "https://www.googleapis.com/auth/youtube.readonly"
          ],

          prompt: "consent"
        });

      res.redirect(url);
    } catch (e) {
      console.error(
        "❌ OAuth start error:",
        e
      );

      res
        .status(500)
        .send(
          "تعذر بدء ربط YouTube: " +
            e.message
        );
    }
  }
);

/* =========================
   API State
========================= */

app.get(
  "/api/state",
  (req, res) => {
    res.json(
      publicState()
    );
  }
);

/* =========================
   إعدادات المسابقة
========================= */

app.post(
  "/api/config",
  (req, res) => {
    if (
      Object.prototype.hasOwnProperty.call(
        req.body,
        "broadcastId"
      )
    ) {
      const newBroadcastId =
        String(
          req.body.broadcastId ||
            ""
        ).trim();

      /*
       * إذا تغير Broadcast ID
       * يجب مسح chatId القديم.
       */

      if (
        newBroadcastId !==
        state.broadcastId
      ) {
        state.chatId = "";

        state.broadcastLookupDone =
          false;
      }

      state.broadcastId =
        newBroadcastId;
    }

    if (
      Object.prototype.hasOwnProperty.call(
        req.body,
        "keyword"
      )
    ) {
      const keyword =
        cleanText(
          req.body.keyword,
          100
        );

      if (!keyword) {
        return res
          .status(400)
          .json({
            ok: false,
            error:
              "كلمة التسجيل لا يمكن أن تكون فارغة."
          });
      }

      state.keyword =
        keyword;
    }

    broadcast();

    res.json({
      ok: true,
      state:
        publicState()
    });
  }
);

/* =========================
   بدء المسابقة
========================= */

app.post(
  "/api/start",
  async (req, res) => {
    try {
      stopStream();

      state.participants =
        [];

      state.history =
        new Set();

      state.queue =
        [];

      /*
       * إذا أرسل المستخدم Broadcast ID
       * جديدًا، نحدثه ونمسح chatId القديم.
       */

      if (
        req.body.broadcastId
      ) {
        const newBroadcastId =
          String(
            req.body.broadcastId
          ).trim();

        if (
          newBroadcastId !==
          state.broadcastId
        ) {
          state.broadcastId =
            newBroadcastId;

          state.chatId = "";

          state.broadcastLookupDone =
            false;
        }
      }

      if (
        req.body.keyword
      ) {
        state.keyword =
          cleanText(
            req.body.keyword,
            100
          );
      }

      if (
        !state.keyword
      ) {
        throw new Error(
          "اكتب كلمة التسجيل أولًا."
        );
      }

      /*
       * بداية مسابقة جديدة
       */

      state.competitionStartedAt =
        Date.now();

      state.active =
        true;

      state.reconnectAttempts =
        0;

      state.status =
        "يستعد لقراءة الشات...";

      broadcast();

      await startStreaming();

      broadcast();

      res.json({
        ok: true,
        state:
          publicState()
      });
    } catch (e) {
      console.error(
        "❌ Start competition error:",
        e
      );

      state.active =
        false;

      state.status =
        "خطأ";

      stopStream();

      broadcast();

      res
        .status(400)
        .json({
          ok: false,
          error:
            e.message
        });
    }
  }
);

/* =========================
   إيقاف المسابقة
========================= */

app.post(
  "/api/stop",
  (req, res) => {
    state.active =
      false;

    state.status =
      "متوقف";

    stopStream();

    broadcast();

    res.json({
      ok: true,
      state:
        publicState()
    });
  }
);

/* =========================
   حذف مشارك
========================= */

app.post(
  "/api/delete/:index",
  (req, res) => {
    deleteParticipant(
      Number(
        req.params.index
      )
    );

    res.json({
      ok: true,
      state:
        publicState()
    });
  }
);

/* =========================
   إعادة ضبط
========================= */

app.post(
  "/api/reset",
  (req, res) => {
    state.active =
      false;

    state.status =
      "متوقف";

    stopStream();

    state.participants =
      [];

    state.history =
      new Set();

    state.queue =
      [];

    state.competitionStartedAt =
      0;

    /*
     * نحتفظ بـ broadcastId و chatId
     * حتى لا نحتاج طلب YouTube جديد
     * بدون سبب.
     */

    broadcast();

    res.json({
      ok: true,
      state:
        publicState()
    });
  }
);

/* =========================
   OAuth Callback
========================= */

app.get(
  "/oauth2callback",
  async (req, res) => {
    try {
      if (
        req.query.error
      ) {
        return res
          .status(400)
          .send(
            "تم إلغاء ربط YouTube: " +
              req.query.error
          );
      }

      if (
        !req.query.code
      ) {
        return res
          .status(400)
          .send(
            "لم يصل رمز المصادقة من Google."
          );
      }

      const oauth2 =
        makeOAuthClient();

      const { tokens } =
        await oauth2.getToken(
          req.query.code
        );

      const tokenPath =
        process.env.YOUTUBE_TOKEN_PATH ||
        path.join(
          __dirname,
          "token.json"
        );

      fs.writeFileSync(
        tokenPath,
        JSON.stringify(
          tokens,
          null,
          2
        )
      );

      res.send(`
        <!doctype html>
        <html lang="ar" dir="rtl">

        <head>
          <meta charset="utf-8">
          <title>تم الربط</title>
        </head>

        <body style="
          font-family:Arial;
          text-align:center;
          padding:50px;
          background:#101318;
          color:white
        ">

          <h2>✅ تم ربط حساب YouTube بنجاح</h2>

          <p>
            يمكنك الآن العودة إلى لوحة التحكم
            والضغط على «بداية».
          </p>

          <a
            href="/control.html"
            style="
              color:white;
              background:#16834a;
              padding:12px 20px;
              border-radius:10px;
              text-decoration:none
            "
          >
            العودة إلى لوحة التحكم
          </a>

        </body>
        </html>
      `);
    } catch (e) {
      console.error(
        "❌ OAuth callback error:",
        e
      );

      res
        .status(500)
        .send(
          "فشل الربط: " +
            e.message
        );
    }
  }
);

/* =========================
   العثور على Live Chat
========================= */

async function findLiveChat(
  youtube
) {
  /*
   * أهم تعديل:
   *
   * إذا كان لدينا chatId بالفعل،
   * لا نرسل أي طلب جديد إلى YouTube.
   */

  if (
    state.chatId
  ) {
    console.log(
      "♻️ استخدام Live Chat ID المحفوظ."
    );

    return state.chatId;
  }

  /*
   * إذا كان لدينا Broadcast ID
   * نبحث عنه مرة واحدة فقط.
   */

  if (
    state.broadcastId
  ) {
    console.log(
      "🔎 البحث عن Live Chat للبث..."
    );

    const b =
      await youtube.liveBroadcasts.list(
        {
          part:
            "snippet,status",

          id: [
            state.broadcastId
          ]
        }
      );

    const item =
      (
        b.data.items ||
        []
      )[0];

    if (!item) {
      throw new Error(
        "لم أجد البث بهذا Broadcast ID."
      );
    }

    if (
      !item.snippet
        ?.liveChatId
    ) {
      throw new Error(
        "لم أجد Live Chat لهذا البث."
      );
    }

    state.chatId =
      item.snippet.liveChatId;

    state.broadcastLookupDone =
      true;

    console.log(
      "✅ تم حفظ Live Chat ID."
    );

    return state.chatId;
  }

  /*
   * إذا لم يضع المستخدم Broadcast ID،
   * نبحث عن بث مباشر يملكه الحساب.
   *
   * هذه العملية لا تتكرر عند reconnect
   * لأن chatId سيتم حفظه بعد نجاحها.
   */

  if (
    state.broadcastLookupDone
  ) {
    throw new Error(
      "تعذر العثور على Live Chat."
    );
  }

  console.log(
    "🔎 البحث عن بث مباشر نشط..."
  );

  const r =
    await youtube.liveBroadcasts.list(
      {
        part:
          "id,snippet,status",

        mine: true,

        maxResults: 50
      }
    );

  const active =
    (
      r.data.items ||
      []
    ).find(
      (item) =>
        item.status
          ?.lifeCycleStatus ===
        "live"
    );

  if (!active) {
    throw new Error(
      "لم أجد بث YouTube مباشرًا نشطًا. ضع Broadcast ID في لوحة التحكم."
    );
  }

  state.broadcastId =
    active.id;

  if (
    !active.snippet
      ?.liveChatId
  ) {
    throw new Error(
      "لم أجد Live Chat لهذا البث."
    );
  }

  state.chatId =
    active.snippet.liveChatId;

  state.broadcastLookupDone =
    true;

  console.log(
    "✅ تم العثور على Live Chat وحفظه."
  );

  return state.chatId;
}

/* =========================
   معرفة خطأ الحصة
========================= */

function isQuotaError(
  err
) {
  const text =
    JSON.stringify(err || {})
      .toLowerCase();

  return (
    text.includes(
      "quotaexceeded"
    ) ||
    text.includes(
      "quota exceeded"
    ) ||
    text.includes(
      "resource_exhausted"
    ) ||
    err?.code === 8
  );
}

/* =========================
   الاتصال بـ YouTube Live Chat
========================= */

async function startStreaming() {
  if (
    !state.active
  ) {
    return;
  }

  /*
   * منع تشغيل اتصالين
   */

  if (
    state.streamCall ||
    state.reconnecting
  ) {
    return;
  }

  const {
    youtube,
    oauth2
  } =
    await getYouTubeClient();

  /*
   * findLiveChat لن يرسل طلبًا
   * جديدًا إذا كان chatId محفوظًا.
   */

  state.chatId =
    await findLiveChat(
      youtube
    );

  const access =
    await oauth2.getAccessToken();

  const accessToken =
    typeof access === "string"
      ? access
      : access?.token;

  if (!accessToken) {
    throw new Error(
      "تعذر الحصول على OAuth access token."
    );
  }

  const client =
    new youtubeProto
      .V3DataLiveChatMessageService(
        "youtube.googleapis.com:443",
        grpc.credentials.createSsl()
      );

  const metadata =
    new grpc.Metadata();

  metadata.set(
    "authorization",
    `Bearer ${accessToken}`
  );

  /*
   * Streaming Live Chat
   *
   * لا يوجد polling كل ثانية.
   */

  const request = {
    liveChatId:
      state.chatId,

    part: [
      "id",
      "snippet",
      "authorDetails"
    ]
  };

  console.log(
    "▶️ بدء الاتصال بـ YouTube Live Chat..."
  );

  const call =
    client.streamList(
      request,
      metadata
    );

  state.streamCall =
    call;

  state.reconnecting =
    false;

  state.reconnectAttempts =
    0;

  state.status =
    "متصل بالشات ويستقبل الرسائل الجديدة";

  broadcast();

  /* =========================
     استقبال الرسائل
  ========================= */

  call.on(
    "data",
    (response) => {
      if (
        !state.active ||
        state.streamCall !== call
      ) {
        return;
      }

      const items =
        response.items ||
        [];

      for (
        const m of items
      ) {
        const publishedAt =
          m.snippet
            ?.publishedAt;

        const publishedMs =
          publishedAt
            ? Date.parse(
                publishedAt
              )
            : NaN;

        /*
         * تجاهل الرسائل القديمة
         */

        if (
          Number.isFinite(
            publishedMs
          ) &&
          publishedMs <
            state.competitionStartedAt
        ) {
          continue;
        }

        const text =
          m.snippet
            ?.displayMessage ||
          "";

        console.log(
          "💬 رسالة YouTube:",
          text
        );

        const comment =
          registrationComment(
            text
          );

        if (!comment) {
          continue;
        }

        const added =
          addParticipant(
            m.authorDetails
              ?.displayName,

            m.authorDetails
              ?.channelId,

            comment
          );

        if (added) {
          console.log(
            "✅ تمت إضافة مشارك:",
            m.authorDetails
              ?.displayName,
            "-",
            comment
          );
        }
      }
    }
  );

  /* =========================
     خطأ في الاتصال
  ========================= */

  call.on(
    "error",
    (err) => {
      console.error(
        "❌ YouTube stream error:",
        err
      );

      if (
        !state.active ||
        state.streamCall !== call
      ) {
        return;
      }

      state.streamCall =
        null;

      /*
       * إذا كانت المشكلة حصة،
       * لا نبدأ حلقة reconnect سريعة.
       */

      if (
        isQuotaError(err)
      ) {
        state.status =
          "تم إيقاف إعادة المحاولة بسبب مشكلة في حصة YouTube.";

        state.active =
          false;

        broadcast();

        return;
      }

      scheduleReconnect();
    }
  );

  /* =========================
     انتهاء الاتصال
  ========================= */

  call.on(
    "end",
    () => {
      console.log(
        "⚠️ YouTube stream ended"
      );

      if (
        !state.active ||
        state.streamCall !== call
      ) {
        return;
      }

      state.streamCall =
        null;

      scheduleReconnect();
    }
  );
}

/* =========================
   إعادة الاتصال الآمنة
========================= */

function scheduleReconnect() {
  if (
    !state.active ||
    state.reconnecting
  ) {
    return;
  }

  /*
   * لا نسمح بأكثر من مؤقت.
   */

  if (
    state.reconnectTimer
  ) {
    return;
  }

  state.reconnecting =
    true;

  state.reconnectAttempts++;

  /*
   * Backoff:
   *
   * 1 محاولة بعد 2 ثواني
   * ثم 4
   * ثم 8
   * ثم 16
   * ثم 30 كحد أقصى
   *
   * هذا يمنع ضرب API
   * بعشرات الطلبات في الثانية.
   */

  const delay =
    Math.min(
      30000,
      2000 *
        Math.pow(
          2,
          Math.min(
            state.reconnectAttempts -
              1,
            4
          )
        )
    );

  state.status =
    `انقطع الاتصال، إعادة المحاولة بعد ${Math.ceil(
      delay / 1000
    )} ثانية...`;

  broadcast();

  console.log(
    `🔄 إعادة الاتصال بعد ${
      delay / 1000
    } ثانية...`
  );

  state.reconnectTimer =
    setTimeout(
      async () => {
        state.reconnectTimer =
          null;

        state.reconnecting =
          false;

        if (
          !state.active
        ) {
          return;
        }

        try {
          /*
           * مهم:
           *
           * startStreaming()
           * سيستخدم chatId المحفوظ.
           *
           * لن يستدعي
           * liveBroadcasts.list
           * مرة أخرى.
           */

          await startStreaming();

          console.log(
            "✅ تمت إعادة الاتصال بنجاح."
          );
        } catch (e) {
          console.error(
            "❌ Reconnect failed:",
            e
          );

          /*
           * إذا كان الخطأ quotaExceeded
           * نوقف المحاولات.
           */

          if (
            isQuotaError(e)
          ) {
            state.active =
              false;

            state.status =
              "تم إيقاف الاتصال بسبب استنفاد حصة YouTube.";

            broadcast();

            return;
          }

          state.status =
            "تعذر الاتصال، ستتم إعادة المحاولة...";

          broadcast();

          scheduleReconnect();
        }
      },
      delay
    );
}

/* =========================
   WebSocket connection
========================= */

wss.on(
  "connection",
  (ws) => {
    console.log(
      "🔌 WebSocket client connected"
    );

    ws.send(
      JSON.stringify({
        type: "state",
        state:
          publicState()
      })
    );

    ws.on(
      "close",
      () => {
        console.log(
          "🔌 WebSocket client disconnected"
        );
      }
    );
  }
);

/* =========================
   تشغيل السيرفر
========================= */

server.listen(
  PORT,
  "0.0.0.0",
  () => {
    console.log(
      `🚀 Server running on port ${PORT}`
    );

    console.log(
      "لوحة التحكم: /control.html"
    );

    console.log(
      "شاشة OBS: /display.html"
    );

    console.log(
      `Public URL: ${
        process.env.RENDER_EXTERNAL_URL ||
        `http://localhost:${PORT}`
      }`
    );
  }
);

/* =========================
   أخطاء السيرفر
========================= */

server.on(
  "error",
  (err) => {
    console.error(
      "❌ Server error:",
      err
    );
  }
);

process.on(
  "uncaughtException",
  (err) => {
    console.error(
      "❌ Uncaught exception:",
      err
    );
  }
);

process.on(
  "unhandledRejection",
  (err) => {
    console.error(
      "❌ Unhandled rejection:",
      err
    );
  }
);
