const express = require("express");
const http = require("http");
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const WebSocket = require("ws");

const PORT = Number(process.env.PORT) || 3000;

const app = express();
const server = http.createServer(app);

app.use(express.json({ limit: "100kb" }));
app.use(express.static(path.join(__dirname, "public")));

/* =========================================================
   الإعدادات
========================================================= */

const PUBLIC_URL = (
  process.env.RENDER_EXTERNAL_URL ||
  "https://live-spirits-board.onrender.com"
).replace(/\/+$/, "");

const REDIRECT_URI =
  process.env.RESTREAM_REDIRECT_URI ||
  `${PUBLIC_URL}/oauth2callback`;

const RESTREAM_CLIENT_ID =
  process.env.RESTREAM_CLIENT_ID || "";

const RESTREAM_CLIENT_SECRET =
  process.env.RESTREAM_CLIENT_SECRET || "";

const MAX_PARTICIPANTS = 40;

const DEFAULT_KEYWORD = "ارواح!";

/*
  الغرفة التي لا يوجد عليها أي نشاط للوحة التحكم
  أو اتصال OBS لفترة طويلة يتم حذفها.

  هذه ليست مدة المسابقة.
  هي فقط مدة بقاء الغرفة في الذاكرة.
*/
const ROOM_TTL_MS = 30 * 60 * 1000;

/*
  OAuth state صالح لمدة 10 دقائق.
*/
const OAUTH_SESSION_TTL_MS = 10 * 60 * 1000;

/*
  إعادة اتصال Restream.
*/
const RESTREAM_RECONNECT_DELAY_MS = 3000;

/*
  Restream heartbeat يكون تقريبًا كل 45 ثانية.
  نعتبر الاتصال غير سليم إذا لم نر heartbeat
  لفترة أطول من ذلك.
*/
const HEARTBEAT_TIMEOUT_MS = 75 * 1000;

const HEARTBEAT_CHECK_INTERVAL_MS = 15 * 1000;

/*
  أنواع أحداث الشات النصية المعروفة في Restream.
  نحن لا نعتمد على كل event، بل على أنواع النص فقط.
*/
const TEXT_EVENT_TYPE_IDS = new Set([
  1,  // Discord Text
  2,  // DLive Text
  4,  // Twitch Text
  5,  // YouTube Text
  11, // Facebook Text
  21, // LinkedIn Text
  22, // Trovo Text
  24, // X Text
  25, // Kick Text
  32  // Rumble Text
]);

/* =========================================================
   فحص إعدادات Restream
========================================================= */

if (!RESTREAM_CLIENT_ID) {
  console.warn(
    "⚠️ RESTREAM_CLIENT_ID غير موجود في Environment Variables."
  );
}

if (!RESTREAM_CLIENT_SECRET) {
  console.warn(
    "⚠️ RESTREAM_CLIENT_SECRET غير موجود في Environment Variables."
  );
}

/* =========================================================
   الغرف
========================================================= */

const rooms = new Map();

/*
  كل OAuth state مرتبط بغرفة واحدة.
*/
const oauthSessions = new Map();

/* =========================================================
   أدوات عامة
========================================================= */

function cleanText(value, maxLength = 500) {
  return String(value ?? "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, maxLength);
}

function normalized(value) {
  return String(value ?? "")
    .trim()
    .toLocaleLowerCase("ar");
}

function randomId(bytes = 18) {
  return crypto.randomBytes(bytes).toString("base64url");
}

function now() {
  return Date.now();
}

function safeUrl(value) {
  try {
    return new URL(value);
  } catch (_) {
    return null;
  }
}

/* =========================================================
   إنشاء غرفة
========================================================= */

function createRoom() {
  const id = randomId(12);

  const room = {
    id,

    createdAt: now(),

    /*
      آخر نشاط فعلي للغرفة.
    */
    lastActivityAt: now(),

    /*
      آخر نشاط من واجهة المتصفح.
    */
    lastBrowserActivityAt: now(),

    /*
      المسابقة نفسها.
    */
    active: false,

    keyword: DEFAULT_KEYWORD,

    participants: [],

    queue: [],

    /*
      مفاتيح الأشخاص الذين تم تسجيلهم.
    */
    history: new Set(),

    /*
      وقت بداية المسابقة.
    */
    competitionStartedAt: 0,

    /*
      وقت فتح WebSocket الجديد.
    */
    chatConnectionStartedAt: 0,

    status: "جاهز",

    /*
      WebSocket clients الخاصة بالواجهة وOBS.
    */
    browserClients: new Set(),

    restream: {
      accessToken: "",
      refreshToken: "",

      /*
        Unix ms.
      */
      expiresAt: 0,

      connected: false,

      socket: null,

      reconnectTimer: null,

      reconnecting: false,

      heartbeatTimer: null,

      lastHeartbeatAt: 0
    }
  };

  rooms.set(id, room);

  console.log(
    `🏠 تم إنشاء غرفة جديدة: ${id}`
  );

  return room;
}

/* =========================================================
   نشاط الغرفة
========================================================= */

function touchRoom(room) {
  if (!room) {
    return;
  }

  room.lastActivityAt = now();
}

/* =========================================================
   حالة الغرفة العامة
========================================================= */

function publicRoomState(room) {
  if (!room) {
    return null;
  }

  return {
    roomId: room.id,

    active: room.active,

    keyword: room.keyword,

    /*
      نخفي البيانات الداخلية مثل:
      platform
      key
      tokens
    */
    participants: room.participants.map((p) => ({
      youtubeName: p.youtubeName,
      comment: p.comment
    })),

    queued: room.queue.length,

    count: room.participants.length,

    status: room.status,

    connected: Boolean(
      room.restream.connected
    ),

    restreamConnected: Boolean(
      room.restream.connected
    ),

    obsUrl:
      `${PUBLIC_URL}/obs/${encodeURIComponent(room.id)}`
  };
}

/* =========================================================
   إرسال حالة الغرفة للواجهات
========================================================= */

function sendRoomState(room) {
  if (!room || !room.browserClients) {
    return;
  }

  const message = JSON.stringify({
    type: "state",
    state: publicRoomState(room)
  });

  for (const client of room.browserClients) {
    if (client.readyState !== WebSocket.OPEN) {
      continue;
    }

    try {
      client.send(message);
    } catch (err) {
      console.error(
        "❌ Browser WebSocket send error:",
        err.message
      );
    }
  }
}

/* =========================================================
   إرسال JSON
========================================================= */

function json(res, status, data) {
  res.status(status).json(data);
}

/* =========================================================
   الحصول على الغرفة من الطلب
========================================================= */

function getRoomFromRequest(req, res) {
  const roomId =
    req.params.roomId ||
    req.body?.roomId ||
    req.query?.roomId;

  if (!roomId) {
    json(res, 400, {
      ok: false,
      error: "معرف الغرفة غير موجود."
    });

    return null;
  }

  const room = rooms.get(roomId);

  if (!room) {
    json(res, 404, {
      ok: false,
      error:
        "هذه الغرفة انتهت أو لم تعد موجودة."
    });

    return null;
  }

  touchRoom(room);

  return room;
}

/* =========================================================
   الصفحة الرئيسية
========================================================= */

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

  return res
    .status(404)
    .send("لم يتم العثور على index.html");
});

/* =========================================================
   لوحة التحكم
========================================================= */

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
      return res.sendFile(publicControl);
    }

    if (fs.existsSync(rootControl)) {
      return res.sendFile(rootControl);
    }

    next();
  }
);

/* =========================================================
   صفحة OBS
========================================================= */

app.get(
  "/obs/:roomId",
  (req, res) => {
    const room = rooms.get(
      req.params.roomId
    );

    if (!room) {
      return res
        .status(404)
        .send("هذه الغرفة انتهت.");
    }

    touchRoom(room);

    const publicObs = path.join(
      __dirname,
      "public",
      "obs.html"
    );

    const rootObs = path.join(
      __dirname,
      "obs.html"
    );

    if (fs.existsSync(publicObs)) {
      return res.sendFile(publicObs);
    }

    if (fs.existsSync(rootObs)) {
      return res.sendFile(rootObs);
    }

    return res
      .status(404)
      .send(
        "لم يتم العثور على obs.html"
      );
  }
);

/* =========================================================
   إنشاء غرفة
========================================================= */

app.post(
  "/api/rooms",
  (req, res) => {
    const room = createRoom();

    json(res, 200, {
      ok: true,

      room: {
        roomId: room.id,

        controlUrl:
          `${PUBLIC_URL}/control.html?room=${encodeURIComponent(
            room.id
          )}`,

        obsUrl:
          `${PUBLIC_URL}/obs/${encodeURIComponent(
            room.id
          )}`,

        state:
          publicRoomState(room)
      }
    });
  }
);

/* =========================================================
   معلومات الغرفة
========================================================= */

app.get(
  "/api/rooms/:roomId",
  (req, res) => {
    const room =
      getRoomFromRequest(
        req,
        res
      );

    if (!room) {
      return;
    }

    json(res, 200, {
      ok: true,
      room: publicRoomState(room)
    });
  }
);

/* =========================================================
   Restream OAuth
========================================================= */

app.get(
  "/api/auth/:roomId",
  (req, res) => {
    const room = rooms.get(
      req.params.roomId
    );

    if (!room) {
      return res
        .status(404)
        .send(
          "الغرفة غير موجودة أو انتهت."
        );
    }

    if (
      !RESTREAM_CLIENT_ID ||
      !RESTREAM_CLIENT_SECRET
    ) {
      return res
        .status(500)
        .send(
          "إعدادات Restream غير موجودة في Render."
        );
    }

    /*
      لا نحتاج scope يدويًا هنا.
      صلاحيات التطبيق يحددها إعداد Restream.
    */

    const state = randomId(32);

    oauthSessions.set(
      state,
      {
        roomId: room.id,
        createdAt: now()
      }
    );

    touchRoom(room);

    const params =
      new URLSearchParams({
        response_type: "code",

        client_id:
          RESTREAM_CLIENT_ID,

        redirect_uri:
          REDIRECT_URI,

        state
      });

    const authorizeUrl =
      `https://api.restream.io/login?${params.toString()}`;

    console.log(
      `🔗 بدء Restream OAuth للغرفة ${room.id}`
    );

    return res.redirect(
      authorizeUrl
    );
  }
);

/* =========================================================
   OAuth Callback
========================================================= */

app.get(
  "/oauth2callback",
  async (req, res) => {
    try {
      const {
        code,
        state,
        error
      } = req.query;

      /*
        Restream قد يرجع بدون code إذا رفض المستخدم.
      */
      if (error) {
        return res
          .status(400)
          .send(
            "تم إلغاء ربط Restream."
          );
      }

      if (!code || !state) {
        return res
          .status(400)
          .send(
            "بيانات OAuth غير مكتملة."
          );
      }

      const session =
        oauthSessions.get(
          String(state)
        );

      /*
        State يستخدم مرة واحدة فقط.
      */
      oauthSessions.delete(
        String(state)
      );

      if (!session) {
        return res
          .status(400)
          .send(
            "جلسة OAuth غير صالحة أو انتهت."
          );
      }

      if (
        now() -
          session.createdAt >
        OAUTH_SESSION_TTL_MS
      ) {
        return res
          .status(400)
          .send(
            "انتهت صلاحية جلسة الربط. حاول مرة أخرى."
          );
      }

      const room =
        rooms.get(
          session.roomId
        );

      if (!room) {
        return res
          .status(404)
          .send(
            "الغرفة انتهت أثناء عملية الربط."
          );
      }

      touchRoom(room);

      const basicAuth =
        Buffer.from(
          `${RESTREAM_CLIENT_ID}:${RESTREAM_CLIENT_SECRET}`
        ).toString("base64");

      const body =
        new URLSearchParams({
          grant_type:
            "authorization_code",

          redirect_uri:
            REDIRECT_URI,

          code: String(code)
        });

      const response =
        await fetch(
          "https://api.restream.io/oauth/token",
          {
            method: "POST",

            headers: {
              "Content-Type":
                "application/x-www-form-urlencoded",

              Authorization:
                `Basic ${basicAuth}`
            },

            body: body.toString()
          }
        );

      const data =
        await response.json();

      if (!response.ok) {
        console.error(
          "❌ Restream token exchange:",
          data
        );

        throw new Error(
          data?.error?.message ||
            "فشل الحصول على Restream token."
        );
      }

      const accessToken =
        data.access_token ||
        data.accessToken ||
        "";

      const refreshToken =
        data.refresh_token ||
        data.refreshToken ||
        "";

      if (!accessToken) {
        throw new Error(
          "Restream لم يرجع Access Token."
        );
      }

      room.restream.accessToken =
        accessToken;

      room.restream.refreshToken =
        refreshToken;

      /*
        Restream يعيد expires_in بالثواني.
      */
      const expiresIn =
        Number(
          data.expires_in ||
            data.accessTokenExpiresIn ||
            3600
        );

      room.restream.expiresAt =
        now() +
        Math.max(
          60,
          expiresIn - 120
        ) *
          1000;

      room.status =
        "تم ربط Restream بنجاح";

      room.restream.connected =
        false;

      touchRoom(room);

      console.log(
        `✅ Restream connected for room ${room.id}`
      );

      sendRoomState(room);

      return res.send(`
        <!doctype html>
        <html lang="ar" dir="rtl">
        <head>
          <meta charset="utf-8">
          <meta
            name="viewport"
            content="width=device-width,initial-scale=1"
          >
          <title>تم الربط</title>
        </head>

        <body style="
          margin:0;
          min-height:100vh;
          display:flex;
          align-items:center;
          justify-content:center;
          background:#101318;
          color:white;
          font-family:Arial,sans-serif;
        ">

          <div style="
            text-align:center;
            padding:35px;
          ">

            <h2>✅ تم ربط Restream بنجاح</h2>

            <p>
              يمكنك العودة إلى لوحة التحكم.
            </p>

            <a
              href="/control.html?room=${encodeURIComponent(
                room.id
              )}"
              style="
                display:inline-block;
                margin-top:15px;
                padding:12px 22px;
                border-radius:10px;
                background:#16834a;
                color:white;
                text-decoration:none;
              "
            >
              العودة إلى لوحة التحكم
            </a>

          </div>

        </body>
        </html>
      `);
    } catch (err) {
      console.error(
        "❌ OAuth callback error:",
        err
      );

      return res
        .status(500)
        .send(
          "فشل ربط Restream: " +
            cleanText(
              err.message,
              500
            )
        );
    }
  }
);

/* =========================================================
   Refresh Access Token
========================================================= */

async function refreshAccessToken(room) {
  if (
    !room?.restream?.refreshToken
  ) {
    throw new Error(
      "لا يوجد Refresh Token للغرفة."
    );
  }

  const basicAuth =
    Buffer.from(
      `${RESTREAM_CLIENT_ID}:${RESTREAM_CLIENT_SECRET}`
    ).toString("base64");

  const body =
    new URLSearchParams({
      grant_type:
        "refresh_token",

      refresh_token:
        room.restream.refreshToken
    });

  const response =
    await fetch(
      "https://api.restream.io/oauth/token",
      {
        method: "POST",

        headers: {
          "Content-Type":
            "application/x-www-form-urlencoded",

          Authorization:
            `Basic ${basicAuth}`
        },

        body: body.toString()
      }
    );

  const data =
    await response.json();

  if (!response.ok) {
    console.error(
      `❌ Restream refresh error (${room.id}):`,
      data
    );

    throw new Error(
      data?.error?.message ||
        "فشل تحديث Restream token."
    );
  }

  const newAccessToken =
    data.access_token ||
    data.accessToken ||
    "";

  const newRefreshToken =
    data.refresh_token ||
    data.refreshToken ||
    "";

  if (!newAccessToken) {
    throw new Error(
      "Restream لم يرجع Access Token جديد."
    );
  }

  /*
    مهم:
    Restream يصدر زوجًا جديدًا عند refresh.
    لذلك يجب حفظ refresh token الجديد أيضًا.
  */
  room.restream.accessToken =
    newAccessToken;

  if (newRefreshToken) {
    room.restream.refreshToken =
      newRefreshToken;
  }

  const expiresIn =
    Number(
      data.expires_in ||
        data.accessTokenExpiresIn ||
        3600
    );

  room.restream.expiresAt =
    now() +
    Math.max(
      60,
      expiresIn - 120
    ) *
      1000;

  console.log(
    `🔄 Restream token refreshed for room ${room.id}`
  );

  return room.restream.accessToken;
}

/* =========================================================
   الحصول على Access Token صالح
========================================================= */

async function getValidAccessToken(room) {
  if (
    !room?.restream?.accessToken
  ) {
    throw new Error(
      "لم يتم ربط حساب Restream بهذه الغرفة."
    );
  }

  if (
    room.restream.expiresAt &&
    now() <
      room.restream.expiresAt
  ) {
    return room.restream.accessToken;
  }

  return refreshAccessToken(
    room
  );
}

/* =========================================================
   كلمة التسجيل
========================================================= */

function registrationComment(
  text,
  keyword
) {
  const comment =
    cleanText(text);

  const key =
    cleanText(
      keyword,
      100
    );

  if (!comment || !key) {
    return null;
  }

  const c =
    normalized(comment);

  const k =
    normalized(key);

  if (!c.startsWith(k)) {
    return null;
  }

  /*
    إذا كانت الكلمة:
    ارواح!

    فـ:
    ارواح! حسن
    مقبول

    أما:
    ارواح!حسن
    غير مقبول

    حتى لا نسجل كلمات تبدأ صدفة بالكلمة.
  */
  const next =
    comment.charAt(
      key.length
    );

  if (
    next &&
    !/\s/.test(next)
  ) {
    return null;
  }

  return comment;
}

/* =========================================================
   إضافة مشارك
========================================================= */

function addParticipant(
  room,
  displayName,
  userId,
  comment,
  platform
) {
  const name =
    cleanText(
      displayName,
      100
    );

  const text =
    cleanText(
      comment,
      500
    );

  const source =
    cleanText(
      platform,
      50
    ) || "unknown";

  if (!name || !text) {
    return false;
  }

  /*
    مهم جدًا:
    لا نستخدم ID وحده.

    مثال:
    YouTube user ID = 123
    Twitch user ID = 123

    هما شخصان مختلفان.

    لذلك المفتاح:
    platform + userId
  */
  const key =
    userId
      ? `id:${source}:${String(userId)}`
      : `name:${source}:${normalized(name)}`;

  if (
    room.history.has(key)
  ) {
    return false;
  }

  room.history.add(key);

  const participant = {
    youtubeName: name,

    comment: text,

    key,

    platform: source,

    userId:
      userId
        ? String(userId)
        : null
  };

  if (
    room.participants.length <
    MAX_PARTICIPANTS
  ) {
    room.participants.push(
      participant
    );
  } else {
    room.queue.push(
      participant
    );
  }

  touchRoom(room);

  sendRoomState(room);

  return true;
}

/* =========================================================
   تعبئة القائمة
========================================================= */

function fillFromQueue(room) {
  while (
    room.participants.length <
      MAX_PARTICIPANTS &&
    room.queue.length > 0
  ) {
    room.participants.push(
      room.queue.shift()
    );
  }
}

/* =========================================================
   حذف مشارك
========================================================= */

function deleteParticipant(
  room,
  index
) {
  if (
    !Number.isInteger(index) ||
    index < 0 ||
    index >=
      room.participants.length
  ) {
    return false;
  }

  room.participants.splice(
    index,
    1
  );

  fillFromQueue(room);

  touchRoom(room);

  sendRoomState(room);

  return true;
}

/* =========================================================
   تنظيف مؤقتات Restream
========================================================= */

function clearRestreamTimers(room) {
  if (!room) {
    return;
  }

  if (
    room.restream.reconnectTimer
  ) {
    clearTimeout(
      room.restream.reconnectTimer
    );

    room.restream.reconnectTimer =
      null;
  }

  if (
    room.restream.heartbeatTimer
  ) {
    clearInterval(
      room.restream.heartbeatTimer
    );

    room.restream.heartbeatTimer =
      null;
  }
}

/* =========================================================
   إيقاف Restream Chat
========================================================= */

function stopRestreamChat(room) {
  if (!room) {
    return;
  }

  clearRestreamTimers(room);

  room.restream.reconnecting =
    false;

  const socket =
    room.restream.socket;

  room.restream.socket =
    null;

  room.restream.connected =
    false;

  room.restream.lastHeartbeatAt =
    0;

  if (socket) {
    try {
      socket.close();
    } catch (_) {}
  }
}

/* =========================================================
   استخراج نص الحدث
========================================================= */

function extractChatMessage(action) {
  if (
    !action ||
    action.action !== "event"
  ) {
    return null;
  }

  const eventTypeId =
    Number(
      action.eventTypeId ??
      action.payload?.eventTypeId ??
      action.data?.eventTypeId
    );

  /*
    Restream الحالي يرسل eventTypeId
    مع eventPayload.
  */
  const payload =
    action.eventPayload ||
    action.payload?.eventPayload ||
    action.data?.eventPayload ||
    {};

  /*
    لا نعالج إلا Text events.
  */
  if (
    !TEXT_EVENT_TYPE_IDS.has(
      eventTypeId
    )
  ) {
    return null;
  }

  const text =
    cleanText(
      payload.text,
      500
    );

  if (!text) {
    return null;
  }

  const author =
    payload.author || {};

  const displayName =
    author.displayName ||
    author.name ||
    author.username ||
    author.nickname ||
    "مستخدم";

  const userId =
    author.id !== undefined &&
    author.id !== null
      ? String(author.id)
      : null;

  /*
    Restream eventSourceId:
    يحدد مصدر الرسالة.
  */
  const eventSourceId =
    Number(
      action.eventSourceId ??
      action.payload?.eventSourceId ??
      action.data?.eventSourceId
    );

  return {
    eventTypeId,

    eventSourceId,

    text,

    displayName:
      cleanText(
        displayName,
        100
      ),

    userId
  };
}

/* =========================================================
   التحقق من أن الحدث ليس أقدم من بداية المسابقة
========================================================= */

function eventIsAfterCompetitionStart(
  room,
  action
) {
  if (
    !room.competitionStartedAt
  ) {
    return false;
  }

  /*
    بعض رسائل Restream قد تحتوي timestamp
    وبعضها قد لا تحتويه.

    إذا وجدنا timestamp واضحًا:
    نقارنه.

    إذا لم يوجد:
    نعتمد على أن WebSocket نفسه تم فتحه
    بعد الضغط على بداية.
  */
  const possibleTimestamp =
    action.timestamp ??
    action.payload?.timestamp ??
    action.eventPayload?.timestamp;

  if (
    possibleTimestamp !== undefined &&
    possibleTimestamp !== null
  ) {
    let eventTime =
      Number(
        possibleTimestamp
      );

    /*
      إذا كان بالثواني نحوله إلى ms.
    */
    if (
      Number.isFinite(eventTime) &&
      eventTime > 0 &&
      eventTime < 100000000000
    ) {
      eventTime *= 1000;
    }

    if (
      Number.isFinite(eventTime) &&
      eventTime > 0
    ) {
      return (
        eventTime >=
        room.competitionStartedAt
      );
    }

    const parsed =
      Date.parse(
        String(possibleTimestamp)
      );

    if (
      Number.isFinite(parsed)
    ) {
      return (
        parsed >=
        room.competitionStartedAt
      );
    }
  }

  /*
    لا يوجد timestamp:
    لأن اتصال الشات الحالي بدأ بعد الضغط
    على "بداية"، نسمح بالحدث.
  */
  return true;
}

/* =========================================================
   بدء Restream Chat WebSocket
========================================================= */

async function startRestreamChat(room) {
  if (!room) {
    return;
  }

  if (!room.active) {
    return;
  }

  /*
    إذا يوجد socket شغال، لا ننشئ واحدًا ثانيًا.
  */
  if (
    room.restream.socket &&
    (
      room.restream.socket.readyState ===
        WebSocket.OPEN ||
      room.restream.socket.readyState ===
        WebSocket.CONNECTING
    )
  ) {
    return;
  }

  /*
    منع سباق الاتصالات.
  */
  if (
    room.restream.reconnecting
  ) {
    return;
  }

  room.restream.reconnecting =
    true;

  room.status =
    "الاتصال بشات Restream...";

  sendRoomState(room);

  let token;

  try {
    token =
      await getValidAccessToken(
        room
      );
  } catch (err) {
    room.restream.reconnecting =
      false;

    throw err;
  }

  if (!room.active) {
    room.restream.reconnecting =
      false;

    return;
  }

  const url =
    `wss://chat.api.restream.io/ws?accessToken=${encodeURIComponent(
      token
    )}`;

  console.log(
    `🔌 Connecting Restream Chat for room ${room.id}`
  );

  let socket;

  try {
    socket =
      new WebSocket(url);
  } catch (err) {
    room.restream.reconnecting =
      false;

    throw err;
  }

  room.restream.socket =
    socket;

  /*
    نثبت وقت فتح هذا الاتصال.
    هذا هو اتصال المسابقة الحالي.
  */
  room.chatConnectionStartedAt =
    now();

  socket.on(
    "open",
    () => {
      /*
        قد يكون socket قديمًا وتم استبداله.
      */
      if (
        room.restream.socket !==
        socket
      ) {
        try {
          socket.close();
        } catch (_) {}

        return;
      }

      room.restream.reconnecting =
        false;

      room.restream.connected =
        true;

      room.restream.lastHeartbeatAt =
        now();

      room.status =
        "متصل بشات Restream ويستقبل الرسائل";

      touchRoom(room);

      console.log(
        `✅ Restream Chat connected: ${room.id}`
      );

      sendRoomState(room);

      clearInterval(
        room.restream.heartbeatTimer
      );

      room.restream.heartbeatTimer =
        setInterval(
          () => {
            if (
              !room.active ||
              room.restream.socket !==
                socket
            ) {
              return;
            }

            const elapsed =
              now() -
              room.restream.lastHeartbeatAt;

            if (
              elapsed >
              HEARTBEAT_TIMEOUT_MS
            ) {
              console.warn(
                `⚠️ Restream heartbeat timeout: ${room.id}`
              );

              try {
                socket.terminate();
              } catch (_) {}
            }
          },
          HEARTBEAT_CHECK_INTERVAL_MS
        );
    }
  );

  socket.on(
    "message",
    (raw) => {
      /*
        تجاهل رسائل socket قديم.
      */
      if (
        room.restream.socket !==
        socket
      ) {
        return;
      }

      let action;

      try {
        action =
          JSON.parse(
            raw.toString()
          );
      } catch (err) {
        console.error(
          `❌ Invalid Restream message (${room.id}):`,
          err.message
        );

        return;
      }

      /*
        Heartbeat.
      */
      if (
        action.action ===
        "heartbeat"
      ) {
        room.restream.lastHeartbeatAt =
          now();

        return;
      }

      /*
        معلومات الاتصال.
      */
      if (
        action.action ===
        "connection_info"
      ) {
        const payload =
          action.payload || {};

        console.log(
          `ℹ️ Restream connection ${
            payload.status || "unknown"
          } - room ${room.id}`
        );

        if (
          payload.status ===
          "error"
        ) {
          console.error(
            `❌ Restream source error (${room.id}):`,
            payload.reason ||
              payload.message ||
              "unknown"
          );
        }

        return;
      }

      /*
        اتصال مصدر انتهى.
      */
      if (
        action.action ===
        "connection_closed"
      ) {
        console.warn(
          `⚠️ Restream source connection closed: ${room.id}`
        );

        return;
      }

      /*
        لا نحتاج أي action آخر.
      */
      if (
        action.action !==
        "event"
      ) {
        return;
      }

      /*
        يجب أن تكون المسابقة ما زالت فعالة.
      */
      if (!room.active) {
        return;
      }

      /*
        لا نريد أحداثًا أقدم من بداية المسابقة
        إذا كان Restream قد أرسل timestamp.
      */
      if (
        !eventIsAfterCompetitionStart(
          room,
          action
        )
      ) {
        return;
      }

      const message =
        extractChatMessage(
          action
        );

      if (!message) {
        return;
      }

      const {
        text,
        displayName,
        userId,
        eventSourceId
      } = message;

      /*
        لا نعرض الـtokens أو أي معلومات حساسة.
      */
      console.log(
        `💬 Restream chat [room=${room.id}] [source=${eventSourceId}]: ${text}`
      );

      /*
        هل يبدأ التعليق بكلمة التسجيل؟
      */
      const comment =
        registrationComment(
          text,
          room.keyword
        );

      if (!comment) {
        return;
      }

      /*
        نستخدم eventSourceId كجزء من
        معرف المنصة.

        هذا يحل مشكلة:
        نفس user ID في منصتين مختلفتين.
      */
      const platform =
        eventSourceId
          ? `source-${eventSourceId}`
          : "unknown";

      const added =
        addParticipant(
          room,
          displayName,
          userId,
          comment,
          platform
        );

      if (added) {
        console.log(
          `✅ تمت إضافة مشارك في الغرفة ${room.id}: ${displayName} - ${comment}`
        );
      }
    }
  );

  socket.on(
    "error",
    (err) => {
      console.error(
        `❌ Restream WebSocket error (${room.id}):`,
        err.message
      );
    }
  );

  socket.on(
    "close",
    (code, reason) => {
      console.log(
        `🔌 Restream Chat disconnected: ${room.id} (${code})`
      );

      if (
        room.restream.socket ===
        socket
      ) {
        room.restream.socket =
          null;

        room.restream.connected =
          false;

        room.restream.lastHeartbeatAt =
          0;
      }

      if (
        room.restream.heartbeatTimer
      ) {
        clearInterval(
          room.restream.heartbeatTimer
        );

        room.restream.heartbeatTimer =
          null;
      }

      if (
        !room.active
      ) {
        room.restream.reconnecting =
          false;

        return;
      }

      /*
        إذا انقطع أثناء المسابقة،
        نعيد الاتصال.
      */
      scheduleRestreamReconnect(
        room
      );
    }
  );
}

/* =========================================================
   إعادة الاتصال بـ Restream
========================================================= */

function scheduleRestreamReconnect(
  room
) {
  if (
    !room ||
    !room.active
  ) {
    return;
  }

  if (
    room.restream.reconnectTimer
  ) {
    return;
  }

  if (
    room.restream.socket &&
    (
      room.restream.socket.readyState ===
        WebSocket.OPEN ||
      room.restream.socket.readyState ===
        WebSocket.CONNECTING
    )
  ) {
    return;
  }

  room.restream.reconnecting =
    true;

  room.status =
    "إعادة الاتصال بـRestream...";

  sendRoomState(room);

  room.restream.reconnectTimer =
    setTimeout(
      async () => {
        room.restream.reconnectTimer =
          null;

        room.restream.reconnecting =
          false;

        if (
          !room.active
        ) {
          return;
        }

        try {
          await startRestreamChat(
            room
          );
        } catch (err) {
          console.error(
            `❌ Restream reconnect failed (${room.id}):`,
            err.message
          );

          if (
            !room.active
          ) {
            return;
          }

          room.status =
            "تعذر الاتصال بـRestream، ستتم إعادة المحاولة...";

          sendRoomState(room);

          scheduleRestreamReconnect(
            room
          );
        }
      },
      RESTREAM_RECONNECT_DELAY_MS
    );
}

/* =========================================================
   بدء المسابقة
========================================================= */

app.post(
  "/api/rooms/:roomId/start",
  async (req, res) => {
    const room =
      getRoomFromRequest(
        req,
        res
      );

    if (!room) {
      return;
    }

    try {
      /*
        تغيير كلمة التسجيل عند البداية
        اختياري.
      */
      if (
        Object.prototype.hasOwnProperty.call(
          req.body || {},
          "keyword"
        )
      ) {
        const keyword =
          cleanText(
            req.body.keyword,
            100
          );

        if (!keyword) {
          throw new Error(
            "كلمة التسجيل لا يمكن أن تكون فارغة."
          );
        }

        room.keyword =
          keyword;
      }

      if (
        !room.restream.accessToken
      ) {
        throw new Error(
          "اربط حساب Restream أولًا."
        );
      }

      /*
        نغلق أي اتصال سابق بالكامل.
      */
      stopRestreamChat(room);

      /*
        نبدأ مسابقة نظيفة.
      */
      room.participants = [];

      room.queue = [];

      room.history =
        new Set();

      /*
        هذا هو الحد الفاصل:
        أي تسجيل بعد هذه اللحظة فقط.
      */
      room.competitionStartedAt =
        now();

      room.chatConnectionStartedAt =
        0;

      room.active = true;

      room.status =
        "يستعد للاتصال بشات Restream...";

      touchRoom(room);

      sendRoomState(room);

      await startRestreamChat(
        room
      );

      json(res, 200, {
        ok: true,

        state:
          publicRoomState(room)
      });
    } catch (err) {
      console.error(
        `❌ Start error (${room.id}):`,
        err
      );

      room.active = false;

      room.status =
        "خطأ";

      stopRestreamChat(room);

      sendRoomState(room);

      json(res, 400, {
        ok: false,

        error:
          cleanText(
            err.message,
            500
          )
      });
    }
  }
);

/* =========================================================
   إيقاف المسابقة
========================================================= */

app.post(
  "/api/rooms/:roomId/stop",
  (req, res) => {
    const room =
      getRoomFromRequest(
        req,
        res
      );

    if (!room) {
      return;
    }

    room.active = false;

    room.status =
      "متوقف";

    room.competitionStartedAt =
      0;

    room.chatConnectionStartedAt =
      0;

    stopRestreamChat(room);

    touchRoom(room);

    sendRoomState(room);

    json(res, 200, {
      ok: true,

      state:
        publicRoomState(room)
    });
  }
);

/* =========================================================
   تغيير كلمة التسجيل
========================================================= */

app.post(
  "/api/rooms/:roomId/config",
  (req, res) => {
    const room =
      getRoomFromRequest(
        req,
        res
      );

    if (!room) {
      return;
    }

    if (
      Object.prototype.hasOwnProperty.call(
        req.body || {},
        "keyword"
      )
    ) {
      const keyword =
        cleanText(
          req.body.keyword,
          100
        );

      if (!keyword) {
        return json(res, 400, {
          ok: false,

          error:
            "كلمة التسجيل لا يمكن أن تكون فارغة."
        });
      }

      room.keyword =
        keyword;
    }

    touchRoom(room);

    sendRoomState(room);

    json(res, 200, {
      ok: true,

      state:
        publicRoomState(room)
    });
  }
);

/* =========================================================
   حذف مشارك
========================================================= */

app.post(
  "/api/rooms/:roomId/delete/:index",
  (req, res) => {
    const room =
      getRoomFromRequest(
        req,
        res
      );

    if (!room) {
      return;
    }

    const deleted =
      deleteParticipant(
        room,
        Number(
          req.params.index
        )
      );

    json(res, 200, {
      ok: true,

      deleted,

      state:
        publicRoomState(room)
    });
  }
);

/* =========================================================
   Reset للغرفة
========================================================= */

app.post(
  "/api/rooms/:roomId/reset",
  (req, res) => {
    const room =
      getRoomFromRequest(
        req,
        res
      );

    if (!room) {
      return;
    }

    room.active = false;

    room.status =
      "جاهز";

    stopRestreamChat(room);

    room.participants = [];

    room.queue = [];

    room.history =
      new Set();

    room.competitionStartedAt =
      0;

    room.chatConnectionStartedAt =
      0;

    touchRoom(room);

    sendRoomState(room);

    json(res, 200, {
      ok: true,

      state:
        publicRoomState(room)
    });
  }
);

/* =========================================================
   Logout / إنهاء الغرفة
========================================================= */

app.post(
  "/api/rooms/:roomId/logout",
  async (req, res) => {
    const room =
      rooms.get(
        req.params.roomId
      );

    if (!room) {
      return json(res, 200, {
        ok: true
      });
    }

    await destroyRoom(
      room,
      true
    );

    json(res, 200, {
      ok: true
    });
  }
);

/* =========================================================
   WebSocket للمتصفحات
========================================================= */

const browserWss =
  new WebSocket.Server({
    noServer: true
  });

server.on(
  "upgrade",
  (request, socket, head) => {
    try {
      const base =
        PUBLIC_URL ||
        `http://127.0.0.1:${PORT}`;

      const url =
        new URL(
          request.url,
          base
        );

      /*
        فقط /ws مسموح له بالدخول.
      */
      if (
        url.pathname !==
        "/ws"
      ) {
        socket.destroy();
        return;
      }

      const roomId =
        url.searchParams.get(
          "room"
        );

      if (!roomId) {
        socket.destroy();
        return;
      }

      const room =
        rooms.get(roomId);

      if (!room) {
        socket.destroy();
        return;
      }

      browserWss.handleUpgrade(
        request,
        socket,
        head,
        (ws) => {
          browserWss.emit(
            "connection",
            ws,
            request,
            room
          );
        }
      );
    } catch (err) {
      console.error(
        "❌ Browser WebSocket upgrade error:",
        err.message
      );

      socket.destroy();
    }
  }
);

browserWss.on(
  "connection",
  (ws, request, room) => {
    if (!room.browserClients) {
      room.browserClients =
        new Set();
    }

    room.browserClients.add(
      ws
    );

    touchRoom(room);

    room.lastBrowserActivityAt =
      now();

    console.log(
      `🔌 Browser connected to room ${room.id}`
    );

    /*
      إرسال الحالة فور الاتصال.
    */
    try {
      ws.send(
        JSON.stringify({
          type: "state",

          state:
            publicRoomState(room)
        })
      );
    } catch (_) {}

    /*
      يسمح للواجهة بإرسال:
      {"type":"ping"}
      لإثبات أن الصفحة ما زالت مفتوحة.
    */
    ws.on(
      "message",
      (raw) => {
        room.lastBrowserActivityAt =
          now();

        touchRoom(room);

        let message;

        try {
          message =
            JSON.parse(
              raw.toString()
            );
        } catch (_) {
          return;
        }

        if (
          message?.type ===
          "ping"
        ) {
          try {
            ws.send(
              JSON.stringify({
                type: "pong"
              })
            );
          } catch (_) {}
        }
      }
    );

    ws.on(
      "close",
      () => {
        room.browserClients.delete(
          ws
        );

        room.lastBrowserActivityAt =
          now();

        touchRoom(room);

        console.log(
          `🔌 Browser disconnected from room ${room.id}`
        );
      }
    );

    ws.on(
      "error",
      (err) => {
        console.error(
          `❌ Browser WS error (${room.id}):`,
          err.message
        );
      }
    );
  }
);

/* =========================================================
   حذف / تنظيف الغرفة
========================================================= */

async function revokeRestreamToken(
  room
) {
  if (
    !room?.restream
  ) {
    return;
  }

  const token =
    room.restream.refreshToken ||
    room.restream.accessToken;

  if (!token) {
    return;
  }

  /*
    نحاول إلغاء التوكن.
    إذا فشل الإلغاء لا نوقف حذف الغرفة.
  */
  try {
    const body =
      new URLSearchParams({
        token,

        token_type_hint:
          room.restream.refreshToken
            ? "refresh_token"
            : "access_token"
      });

    const response =
      await fetch(
        "https://api.restream.io/oauth/revoke",
        {
          method: "POST",

          headers: {
            "Content-Type":
              "application/x-www-form-urlencoded"
          },

          body:
            body.toString()
        }
      );

    if (!response.ok) {
      console.warn(
        `⚠️ Restream token revoke failed for room ${room.id}: HTTP ${response.status}`
      );
    }
  } catch (err) {
    console.warn(
      `⚠️ Restream token revoke error for room ${room.id}:`,
      err.message
    );
  }
}

async function destroyRoom(
  room,
  revokeToken = false
) {
  if (!room) {
    return;
  }

  /*
    نمنع استدعاء التنظيف أكثر من مرة.
  */
  if (room.destroying) {
    return;
  }

  room.destroying = true;

  console.log(
    `🧹 حذف الغرفة: ${room.id}`
  );

  room.active = false;

  /*
    أوقف Restream أولًا.
  */
  stopRestreamChat(room);

  /*
    إلغاء التوكن اختياري.
    عند انتهاء TTL نكتفي بمسحه من الذاكرة.
    عند logout يمكننا إلغاءه من Restream أيضًا.
  */
  if (revokeToken) {
    await revokeRestreamToken(
      room
    );
  }

  /*
    إغلاق واجهات المتصفح.
  */
  if (
    room.browserClients
  ) {
    for (
      const ws of room.browserClients
    ) {
      try {
        ws.close(
          1000,
          "Room closed"
        );
      } catch (_) {}
    }

    room.browserClients.clear();
  }

  /*
    حذف جميع البيانات الحساسة.
  */
  room.participants = [];

  room.queue = [];

  room.history.clear();

  room.restream.accessToken =
    "";

  room.restream.refreshToken =
    "";

  room.restream.expiresAt =
    0;

  room.competitionStartedAt =
    0;

  room.chatConnectionStartedAt =
    0;

  /*
    حذف الغرفة من الذاكرة.
  */
  rooms.delete(
    room.id
  );
}

/* =========================================================
   تنظيف الغرف القديمة
========================================================= */

setInterval(
  () => {
    const current =
      now();

    for (
      const room of rooms.values()
    ) {
      /*
        إذا كان هناك نشاط حديث،
        لا نحذف الغرفة.
      */
      if (
        current -
          room.lastActivityAt <=
        ROOM_TTL_MS
      ) {
        continue;
      }

      /*
        لا نحذف غرفة عليها متصفح متصل
        حتى لو كانت المسابقة متوقفة.
      */
      if (
        room.browserClients &&
        room.browserClients.size > 0
      ) {
        continue;
      }

      destroyRoom(
        room,
        false
      ).catch(
        (err) => {
          console.error(
            `❌ Room cleanup error (${room.id}):`,
            err.message
          );
        }
      );
    }
  },
  60 * 1000
);

/* =========================================================
   تنظيف OAuth Sessions القديمة
========================================================= */

setInterval(
  () => {
    const current =
      now();

    for (
      const [
        state,
        session
      ] of oauthSessions
    ) {
      if (
        current -
          session.createdAt >
        OAUTH_SESSION_TTL_MS
      ) {
        oauthSessions.delete(
          state
        );
      }
    }
  },
  60 * 1000
);

/* =========================================================
   تشغيل السيرفر
========================================================= */

server.listen(
  PORT,
  "0.0.0.0",
  () => {
    console.log(
      `🚀 Server running on port ${PORT}`
    );

    console.log(
      `🌐 Public URL: ${PUBLIC_URL}`
    );

    console.log(
      `🔐 Restream Redirect URI: ${REDIRECT_URI}`
    );
  }
);

/* =========================================================
   أخطاء السيرفر
========================================================= */

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
