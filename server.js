const express = require("express");
const http = require("http");
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;

const app = express();
const server = http.createServer(app);

app.use(express.json({ limit: "100kb" }));
app.use(express.static(path.join(__dirname, "public")));

/* =========================================================
   الإعدادات
========================================================= */

const PUBLIC_URL =
  process.env.RENDER_EXTERNAL_URL ||
  "https://live-spirits-board.onrender.com";

const REDIRECT_URI =
  process.env.RESTREAM_REDIRECT_URI ||
  `${PUBLIC_URL}/oauth2callback`;

const RESTREAM_CLIENT_ID =
  process.env.RESTREAM_CLIENT_ID || "";

const RESTREAM_CLIENT_SECRET =
  process.env.RESTREAM_CLIENT_SECRET || "";

const ROOM_TTL_MS =
  30 * 60 * 1000; // 30 دقيقة بعد آخر نشاط

const MAX_PARTICIPANTS = 40;

const DEFAULT_KEYWORD = "ارواح!";

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

/*
  كل غرفة لها:
  - معرف خاص
  - كلمة تسجيل خاصة
  - مشاركون خاصون
  - Restream OAuth خاص
  - WebSocket خاص
  - رابط OBS خاص

  لا يوجد state عالمي للمستخدمين.
*/

const rooms = new Map();

/* =========================================================
   أدوات عامة
========================================================= */

function cleanText(value, maxLength = 500) {
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

function randomId(bytes = 18) {
  return crypto.randomBytes(bytes).toString("base64url");
}

function now() {
  return Date.now();
}

/* =========================================================
   ملفات الجلسات المؤقتة
========================================================= */

/*
  لا نخزن بيانات المستخدم في GitHub.
  كل شيء مؤقت داخل ذاكرة السيرفر.

  إذا أعاد Render التشغيل:
  تختفي الغرف والـ tokens من الذاكرة.
*/

const oauthSessions = new Map();

/* =========================================================
   إنشاء غرفة
========================================================= */

function createRoom() {
  const id = randomId(12);

  const room = {
    id,

    createdAt: now(),

    lastActivityAt: now(),

    active: false,

    keyword: DEFAULT_KEYWORD,

    participants: [],

    queue: [],

    history: new Set(),

    competitionStartedAt: 0,

    status: "جاهز",

    restream: {
      accessToken: "",
      refreshToken: "",
      expiresAt: 0,

      connected: false,

      socket: null,

      reconnectTimer: null,

      reconnecting: false,

      heartbeatTimer: null
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

    participants:
      room.participants.map((p) => ({
        youtubeName: p.youtubeName,
        comment: p.comment
      })),

    queued: room.queue.length,

    count: room.participants.length,

    status: room.status,

    obsUrl:
      `${PUBLIC_URL}/obs/${room.id}`
  };
}

/* =========================================================
   WebSocket للواجهة
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
    if (client.readyState === WebSocket.OPEN) {
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
}

/* =========================================================
   إرسال JSON
========================================================= */

function json(res, status, data) {
  res
    .status(status)
    .json(data);
}

/* =========================================================
   التحقق من الغرفة
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
  const publicIndex =
    path.join(
      __dirname,
      "public",
      "index.html"
    );

  const rootIndex =
    path.join(
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
    .send(
      "لم يتم العثور على index.html"
    );
});

/* =========================================================
   لوحة التحكم
========================================================= */

app.get(
  "/control.html",
  (req, res, next) => {
    const publicControl =
      path.join(
        __dirname,
        "public",
        "control.html"
      );

    const rootControl =
      path.join(
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

/* =========================================================
   صفحة OBS
========================================================= */

app.get(
  "/obs/:roomId",
  (req, res) => {
    const room =
      rooms.get(req.params.roomId);

    if (!room) {
      return res
        .status(404)
        .send(
          "هذه الغرفة انتهت."
        );
    }

    const publicObs =
      path.join(
        __dirname,
        "public",
        "obs.html"
      );

    const rootObs =
      path.join(
        __dirname,
        "obs.html"
      );

    if (fs.existsSync(publicObs)) {
      return res.sendFile(publicObs);
    }

    if (fs.existsSync(rootObs)) {
      return res.sendFile(rootObs);
    }

    res
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
    const room =
      createRoom();

    json(res, 200, {
      ok: true,

      room: {
        roomId: room.id,

        controlUrl:
          `${PUBLIC_URL}/control.html?room=${room.id}`,

        obsUrl:
          `${PUBLIC_URL}/obs/${room.id}`,

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
   OAuth
========================================================= */

/*
  نستخدم state مختلف لكل محاولة تسجيل دخول.
  هذا يمنع CSRF.
*/

app.get(
  "/api/auth/:roomId",
  (req, res) => {
    const room =
      rooms.get(
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

    touchRoom(room);

    const state =
      randomId(24);

    oauthSessions.set(
      state,
      {
        roomId: room.id,

        createdAt: now()
      }
    );

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

    res.redirect(
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
          state
        );

      oauthSessions.delete(
        state
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
        10 * 60 * 1000
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
        ).toString(
          "base64"
        );

      const body =
        new URLSearchParams({
          grant_type:
            "authorization_code",

          redirect_uri:
            REDIRECT_URI,

          code
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

            body
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

      room.restream.accessToken =
        data.access_token ||
        data.accessToken ||
        "";

      room.restream.refreshToken =
        data.refresh_token ||
        data.refreshToken ||
        "";

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

      touchRoom(room);

      console.log(
        `✅ Restream connected for room ${room.id}`
      );

      sendRoomState(room);

      res.send(`
        <!doctype html>
        <html lang="ar" dir="rtl">
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width,initial-scale=1">
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

      res
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

async function refreshAccessToken(
  room
) {
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
    ).toString(
      "base64"
    );

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

        body
      }
    );

  const data =
    await response.json();

  if (!response.ok) {
    console.error(
      "❌ Restream refresh error:",
      data
    );

    throw new Error(
      data?.error?.message ||
        "فشل تحديث Restream token."
    );
  }

  room.restream.accessToken =
    data.access_token ||
    data.accessToken ||
    room.restream.accessToken;

  if (
    data.refresh_token ||
    data.refreshToken
  ) {
    room.restream.refreshToken =
      data.refresh_token ||
      data.refreshToken;
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

async function getValidAccessToken(
  room
) {
  if (!room.restream.accessToken) {
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
  comment
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

  if (!name || !text) {
    return false;
  }

  const key =
    userId
      ? `id:${userId}`
      : `name:${normalized(name)}`;

  if (
    room.history.has(key)
  ) {
    return false;
  }

  room.history.add(key);

  const participant = {
    youtubeName: name,

    comment: text,

    key
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
   إيقاف Restream Chat
========================================================= */

function stopRestreamChat(
  room
) {
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

  room.restream.reconnecting =
    false;

  if (
    room.restream.socket
  ) {
    try {
      room.restream.socket.close();
    } catch (_) {}
  }

  room.restream.socket =
    null;

  room.restream.connected =
    false;
}

/* =========================================================
   بدء Restream Chat WebSocket
========================================================= */

async function startRestreamChat(
  room
) {
  if (!room) {
    return;
  }

  if (
    !room.active
  ) {
    return;
  }

  if (
    room.restream.socket &&
    room.restream.socket.readyState ===
      WebSocket.OPEN
  ) {
    return;
  }

  if (
    room.restream.reconnecting
  ) {
    return;
  }

  const token =
    await getValidAccessToken(
      room
    );

  room.restream.reconnecting =
    true;

  room.status =
    "الاتصال بشات Restream...";

  sendRoomState(room);

  const url =
    `wss://chat.api.restream.io/ws?accessToken=${encodeURIComponent(
      token
    )}`;

  console.log(
    `🔌 Connecting Restream Chat for room ${room.id}`
  );

  const socket =
    new WebSocket(url);

  room.restream.socket =
    socket;

  socket.on(
    "open",
    () => {
      room.restream.reconnecting =
        false;

      room.restream.connected =
        true;

      room.status =
        "متصل بشات Restream ويستقبل الرسائل";

      touchRoom(room);

      console.log(
        `✅ Restream Chat connected: ${room.id}`
      );

      sendRoomState(room);

      if (
        room.restream.heartbeatTimer
      ) {
        clearInterval(
          room.restream.heartbeatTimer
        );
      }

      /*
        Restream يرسل heartbeat تقريبًا كل 45 ثانية.
        إذا لم يصل heartbeat خلال 60 ثانية
        نعيد إنشاء الاتصال.
      */

      room.restream.lastHeartbeatAt =
        now();

      room.restream.heartbeatTimer =
        setInterval(
          () => {
            if (
              !room.active
            ) {
              return;
            }

            if (
              now() -
                room.restream.lastHeartbeatAt >
              70 * 1000
            ) {
              console.warn(
                `⚠️ Restream heartbeat timeout: ${room.id}`
              );

              try {
                socket.close();
              } catch (_) {}
            }
          },
          15000
        );
    }
  );

  socket.on(
    "message",
    (raw) => {
      if (
        room.restream.socket !==
        socket
      ) {
        return;
      }

      touchRoom(room);

      let action;

      try {
        action =
          JSON.parse(
            raw.toString()
          );
      } catch (err) {
        console.error(
          "❌ Invalid Restream message:",
          err.message
        );

        return;
      }

      /*
        heartbeat
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
        connection_info
      */

      if (
        action.action ===
        "connection_info"
      ) {
        const payload =
          action.payload || {};

        console.log(
          `ℹ️ Restream connection ${payload.status} - room ${room.id}`
        );

        if (
          payload.status ===
          "error"
        ) {
          console.error(
            "❌ Restream source error:",
            payload.reason
          );
        }

        return;
      }

      /*
        connection_closed
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
        event
      */

      if (
        action.action !==
        "event"
      ) {
        return;
      }

      const payload =
        action.payload || {};

      const eventPayload =
        payload.eventPayload || {};

      /*
        نحتاج فقط رسائل النص.
        نتجاهل Super Chat / Stickers / Memberships
        وغيرها.

        eventTypeId:
        1  Discord
        2  DLive
        4  Twitch
        5  YouTube
        11 Facebook
        21 LinkedIn
        22 Trovo
        24 X
        25 Kick
        32 Rumble
      */

      const text =
        cleanText(
          eventPayload.text,
          500
        );

      if (!text) {
        return;
      }

      /*
        اسم المستخدم من مختلف المنصات.
      */

      const author =
        eventPayload.author || {};

      const displayName =
        author.displayName ||
        author.name ||
        author.username ||
        author.nickname ||
        "مستخدم";

      const userId =
        author.id
          ? String(author.id)
          : null;

      console.log(
        `💬 Restream chat: ${text}`
      );

      /*
        البحث عن كلمة التسجيل فقط.
      */

      const comment =
        registrationComment(
          text,
          room.keyword
        );

      if (!comment) {
        return;
      }

      const added =
        addParticipant(
          room,
          displayName,
          userId,
          comment
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
    () => {
      console.log(
        `🔌 Restream Chat disconnected: ${room.id}`
      );

      if (
        room.restream.socket ===
        socket
      ) {
        room.restream.socket =
          null;

        room.restream.connected =
          false;
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
        room.active
      ) {
        scheduleRestreamReconnect(
          room
        );
      }
    }
  );
}

/* =========================================================
   إعادة الاتصال بـRestream
========================================================= */

function scheduleRestreamReconnect(
  room
) {
  if (
    !room ||
    !room.active ||
    room.restream.reconnecting
  ) {
    return;
  }

  room.restream.reconnecting =
    true;

  room.status =
    "إعادة الاتصال بـRestream...";

  sendRoomState(room);

  if (
    room.restream.reconnectTimer
  ) {
    clearTimeout(
      room.restream.reconnectTimer
    );
  }

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

          room.status =
            "تعذر الاتصال بـRestream، ستتم إعادة المحاولة...";

          sendRoomState(room);

          setTimeout(
            () => {
              if (
                room.active
              ) {
                scheduleRestreamReconnect(
                  room
                );
              }
            },
            5000
          );
        }
      },
      2000
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
      if (
        req.body?.keyword
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

      stopRestreamChat(room);

      room.participants = [];

      room.queue = [];

      room.history =
        new Set();

      room.competitionStartedAt =
        now();

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
          err.message
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
  (req, res) => {
    const room =
      rooms.get(
        req.params.roomId
      );

    if (!room) {
      return json(res, 200, {
        ok: true
      });
    }

    destroyRoom(room);

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
      const url =
        new URL(
          request.url,
          PUBLIC_URL
        );

      /*
        واجهة المستخدم:
        /ws?room=ROOM_ID
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

    console.log(
      `🔌 Browser connected to room ${room.id}`
    );

    ws.send(
      JSON.stringify({
        type: "state",

        state:
          publicRoomState(room)
      })
    );

    ws.on(
      "close",
      () => {
        room.browserClients.delete(
          ws
        );

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
   تنظيف الغرف القديمة
========================================================= */

function destroyRoom(room) {
  if (!room) {
    return;
  }

  console.log(
    `🧹 حذف الغرفة: ${room.id}`
  );

  room.active = false;

  stopRestreamChat(room);

  if (
    room.browserClients
  ) {
    for (const ws of room.browserClients) {
      try {
        ws.close();
      } catch (_) {}
    }

    room.browserClients.clear();
  }

  room.participants = [];

  room.queue = [];

  room.history.clear();

  room.restream.accessToken =
    "";

  room.restream.refreshToken =
    "";

  rooms.delete(
    room.id
  );
}

setInterval(
  () => {
    const current =
      now();

    for (const room of rooms.values()) {
      if (
        current -
          room.lastActivityAt >
        ROOM_TTL_MS
      ) {
        destroyRoom(room);
      }
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
        10 * 60 * 1000
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
   أخطاء
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
