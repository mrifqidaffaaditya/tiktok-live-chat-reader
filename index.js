require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { WebcastPushConnection } = require('tiktok-live-connector');
const googleTTS = require('google-tts-api');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static('public'));

// ─── Konstanta & Validasi Config ──────────────────────────────────────────────
const PORT = parseInt(process.env.PORT) || 3000;
const DEBUG_MODE = process.env.DEBUG ? process.env.DEBUG.toLowerCase() : '';

// FIX: Validasi CACHE_TIMEOUT — jika NaN atau tidak wajar, fallback ke default
const _rawCacheTimeout = parseInt(process.env.CACHE_TIMEOUT);
const CACHE_TIMEOUT = (!isNaN(_rawCacheTimeout) && _rawCacheTimeout > 0) ? _rawCacheTimeout : 300000;

// Batas panjang maksimum untuk stream key (username TikTok) dari query param
const MAX_STREAM_KEY_LENGTH = 100;
// Batas panjang komentar yang diteruskan ke payload WS
const MAX_COMMENT_LENGTH = 300;
// Batas panjang username dari TikTok
const MAX_USERNAME_LENGTH = 100;

const audioCache = new Map();

// ─── Helper Sanitasi ──────────────────────────────────────────────────────────

/**
 * Sanitasi stream key dari query param ?stream=
 * Hanya izinkan karakter aman (alfanumerik, _, -, .)
 * Cegah log injection, XSS, dan path traversal.
 */
function sanitizeStreamKey(raw) {
    if (!raw || typeof raw !== 'string') return '*';
    const cleaned = raw.trim().substring(0, MAX_STREAM_KEY_LENGTH);
    // Hanya izinkan karakter aman
    if (!/^[a-zA-Z0-9_.\-*]+$/.test(cleaned)) return '*';
    return cleaned;
}

/**
 * Sanitasi string umum — hapus karakter kontrol dan batasi panjang.
 */
function sanitizeString(str, maxLen = 300) {
    if (typeof str !== 'string') return '';
    // Hapus karakter kontrol (newline injection, dll.) kecuali spasi biasa
    return str.replace(/[\x00-\x08\x0b-\x1f\x7f]/g, '').substring(0, maxLen);
}

// ─── Endpoint Audio (Lavalink Streaming) ─────────────────────────────────────
app.get('/audio/:id.mp3', (req, res) => {
    const id = req.params.id;
    // FIX: Validasi format audioId — hanya alfanumerik dan dash
    if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
        return res.status(400).send('Invalid audio ID');
    }
    const audioBuffer = audioCache.get(id);
    if (!audioBuffer) {
        return res.status(404).send('Audio not found or expired');
    }
    res.set('Content-Type', 'audio/mpeg');
    // FIX: Tambahkan header keamanan
    res.set('X-Content-Type-Options', 'nosniff');
    res.set('Cache-Control', 'no-store');
    res.send(audioBuffer);
});

// ─── Plain WebSocket Server ───────────────────────────────────────────────────
// Path: ws://localhost:<PORT>/ws/chat
// Path dengan filter sesi: ws://localhost:<PORT>/ws/chat?stream=<username>
const wss = new WebSocketServer({ noServer: true });

// Map sesi: streamKey → Set<ws>
// Key '*' digunakan untuk client yang ingin menerima SEMUA stream
const wsSessionMap = new Map();

// ─── Auto-Connect: Shared Live Sessions ───────────────────────────────────────
// Map: streamKey → { connection: WebcastPushConnection, refCount: number, status: string }
const liveSessionMap = new Map();

// FIX: Rate limiting koneksi WS per IP — cegah flood
const WS_MAX_CONNECTIONS_PER_IP = 10;
const wsIpCount = new Map();

function addWsClient(ws, streamKey) {
    if (!wsSessionMap.has(streamKey)) {
        wsSessionMap.set(streamKey, new Set());
    }
    wsSessionMap.get(streamKey).add(ws);
}

function removeWsClient(ws, streamKey) {
    const set = wsSessionMap.get(streamKey);
    if (set) {
        set.delete(ws);
        if (set.size === 0) wsSessionMap.delete(streamKey);
    }
}

const ioSessionMap = new Map();

function addIoClient(socket, streamKey) {
    if (!ioSessionMap.has(streamKey)) ioSessionMap.set(streamKey, new Set());
    ioSessionMap.get(streamKey).add(socket);
}

function removeIoClient(socket, streamKey) {
    const set = ioSessionMap.get(streamKey);
    if (set) {
        set.delete(socket);
        if (set.size === 0) ioSessionMap.delete(streamKey);
    }
}

// ─── Logger Helpers ───────────────────────────────────────────────────────────
function getWsClientCount(streamKey) {
    const allWs = wsSessionMap.get('*') ? wsSessionMap.get('*').size : 0;
    const specificWs = wsSessionMap.get(streamKey) ? wsSessionMap.get(streamKey).size : 0;
    const ioClients = ioSessionMap.get(streamKey) ? ioSessionMap.get(streamKey).size : 0;
    return allWs + specificWs + ioClients;
}

function getTotalWsClientCount() {
    let total = 0;
    for (const clients of wsSessionMap.values()) total += clients.size;
    for (const clients of ioSessionMap.values()) total += clients.size;
    return total;
}

function logWsConnection(action, ip, streamKey) {
    const time = new Date().toISOString();
    if (DEBUG_MODE === 'all' || DEBUG_MODE === 'connection') {
        const streamCount = getWsClientCount(streamKey);
        const totalCount = getTotalWsClientCount();
        console.log(`[${time}] [DEBUG-CONN] ${action} | IP: ${ip} | Stream: @${streamKey} | Stream Clients: ${streamCount} | Total Clients: ${totalCount}`);
    } else {
        if (action === 'CONNECTED') console.log(`[${time}] [WS] Client terhubung: ${ip} (stream: ${streamKey})`);
        else if (action === 'DISCONNECTED') console.log(`[${time}] [WS] Client terputus: ${ip} (stream: ${streamKey})`);
    }
}

function logIoConnection(action, socketId, targetUsername = null) {
    const time = new Date().toISOString();
    if (DEBUG_MODE === 'all' || DEBUG_MODE === 'connection') {
        const totalCount = io.engine.clientsCount;
        const streamInfo = targetUsername ? ` | Stream: @${targetUsername}` : '';
        console.log(`[${time}] [DEBUG-CONN] Socket.IO ${action} | ID: ${socketId}${streamInfo} | Total IO Clients: ${totalCount}`);
    } else {
        if (action === 'CONNECTED') console.log(`[${time}] [INFO] Client terhubung: ${socketId}`);
        else if (action === 'DISCONNECTED') console.log(`[${time}] [INFO] Client terputus: ${socketId}`);
    }
}

function logChatInfo(platform, streamKey, username, comment, isSuperChat = false, amount = null) {
    if (DEBUG_MODE === 'all') {
        const streamCount = getWsClientCount(streamKey);
        const superChatInfo = isSuperChat ? `[SUPERCHAT ${amount}] ` : '';
        console.log(`[DEBUG-CHAT] [${platform.toUpperCase()} - @${streamKey}] | WS Clients: ${streamCount} | ${superChatInfo}${username}: ${comment}`);
    } else if (DEBUG_MODE === 'connection') {
        // Mute chat logs in connection mode
        return;
    } else {
        const superChatInfo = isSuperChat ? `💛 SUPERCHAT ` : '';
        console.log(`[CHAT] ${superChatInfo}${username}: ${comment}`);
    }
}

// FIX: Host header injection — gunakan hostname yang aman, bukan dari header langsung
server.on('upgrade', (request, socket, head) => {
    // FIX: Gunakan localhost sebagai base URL, BUKAN request.headers.host
    // untuk mencegah Host header injection attack
    let pathname, streamParam;
    try {
        const url = new URL(request.url, 'http://localhost');
        pathname = url.pathname;
        streamParam = url.searchParams.get('stream');
    } catch (e) {
        socket.destroy();
        return;
    }

    if (pathname === '/ws/chat') {
        // FIX: Rate limiting per IP
        const ip = request.socket.remoteAddress || 'unknown';
        const currentCount = wsIpCount.get(ip) || 0;
        if (currentCount >= WS_MAX_CONNECTIONS_PER_IP) {
            socket.write('HTTP/1.1 429 Too Many Requests\r\n\r\n');
            socket.destroy();
            return;
        }
        wsIpCount.set(ip, currentCount + 1);

        wss.handleUpgrade(request, socket, head, (ws) => {
            // FIX: Sanitasi stream key sebelum digunakan
            ws._streamKey = sanitizeStreamKey(streamParam);
            ws._ip = ip;
            wss.emit('connection', ws, request);
        });
    } else {
        socket.destroy();
    }
});

/**
 * Mulai koneksi TikTok Live untuk streamKey tertentu (shared/reference-counted).
 */
function startLiveSession(streamKey) {
    if (liveSessionMap.has(streamKey)) {
        console.log(`[WS-Auto] Reuse session untuk @${streamKey}`);
        return;
    }

    // Validasi username TikTok
    if (!/^[a-zA-Z0-9_.]+$/.test(streamKey)) {
        console.warn(`[WS-Auto] Stream key tidak valid sebagai username TikTok: ${streamKey}`);
        return;
    }

    console.log(`[WS-Auto] Memulai koneksi TikTok Live untuk @${streamKey}...`);

    const connection = new WebcastPushConnection(streamKey);
    const session = { connection, status: 'connecting' };
    liveSessionMap.set(streamKey, session);

    // Broadcast status ke semua WS & IO subscriber
    const notifySubscribers = (msg) => {
        broadcastToWsClients({ type: 'status', message: msg, stream: streamKey }, streamKey);
        const ioClients = ioSessionMap.get(streamKey);
        if (ioClients) {
            for (const socket of ioClients) {
                socket.emit('sys-message', msg);
            }
        }
    };

    notifySubscribers(`⏳ Menghubungkan ke @${streamKey}...`);

    connection.on('error', (err) => {
        console.error(`[WS-Auto] TikTok Error @${streamKey}:`, sanitizeString(err.message, 200));
    });

    connection.on('disconnected', () => {
        console.log(`[WS-Auto] TikTok Live disconnected @${streamKey}`);
        notifySubscribers(`⚠️ Koneksi ke @${streamKey} terputus.`);
        liveSessionMap.delete(streamKey);
    });

    connection.on('chat', async (data) => {
        // Pastikan session masih aktif
        if (!liveSessionMap.has(streamKey)) return;

        const username = sanitizeString(
            String(data.uniqueId || '').replace(/^@/, ''),
            MAX_USERNAME_LENGTH
        );
        const comment = sanitizeString(String(data.comment || ''), MAX_COMMENT_LENGTH);
        if (!comment) return;

        logChatInfo('tiktok', streamKey, username, comment);

        let audioBase64 = null;
        let audioUrl = null;

        try {
            const textToSpeak = `${username} berkata, ${comment}`.substring(0, 200);
            audioBase64 = await googleTTS.getAudioBase64(textToSpeak, {
                lang: 'id', slow: false, host: 'https://translate.google.com', timeout: 10000,
            });
            if (audioBase64) {
                const audioId = `${Date.now()}-${crypto.randomBytes(8).toString('hex')}`;
                audioCache.set(audioId, Buffer.from(audioBase64, 'base64'));
                setTimeout(() => { audioCache.delete(audioId); }, CACHE_TIMEOUT);
                audioUrl = `/audio/${audioId}.mp3`;
            }
        } catch (err) {
            console.error('[TTS Error]', sanitizeString(err.message, 200));
        }

        const chatPayload = {
            type: 'chat',
            platform: 'tiktok',
            stream: streamKey,
            username,
            comment,
            audioUrl,
        };

        const ioPayload = {
            username,
            comment,
            audioData: audioBase64 ? `data:audio/mp3;base64,${audioBase64}` : null,
            audioUrl,
        };

        broadcastToWsClients(chatPayload, streamKey);

        const ioClients = ioSessionMap.get(streamKey);
        if (ioClients) {
            for (const socket of ioClients) {
                socket.emit('chat', ioPayload);
            }
        }
    });

    connection.connect().then(() => {
        console.log(`[WS-Auto] ✅ Tersambung ke @${streamKey}`);
        session.status = 'connected';
        notifySubscribers(`✅ Terhubung ke @${streamKey}`);
    }).catch(err => {
        console.error(`[WS-Auto] ❌ Gagal ke @${streamKey}:`, sanitizeString(err.message, 200));
        notifySubscribers(`❌ Gagal terhubung ke @${streamKey}. Pastikan akun sedang Live.`);
        liveSessionMap.delete(streamKey);
    });
}

/**
 * Hentikan koneksi TikTok Live jika tidak ada client lagi.
 */
function stopLiveSessionIfEmpty(streamKey) {
    const session = liveSessionMap.get(streamKey);
    if (!session) return;

    // Hitung jumlah client aktif secara dinamis (WS specific + IO specific)
    const specificWsCount = wsSessionMap.get(streamKey) ? wsSessionMap.get(streamKey).size : 0;
    const specificIoCount = ioSessionMap.get(streamKey) ? ioSessionMap.get(streamKey).size : 0;
    const activeSubscribers = specificWsCount + specificIoCount;

    console.log(`[WS-Auto] Active subscribers for @${streamKey}: ${activeSubscribers}`);

    if (activeSubscribers <= 0) {
        console.log(`[WS-Auto] Semua client disconnect, menghentikan @${streamKey}`);
        try { session.connection.disconnect(); } catch (e) {}
        liveSessionMap.delete(streamKey);
    }
}

wss.on('connection', (ws) => {
    const streamKey = ws._streamKey;
    const ip = ws._ip || 'unknown';
    addWsClient(ws, streamKey);
    logWsConnection('CONNECTED', ip, streamKey);

    // FIX: try/catch pada ws.send() untuk cegah crash jika koneksi sudah tutup
    try {
        ws.send(JSON.stringify({
            type: 'connected',
            message: 'TikTok Live Chat WebSocket ready.',
            stream: streamKey === '*' ? 'all' : streamKey
        }));
    } catch (e) {
        // Abaikan jika koneksi sudah tutup sebelum send selesai
    }

    // ─── Auto-Connect: Langsung mulai koneksi TikTok Live ─────────────────────
    if (streamKey !== '*') {
        startLiveSession(streamKey);
    }

    // ─── Handle pesan dari WS client ──────────────────────────────────────────
    ws.on('message', (rawMsg) => {
        try {
            const msg = JSON.parse(rawMsg.toString());
            if (msg.action === 'stop' && streamKey !== '*') {
                console.log(`[WS] Client ${ip} mengirim stop untuk @${streamKey}`);
                stopLiveSessionIfEmpty(streamKey);
            }
        } catch (e) {
            // Abaikan pesan non-JSON
        }
    });

    ws.on('close', () => {
        removeWsClient(ws, streamKey);
        logWsConnection('DISCONNECTED', ip, streamKey);
        // FIX: Bersihkan counter IP
        const count = wsIpCount.get(ip) || 1;
        if (count <= 1) wsIpCount.delete(ip);
        else wsIpCount.set(ip, count - 1);

        // Auto-cleanup: hentikan live jika tidak ada subscriber lagi
        if (streamKey !== '*') {
            stopLiveSessionIfEmpty(streamKey);
        }
    });

    ws.on('error', (err) => {
        console.error(`[WS] Error client ${ip}:`, err.message);
        removeWsClient(ws, streamKey);
        const count = wsIpCount.get(ip) || 1;
        if (count <= 1) wsIpCount.delete(ip);
        else wsIpCount.set(ip, count - 1);

        // Auto-cleanup
        if (streamKey !== '*') {
            stopLiveSessionIfEmpty(streamKey);
        }
    });
});

/**
 * Broadcast chat event ke WS client yang subscribe stream tertentu + client '*'.
 * @param {object} payload - Sudah disanitasi sebelum masuk ke sini
 * @param {string} streamKey - username TikTok yang sedang dipantau
 */
function broadcastToWsClients(payload, streamKey) {
    // FIX: Wrap JSON.stringify dalam try/catch untuk keamanan
    let data;
    try {
        data = JSON.stringify(payload);
    } catch (e) {
        console.error('[WS] Gagal serialize payload:', e.message);
        return;
    }

    const sendSafe = (ws) => {
        // FIX: try/catch pada setiap ws.send()
        if (ws.readyState === ws.OPEN) {
            try { ws.send(data); } catch (e) { /* Abaikan */ }
        }
    };

    const specificClients = wsSessionMap.get(streamKey);
    if (specificClients) {
        for (const ws of specificClients) sendSafe(ws);
    }

    const allClients = wsSessionMap.get('*');
    if (allClients) {
        for (const ws of allClients) sendSafe(ws);
    }
}

// ─── Socket.IO (untuk browser & Discord Bot) ──────────────────────────────────
io.on('connection', (socket) => {
    logIoConnection('CONNECTED', socket.id);
    let currentStreamKey = null;

    socket.on('set-username', (targetUsername) => {
        if (!targetUsername || typeof targetUsername !== 'string') {
            socket.emit('sys-message', '❌ Username tidak valid.');
            return;
        }
        targetUsername = targetUsername.trim();
        if (!targetUsername) {
            socket.emit('sys-message', '❌ Username tidak boleh kosong.');
            return;
        }

        if (targetUsername.length > MAX_USERNAME_LENGTH) {
            socket.emit('sys-message', '❌ Username terlalu panjang.');
            return;
        }

        if (!/^[a-zA-Z0-9_.]+$/.test(targetUsername)) {
            socket.emit('sys-message', '❌ Username mengandung karakter tidak valid.');
            return;
        }

        console.log(`[INFO] Memantau live: ${targetUsername}`);

        if (currentStreamKey) {
            removeIoClient(socket, currentStreamKey);
            stopLiveSessionIfEmpty(currentStreamKey);
        }

        currentStreamKey = targetUsername;
        addIoClient(socket, currentStreamKey);
        
        const isNewSession = !liveSessionMap.has(currentStreamKey);
        startLiveSession(currentStreamKey);
        
        if (!isNewSession) {
            const session = liveSessionMap.get(currentStreamKey);
            if (session && session.status === 'connected') {
                socket.emit('sys-message', `✅ Terhubung ke @${currentStreamKey}`);
            } else if (session && session.status === 'connecting') {
                socket.emit('sys-message', `⏳ Menghubungkan ke @${currentStreamKey}...`);
            }
        }
    });

    socket.on('stop', () => {
        if (currentStreamKey) {
            removeIoClient(socket, currentStreamKey);
            stopLiveSessionIfEmpty(currentStreamKey);
            currentStreamKey = null;
        }
        socket.emit('sys-message', '⏹️ Pemantauan dihentikan.');
    });

    socket.on('disconnect', () => {
        logIoConnection('DISCONNECTED', socket.id, currentStreamKey);
        if (currentStreamKey) {
            removeIoClient(socket, currentStreamKey);
            stopLiveSessionIfEmpty(currentStreamKey);
            currentStreamKey = null;
        }
    });
});

// ─── Graceful Shutdown ────────────────────────────────────────────────────────
process.on('SIGINT', () => {
    console.log('[Shutdown] Menutup server...');
    wss.close();
    io.close();
    server.close();
    process.exit(0);
});

process.on('unhandledRejection', (err) => {
    console.error('[Unhandled Rejection]', err);
});

server.listen(PORT, () => {
    console.log(`\n✅ Server berjalan di http://localhost:${PORT}`);
    console.log(`   Plain WebSocket tersedia di ws://localhost:${PORT}/ws/chat\n`);
});