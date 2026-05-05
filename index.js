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

wss.on('connection', (ws) => {
    const streamKey = ws._streamKey;
    const ip = ws._ip || 'unknown';
    console.log(`[WS] Client terhubung: ${ip} (stream: ${streamKey})`);
    addWsClient(ws, streamKey);

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

    ws.on('close', () => {
        console.log(`[WS] Client terputus: ${ip} (stream: ${streamKey})`);
        removeWsClient(ws, streamKey);
        // FIX: Bersihkan counter IP
        const count = wsIpCount.get(ip) || 1;
        if (count <= 1) wsIpCount.delete(ip);
        else wsIpCount.set(ip, count - 1);
    });

    ws.on('error', (err) => {
        console.error(`[WS] Error client ${ip}:`, err.message);
        removeWsClient(ws, streamKey);
        const count = wsIpCount.get(ip) || 1;
        if (count <= 1) wsIpCount.delete(ip);
        else wsIpCount.set(ip, count - 1);
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
    console.log(`[INFO] Client terhubung: ${socket.id}`);
    let tiktokLiveConnection = null;

    socket.on('set-username', (targetUsername) => {
        // FIX: Validasi tipe dan keberadaan
        if (!targetUsername || typeof targetUsername !== 'string') {
            socket.emit('sys-message', '❌ Username tidak valid.');
            return;
        }
        targetUsername = targetUsername.trim();
        if (!targetUsername) {
            socket.emit('sys-message', '❌ Username tidak boleh kosong.');
            return;
        }

        // FIX: Batasi panjang username untuk cegah abuse
        if (targetUsername.length > MAX_USERNAME_LENGTH) {
            socket.emit('sys-message', '❌ Username terlalu panjang.');
            return;
        }

        // FIX: Validasi karakter username TikTok — hanya alfanumerik, _, .
        if (!/^[a-zA-Z0-9_.]+$/.test(targetUsername)) {
            socket.emit('sys-message', '❌ Username mengandung karakter tidak valid.');
            return;
        }

        console.log(`[INFO] Memantau live: ${targetUsername}`);

        // Putuskan koneksi sebelumnya jika ada
        if (tiktokLiveConnection) {
            try { tiktokLiveConnection.disconnect(); } catch (e) {}
            tiktokLiveConnection = null;
        }

        tiktokLiveConnection = new WebcastPushConnection(targetUsername);
        const currentConnection = tiktokLiveConnection;

        // Error handler agar unhandled error tidak crash server
        currentConnection.on('error', (err) => {
            console.error(`[TikTok Error] ${sanitizeString(err.message, 200)}`);
        });

        currentConnection.on('disconnected', () => {
            console.log(`[INFO] TikTok Live disconnected untuk ${targetUsername}`);
            socket.emit('sys-message', `⚠️ Koneksi ke @${targetUsername} terputus.`);
            if (tiktokLiveConnection === currentConnection) {
                tiktokLiveConnection = null;
            }
        });

        currentConnection.on('chat', async (data) => {
            if (tiktokLiveConnection !== currentConnection) return;

            // FIX: Sanitasi semua data dari TikTok sebelum diproses
            const username = sanitizeString(
                String(data.uniqueId || '').replace(/^@/, ''),
                MAX_USERNAME_LENGTH
            );
            const comment = sanitizeString(String(data.comment || ''), MAX_COMMENT_LENGTH);

            // FIX: Jangan proses jika komentar kosong setelah sanitasi
            if (!comment) return;

            let audioBase64 = null;
            let audioUrl = null;

            try {
                // FIX: Gunakan comment yang sudah disanitasi untuk TTS
                const textToSpeak = `${username} berkata, ${comment}`.substring(0, 200);

                audioBase64 = await googleTTS.getAudioBase64(textToSpeak, {
                    lang: 'id',
                    slow: false,
                    host: 'https://translate.google.com',
                    timeout: 10000,
                });

                if (audioBase64) {
                    // FIX: Gunakan crypto.randomBytes() untuk ID yang lebih kuat (hindari collision)
                    const audioId = `${Date.now()}-${crypto.randomBytes(8).toString('hex')}`;
                    audioCache.set(audioId, Buffer.from(audioBase64, 'base64'));
                    setTimeout(() => { audioCache.delete(audioId); }, CACHE_TIMEOUT);
                    audioUrl = `/audio/${audioId}.mp3`;
                }
            } catch (err) {
                console.error('[TTS Error] Gagal memuat suara:', sanitizeString(err.message, 200));
            }

            // FIX: Payload hanya berisi data yang sudah disanitasi
            const chatPayload = {
                type: 'chat',
                platform: 'tiktok',
                stream: targetUsername,
                username,
                comment,
                audioUrl,
            };

            socket.emit('chat', {
                username,
                comment,
                audioData: audioBase64 ? `data:audio/mp3;base64,${audioBase64}` : null,
                audioUrl,
            });

            broadcastToWsClients(chatPayload, targetUsername);
        });

        currentConnection.connect().then(() => {
            console.log(`[BERHASIL] Tersambung ke room ${targetUsername}`);
            socket.emit('sys-message', `Widget Aktif! Terhubung ke: @${targetUsername}`);
        }).catch(err => {
            console.error(`[GAGAL] ke ${targetUsername}`, sanitizeString(err.message, 200));
            socket.emit('sys-message', `Gagal terhubung ke ${targetUsername}. Pastikan akun sedang Live.`);
            if (tiktokLiveConnection === currentConnection) {
                tiktokLiveConnection = null;
            }
        });
    });

    socket.on('stop', () => {
        if (tiktokLiveConnection) {
            try { tiktokLiveConnection.disconnect(); } catch (e) {}
            tiktokLiveConnection = null;
        }
        socket.emit('sys-message', '⏹️ Pemantauan dihentikan.');
    });

    socket.on('disconnect', () => {
        console.log(`[INFO] Client terputus: ${socket.id}`);
        if (tiktokLiveConnection) {
            try { tiktokLiveConnection.disconnect(); } catch (e) {}
            tiktokLiveConnection = null;
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