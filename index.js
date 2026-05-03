require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { WebcastPushConnection } = require('tiktok-live-connector');
const googleTTS = require('google-tts-api');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static('public'));

// ─── Endpoint & Cache untuk dukungan Lavalink Streaming ───────────────────────
const CACHE_TIMEOUT = parseInt(process.env.CACHE_TIMEOUT) || 300000;
const audioCache = new Map();

// FIX: Validasi format audioId untuk mencegah path traversal
app.get('/audio/:id.mp3', (req, res) => {
    const id = req.params.id;
    // Hanya izinkan karakter alfanumerik dan dash
    if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
        return res.status(400).send('Invalid audio ID');
    }
    const audioBuffer = audioCache.get(id);
    if (!audioBuffer) {
        return res.status(404).send('Audio not found or expired');
    }
    res.set('Content-Type', 'audio/mpeg');
    res.send(audioBuffer);
});

io.on('connection', (socket) => {
    console.log(`[INFO] Client terhubung: ${socket.id}`);
    let tiktokLiveConnection = null;

    socket.on('set-username', (targetUsername) => {
        // FIX: Validasi input — pastikan targetUsername adalah string dan tidak kosong
        if (!targetUsername || typeof targetUsername !== 'string') {
            socket.emit('sys-message', '❌ Username tidak valid.');
            return;
        }
        targetUsername = targetUsername.trim();
        if (!targetUsername) {
            socket.emit('sys-message', '❌ Username tidak boleh kosong.');
            return;
        }

        console.log(`[INFO] Memantau live: ${targetUsername}`);

        // Putuskan koneksi sebelumnya jika ada
        if (tiktokLiveConnection) {
            try {
                tiktokLiveConnection.disconnect();
            } catch (e) {
                // Abaikan error saat disconnect
            }
            tiktokLiveConnection = null;
        }

        tiktokLiveConnection = new WebcastPushConnection(targetUsername);

        // FIX: Registrasi event handler dilakukan SEBELUM connect() dipanggil
        // agar tidak ada event yang terlewat, tapi tetap aman karena
        // WebcastPushConnection buffer events sampai connect selesai.
        const currentConnection = tiktokLiveConnection;

        // FIX: Error handler agar unhandled error tidak mencrash server
        currentConnection.on('error', (err) => {
            console.error(`[TikTok Error] ${err.message}`);
        });

        // FIX: Tangani event disconnected dari TikTok
        currentConnection.on('disconnected', () => {
            console.log(`[INFO] TikTok Live disconnected untuk ${targetUsername}`);
            socket.emit('sys-message', `⚠️ Koneksi ke @${targetUsername} terputus.`);
            // Hanya set null jika masih koneksi yang sama (bukan yang baru)
            if (tiktokLiveConnection === currentConnection) {
                tiktokLiveConnection = null;
            }
        });

        currentConnection.on('chat', async (data) => {
            // FIX: Pastikan koneksi ini masih yang aktif sebelum memproses
            if (tiktokLiveConnection !== currentConnection) return;

            // Hilangkan '@' di awal nama jika ada
            const username = String(data.uniqueId || '').replace(/^@/, '');

            let audioBase64 = null;
            let audioUrl = null;
            
            try {
                // Teks yang akan dibacakan (Maksimal 200 karakter agar Google tidak error)
                const textToSpeak = `${username} berkata, ${data.comment}`.substring(0, 200);
                
                // Ambil audio dari Google TTS di sisi Server
                audioBase64 = await googleTTS.getAudioBase64(textToSpeak, {
                    lang: 'id',
                    slow: false,
                    host: 'https://translate.google.com',
                    timeout: 10000,
                });
                
                if (audioBase64) {
                    const audioId = Date.now() + '-' + Math.round(Math.random() * 10000);
                    audioCache.set(audioId, Buffer.from(audioBase64, 'base64'));
                    // Hapus cache sesuai timeout agar memori tidak penuh
                    setTimeout(() => {
                        audioCache.delete(audioId);
                    }, CACHE_TIMEOUT);
                    audioUrl = `/audio/${audioId}.mp3`;
                }
            } catch (err) {
                console.error("[TTS Error] Gagal memuat suara:", err.message);
            }

            // Kirim chat dan file audio ke client
            socket.emit('chat', { 
                username: username, 
                comment: data.comment,
                audioData: audioBase64 ? `data:audio/mp3;base64,${audioBase64}` : null,
                audioUrl: audioUrl
            });
        });

        currentConnection.connect().then(state => {
            console.log(`[BERHASIL] Tersambung ke room ${targetUsername}`);
            socket.emit('sys-message', `Widget Aktif! Terhubung ke: @${targetUsername}`);
        }).catch(err => {
            console.error(`[GAGAL] ke ${targetUsername}`, err.message);
            socket.emit('sys-message', `Gagal terhubung ke ${targetUsername}. Pastikan akun sedang Live.`);
            // Hanya bersihkan jika masih koneksi yang sama
            if (tiktokLiveConnection === currentConnection) {
                tiktokLiveConnection = null;
            }
        });
    });

    // FIX: Tambahkan handler 'stop' agar konsisten dengan YouTube
    socket.on('stop', () => {
        if (tiktokLiveConnection) {
            try {
                tiktokLiveConnection.disconnect();
            } catch (e) {}
            tiktokLiveConnection = null;
        }
        socket.emit('sys-message', '⏹️ Pemantauan dihentikan.');
    });

    socket.on('disconnect', () => {
        console.log(`[INFO] Client terputus: ${socket.id}`);
        if (tiktokLiveConnection) {
            try {
                tiktokLiveConnection.disconnect();
            } catch (e) {}
            tiktokLiveConnection = null;
        }
    });
});

// --- Graceful Shutdown ---
process.on('SIGINT', () => {
    console.log('[Shutdown] Menutup server...');
    io.close();
    server.close();
    process.exit(0);
});

process.on('unhandledRejection', (err) => {
    console.error('[Unhandled Rejection]', err);
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server berjalan di http://localhost:${PORT}`);
});