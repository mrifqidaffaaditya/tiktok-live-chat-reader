const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { WebcastPushConnection } = require('tiktok-live-connector');
const googleTTS = require('google-tts-api'); // Library baru pemanggil Suara Google

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static('public'));

// ─── Endpoint & Cache untuk dukungan Lavalink Streaming ───────────────────────
const CACHE_TIMEOUT = parseInt(process.env.CACHE_TIMEOUT) || 300000;
const audioCache = new Map();

app.get('/audio/:id.mp3', (req, res) => {
    const id = req.params.id;
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
        console.log(`[INFO] Memantau live: ${targetUsername}`);

        if (tiktokLiveConnection) {
            tiktokLiveConnection.disconnect();
        }

        tiktokLiveConnection = new WebcastPushConnection(targetUsername);

        tiktokLiveConnection.connect().then(state => {
            console.log(`[BERHASIL] Tersambung ke room ${targetUsername}`);
            socket.emit('sys-message', `Widget Aktif! Terhubung ke: @${targetUsername}`);
        }).catch(err => {
            console.error(`[GAGAL] ke ${targetUsername}`, err);
            socket.emit('sys-message', `Gagal terhubung ke ${targetUsername}. Pastikan akun sedang Live.`);
        });

        tiktokLiveConnection.on('chat', async (data) => {
            let audioBase64 = null;
            let audioUrl = null;
            
            try {
                // Teks yang akan dibacakan (Maksimal 200 karakter agar Google tidak error)
                const textToSpeak = `${data.uniqueId} berkata, ${data.comment}`.substring(0, 200);
                
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

            // Kirim chat dan file audio ke OBS
            socket.emit('chat', { 
                username: data.uniqueId, 
                comment: data.comment,
                audioData: audioBase64 ? `data:audio/mp3;base64,${audioBase64}` : null,
                audioUrl: audioUrl
            });
        });
    });

    socket.on('disconnect', () => {
        console.log(`[INFO] Client terputus: ${socket.id}`);
        if (tiktokLiveConnection) {
            tiktokLiveConnection.disconnect();
        }
    });
});

server.listen(3000, () => {
    console.log('Server berjalan di http://localhost:3000');
});