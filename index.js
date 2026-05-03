const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { WebcastPushConnection } = require('tiktok-live-connector');
const googleTTS = require('google-tts-api'); // Library baru pemanggil Suara Google

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static('public'));

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
            } catch (err) {
                console.error("[TTS Error] Gagal memuat suara:", err.message);
            }

            // Kirim chat dan file audio ke OBS
            socket.emit('chat', { 
                username: data.uniqueId, 
                comment: data.comment,
                audioData: audioBase64 ? `data:audio/mp3;base64,${audioBase64}` : null
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