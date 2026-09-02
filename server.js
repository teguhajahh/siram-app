import express from 'express';
import mqtt from 'mqtt';
import { createServer } from 'node:http';
import { Server } from 'socket.io';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import 'dotenv/config';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const app = express();
const server = createServer(app);
const io = new Server(server);

// ===== Sajikan dashboard (index.html + styles.css) =====
app.use(express.static(path.join(__dirname, 'public')));

// ===== Koneksi ke broker Mosquitto/EMQX =====
// Ganti lewat file .env — jangan hardcode IP di sini.
const MQTT_HOST = process.env.MQTT_HOST;
const MQTT_PORT = process.env.MQTT_PORT;
const MQTT_USER = process.env.MQTT_USER;
const MQTT_PASS = process.env.MQTT_PASS;
const TOPIC_SENSOR  = process.env.TOPIC_SENSOR;
const TOPIC_CONTROL = process.env.TOPIC_CONTROL;
const TOPIC_STATUS  = process.env.TOPIC_STATUS;

const mqttUrl = `mqtt://${MQTT_HOST}:${MQTT_PORT}`;
const mqttOptions = MQTT_USER
  ? { username: MQTT_USER, password: MQTT_PASS }
  : {};

const mqttClient = mqtt.connect(mqttUrl, mqttOptions);

// ===== Deteksi data basi (stale) =====
// Kalau ESP2 mati/dicabut, server TIDAK akan tahu secara otomatis lewat
// MQTT (broker tidak selalu punya LWT diset di firmware ini). Jadi kita
// pakai timer: kalau tidak ada pesan sensor baru dalam STALE_TIMEOUT_MS,
// anggap datanya basi dan beri tahu semua klien, supaya dashboard tidak
// menampilkan angka lama seolah-olah itu masih live.
const STALE_TIMEOUT_MS = 20000; // 4x interval publish ESP2 (5 detik)
let staleTimer = null;
let isStale = false;

function markFresh() {
  isStale = false;
  if (staleTimer) clearTimeout(staleTimer);
  staleTimer = setTimeout(markStale, STALE_TIMEOUT_MS);
}

function markStale() {
  isStale = true;
  io.emit('sensorStale', { stale: true });
  console.warn('[Stale] Tidak ada data sensor baru, menandai data sebagai basi.');
}

// ===== State terakhir di memori =====
// Supaya klien yang baru connect langsung dapat nilai/kondisi terkini,
// tidak perlu nunggu publish MQTT berikutnya.
let lastSensorData = { tanah: null, udara: null, suhu: null, updatedAt: null };
let lastRelayStatus = { pompa_air: false, pompa_pupuk: false }; // status ASLI dari ESP1, bukan asumsi

mqttClient.on('connect', () => {
  console.log(`[MQTT] Tersambung ke broker ${mqttUrl}`);
  mqttClient.subscribe([TOPIC_SENSOR, TOPIC_STATUS], (err) => {
    if (err) {
      console.error('[MQTT] Gagal subscribe:', err.message);
    } else {
      console.log(`[MQTT] Subscribe ke "${TOPIC_SENSOR}" dan "${TOPIC_STATUS}"`);
    }
  });
});

mqttClient.on('reconnect', () => {
  console.log('[MQTT] Mencoba menyambung ulang ke broker...');
});

mqttClient.on('error', (err) => {
  console.error('[MQTT] Error koneksi:', err.message);
});

mqttClient.on('message', (topic, payload) => {
  let data;
  try {
    data = JSON.parse(payload.toString());
  } catch (err) {
    console.error(`[MQTT] Payload dari "${topic}" bukan JSON valid:`, payload.toString());
    return;
  }

  if (topic === TOPIC_SENSOR) {
    lastSensorData = {
      tanah: data.tanah,
      udara: data.udara,
      suhu: data.suhu,
      updatedAt: new Date().toISOString(),
    };
    io.emit('sensorData', lastSensorData);
    markFresh(); // data baru masuk -> reset timer stale
    io.emit('sensorStale', { stale: false }); // selalu kabari klien: data sudah fresh lagi
    console.log('[MQTT -> Socket.io] sensorData:', lastSensorData);
    return;
  }

  if (topic === TOPIC_STATUS) {
    lastRelayStatus = {
      pompa_air: !!data.pompa_air,
      pompa_pupuk: !!data.pompa_pupuk,
    };
    io.emit('relayStatus', lastRelayStatus);
    console.log('[MQTT -> Socket.io] relayStatus:', lastRelayStatus);
    return;
  }
});

// ===== Socket.io: komunikasi ke browser =====
io.on('connection', (socket) => {
  console.log(`[Socket.io] Client terhubung: ${socket.id}`);

  // Kirim state terakhir yang tersimpan, biar dashboard yang baru dibuka
  // langsung sinkron, tidak nunggu update berikutnya
  if (lastSensorData.updatedAt) {
    socket.emit('sensorData', lastSensorData);
  }
  socket.emit('relayStatus', lastRelayStatus);
  // Kabari klien baru soal status stale saat ini juga (bukan cuma klien lama)
  socket.emit('sensorStale', { stale: isStale });

  // Browser mengirim perintah, contoh: socket.emit('controlRelay', { pompa_air: true })
  // Field yang tidak disertakan tidak diubah — sama seperti kontrak di ESP1.
  socket.on('controlRelay', (cmd) => {
    if (typeof cmd !== 'object' || cmd === null) return;

    const payload = {};
    if (typeof cmd.pompa_air === 'boolean') payload.pompa_air = cmd.pompa_air;
    if (typeof cmd.pompa_pupuk === 'boolean') payload.pompa_pupuk = cmd.pompa_pupuk;

    if (Object.keys(payload).length === 0) {
      console.warn('[Socket.io] controlRelay diabaikan, payload tidak valid:', cmd);
      return;
    }

    mqttClient.publish(TOPIC_CONTROL, JSON.stringify(payload));
    console.log('[Socket.io -> MQTT] controlRelay:', payload);
  });

  socket.on('disconnect', () => {
    console.log(`[Socket.io] Client terputus: ${socket.id}`);
  });
});

// ===== Endpoint kecil buat cek server hidup =====
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    mqttConnected: mqttClient.connected,
    lastSensorData,
    lastRelayStatus,
    sensorStale: isStale,
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`[Server] Dashboard SIRAMLI-V2 jalan di http://localhost:${PORT}`);
  // Kalau server baru start dan sudah lama tidak dapat data (mis. restart
  // setelah lama mati), langsung anggap stale sampai ada pesan MQTT baru.
  staleTimer = setTimeout(markStale, STALE_TIMEOUT_MS);
});